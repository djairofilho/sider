//! Owning-store lifecycle for native and RESP2 commands.
//! The record-2 durable profile remains an explicit library-only development opt-in.

use super::memory::{Collection, Descriptor, Edit, MemoryError, Operation, Ttl};
use super::*;
use crate::persistence::memory_format;
use std::collections::BTreeMap;

/// A candidate and its record-2 resolved mutation. Apply only after confirmation.
pub struct PreparedMemory {
    prepared: Prepared,
    resolved: Option<memory_format::Mutation>,
}

impl PreparedMemory {
    pub fn reply(&self) -> &Reply {
        &self.prepared.reply
    }
    pub fn resolved(&self) -> Option<&memory_format::Mutation> {
        self.resolved.as_ref()
    }
    pub fn origin(&self) -> MutationOrigin {
        self.prepared.batch.origin
    }
}

struct Sample {
    now: Instant,
    unix_ms: i64,
}
impl Clock for Sample {
    fn now(&self) -> Instant {
        self.now
    }
    fn unix_millis(&self) -> i64 {
        self.unix_ms
    }
}

impl Store {
    /// Resolves one native memory operation using the existing owning Store.
    /// Durable profile activation is a separate gate from native/RESP2 admission.
    pub fn prepare_memory(&self, key: Bytes, operation: Operation) -> PreparedMemory {
        self.prepare_memory_with_limit(key, operation, crate::persistence::format::MAX_RECORD_BYTES)
    }

    /// Both the incremental batch and future complete snapshot must fit.
    pub fn prepare_memory_with_limit(
        &self,
        key: Bytes,
        operation: Operation,
        record_limit: usize,
    ) -> PreparedMemory {
        let origin = if operation.writes_dataset() {
            MutationOrigin::Client
        } else {
            MutationOrigin::Expiration
        };
        let mut shadow = Self::with_config(
            self.config,
            Arc::new(Sample {
                now: self.clock.now(),
                unix_ms: self.clock.unix_millis(),
            }),
        )
        .expect("validated configuration");
        shadow.used_bytes = self.used_bytes;
        shadow.generation = self.generation;
        if let Some(entry) = self.values.get(&key) {
            shadow.values.insert(key.clone(), entry.clone());
            if let Some(deadline) = entry.expires_at {
                shadow
                    .expirations
                    .insert((deadline, entry.generation, key.clone()));
            }
            for (id, deadline) in &entry.member_deadlines {
                shadow
                    .member_expirations
                    .insert((*deadline, key.clone(), id.clone()));
            }
        }
        let (reply, mut resolved) = shadow.memory_inner(&key, operation);
        let before = self.values.get(&key);
        let after = shadow.values.get(&key);
        let changed = before.map(|entry| entry.generation) != after.map(|entry| entry.generation);
        let entries = if changed {
            vec![(key.clone(), after.cloned())]
        } else {
            Vec::new()
        };
        if changed && resolved.is_none() {
            resolved = Some(memory_format::Mutation::Legacy(Mutation::Delete {
                key: key.clone(),
            }));
        }
        if let Some(mutation) = &resolved {
            let batch_size = mutation
                .encoded_bytes()
                .ok()
                .and_then(|size| size.checked_add(14));
            let snapshot_size = after.and_then(|entry| match &entry.value {
                Value::Memory(collection) => Some(15 + key.len() + collection.encoded_bytes()),
                _ => None,
            });
            if batch_size.is_none_or(|size| {
                size > record_limit || size > crate::persistence::format::MAX_RECORD_BYTES
            }) || snapshot_size.is_some_and(|size| {
                size > record_limit || size > crate::persistence::format::MAX_RECORD_BYTES
            }) {
                return PreparedMemory {
                    prepared: self.prepared(
                        Reply::Error(ExecutionError::AofRecordLimit),
                        Vec::new(),
                        origin,
                    ),
                    resolved: None,
                };
            }
        }
        PreparedMemory {
            prepared: self.prepared(reply, entries, origin),
            resolved,
        }
    }

    /// The owner applies the exact prepared deadlines without sampling time again.
    pub fn apply_memory(&mut self, prepared: PreparedMemory) -> Reply {
        self.apply(prepared.prepared)
    }

    pub(super) fn resolve_member_deadlines(
        &self,
        key: &Bytes,
        value: &Value,
    ) -> Result<BTreeMap<Bytes, Instant>, MemoryError> {
        self.resolve_member_deadlines_at(key, value, self.clock.now(), self.clock.unix_millis())
    }

    pub(super) fn resolve_member_deadlines_at(
        &self,
        key: &Bytes,
        value: &Value,
        now: Instant,
        unix_ms: i64,
    ) -> Result<BTreeMap<Bytes, Instant>, MemoryError> {
        let mut deadlines = BTreeMap::new();
        let Value::Memory(collection) = value else {
            return Ok(deadlines);
        };
        let previous = self.values.get(key);
        for (id, member) in collection.members() {
            if let Some(absolute) = member.deadline {
                let retained = previous.and_then(|entry| match &entry.value {
                    Value::Memory(old)
                        if old
                            .members()
                            .get(id)
                            .is_some_and(|old| old.deadline == Some(absolute)) =>
                    {
                        entry.member_deadlines.get(id).copied()
                    }
                    _ => None,
                });
                let deadline = if let Some(retained) = retained {
                    retained
                } else if absolute <= unix_ms {
                    now
                } else {
                    let remaining = u64::try_from(i128::from(absolute) - i128::from(unix_ms))
                        .map_err(|_| MemoryError::Deadline)?;
                    now.checked_add(Duration::from_millis(remaining))
                        .ok_or(MemoryError::Deadline)?
                };
                deadlines.insert(id.clone(), deadline);
            }
        }
        Ok(deadlines)
    }

    /// Native record-2 replay validates guards and net quota before any key changes.
    /// Historical memory images and deltas retain elapsed physical state.
    pub fn prepare_memory_replay(
        &self,
        mutations: &[memory_format::Mutation],
        origin: MutationOrigin,
    ) -> Result<Prepared, ReplayError> {
        let mut keys = BTreeSet::new();
        let mut images = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            if !keys.insert(mutation.key()) {
                return Err(ReplayError::DuplicateKey);
            }
            images.push(match mutation {
                memory_format::Mutation::Legacy(mutation) => mutation.clone(),
                memory_format::Mutation::Image {
                    key,
                    collection,
                    key_deadline,
                } => {
                    collection.validate_key(key)?;
                    Mutation::Put {
                        key: key.clone(),
                        value: Value::Memory(Arc::new(collection.clone())),
                        expires_at_unix_ms: *key_deadline,
                    }
                }
                memory_format::Mutation::Delta {
                    key,
                    descriptor,
                    edits,
                    key_deadline,
                } => {
                    let collection = match self.values.get(key).map(|entry| &entry.value) {
                        Some(Value::Memory(collection)) => collection,
                        _ => return Err(ReplayError::Memory(MemoryError::Identity)),
                    };
                    let candidate = collection.apply_edits(descriptor, edits, key)?;
                    Mutation::Put {
                        key: key.clone(),
                        value: Value::Memory(Arc::new(candidate)),
                        expires_at_unix_ms: *key_deadline,
                    }
                }
            });
        }
        self.prepare_replay(&images, origin)
    }

    pub fn replay_memory(
        &mut self,
        mutations: &[memory_format::Mutation],
    ) -> Result<(), ReplayError> {
        let prepared = self.prepare_memory_replay(mutations, MutationOrigin::Client)?;
        self.apply(prepared);
        Ok(())
    }

    /// Complete record-2 images for native snapshot/codec validation.
    pub fn snapshot_memory(&self) -> Vec<memory_format::Mutation> {
        self.snapshot()
            .into_iter()
            .map(memory_format::Mutation::from_storage)
            .collect()
    }

    /// Converts a generic prepared key TTL change or bounded cleanup into guarded
    /// deltas. Call before apply; routine member writes use PreparedMemory::resolved.
    pub fn resolved_memory_mutations(&self, prepared: &Prepared) -> Vec<memory_format::Mutation> {
        prepared
            .batch
            .mutations
            .iter()
            .map(|mutation| {
                if let Mutation::Put {
                    key,
                    value: Value::Memory(after),
                    expires_at_unix_ms,
                } = mutation
                    && let Some(Entry {
                        value: Value::Memory(before),
                        ..
                    }) = self.values.get(key)
                    && before.descriptor() == after.descriptor()
                {
                    let mut edits = BTreeMap::new();
                    for id in before.members().keys() {
                        if !after.members().contains_key(id) {
                            edits.insert(id.clone(), Edit::Remove(id.clone()));
                        }
                    }
                    for (id, member) in after.members() {
                        if before.members().get(id) != Some(member) {
                            edits.insert(id.clone(), Edit::Upsert(member.clone()));
                        }
                    }
                    memory_format::Mutation::Delta {
                        key: key.clone(),
                        descriptor: after.descriptor().clone(),
                        edits: edits.into_values().collect(),
                        key_deadline: *expires_at_unix_ms,
                    }
                } else {
                    memory_format::Mutation::from_storage(mutation.clone())
                }
            })
            .collect()
    }

    fn memory_inner(
        &mut self,
        key: &Bytes,
        operation: Operation,
    ) -> (Reply, Option<memory_format::Mutation>) {
        match self.try_memory_inner(key, operation) {
            Ok(result) => result,
            Err(error) => (Reply::Error(error), None),
        }
    }

    fn try_memory_inner(
        &mut self,
        key: &Bytes,
        operation: Operation,
    ) -> Result<(Reply, Option<memory_format::Mutation>), ExecutionError> {
        if key.is_empty() || key.len() > 256 {
            return Err(ExecutionError::Memory("ERR memory limit exceeded"));
        }
        if let Operation::Create(descriptor) = &operation {
            descriptor.validate().map_err(memory_error)?;
        }
        if let Operation::Put {
            descriptor, member, ..
        } = &operation
        {
            member.validate(descriptor).map_err(memory_error)?;
        }
        let now = self.clock.now();
        self.expire_key(key, now);
        let current = match self.values.get(key).map(|entry| &entry.value) {
            Some(Value::Memory(collection)) => Some(collection.clone()),
            Some(_) => return Err(ExecutionError::WrongType),
            None => None,
        };
        if let Operation::Create(descriptor) = operation {
            if let Some(collection) = current {
                return if collection.descriptor() == &descriptor {
                    Ok((Reply::Ok, None))
                } else {
                    Err(ExecutionError::Memory("ERR memory schema mismatch"))
                };
            }
            let collection = Collection::new(descriptor).map_err(memory_error)?;
            collection.validate_key(key).map_err(memory_error)?;
            let value = Value::Memory(Arc::new(collection.clone()));
            if !self.can_replace(key, &value) {
                return Err(ExecutionError::OutOfMemory);
            }
            self.insert(key.clone(), value, None);
            return Ok((
                Reply::Ok,
                Some(memory_format::Mutation::Image {
                    key: key.clone(),
                    collection,
                    key_deadline: None,
                }),
            ));
        }
        let Some(collection) = current else {
            return match operation {
                Operation::Put { .. } => Err(ExecutionError::Memory(
                    "ERR memory collection does not exist",
                )),
                Operation::Info | Operation::Get { .. } => Ok((Reply::Bulk(None), None)),
                Operation::Pttl { .. } => Ok((Reply::Integer(-2), None)),
                _ => Ok((Reply::Integer(0), None)),
            };
        };
        if matches!(operation, Operation::Info) {
            return Ok((descriptor_reply(collection.descriptor()), None));
        }
        let id = match &operation {
            Operation::Put { member, .. } => &member.id,
            Operation::Get { id }
            | Operation::Delete { id }
            | Operation::Pttl { id }
            | Operation::Expire { id, .. }
            | Operation::Persist { id } => id,
            _ => unreachable!("create and info handled above"),
        };
        if id.is_empty() || id.len() > 256 {
            return Err(ExecutionError::Memory("ERR memory limit exceeded"));
        }
        let entry = self.values.get(key).expect("existing memory entry");
        let expired = entry
            .member_deadlines
            .get(id)
            .is_some_and(|deadline| *deadline <= now);
        let live = collection.members().get(id).filter(|_| !expired);
        let edit = match operation {
            Operation::Put {
                descriptor,
                mut member,
                ttl,
                condition,
            } => {
                let stored = collection.descriptor();
                if descriptor.provider != stored.provider
                    || descriptor.model != stored.model
                    || descriptor.revision != stored.revision
                    || descriptor.transform != stored.transform
                {
                    return Err(ExecutionError::Memory("ERR memory model identity mismatch"));
                }
                if descriptor.dimension != stored.dimension {
                    return Err(ExecutionError::Memory("ERR memory vector length mismatch"));
                }
                if (condition == SetCondition::Missing && live.is_some())
                    || (condition == SetCondition::Present && live.is_none())
                {
                    return Ok((Reply::Integer(0), None));
                }
                member.deadline = match ttl {
                    Ttl::Persistent => None,
                    Ttl::Keep => live.and_then(|member| member.deadline),
                    Ttl::After(milliseconds) => {
                        if milliseconds <= 0 {
                            return Err(ExecutionError::Memory("ERR invalid memory ttl"));
                        }
                        let (_, absolute) = self
                            .deadline(milliseconds, now)
                            .filter(|(_, absolute)| *absolute > 0)
                            .ok_or(ExecutionError::Memory("ERR invalid memory ttl"))?;
                        Some(absolute)
                    }
                };
                Edit::Upsert(*member)
            }
            Operation::Get { .. } | Operation::Pttl { .. } => {
                if let Some(member) = live {
                    let reply = if matches!(operation, Operation::Get { .. }) {
                        member_reply(member)
                    } else {
                        let effective = entry
                            .expires_at
                            .into_iter()
                            .chain(entry.member_deadlines.get(&member.id).copied())
                            .min();
                        Reply::Integer(
                            effective
                                .map(|deadline| {
                                    i64::try_from(
                                        deadline.saturating_duration_since(now).as_millis(),
                                    )
                                    .unwrap_or(i64::MAX)
                                })
                                .unwrap_or(-1),
                        )
                    };
                    return Ok((reply, None));
                }
                let reply = if matches!(operation, Operation::Get { .. }) {
                    Reply::Bulk(None)
                } else {
                    Reply::Integer(-2)
                };
                if expired {
                    let resolved =
                        self.apply_memory_edit(key, &collection, Edit::Remove(id.clone()))?;
                    return Ok((reply, Some(resolved)));
                }
                return Ok((reply, None));
            }
            Operation::Delete { .. } => {
                if live.is_none() {
                    return Ok((Reply::Integer(0), None));
                }
                Edit::Remove(id.clone())
            }
            Operation::Expire { milliseconds, .. } => {
                let Some(member) = live else {
                    return Ok((Reply::Integer(0), None));
                };
                if milliseconds <= 0 {
                    Edit::Remove(member.id.clone())
                } else {
                    let mut replacement = member.clone();
                    let (_, absolute) = self
                        .deadline(milliseconds, now)
                        .filter(|(_, absolute)| *absolute > 0)
                        .ok_or(ExecutionError::Memory("ERR invalid memory ttl"))?;
                    replacement.deadline = Some(absolute);
                    Edit::Upsert(replacement)
                }
            }
            Operation::Persist { .. } => {
                let Some(member) = live else {
                    return Ok((Reply::Integer(0), None));
                };
                if member.deadline.is_none() {
                    return Ok((Reply::Integer(0), None));
                }
                let mut replacement = member.clone();
                replacement.deadline = None;
                Edit::Upsert(replacement)
            }
            _ => unreachable!("create and info handled above"),
        };
        let mutation = self.apply_memory_edit(key, &collection, edit)?;
        Ok((Reply::Integer(1), Some(mutation)))
    }

    fn apply_memory_edit(
        &mut self,
        key: &Bytes,
        collection: &Collection,
        edit: Edit,
    ) -> Result<memory_format::Mutation, ExecutionError> {
        let candidate = collection
            .apply_edits(collection.descriptor(), std::slice::from_ref(&edit), key)
            .map_err(memory_error)?;
        let value = Value::Memory(Arc::new(candidate));
        if !self.can_replace(key, &value) {
            return Err(ExecutionError::OutOfMemory);
        }
        self.resolve_member_deadlines(key, &value)
            .map_err(memory_error)?;
        let expiry = self.values.get(key).and_then(Self::entry_expiry);
        let key_deadline = expiry.map(|expiry| expiry.1);
        self.insert(key.clone(), value, expiry);
        Ok(memory_format::Mutation::Delta {
            key: key.clone(),
            descriptor: collection.descriptor().clone(),
            edits: vec![edit],
            key_deadline,
        })
    }
}

fn memory_error(error: MemoryError) -> ExecutionError {
    ExecutionError::Memory(match error {
        MemoryError::Dimension => "ERR memory vector length mismatch",
        MemoryError::NonFinite => "ERR memory vector contains non-finite value",
        MemoryError::Deadline => "ERR invalid memory ttl",
        MemoryError::Identity => "ERR memory model identity mismatch",
        _ => "ERR memory limit exceeded",
    })
}
fn bulk(value: &Bytes) -> Reply {
    Reply::Bulk(Some(value.clone()))
}
fn descriptor_reply(descriptor: &Descriptor) -> Reply {
    Reply::Array(vec![
        Reply::Integer(1),
        Reply::Integer(i64::from(descriptor.dimension)),
        Reply::Bulk(Some(Bytes::from_static(b"L2SQ"))),
        bulk(&descriptor.provider),
        bulk(&descriptor.model),
        bulk(&descriptor.revision),
        bulk(&descriptor.transform),
    ])
}
fn member_reply(member: &memory::Member) -> Reply {
    let metadata = member
        .metadata
        .iter()
        .map(|(field, value)| {
            let (kind, value) = match value {
                memory::Scalar::Bytes(value) => (Bytes::from_static(b"BYTES"), value.clone()),
                memory::Scalar::Integer(value) => {
                    (Bytes::from_static(b"I64"), Bytes::from(value.to_string()))
                }
                memory::Scalar::Bool(value) => (
                    Bytes::from_static(b"BOOL"),
                    Bytes::from_static(if *value { b"1" } else { b"0" }),
                ),
            };
            Reply::Array(vec![
                bulk(field),
                Reply::Bulk(Some(kind)),
                Reply::Bulk(Some(value)),
            ])
        })
        .collect();
    let provenance = member
        .provenance
        .iter()
        .map(|(field, value)| Reply::Array(vec![bulk(field), bulk(value)]))
        .collect();
    Reply::Array(vec![
        bulk(&member.namespace),
        bulk(&member.content),
        bulk(&member.vector),
        Reply::Integer(member.deadline.unwrap_or(-1)),
        Reply::Array(metadata),
        Reply::Array(provenance),
    ])
}
