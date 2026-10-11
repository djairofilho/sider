//! Validated memory images and guarded resolved edits for `sider-memory/1`.
//!
//! This schema is shared by native and RESP2 lifecycle commands. It never
//! prunes historical state during delta replay. Owning-worker admission supplies
//! the operation clock, shard quota, and durable record limit.

use bytes::Bytes;
use std::collections::BTreeMap;
use thiserror::Error;

/// Native and RESP2 lifecycle request; durable activation remains a separate gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Operation {
    Create(Descriptor),
    Info,
    Put {
        descriptor: Descriptor,
        member: Box<Member>,
        ttl: Ttl,
        condition: crate::command::SetCondition,
    },
    Get {
        id: Bytes,
    },
    Delete {
        id: Bytes,
    },
    Pttl {
        id: Bytes,
    },
    Expire {
        id: Bytes,
        milliseconds: i64,
    },
    Persist {
        id: Bytes,
    },
}

impl Operation {
    /// Exact grant/command name; IDs and record fields are never authority keys.
    pub fn command_name(&self) -> &'static str {
        match self {
            Self::Create(_) => "MEM.CREATE",
            Self::Info => "MEM.INFO",
            Self::Put { .. } => "MEM.PUT",
            Self::Get { .. } => "MEM.GET",
            Self::Delete { .. } => "MEM.DEL",
            Self::Pttl { .. } => "MEM.PTTL",
            Self::Expire { .. } => "MEM.EXPIRE",
            Self::Persist { .. } => "MEM.PERSIST",
        }
    }
    pub fn writes_dataset(&self) -> bool {
        !matches!(self, Self::Info | Self::Get { .. } | Self::Pttl { .. })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ttl {
    Persistent,
    Keep,
    After(i64),
}

pub const MAX_DIMENSION: usize = 4096;
pub const MAX_MEMBERS: usize = 1024;
pub const MAX_COLLECTION_BYTES: usize = 8_388_608;
pub const MAX_COMPONENTS: usize = 1_048_576;
pub const MAX_EDITS: usize = 64;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum MemoryError {
    #[error("invalid memory argument")]
    Argument,
    #[error("memory vector dimension mismatch")]
    Dimension,
    #[error("memory vector contains non-finite values")]
    NonFinite,
    #[error("memory model identity mismatch")]
    Identity,
    #[error("invalid memory TTL")]
    Deadline,
    #[error("memory collection quota exceeded")]
    Quota,
    #[error("memory delta contains unordered or duplicate edits")]
    EditOrder,
    #[error("memory delta removes an absent member")]
    MissingMember,
}

/// Version, encoding, and metric are fixed at 1, F32LE, and L2SQ.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Descriptor {
    pub dimension: u32,
    pub provider: Bytes,
    pub model: Bytes,
    pub revision: Bytes,
    pub transform: Bytes,
}

impl Descriptor {
    pub fn validate(&self) -> Result<(), MemoryError> {
        if !(1..=MAX_DIMENSION).contains(&(self.dimension as usize)) {
            return Err(MemoryError::Dimension);
        }
        for value in [&self.provider, &self.model, &self.revision, &self.transform] {
            bounded(value, 1, 256)?;
        }
        Ok(())
    }

    pub fn logical_bytes(&self) -> usize {
        256 + self.provider.len() + self.model.len() + self.revision.len() + self.transform.len()
    }

    pub fn encoded_bytes(&self) -> usize {
        26 + self.provider.len() + self.model.len() + self.revision.len() + self.transform.len()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scalar {
    Bytes(Bytes),
    Integer(i64),
    Bool(bool),
}

impl Scalar {
    fn logical_bytes(&self) -> usize {
        match self {
            Self::Bytes(value) => value.len(),
            Self::Integer(_) => 8,
            Self::Bool(_) => 1,
        }
    }
    fn canonical_bytes(&self) -> usize {
        match self {
            Self::Bytes(value) => value.len(),
            Self::Integer(value) => value.to_string().len(),
            Self::Bool(_) => 1,
        }
    }
    fn encoded_bytes(&self) -> usize {
        match self {
            Self::Bytes(value) => 5 + value.len(),
            Self::Integer(_) => 9,
            Self::Bool(_) => 2,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub id: Bytes,
    pub namespace: Bytes,
    pub content: Bytes,
    /// Original finite IEEE-754 binary32 bits, including signed zero.
    pub vector: Bytes,
    pub deadline: Option<i64>,
    pub metadata: BTreeMap<Bytes, Scalar>,
    pub provenance: BTreeMap<Bytes, Bytes>,
}

impl Member {
    pub fn validate(&self, descriptor: &Descriptor) -> Result<(), MemoryError> {
        descriptor.validate()?;
        bounded(&self.id, 1, 256)?;
        bounded(&self.namespace, 0, 256)?;
        bounded(&self.content, 0, 65_536)?;
        if self.vector.len() != descriptor.dimension as usize * 4 {
            return Err(MemoryError::Dimension);
        }
        for coordinate in self.vector.chunks_exact(4) {
            let bits = u32::from_le_bytes(coordinate.try_into().expect("four-byte coordinate"));
            if bits & 0x7f80_0000 == 0x7f80_0000 {
                return Err(MemoryError::NonFinite);
            }
        }
        if self.deadline.is_some_and(|deadline| deadline <= 0) {
            return Err(MemoryError::Deadline);
        }
        if self.metadata.len() > 32 || self.provenance.len() > 16 {
            return Err(MemoryError::Argument);
        }
        let mut aggregate = 0usize;
        for (field, scalar) in &self.metadata {
            bounded(field, 1, 64)?;
            if let Scalar::Bytes(value) = scalar {
                bounded(value, 0, 4096)?;
            }
            aggregate = aggregate
                .checked_add(field.len())
                .and_then(|size| size.checked_add(scalar.canonical_bytes()))
                .ok_or(MemoryError::Argument)?;
        }
        for (field, value) in &self.provenance {
            bounded(field, 1, 64)?;
            bounded(value, 0, 4096)?;
            aggregate = aggregate
                .checked_add(field.len())
                .and_then(|size| size.checked_add(value.len()))
                .ok_or(MemoryError::Argument)?;
        }
        if aggregate > 32_768 {
            return Err(MemoryError::Argument);
        }
        Ok(())
    }

    pub fn logical_bytes(&self) -> usize {
        256 + self.id.len()
            + self.namespace.len()
            + self.content.len()
            + self.vector.len()
            + self
                .metadata
                .iter()
                .map(|(field, value)| 64 + field.len() + value.logical_bytes())
                .sum::<usize>()
            + self
                .provenance
                .iter()
                .map(|(field, value)| 64 + field.len() + value.len())
                .sum::<usize>()
    }

    pub fn encoded_bytes(&self) -> usize {
        33 + self.id.len()
            + self.namespace.len()
            + self.content.len()
            + self.vector.len()
            + self
                .metadata
                .iter()
                .map(|(field, value)| 4 + field.len() + value.encoded_bytes())
                .sum::<usize>()
            + self
                .provenance
                .iter()
                .map(|(field, value)| 8 + field.len() + value.len())
                .sum::<usize>()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Edit {
    Upsert(Member),
    Remove(Bytes),
}

impl Edit {
    pub fn id(&self) -> &Bytes {
        match self {
            Self::Upsert(member) => &member.id,
            Self::Remove(id) => id,
        }
    }
    pub fn encoded_bytes(&self) -> usize {
        match self {
            Self::Upsert(member) => 1 + member.encoded_bytes(),
            Self::Remove(id) => 5 + id.len(),
        }
    }
}

/// Immutable collection images share member payloads through `Bytes`.
/// Cached charges and image lengths exclude the key and outer entry overhead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Collection {
    descriptor: Descriptor,
    members: BTreeMap<Bytes, Member>,
    logical_bytes: usize,
    encoded_bytes: usize,
}

impl Collection {
    pub fn new(descriptor: Descriptor) -> Result<Self, MemoryError> {
        descriptor.validate()?;
        let logical_bytes = descriptor.logical_bytes();
        let encoded_bytes = descriptor.encoded_bytes() + 4;
        Ok(Self {
            descriptor,
            members: BTreeMap::new(),
            logical_bytes,
            encoded_bytes,
        })
    }

    pub fn from_members(
        descriptor: Descriptor,
        members: Vec<Member>,
        key: &[u8],
    ) -> Result<Self, MemoryError> {
        let mut collection = Self::new(descriptor)?;
        let mut previous: Option<Bytes> = None;
        for member in members {
            member.validate(&collection.descriptor)?;
            if previous.as_ref().is_some_and(|id| id >= &member.id) {
                return Err(MemoryError::EditOrder);
            }
            previous = Some(member.id.clone());
            collection.logical_bytes = collection
                .logical_bytes
                .checked_add(member.logical_bytes())
                .ok_or(MemoryError::Quota)?;
            collection.encoded_bytes = collection
                .encoded_bytes
                .checked_add(member.encoded_bytes())
                .ok_or(MemoryError::Quota)?;
            collection.members.insert(member.id.clone(), member);
        }
        collection.validate_key(key)?;
        Ok(collection)
    }

    pub fn descriptor(&self) -> &Descriptor {
        &self.descriptor
    }
    pub fn members(&self) -> &BTreeMap<Bytes, Member> {
        &self.members
    }
    pub fn retained_count(&self) -> usize {
        self.members.len()
    }
    pub fn logical_bytes(&self) -> usize {
        self.logical_bytes
    }
    pub fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    pub fn validate_key(&self, key: &[u8]) -> Result<(), MemoryError> {
        bounded(key, 1, 256)?;
        if self.members.len() > MAX_MEMBERS
            || self
                .members
                .len()
                .checked_mul(self.descriptor.dimension as usize)
                .is_none_or(|work| work > MAX_COMPONENTS)
            || self
                .logical_bytes
                .checked_add(128)
                .and_then(|size| size.checked_add(key.len()))
                .is_none_or(|size| size > MAX_COLLECTION_BYTES)
        {
            return Err(MemoryError::Quota);
        }
        Ok(())
    }

    /// Visibility is separate from retained state. Replay never calls this method.
    pub fn live_member(&self, id: &[u8], unix_ms: i64) -> Option<&Member> {
        self.members
            .get(id)
            .filter(|member| member.deadline.is_none_or(|deadline| deadline > unix_ms))
    }

    /// Returns a complete candidate or an error, preserving the original image.
    /// Removes validate physical membership even when the member deadline elapsed.
    pub fn apply_edits(
        &self,
        descriptor: &Descriptor,
        edits: &[Edit],
        key: &[u8],
    ) -> Result<Self, MemoryError> {
        if descriptor != &self.descriptor {
            return Err(MemoryError::Identity);
        }
        if edits.len() > MAX_EDITS {
            return Err(MemoryError::Argument);
        }
        validate_edits(descriptor, edits)?;
        let mut candidate = self.clone();
        for edit in edits {
            if let Some(previous) = candidate.members.remove(edit.id()) {
                candidate.logical_bytes -= previous.logical_bytes();
                candidate.encoded_bytes -= previous.encoded_bytes();
            } else if matches!(edit, Edit::Remove(_)) {
                return Err(MemoryError::MissingMember);
            }
            if let Edit::Upsert(member) = edit {
                candidate.logical_bytes = candidate
                    .logical_bytes
                    .checked_add(member.logical_bytes())
                    .ok_or(MemoryError::Quota)?;
                candidate.encoded_bytes = candidate
                    .encoded_bytes
                    .checked_add(member.encoded_bytes())
                    .ok_or(MemoryError::Quota)?;
                candidate.members.insert(member.id.clone(), member.clone());
            }
        }
        candidate.validate_key(key)?;
        Ok(candidate)
    }

    /// Sorted resolved removes, capped independently of the number of overdue members.
    pub fn expired_edits(&self, unix_ms: i64, budget: usize) -> Vec<Edit> {
        self.members
            .iter()
            .filter(|(_, member)| member.deadline.is_some_and(|deadline| deadline <= unix_ms))
            .take(budget.min(MAX_EDITS))
            .map(|(id, _)| Edit::Remove(id.clone()))
            .collect()
    }
}

pub(crate) fn validate_edits(descriptor: &Descriptor, edits: &[Edit]) -> Result<(), MemoryError> {
    descriptor.validate()?;
    let mut previous: Option<&Bytes> = None;
    for edit in edits {
        bounded(edit.id(), 1, 256)?;
        if previous.is_some_and(|id| id >= edit.id()) {
            return Err(MemoryError::EditOrder);
        }
        if let Edit::Upsert(member) = edit {
            member.validate(descriptor)?;
        }
        previous = Some(edit.id());
    }
    Ok(())
}

fn bounded(value: &[u8], minimum: usize, maximum: usize) -> Result<(), MemoryError> {
    if (minimum..=maximum).contains(&value.len()) {
        Ok(())
    } else {
        Err(MemoryError::Argument)
    }
}
