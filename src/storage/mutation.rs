//! Prepara somente as chaves tocadas; o estado real muda após a confirmação durável.

use std::collections::BTreeSet;

use thiserror::Error;

use super::*;

/// Estado final resolvido, independente do comando e do relógio de replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mutation {
    Put {
        key: Bytes,
        value: Value,
        expires_at_unix_ms: Option<i64>,
    },
    Delete {
        key: Bytes,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(key: &'static [u8], value: &'static [u8]) -> Command {
        Command::Set {
            key: Bytes::from_static(key),
            value: Bytes::from_static(value),
        }
    }

    #[test]
    fn preparation_keeps_real_state_and_uses_total_dataset_quota() {
        let mut store = Store::with_config(
            StoreConfig {
                max_dataset_bytes: 270,
            },
            Arc::new(SystemClock),
        )
        .unwrap();
        store.execute(set(b"unrelated", b"long"));
        let prepared = store.prepare(set(b"key", b"value"));
        assert_eq!(prepared.reply, Reply::Error(ExecutionError::OutOfMemory));
        assert!(prepared.batch.mutations.is_empty());
        assert_eq!(store.len(), 1);
        let prepared = store.prepare(Command::MSet {
            entries: vec![
                (Bytes::from_static(b"unrelated"), Bytes::new()),
                (Bytes::from_static(b"k"), Bytes::new()),
            ],
        });
        assert_eq!(prepared.reply, Reply::Ok);
        assert_eq!(store.len(), 1);
        store.apply(prepared);
        assert_eq!(store.len(), 2);
        assert_eq!(store.used_bytes(), 266);
    }

    #[tokio::test(start_paused = true)]
    async fn apply_keeps_prepared_deadline_and_expiration_is_resolved_tombstone() {
        let mut store = Store::with_clock(Arc::new(FrozenClock {
            now: Instant::now(),
            unix_ms: 1000,
        }));
        let prepared = store.prepare(Command::SetWithOptions {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"1"),
            options: SetOptions {
                expiry: SetExpiry::After(Duration::from_millis(10)),
                ..SetOptions::default()
            },
        });
        assert!(matches!(
            prepared.batch.mutations[0],
            Mutation::Put {
                expires_at_unix_ms: Some(1010),
                ..
            }
        ));
        tokio::time::advance(Duration::from_millis(20)).await;
        store.apply(prepared);
        store.clock = Arc::new(FrozenClock {
            now: Instant::now(),
            unix_ms: 500,
        });
        let passive = store.prepare(Command::Get {
            key: Bytes::from_static(b"k"),
        });
        assert_eq!(passive.batch.origin, MutationOrigin::Expiration);
        assert_eq!(passive.reply, Reply::Bulk(None));
        assert_eq!(store.len(), 1);
        let expiration = store.prepare_expiration(1);
        assert_eq!(expiration.batch.origin, MutationOrigin::Expiration);
        assert_eq!(
            expiration.batch.mutations,
            vec![Mutation::Delete {
                key: Bytes::from_static(b"k")
            }]
        );
        assert_eq!(store.len(), 1);
        store.apply(expiration);
        assert!(store.is_empty());
    }

    #[test]
    fn replay_is_atomic_and_expired_replacement_removes_previous_value() {
        let mut store = Store::with_config(
            StoreConfig {
                max_dataset_bytes: 140,
            },
            Arc::new(FrozenClock {
                now: Instant::now(),
                unix_ms: 1000,
            }),
        )
        .unwrap();
        store.execute(set(b"k", b"before"));
        let invalid = vec![
            Mutation::Delete {
                key: Bytes::from_static(b"k"),
            },
            Mutation::Put {
                key: Bytes::from_static(b"other"),
                value: Bytes::from_static(b"too big for quota").into(),
                expires_at_unix_ms: None,
            },
        ];
        assert!(matches!(store.replay(&invalid), Err(ReplayError::Quota)));
        assert_eq!(
            store.execute(Command::Get {
                key: Bytes::from_static(b"k")
            }),
            Reply::Bulk(Some(Bytes::from_static(b"before")))
        );
        store
            .replay(&[Mutation::Put {
                key: Bytes::from_static(b"k"),
                value: Bytes::new().into(),
                expires_at_unix_ms: Some(900),
            }])
            .unwrap();
        assert!(store.is_empty());
    }
}

impl Mutation {
    pub fn key(&self) -> &Bytes {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// Manutenção de TTL não é uma escrita recebida de um cliente.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutationOrigin {
    Client,
    Expiration,
}

/// Unidade indivisível de persistência; a sequência é atribuída pelo escritor global.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedBatch {
    pub origin: MutationOrigin,
    pub mutations: Vec<Mutation>,
}

/// Resultado pré-validado. Deve ser aplicado no mesmo store, antes de outra mutação.
pub struct Prepared {
    pub reply: Reply,
    pub batch: ResolvedBatch,
    generation: u64,
    entries: Vec<(Bytes, Option<Entry>)>,
}

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("lote contém chaves duplicadas")]
    DuplicateKey,
    #[error("dataset recuperado excede a quota configurada")]
    Quota,
    #[error("prazo absoluto não é representável pelo relógio")]
    Deadline,
}

struct FrozenClock {
    now: Instant,
    unix_ms: i64,
}
impl Clock for FrozenClock {
    fn now(&self) -> Instant {
        self.now
    }
    fn unix_millis(&self) -> i64 {
        self.unix_ms
    }
}

impl Store {
    /// Resolve condições, inteiros, TTL e quota sem copiar nem modificar o dataset.
    /// A cópia temporária contém somente metadados das chaves do comando; Bytes é compartilhado.
    pub fn prepare(&self, command: Command) -> Prepared {
        let origin = if matches!(
            &command,
            Command::Get { .. }
                | Command::MGet { .. }
                | Command::Exists { .. }
                | Command::Ttl { .. }
                | Command::Hash {
                    operation: crate::command::HashCommand::Get { .. }
                        | crate::command::HashCommand::Exists { .. }
                        | crate::command::HashCommand::Len
                        | crate::command::HashCommand::GetAll,
                    ..
                }
                | Command::List {
                    operation: crate::command::ListCommand::Len
                        | crate::command::ListCommand::Range { .. },
                    ..
                }
                | Command::SetCollection {
                    operation: crate::command::SetCommand::IsMember { .. }
                        | crate::command::SetCommand::Card
                        | crate::command::SetCommand::Members,
                    ..
                }
                | Command::SortedSet {
                    operation: crate::command::SortedSetCommand::Score { .. }
                        | crate::command::SortedSetCommand::Card
                        | crate::command::SortedSetCommand::Range { .. },
                    ..
                }
        ) {
            MutationOrigin::Expiration
        } else {
            MutationOrigin::Client
        };
        let mut keys = BTreeSet::new();
        command.visit_keys(|key| {
            keys.insert(key.clone());
        });
        let mut shadow = Self::with_config(
            self.config,
            Arc::new(FrozenClock {
                now: self.clock.now(),
                unix_ms: self.clock.unix_millis(),
            }),
        )
        .expect("configuração já validada");
        shadow.used_bytes = self.used_bytes;
        shadow.generation = self.generation;
        for key in &keys {
            if let Some(entry) = self.values.get(key) {
                shadow.values.insert(key.clone(), entry.clone());
                if let Some(deadline) = entry.expires_at {
                    shadow
                        .expirations
                        .insert((deadline, entry.generation, key.clone()));
                }
            }
        }
        let reply = shadow.execute_inner(command);
        let mut entries = Vec::new();
        for key in keys {
            let before = self.values.get(&key);
            let after = shadow.values.get(&key);
            if before.map(|entry| entry.generation) != after.map(|entry| entry.generation) {
                entries.push((key, after.cloned()));
            }
        }
        self.prepared(reply, entries, origin)
    }

    /// Prepara tombstones limitados, sem remover uma chave antes do append.
    pub fn prepare_expiration(&self, budget: usize) -> Prepared {
        let now = self.clock.now();
        let entries = self
            .expirations
            .iter()
            .take(budget)
            .take_while(|(deadline, _, _)| *deadline <= now)
            .filter(|(deadline, generation, key)| {
                self.values.get(key).is_some_and(|entry| {
                    entry.generation == *generation && entry.expires_at == Some(*deadline)
                })
            })
            .map(|(_, _, key)| (key.clone(), None))
            .collect::<Vec<_>>();
        self.prepared(
            Reply::Integer(entries.len() as i64),
            entries,
            MutationOrigin::Expiration,
        )
    }

    fn prepared(
        &self,
        reply: Reply,
        entries: Vec<(Bytes, Option<Entry>)>,
        origin: MutationOrigin,
    ) -> Prepared {
        let mutations = entries
            .iter()
            .map(|(key, entry)| match entry {
                Some(entry) => Mutation::Put {
                    key: key.clone(),
                    value: entry.value.clone(),
                    expires_at_unix_ms: entry.expires_at_unix_ms,
                },
                None => Mutation::Delete { key: key.clone() },
            })
            .collect();
        Prepared {
            reply,
            batch: ResolvedBatch { origin, mutations },
            generation: self.generation,
            entries,
        }
    }

    /// Aplica sem reavaliar condições, quota ou relógio. O proprietário não intercala preparações.
    pub fn apply(&mut self, prepared: Prepared) -> Reply {
        assert_eq!(self.generation, prepared.generation, "preparação obsoleta");
        for (key, _) in &prepared.entries {
            self.remove(key);
        }
        for (key, entry) in prepared.entries {
            if let Some(entry) = entry {
                self.insert(key, entry.value.clone(), Self::entry_expiry(&entry));
            }
        }
        prepared.reply
    }

    /// Snapshot consistente e ordenado; valores imutáveis compartilham armazenamento.
    /// Entradas expiradas ainda presentes são mantidas com deadline, nunca como persistentes.
    pub fn snapshot(&self) -> Vec<Mutation> {
        let mut entries: Vec<_> = self
            .values
            .iter()
            .map(|(key, entry)| Mutation::Put {
                key: key.clone(),
                value: entry.value.clone(),
                expires_at_unix_ms: entry.expires_at_unix_ms,
            })
            .collect();
        entries.sort_unstable_by(|left, right| left.key().cmp(right.key()));
        entries
    }

    /// Valida o lote completo antes de aplicar; TTL vencido vira remoção e não ressuscita valor anterior.
    pub fn replay(&mut self, mutations: &[Mutation]) -> Result<(), ReplayError> {
        let now = self.clock.now();
        let unix_ms = self.clock.unix_millis();
        let mut keys = BTreeSet::new();
        let mut entries = Vec::with_capacity(mutations.len());
        let mut used = self.used_bytes;
        for mutation in mutations {
            let key = mutation.key();
            if !keys.insert(key) {
                return Err(ReplayError::DuplicateKey);
            }
            if let Some(old) = self.values.get(key) {
                used -= Self::entry_bytes(key, &old.value).ok_or(ReplayError::Quota)?;
            }
        }
        for mutation in mutations {
            let key = mutation.key().clone();
            let entry = match mutation {
                Mutation::Delete { .. } => None,
                Mutation::Put {
                    expires_at_unix_ms: Some(deadline),
                    ..
                } if *deadline <= unix_ms => None,
                Mutation::Put {
                    value,
                    expires_at_unix_ms,
                    ..
                } => {
                    let expires_at = expires_at_unix_ms
                        .map(|deadline| {
                            let remaining = i128::from(deadline) - i128::from(unix_ms);
                            let millis =
                                u64::try_from(remaining).map_err(|_| ReplayError::Deadline)?;
                            now.checked_add(Duration::from_millis(millis))
                                .ok_or(ReplayError::Deadline)
                        })
                        .transpose()?;
                    used = used
                        .checked_add(Self::entry_bytes(&key, value).ok_or(ReplayError::Quota)?)
                        .ok_or(ReplayError::Quota)?;
                    Some(Entry {
                        value: value.clone(),
                        expires_at,
                        expires_at_unix_ms: *expires_at_unix_ms,
                        generation: 0,
                    })
                }
            };
            entries.push((key, entry));
        }
        if used > self.config.max_dataset_bytes {
            return Err(ReplayError::Quota);
        }
        let prepared = self.prepared(Reply::Ok, entries, MutationOrigin::Client);
        self.apply(prepared);
        Ok(())
    }
}
