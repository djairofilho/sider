//! Armazenamento síncrono de chaves e valores binários, sem acesso ao protocolo.

mod clock;
pub mod routing;
mod mutation;
pub mod worker;
pub use clock::{Clock, SystemClock};
pub use mutation::{Mutation, MutationOrigin, Prepared, ReplayError, ResolvedBatch};

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::time::Instant;

use crate::ConfigError;
use crate::command::{
    Command, ExecutionError, ExpiryUnit, Reply, SetCondition, SetExpiry, SetOptions, parse_decimal,
};

/// Taxa lógica fixa por entrada; inclui metadados e índice de expiração.
pub const ENTRY_OVERHEAD_BYTES: usize = 128;

/// Orçamento lógico, independente das alocações reais e do RSS.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreConfig {
    pub max_dataset_bytes: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            max_dataset_bytes: 64 * 1024 * 1024,
        }
    }
}

impl StoreConfig {
    pub fn validate(self) -> Result<(), ConfigError> {
        if self.max_dataset_bytes == 0 || self.max_dataset_bytes > isize::MAX as usize {
            return Err(ConfigError::InvalidServerLimits {
                reason: "SIDER_MAX_DATASET_BYTES precisa estar entre 1 e isize::MAX",
            });
        }
        Ok(())
    }
}

/// Valor e metadados comuns. O prazo absoluto registra o instante resolvido da escrita.
#[derive(Clone, Debug)]
pub struct Entry {
    pub value: Bytes,
    pub expires_at: Option<Instant>,
    pub expires_at_unix_ms: Option<i64>,
    pub generation: u64,
}

/// Mapa em memória com proprietário único e execução sequencial dos comandos.
///
/// O relógio é injetável; eventos antigos são retirados em toda substituição.
pub struct Store {
    values: HashMap<Bytes, Entry>,
    expirations: BTreeSet<(Instant, u64, Bytes)>,
    generation: u64,
    clock: Arc<dyn Clock>,
    config: StoreConfig,
    used_bytes: usize,
}

impl Default for Store {
    fn default() -> Self {
        Self::with_clock(Arc::new(SystemClock))
    }
}

impl Store {
    /// Cria um armazenamento vazio, usando o hasher padrão de `HashMap`.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        Self::with_config(StoreConfig::default(), clock).expect("configuração padrão válida")
    }

    pub fn with_config(config: StoreConfig, clock: Arc<dyn Clock>) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self {
            values: HashMap::new(),
            expirations: BTreeSet::new(),
            generation: 0,
            clock,
            config,
            used_bytes: 0,
        })
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    /// Quantidade física de entradas, incluindo expiradas ainda não visitadas.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Processa no máximo `budget` eventos sem percorrer o mapa inteiro.
    pub fn expire_due(&mut self, budget: usize) -> usize {
        let now = self.clock.now();
        let mut removed = 0;
        for _ in 0..budget {
            if !self
                .expirations
                .first()
                .is_some_and(|(deadline, _, _)| *deadline <= now)
            {
                break;
            }
            let Some((deadline, generation, key)) = self.expirations.pop_first() else {
                break;
            };
            if self.values.get(&key).is_some_and(|entry| {
                entry.generation == generation && entry.expires_at == Some(deadline)
            }) {
                self.remove(&key);
                removed += 1;
            }
        }
        removed
    }

    /// Aplica um comando validado sem suspender a execução.
    ///
    /// `GET` compartilha o conteúdo imutável de `Bytes` com a resposta. Alterar ou
    /// remover a chave depois não altera uma resposta já devolvida.
    pub fn execute(&mut self, command: Command) -> Reply {
        let prepared = self.prepare(command);
        self.apply(prepared)
    }

    fn execute_inner(&mut self, command: Command) -> Reply {
        let now = self.clock.now();
        match command {
            Command::Ping(None) => Reply::Pong,
            Command::Ping(Some(message)) | Command::Echo(message) => Reply::Bulk(Some(message)),
            Command::Get { key } => {
                self.expire_key(&key, now);
                Reply::Bulk(self.values.get(&key).map(|entry| entry.value.clone()))
            }
            Command::Set { key, value } => self.set(key, value, SetOptions::default(), now),
            Command::SetWithOptions {
                key,
                value,
                options,
            } => self.set(key, value, options, now),
            Command::Del { keys } => {
                // Nos alvos de 32/64 bits, uma Vec<Bytes> válida possui menos de
                // i64::MAX elementos. Cada elemento causa no máximo um incremento.
                let mut removed = 0_i64;
                for key in keys {
                    self.expire_key(&key, now);
                    if self.remove(&key).is_some() {
                        removed += 1;
                    }
                }
                Reply::Integer(removed)
            }
            Command::Exists { keys } => Reply::Integer(
                keys.iter()
                    .filter(|key| {
                        self.expire_key(key, now);
                        self.values.contains_key(*key)
                    })
                    .count() as i64,
            ),
            Command::MGet { keys } => Reply::Array(
                keys.iter()
                    .map(|key| {
                        self.expire_key(key, now);
                        Reply::Bulk(self.values.get(key).map(|entry| entry.value.clone()))
                    })
                    .collect(),
            ),
            Command::MSet { entries } => self.mset(entries, now),
            Command::Incr { key } => self.increment(key, 1, now),
            Command::Decr { key } => self.increment(key, -1, now),
            Command::Expire { key, value, unit } => self.expire(key, value, unit, now),
            Command::Ttl { key, milliseconds } => {
                self.expire_key(&key, now);
                let value = match self.values.get(&key) {
                    None => -2,
                    Some(Entry {
                        expires_at: None, ..
                    }) => -1,
                    Some(entry) => {
                        let ttl = entry
                            .expires_at
                            .unwrap()
                            .saturating_duration_since(now)
                            .as_millis();
                        let result = if milliseconds {
                            ttl
                        } else {
                            (ttl + 500) / 1000
                        };
                        i64::try_from(result).unwrap_or(i64::MAX)
                    }
                };
                Reply::Integer(value)
            }
            Command::Persist { key } => {
                self.expire_key(&key, now);
                let Some(entry) = self.values.get(&key) else {
                    return Reply::Integer(0);
                };
                if entry.expires_at.is_none() {
                    return Reply::Integer(0);
                }
                self.insert(key, entry.value.clone(), None);
                Reply::Integer(1)
            }
            Command::Subscribe { .. } | Command::Unsubscribe { .. } | Command::Publish { .. } => {
                Reply::Error(ExecutionError::ConnectionOnly)
            }
        }
    }

    fn increment(&mut self, key: Bytes, delta: i64, now: Instant) -> Reply {
        self.expire_key(&key, now);
        let previous = match self.values.get(&key) {
            Some(entry) => match parse_decimal(&entry.value) {
                Some(value) => value,
                None => return Reply::Error(ExecutionError::InvalidInteger),
            },
            None => 0,
        };
        let Some(value) = previous.checked_add(delta) else {
            return Reply::Error(ExecutionError::IntegerOverflow);
        };
        let expiry = self.values.get(&key).and_then(Self::entry_expiry);
        let encoded = Bytes::from(value.to_string());
        if !self.can_replace(&key, &encoded) {
            return Reply::Error(ExecutionError::OutOfMemory);
        }
        self.insert(key, encoded, expiry);
        Reply::Integer(value)
    }

    fn entry_expiry(entry: &Entry) -> Option<(Instant, i64)> {
        entry.expires_at.zip(entry.expires_at_unix_ms)
    }

    fn deadline(&self, milliseconds: i64, now: Instant) -> Option<(Instant, i64)> {
        let absolute = self.clock.unix_millis().checked_add(milliseconds)?;
        let monotonic =
            now.checked_add(Duration::from_millis(u64::try_from(milliseconds).ok()?))?;
        Some((monotonic, absolute))
    }

    fn set(&mut self, key: Bytes, value: Bytes, options: SetOptions, now: Instant) -> Reply {
        let deadline = match options.expiry {
            SetExpiry::After(duration) => {
                let Some(deadline) = i64::try_from(duration.as_millis())
                    .ok()
                    .filter(|millis| *millis > 0)
                    .and_then(|millis| self.deadline(millis, now))
                else {
                    return Reply::Error(ExecutionError::InvalidSetExpiry);
                };
                Some(deadline)
            }
            _ => None,
        };
        self.expire_key(&key, now);
        let previous = self.values.get(&key);
        let reply = if options.return_previous {
            Reply::Bulk(previous.map(|entry| entry.value.clone()))
        } else {
            Reply::Ok
        };
        if (options.condition == SetCondition::Missing && previous.is_some())
            || (options.condition == SetCondition::Present && previous.is_none())
        {
            return if options.return_previous {
                reply
            } else {
                Reply::Bulk(None)
            };
        }
        let expiry = if options.expiry == SetExpiry::Keep {
            previous.and_then(Self::entry_expiry)
        } else {
            deadline
        };
        if !self.can_replace(&key, &value) {
            return Reply::Error(ExecutionError::OutOfMemory);
        }
        self.insert(key, value, expiry);
        reply
    }

    fn expire(&mut self, key: Bytes, value: i64, unit: ExpiryUnit, now: Instant) -> Reply {
        let name = if unit == ExpiryUnit::Seconds {
            "expire"
        } else {
            "pexpire"
        };
        let Some(millis) = (if unit == ExpiryUnit::Seconds {
            value.checked_mul(1000)
        } else {
            Some(value)
        }) else {
            return Reply::Error(ExecutionError::InvalidExpiry(name));
        };
        let deadline = if millis > 0 {
            let Some(deadline) = self.deadline(millis, now) else {
                return Reply::Error(ExecutionError::InvalidExpiry(name));
            };
            Some(deadline)
        } else {
            None
        };
        self.expire_key(&key, now);
        let Some(entry) = self.values.get(&key) else {
            return Reply::Integer(0);
        };
        if millis <= 0 {
            self.remove(&key);
        } else {
            self.insert(key, entry.value.clone(), deadline);
        }
        Reply::Integer(1)
    }

    fn expire_key(&mut self, key: &Bytes, now: Instant) {
        if self
            .values
            .get(key)
            .is_some_and(|entry| entry.expires_at.is_some_and(|deadline| deadline <= now))
        {
            self.remove(key);
        }
    }

    fn remove(&mut self, key: &Bytes) -> Option<Entry> {
        let entry = self.values.remove(key)?;
        self.generation = self.generation.wrapping_add(1);
        self.used_bytes -= Self::entry_bytes(key, &entry.value).expect("entrada contabilizada");
        if let Some(deadline) = entry.expires_at {
            self.expirations
                .remove(&(deadline, entry.generation, key.clone()));
        }
        Some(entry)
    }

    fn insert(&mut self, key: Bytes, value: Bytes, expiry: Option<(Instant, i64)>) {
        self.remove(&key);
        self.used_bytes += Self::entry_bytes(&key, &value).expect("mutação pré-validada");
        // Há no máximo um evento por chave; o anterior foi removido antes da geração avançar.
        self.generation = self.generation.wrapping_add(1);
        let entry = Entry {
            value,
            expires_at: expiry.map(|value| value.0),
            expires_at_unix_ms: expiry.map(|value| value.1),
            generation: self.generation,
        };
        if let Some(deadline) = entry.expires_at {
            self.expirations
                .insert((deadline, entry.generation, key.clone()));
        }
        self.values.insert(key, entry);
    }

    fn entry_bytes(key: &Bytes, value: &Bytes) -> Option<usize> {
        key.len()
            .checked_add(value.len())?
            .checked_add(ENTRY_OVERHEAD_BYTES)
    }

    fn can_replace(&self, key: &Bytes, value: &Bytes) -> bool {
        let old = self
            .values
            .get(key)
            .and_then(|entry| Self::entry_bytes(key, &entry.value))
            .unwrap_or(0);
        let proposed = Self::entry_bytes(key, value)
            .and_then(|new| self.used_bytes.checked_sub(old)?.checked_add(new));
        proposed
            .is_some_and(|usage| usage <= self.config.max_dataset_bytes || usage <= self.used_bytes)
    }

    fn mset(&mut self, entries: Vec<(Bytes, Bytes)>, now: Instant) -> Reply {
        // Reduz duplicatas antes de contabilizar: só o último valor pertence ao estado final.
        let entries: HashMap<_, _> = entries.into_iter().collect();
        for key in entries.keys() {
            self.expire_key(key, now);
        }
        let mut base = self.used_bytes;
        for key in entries.keys() {
            if let Some(old) = self.values.get(key) {
                base -= Self::entry_bytes(key, &old.value).expect("entrada contabilizada");
            }
        }
        let proposed = entries.iter().try_fold(base, |usage, (key, value)| {
            usage.checked_add(Self::entry_bytes(key, value)?)
        });
        if !proposed
            .is_some_and(|usage| usage <= self.config.max_dataset_bytes || usage <= self.used_bytes)
        {
            return Reply::Error(ExecutionError::OutOfMemory);
        }
        // Retira os valores anteriores após validar o lote completo, evitando pico contábil.
        for key in entries.keys() {
            self.remove(key);
        }
        for (key, value) in entries {
            self.insert(key, value, None);
        }
        Reply::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiration_index_stays_bounded_during_repeated_rewrites() {
        let mut store = Store::new();
        for _ in 0..1000 {
            let reply = store.execute(Command::SetWithOptions {
                key: Bytes::from_static(b"key"),
                value: Bytes::from_static(b"value"),
                options: SetOptions {
                    expiry: SetExpiry::After(Duration::from_secs(60)),
                    ..SetOptions::default()
                },
            });
            assert_eq!(reply, Reply::Ok);
            assert_eq!(store.expirations.len(), 1);
        }
        store.execute(Command::Persist {
            key: Bytes::from_static(b"key"),
        });
        assert!(store.expirations.is_empty());
    }

    #[test]
    fn outdated_expiration_generation_cannot_remove_replacement() {
        let mut store = Store::new();
        let key = Bytes::from_static(b"key");
        let now = store.clock.now();
        store.insert(key.clone(), Bytes::from_static(b"old"), Some((now, 0)));
        let old = store.expirations.first().unwrap().clone();
        store.insert(key.clone(), Bytes::from_static(b"new"), None);
        store.expirations.insert(old);
        assert_eq!(store.expire_due(1), 0);
        assert_eq!(
            store.execute(Command::Get { key }),
            Reply::Bulk(Some(Bytes::from_static(b"new")))
        );
    }

    fn set(store: &mut Store, key: &'static [u8], value: &'static [u8]) {
        assert_eq!(
            store.execute(Command::Set {
                key: Bytes::from_static(key),
                value: Bytes::from_static(value),
            }),
            Reply::Ok
        );
    }

    fn get(store: &mut Store, key: &'static [u8]) -> Reply {
        store.execute(Command::Get {
            key: Bytes::from_static(key),
        })
    }

    #[test]
    fn new_and_default_start_without_keys() {
        for mut store in [Store::new(), Store::default()] {
            assert_eq!(get(&mut store, b"missing"), Reply::Bulk(None));
            assert_eq!(get(&mut store, b""), Reply::Bulk(None));
        }
    }

    #[test]
    fn ping_and_echo_preserve_binary_messages_and_state() {
        let mut store = Store::new();
        set(&mut store, b"kept", b"value");

        assert_eq!(store.execute(Command::Ping(None)), Reply::Pong);
        for message in [Bytes::new(), Bytes::from_static(b"\xff\0\r\n\x80")] {
            assert_eq!(
                store.execute(Command::Ping(Some(message.clone()))),
                Reply::Bulk(Some(message.clone()))
            );
            assert_eq!(
                store.execute(Command::Echo(message.clone())),
                Reply::Bulk(Some(message))
            );
        }
        assert_eq!(
            get(&mut store, b"kept"),
            Reply::Bulk(Some(Bytes::from_static(b"value")))
        );
    }

    #[test]
    fn empty_key_and_empty_value_are_not_missing() {
        let mut store = Store::new();
        set(&mut store, b"", b"");
        set(&mut store, b"nonempty", b"");

        assert_eq!(get(&mut store, b""), Reply::Bulk(Some(Bytes::new())));
        assert_eq!(
            get(&mut store, b"nonempty"),
            Reply::Bulk(Some(Bytes::new()))
        );
        assert_eq!(get(&mut store, b"absent"), Reply::Bulk(None));
    }

    #[test]
    fn binary_keys_and_values_keep_every_byte() {
        let mut store = Store::new();
        set(&mut store, b"\xff\0\r\n\x80", b"\0\xff\x80\r\n");
        set(&mut store, b"\xff", b"prefix only");

        assert_eq!(
            get(&mut store, b"\xff\0\r\n\x80"),
            Reply::Bulk(Some(Bytes::from_static(b"\0\xff\x80\r\n")))
        );
        assert_eq!(
            get(&mut store, b"\xff"),
            Reply::Bulk(Some(Bytes::from_static(b"prefix only")))
        );
        assert_eq!(get(&mut store, b"\xff\0"), Reply::Bulk(None));
    }

    #[test]
    fn set_overwrites_without_changing_other_keys() {
        let mut store = Store::new();
        set(&mut store, b"key", b"first");
        set(&mut store, b"other", b"retained");
        set(&mut store, b"key", b"second");

        assert_eq!(
            get(&mut store, b"key"),
            Reply::Bulk(Some(Bytes::from_static(b"second")))
        );
        assert_eq!(
            get(&mut store, b"other"),
            Reply::Bulk(Some(Bytes::from_static(b"retained")))
        );
        set(&mut store, b"key", b"");
        assert_eq!(get(&mut store, b"key"), Reply::Bulk(Some(Bytes::new())));
    }

    #[test]
    fn key_comparison_is_case_sensitive() {
        let mut store = Store::new();
        set(&mut store, b"key", b"lower");
        set(&mut store, b"KEY", b"upper");

        assert_eq!(
            get(&mut store, b"key"),
            Reply::Bulk(Some(Bytes::from_static(b"lower")))
        );
        assert_eq!(
            get(&mut store, b"KEY"),
            Reply::Bulk(Some(Bytes::from_static(b"upper")))
        );
        assert_eq!(get(&mut store, b"Key"), Reply::Bulk(None));
    }

    #[test]
    fn del_counts_only_actual_removals_and_preserves_other_keys() {
        let mut store = Store::new();
        set(&mut store, b"a", b"first");
        set(&mut store, b"b", b"second");
        set(&mut store, b"c", b"kept");

        let keys = ["a", "a", "missing", "b", "b", "missing"]
            .into_iter()
            .map(|key| Bytes::from_static(key.as_bytes()))
            .collect();
        assert_eq!(store.execute(Command::Del { keys }), Reply::Integer(2));
        assert_eq!(get(&mut store, b"a"), Reply::Bulk(None));
        assert_eq!(get(&mut store, b"b"), Reply::Bulk(None));
        assert_eq!(
            get(&mut store, b"c"),
            Reply::Bulk(Some(Bytes::from_static(b"kept")))
        );
        assert_eq!(
            store.execute(Command::Del {
                keys: vec![Bytes::from_static(b"a"), Bytes::from_static(b"missing")],
            }),
            Reply::Integer(0)
        );
    }

    #[test]
    fn del_accepts_empty_and_binary_keys_and_counts_empty_values() {
        let mut store = Store::new();
        set(&mut store, b"", b"");
        set(&mut store, b"\xff\0\r\n", b"");

        assert_eq!(
            store.execute(Command::Del {
                keys: vec![
                    Bytes::new(),
                    Bytes::from_static(b"\xff\0\r\n"),
                    Bytes::new(),
                ],
            }),
            Reply::Integer(2)
        );
        assert_eq!(get(&mut store, b""), Reply::Bulk(None));
        assert_eq!(get(&mut store, b"\xff\0\r\n"), Reply::Bulk(None));
    }

    #[test]
    fn directly_constructed_empty_del_is_a_noop() {
        let mut store = Store::new();
        set(&mut store, b"kept", b"value");

        assert_eq!(
            store.execute(Command::Del { keys: Vec::new() }),
            Reply::Integer(0)
        );
        assert_eq!(
            get(&mut store, b"kept"),
            Reply::Bulk(Some(Bytes::from_static(b"value")))
        );
    }

    #[test]
    fn get_shares_immutable_bytes_and_reply_survives_overwrite_and_removal() {
        let mut store = Store::new();
        let value = Bytes::from(vec![0xff, 0, b'\r', b'\n', 0x80]);
        let original_pointer = value.as_ptr();
        assert_eq!(
            store.execute(Command::Set {
                key: Bytes::from_static(b"key"),
                value,
            }),
            Reply::Ok
        );

        let Reply::Bulk(Some(first)) = get(&mut store, b"key") else {
            panic!("GET deve devolver o valor armazenado");
        };
        let Reply::Bulk(Some(second)) = get(&mut store, b"key") else {
            panic!("GET repetido deve devolver o mesmo conteúdo");
        };
        assert_eq!(first.as_ptr(), original_pointer);
        assert_eq!(second.as_ptr(), original_pointer);

        set(&mut store, b"key", b"replacement");
        assert_eq!(
            store.execute(Command::Del {
                keys: vec![Bytes::from_static(b"key")],
            }),
            Reply::Integer(1)
        );
        drop(store);
        assert_eq!(first.as_ref(), &[0xff, 0, b'\r', b'\n', 0x80]);
        assert_eq!(second, first);
    }

    #[test]
    fn pubsub_commands_cannot_execute_in_the_store() {
        let mut store = Store::new();
        for command in [
            Command::Subscribe {
                channels: vec![Bytes::new()],
            },
            Command::Unsubscribe { channels: vec![] },
            Command::Publish {
                channel: Bytes::new(),
                message: Bytes::new(),
            },
        ] {
            assert_eq!(
                store.execute(command),
                Reply::Error(ExecutionError::ConnectionOnly)
            );
        }
    }
}
