//! Preparation of a whole EXEC over touched keys, with a frozen clock.

use super::*;

struct TransactionClock {
    now: Instant,
    unix_ms: i64,
}
impl Clock for TransactionClock {
    fn now(&self) -> Instant {
        self.now
    }
    fn unix_millis(&self) -> i64 {
        self.unix_ms
    }
}

impl Store {
    /// Resolves the batch without changing the real Store. Individual errors do not interrupt other commands.
    /// All post-images form one record, including tombstones from creations undone in the batch.
    pub fn prepare_batch(&self, commands: Vec<Command>) -> Prepared {
        let mut keys = BTreeSet::new();
        for command in &commands {
            command.visit_keys(|key| {
                keys.insert(key.clone());
            });
        }
        let now = self.clock.now();
        let mut shadow = Self::with_config(
            self.config,
            Arc::new(TransactionClock {
                now,
                unix_ms: self.clock.unix_millis(),
            }),
        )
        .expect("configuration already validated");
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
        // The trace uses the same hook as real writes, but records separately from the original Store.
        let trace = shadow.watch(keys.iter().cloned().collect());
        // UNWATCH in MULTI runs only after the initial observation check.
        let replies = commands
            .into_iter()
            .map(|command| match command {
                Command::Unwatch => Reply::Ok,
                command => shadow.execute_inner(command),
            })
            .collect();
        let entries = trace
            .into_iter()
            .filter(|token| token.invalidated())
            .map(|token| (token.key().clone(), shadow.values.get(token.key()).cloned()))
            .collect();
        self.prepared(Reply::Array(replies), entries, MutationOrigin::Client)
    }
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
    fn transactions_prepare_resolves_one_batch_without_rollback_or_early_effects() {
        let mut store = Store::new();
        let prepared = store.prepare_batch(vec![
            set(b"a", b"bad"),
            Command::Incr {
                key: Bytes::from_static(b"a"),
            },
            set(b"b", b"2"),
        ]);
        assert_eq!(
            prepared.reply,
            Reply::Array(vec![
                Reply::Ok,
                Reply::Error(ExecutionError::InvalidInteger),
                Reply::Ok
            ])
        );
        assert_eq!(prepared.batch.mutations.len(), 2);
        assert!(store.is_empty());
        store.apply(prepared);
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn transactions_absent_created_deleted_key_still_invalidates_watch() {
        let mut store = Store::new();
        let tokens = store.watch(vec![Bytes::from_static(b"k")]);
        let prepared = store.prepare_batch(vec![
            set(b"k", b"1"),
            Command::Del {
                keys: vec![Bytes::from_static(b"k")],
            },
        ]);
        assert_eq!(
            prepared.batch.mutations,
            vec![Mutation::Delete {
                key: Bytes::from_static(b"k")
            }]
        );
        assert!(store.watches_valid(&tokens));
        store.apply(prepared);
        assert!(store.is_empty());
        assert!(!store.watches_valid(&tokens));
    }

    #[tokio::test(start_paused = true)]
    async fn transactions_watch_expiration_invalidates_before_active_or_passive_cleanup() {
        for cleanup in [0, 1, 2] {
            let mut store = Store::new();
            store.execute(Command::SetWithOptions {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                options: SetOptions {
                    expiry: SetExpiry::After(Duration::from_secs(1)),
                    ..SetOptions::default()
                },
            });
            let watched = store.watch(vec![Bytes::from_static(b"k")]);
            assert!(store.watches_valid(&watched));
            tokio::time::advance(Duration::from_secs(1)).await;
            if cleanup == 1 {
                let prepared = store.prepare_expiration(64);
                store.apply(prepared);
            } else if cleanup == 2 {
                store.execute(Command::Get {
                    key: Bytes::from_static(b"k"),
                });
            }
            assert!(!store.watches_valid(&watched));
            drop(watched);
            // A new observation starts after clearing the expired tombstone.
            store.execute(Command::Get {
                key: Bytes::from_static(b"k"),
            });
            assert!(store.watches_valid(&store.watch(vec![Bytes::from_static(b"k")])));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn transactions_read_expiration_is_recorded_but_failed_command_has_no_phantom_write() {
        let mut store = Store::new();
        store.execute(Command::SetWithOptions {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            options: SetOptions {
                expiry: SetExpiry::After(Duration::from_secs(1)),
                ..SetOptions::default()
            },
        });
        tokio::time::advance(Duration::from_secs(1)).await;
        let failed = store.prepare_batch(vec![Command::SetWithOptions {
            key: Bytes::from_static(b"k"),
            value: Bytes::new(),
            options: SetOptions {
                expiry: SetExpiry::After(Duration::ZERO),
                ..SetOptions::default()
            },
        }]);
        assert!(
            matches!(failed.reply, Reply::Array(ref replies) if matches!(replies[0], Reply::Error(_)))
        );
        assert!(failed.batch.mutations.is_empty());
        let read = store.prepare_batch(vec![Command::Get {
            key: Bytes::from_static(b"k"),
        }]);
        assert_eq!(read.reply, Reply::Array(vec![Reply::Bulk(None)]));
        assert_eq!(
            read.batch.mutations,
            vec![Mutation::Delete {
                key: Bytes::from_static(b"k")
            }]
        );
    }
}
