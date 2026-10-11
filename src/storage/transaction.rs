//! Preparation of a whole EXEC over touched keys, with a frozen clock.

use super::*;

/// One execution instant shared by WATCH validation and all owner preparations.
/// Capture it after acquiring transaction admission; retain it across writer waits.
#[derive(Clone, Copy, Debug)]
pub struct TransactionTime {
    now: Instant,
    unix_ms: i64,
}
impl Clock for TransactionTime {
    fn now(&self) -> Instant {
        self.now
    }
    fn unix_millis(&self) -> i64 {
        self.unix_ms
    }
}

/// Tentative state remains owned by the worker. Each command first produces a
/// candidate; dropping it rejects that command without undoing earlier commands.
/// Only accepted candidates affect the final postimages and WATCH invalidations.
pub(crate) struct OwnerTransaction {
    shadow: Store,
    trace: Vec<WatchToken>,
}

impl OwnerTransaction {
    pub(crate) fn prepare_candidate(&self, command: Command) -> Prepared {
        self.shadow.prepare(command)
    }

    pub(crate) fn accept_candidate(&mut self, candidate: Prepared) -> Reply {
        self.shadow.apply(candidate)
    }

    pub(crate) fn seal(self, owner: &Store, replies: Vec<Reply>) -> Prepared {
        let entries = self
            .trace
            .into_iter()
            .filter(|token| token.invalidated())
            .map(|token| {
                (
                    token.key().clone(),
                    self.shadow.values.get(token.key()).cloned(),
                )
            })
            .collect();
        owner.prepared(Reply::Array(replies), entries, MutationOrigin::Client)
    }
}

impl Store {
    pub fn transaction_time(&self) -> TransactionTime {
        TransactionTime {
            now: self.clock.now(),
            unix_ms: self.clock.unix_millis(),
        }
    }

    /// Resolves the batch without changing the real Store. Individual errors do not interrupt other commands.
    /// All post-images form one record, including tombstones from creations undone in the batch.
    pub fn prepare_batch(&self, commands: Vec<Command>) -> Prepared {
        self.prepare_batch_at(commands, self.transaction_time())
    }

    /// Resolves an owner's commands at a coordinator-supplied execution instant.
    /// The caller must exclude mutations from capture through application.
    /// This prepares no durable decision and does not enable mixed-owner EXEC.
    pub fn prepare_batch_at(&self, commands: Vec<Command>, time: TransactionTime) -> Prepared {
        // Version 1 memory commands cannot enter EXEC, including native callers.
        if commands
            .iter()
            .any(|command| matches!(command, Command::Memory { .. }))
        {
            return self.prepared(
                Reply::Error(ExecutionError::Memory(
                    "ERR memory commands are not supported in MULTI",
                )),
                Vec::new(),
                MutationOrigin::Client,
            );
        }
        let mut keys = BTreeSet::new();
        for command in &commands {
            command.visit_keys(|key| {
                keys.insert(key.clone());
            });
        }
        let mut tentative = self.begin_owner_transaction(keys, time);
        let replies = commands
            .into_iter()
            .map(|command| match command {
                Command::Unwatch => Reply::Ok,
                command => {
                    let candidate = tentative.prepare_candidate(command);
                    tentative.accept_candidate(candidate)
                }
            })
            .collect();
        tentative.seal(self, replies)
    }

    pub(crate) fn begin_owner_transaction(
        &self,
        keys: BTreeSet<Bytes>,
        time: TransactionTime,
    ) -> OwnerTransaction {
        let mut shadow = Self::with_config(self.config, Arc::new(time))
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
                for (id, deadline) in &entry.member_deadlines {
                    shadow
                        .member_expirations
                        .insert((*deadline, key.clone(), id.clone()));
                }
            }
        }
        // The trace uses the same hook as real writes, but records separately from the original Store.
        let trace = shadow.watch(keys.iter().cloned().collect());
        OwnerTransaction { shadow, trace }
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
    fn transactions_rejected_multikey_candidates_preserve_prior_and_later_owner_steps() {
        let mut first = Store::with_config(
            StoreConfig {
                max_dataset_bytes: 200,
            },
            Arc::new(SystemClock),
        )
        .unwrap();
        let mut second = Store::with_config(
            StoreConfig {
                max_dataset_bytes: 130,
            },
            Arc::new(SystemClock),
        )
        .unwrap();
        let first_watch = first.watch(vec![Bytes::from_static(b"a")]);
        let second_watch = second.watch(vec![Bytes::from_static(b"b")]);
        let time = first.transaction_time();
        let mut left = first.begin_owner_transaction([Bytes::from_static(b"a")].into(), time);
        let mut right = second.begin_owner_transaction([Bytes::from_static(b"b")].into(), time);

        let earlier = left.prepare_candidate(set(b"a", b"1"));
        assert_eq!(left.accept_candidate(earlier), Reply::Ok);
        // One MSET is prepared on both owners, but the last owner's quota error
        // rejects every candidate from that command, including the valid first one.
        let a = left.prepare_candidate(Command::MSet {
            entries: vec![(Bytes::from_static(b"a"), Bytes::from_static(b"changed"))],
        });
        let b = right.prepare_candidate(Command::MSet {
            entries: vec![(Bytes::from_static(b"b"), Bytes::from_static(b"too-large"))],
        });
        assert_eq!(a.reply, Reply::Ok);
        assert_eq!(b.reply, Reply::Error(ExecutionError::OutOfMemory));
        drop((a, b));
        let later = right.prepare_candidate(set(b"b", b"2"));
        assert_eq!(right.accept_candidate(later), Reply::Ok);
        let observed = left.prepare_candidate(Command::Get {
            key: Bytes::from_static(b"a"),
        });
        assert_eq!(observed.reply, Reply::Bulk(Some(Bytes::from_static(b"1"))));
        left.accept_candidate(observed);

        assert!(first.is_empty() && second.is_empty());
        assert!(first.watches_valid(&first_watch));
        assert!(second.watches_valid(&second_watch));
        let a = left.seal(&first, vec![]);
        let b = right.seal(&second, vec![]);
        assert_eq!(a.batch.mutations.len(), 1);
        assert_eq!(b.batch.mutations.len(), 1);
        first.apply(a);
        second.apply(b);
        assert_eq!(
            first.execute(Command::Get {
                key: Bytes::from_static(b"a")
            }),
            Reply::Bulk(Some(Bytes::from_static(b"1")))
        );
        assert_eq!(
            second.execute(Command::Get {
                key: Bytes::from_static(b"b")
            }),
            Reply::Bulk(Some(Bytes::from_static(b"2")))
        );
        assert!(!first.watches_valid(&first_watch));
        assert!(!second.watches_valid(&second_watch));
    }

    #[tokio::test(start_paused = true)]
    async fn transactions_owner_preparations_share_one_deadline_despite_clock_drift() {
        let now = Instant::now();
        let time = TransactionTime { now, unix_ms: 1000 };
        let mut first = Store::with_clock(Arc::new(TransactionTime { now, unix_ms: 9000 }));
        let mut second = Store::with_clock(Arc::new(TransactionTime {
            now: now + Duration::from_secs(10),
            unix_ms: 19000,
        }));
        let command = |key| Command::SetWithOptions {
            key: Bytes::from_static(key),
            value: Bytes::from_static(b"v"),
            options: SetOptions {
                expiry: SetExpiry::After(Duration::from_millis(20)),
                ..SetOptions::default()
            },
        };
        let a = first.prepare_batch_at(vec![command(b"a")], time);
        tokio::time::advance(Duration::from_secs(1)).await;
        let b = second.prepare_batch_at(vec![command(b"b")], time);
        for prepared in [&a, &b] {
            assert!(matches!(
                prepared.batch.mutations.as_slice(),
                [Mutation::Put {
                    expires_at_unix_ms: Some(1020),
                    ..
                }]
            ));
        }
        assert!(first.is_empty() && second.is_empty());
        first.apply(a);
        second.apply(b);
        for (store, key) in [(&first, b"a".as_slice()), (&second, b"b".as_slice())] {
            assert_eq!(
                store.values.get(key).unwrap().expires_at,
                Some(now + Duration::from_millis(20))
            );
        }
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn transactions_watch_and_preparation_use_the_same_expiration_cut() {
        let now = Instant::now();
        let mut store = Store::with_clock(Arc::new(TransactionTime { now, unix_ms: 1000 }));
        store.execute(Command::SetWithOptions {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            options: SetOptions {
                expiry: SetExpiry::After(Duration::from_millis(10)),
                ..SetOptions::default()
            },
        });
        let tokens = store.watch(vec![Bytes::from_static(b"k")]);
        let before = store.transaction_time();
        store.clock = Arc::new(TransactionTime {
            now: now + Duration::from_millis(10),
            unix_ms: 1010,
        });
        assert!(store.watches_valid_at(&tokens, before.now()));
        let prepared = store.prepare_batch_at(
            vec![Command::Get {
                key: Bytes::from_static(b"k"),
            }],
            before,
        );
        assert_eq!(
            prepared.reply,
            Reply::Array(vec![Reply::Bulk(Some(Bytes::from_static(b"v")))])
        );
        assert!(prepared.batch.mutations.is_empty());
        assert!(!store.watches_valid_at(&tokens, store.transaction_time().now()));
        assert_eq!(store.len(), 1);
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
