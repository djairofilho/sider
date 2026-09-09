//! Barreira, readonly, TTL e publicação durável com workers e AOF reais.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use sider::command::{Command, ExecutionError, Reply};
use sider::persistence::{self, AofConfig, ReplicationMetadata, Role};
use sider::replication::{Cursor, Error, journal, state::Runtime};
use sider::storage::{
    Clock, Mutation, MutationOrigin, ResolvedBatch, Store, StoreConfig,
    replication::Context,
    worker::{self, DbHandle},
};
use tokio::sync::watch;

struct ManualClock {
    now: tokio::time::Instant,
    elapsed: AtomicU64,
}
impl Clock for ManualClock {
    fn now(&self) -> tokio::time::Instant {
        self.now + Duration::from_millis(self.elapsed.load(Ordering::SeqCst))
    }
    fn unix_millis(&self) -> i64 {
        1000 + self.elapsed.load(Ordering::SeqCst) as i64
    }
}

struct Harness {
    database: DbHandle,
    context: Context,
    clock: Arc<ManualClock>,
    stop: watch::Sender<bool>,
    workers: tokio::task::JoinSet<()>,
    writer: tokio::task::JoinHandle<Result<(), persistence::AofError>>,
    directory: PathBuf,
}

impl Harness {
    async fn new(role: Role) -> Self {
        Self::with_faults(role, Arc::new(persistence::NoFaults)).await
    }

    async fn with_faults(role: Role, faults: Arc<dyn persistence::FaultInjector>) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "sider-repl-workers-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let clock = Arc::new(ManualClock {
            now: tokio::time::Instant::now(),
            elapsed: AtomicU64::new(0),
        });
        let mut config = AofConfig::new(directory.clone());
        config.compact_after_bytes = 0;
        config.layout.shard_count = 4;
        let layout = config.layout;
        let store_config = StoreConfig {
            max_dataset_bytes: 16384,
        };
        let recovered =
            persistence::recover_with_faults(config, store_config, clock.clone(), faults).unwrap();
        let (_, aof, writer) = recovered.start();
        let cursor = Cursor {
            epoch: [0; 16],
            sequence: 0,
        };
        aof.install_snapshot(
            Vec::new(),
            0,
            ReplicationMetadata {
                role: Role::Replica,
                epoch: cursor.epoch,
            },
        )
        .await
        .unwrap();
        let runtime = Runtime::new(role, cursor);
        let stores = (0..4)
            .map(|_| {
                Store::with_config(
                    StoreConfig {
                        max_dataset_bytes: 4096,
                    },
                    clock.clone(),
                )
                .unwrap()
            })
            .collect();
        let (stop, shutdown) = watch::channel(false);
        let (database, owners) =
            worker::channel_with_stores(8, Duration::from_secs(5), shutdown, stores).unwrap();
        let database = database.with_replication(runtime.clone());
        let mut workers = tokio::task::JoinSet::new();
        for worker in owners {
            workers.spawn(
                worker
                    .with_aof(aof.clone(), 0)
                    .with_replication(runtime.clone())
                    .run(),
            );
        }
        let context = Context {
            aof,
            runtime,
            layout,
            store_config,
            clock: clock.clone(),
            journal_limits: journal::Limits {
                max_bytes: 32768,
                max_batches: 128,
                max_frame_bytes: 32768,
            },
            aof_limits: persistence::format::Limits::default(),
        };
        if role == Role::Primary {
            database
                .promote_replica(context.clone(), [1; 16])
                .await
                .unwrap();
        }
        Self {
            database,
            context,
            clock,
            stop,
            workers,
            writer,
            directory,
        }
    }

    async fn finish(mut self) {
        self.stop.send_replace(true);
        while let Some(result) = self.workers.join_next().await {
            result.unwrap();
        }
        drop(self.database);
        drop(self.context);
        self.writer.await.unwrap().unwrap();
        std::fs::remove_dir_all(self.directory).unwrap();
    }
}

struct InstallGate {
    armed: std::sync::atomic::AtomicBool,
    entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    released: std::sync::Mutex<bool>,
    wake: std::sync::Condvar,
}
impl InstallGate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}
impl persistence::FaultInjector for InstallGate {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if point == "replication_before_publish" && self.armed.swap(false, Ordering::SeqCst) {
            if let Some(entered) = self.entered.lock().unwrap().take() {
                let _ = entered.send(());
            }
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.wake.wait(released).unwrap();
            }
        }
        Ok(())
    }
}
struct ReleaseGate(Arc<InstallGate>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test]
async fn cancelled_snapshot_caller_does_not_release_barrier_before_all_stores_are_installed() {
    use std::future::{Future, poll_fn};
    use std::task::Poll;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let gate = Arc::new(InstallGate {
        armed: std::sync::atomic::AtomicBool::new(false),
        entered: std::sync::Mutex::new(Some(entered)),
        released: std::sync::Mutex::new(false),
        wake: std::sync::Condvar::new(),
    });
    let _release_on_failure = ReleaseGate(gate.clone());
    let replica = Harness::with_faults(Role::Replica, gate.clone()).await;
    let generation = replica.context.runtime.begin_session();
    let database = replica.database.clone();
    let context = replica.context.clone();
    gate.armed.store(true, Ordering::SeqCst);
    let installing = tokio::spawn(async move {
        let image = (0..4)
            .map(|shard| Mutation::Put {
                key: Bytes::from(format!("{{{shard}}}:key")),
                value: Bytes::from_static(b"complete").into(),
                expires_at_unix_ms: None,
            })
            .collect();
        database
            .install_replica(
                context,
                generation,
                Cursor {
                    epoch: [5; 16],
                    sequence: 40,
                },
                image,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entering)
        .await
        .unwrap()
        .unwrap();
    installing.abort();
    assert!(installing.await.unwrap_err().is_cancelled());
    let mut observing = Box::pin(replica.database.snapshot(None));
    poll_fn(|context| {
        assert!(observing.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    gate.release();
    let snapshot = observing.await.unwrap();
    assert_eq!(snapshot.mutations.len(), 4);
    assert_eq!(replica.context.runtime.status().applied.sequence, 40);
    assert_eq!(replica.context.aof.flush().await.unwrap(), 40);
    replica.finish().await;
}

fn put(key: &'static [u8], value: &'static [u8]) -> Mutation {
    Mutation::Put {
        key: Bytes::from_static(key),
        value: Bytes::from_static(value).into(),
        expires_at_unix_ms: None,
    }
}

#[tokio::test]
async fn replica_reads_hide_expired_values_without_local_aof_writes_and_reject_client_batches() {
    let replica = Harness::new(Role::Replica).await;
    let generation = replica.context.runtime.begin_session();
    let cursor = Cursor {
        epoch: [4; 16],
        sequence: 20,
    };
    let mut expiring = put(b"{a}:ttl", b"value");
    if let Mutation::Put {
        expires_at_unix_ms, ..
    } = &mut expiring
    {
        *expires_at_unix_ms = Some(1010);
    }
    replica
        .database
        .install_replica(replica.context.clone(), generation, cursor, vec![expiring])
        .await
        .unwrap();
    assert_eq!(
        replica
            .database
            .execute(Command::Set {
                key: Bytes::from_static(b"{a}:ttl"),
                value: Bytes::new()
            })
            .await
            .unwrap(),
        Reply::Error(ExecutionError::ReadOnly)
    );
    assert_eq!(
        replica
            .database
            .execute_batch(
                vec![Command::Del {
                    keys: vec![Bytes::from_static(b"{a}:ttl")]
                }],
                Vec::new()
            )
            .await
            .unwrap(),
        Reply::Error(ExecutionError::ReadOnly)
    );
    assert_eq!(
        replica
            .database
            .execute(Command::Get {
                key: Bytes::from_static(b"{a}:ttl")
            })
            .await
            .unwrap(),
        Reply::Bulk(Some(Bytes::from_static(b"value")))
    );
    replica.clock.elapsed.store(20, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert_eq!(
        replica
            .database
            .execute(Command::Get {
                key: Bytes::from_static(b"{a}:ttl")
            })
            .await
            .unwrap(),
        Reply::Bulk(None)
    );
    assert_eq!(
        replica
            .database
            .snapshot(Some(&replica.context.aof))
            .await
            .unwrap()
            .mutations
            .len(),
        1
    );
    assert_eq!(replica.context.aof.flush().await.unwrap(), 20);
    replica
        .database
        .apply_replica(
            replica.context.clone(),
            generation,
            Cursor {
                sequence: 21,
                ..cursor
            },
            ResolvedBatch {
                origin: MutationOrigin::Expiration,
                mutations: vec![Mutation::Delete {
                    key: Bytes::from_static(b"{a}:ttl"),
                }],
            },
        )
        .await
        .unwrap();
    assert!(
        replica
            .database
            .snapshot(None)
            .await
            .unwrap()
            .mutations
            .is_empty()
    );
    assert_eq!(replica.context.aof.flush().await.unwrap(), 21);
    replica.finish().await;
}

#[tokio::test]
async fn global_snapshot_plus_subscription_has_no_gap_while_independent_shards_write() {
    let primary = Harness::new(Role::Primary).await;
    let mut writers = tokio::task::JoinSet::new();
    for tag in 0..4 {
        let database = primary.database.clone();
        writers.spawn(async move {
            for number in 0..12 {
                let value = Bytes::from(number.to_string());
                let command = Command::MSet {
                    entries: vec![
                        (Bytes::from(format!("{{{tag}}}:a")), value.clone()),
                        (Bytes::from(format!("{{{tag}}}:b")), value),
                    ],
                };
                assert_eq!(database.execute(command).await.unwrap(), Reply::Ok);
            }
        });
    }
    let mut captured = primary
        .database
        .replication_snapshot(&primary.context, true)
        .await
        .unwrap();
    let mut copy = Store::with_clock(primary.clock.clone());
    copy.replay(&captured.mutations).unwrap();
    while let Some(result) = writers.join_next().await {
        result.unwrap();
    }
    let final_snapshot = primary
        .database
        .snapshot(Some(&primary.context.aof))
        .await
        .unwrap();
    let mut sequence = captured.cursor.sequence;
    while sequence < final_snapshot.sequence.unwrap() {
        let entry = captured
            .subscription
            .as_mut()
            .unwrap()
            .next()
            .await
            .unwrap();
        assert_eq!(entry.cursor.sequence, sequence + 1);
        let sider::replication::protocol::Message::Batch { batch, .. } =
            sider::replication::protocol::decode(entry.frame, Default::default()).unwrap()
        else {
            panic!("lote ausente")
        };
        assert_eq!(batch.mutations.len(), 2);
        copy.replay(&batch.mutations).unwrap();
        sequence += 1;
    }
    assert_eq!(copy.snapshot(), final_snapshot.mutations);
    assert_eq!(copy.len(), 8);
    primary.finish().await;
}

#[tokio::test]
async fn stale_sessions_gaps_duplicate_keys_and_cross_shard_batches_cannot_mutate_replica() {
    let replica = Harness::new(Role::Replica).await;
    let generation = replica.context.runtime.begin_session();
    let cursor = Cursor {
        epoch: [4; 16],
        sequence: 20,
    };
    replica
        .database
        .install_replica(
            replica.context.clone(),
            generation,
            cursor,
            vec![put(b"{a}:1", b"old")],
        )
        .await
        .unwrap();
    let valid = ResolvedBatch {
        origin: MutationOrigin::Client,
        mutations: vec![put(b"{a}:1", b"new"), put(b"{a}:2", b"new")],
    };
    assert!(matches!(
        replica
            .database
            .apply_replica(
                replica.context.clone(),
                generation,
                Cursor {
                    sequence: 22,
                    ..cursor
                },
                valid.clone()
            )
            .await,
        Err(Error::Sequence)
    ));
    let duplicate = ResolvedBatch {
        origin: MutationOrigin::Client,
        mutations: vec![put(b"{a}:1", b"x"), put(b"{a}:1", b"y")],
    };
    assert!(matches!(
        replica
            .database
            .apply_replica(
                replica.context.clone(),
                generation,
                Cursor {
                    sequence: 21,
                    ..cursor
                },
                duplicate
            )
            .await,
        Err(Error::Replay(_))
    ));
    let cross = ResolvedBatch {
        origin: MutationOrigin::Client,
        mutations: vec![put(b"a", b"x"), put(b"b", b"y")],
    };
    assert!(matches!(
        replica
            .database
            .apply_replica(
                replica.context.clone(),
                generation,
                Cursor {
                    sequence: 21,
                    ..cursor
                },
                cross
            )
            .await,
        Err(Error::Sequence)
    ));
    assert_eq!(replica.context.aof.flush().await.unwrap(), 20);
    replica
        .database
        .apply_replica(
            replica.context.clone(),
            generation,
            Cursor {
                sequence: 21,
                ..cursor
            },
            valid,
        )
        .await
        .unwrap();
    let promoted = replica
        .database
        .promote_replica(replica.context.clone(), [8; 16])
        .await
        .unwrap();
    assert_eq!(promoted.sequence, 21);
    assert!(!replica.database.readonly());
    assert!(!replica.context.runtime.accepts(generation));
    assert!(matches!(
        replica
            .database
            .install_replica(replica.context.clone(), generation, cursor, Vec::new())
            .await,
        Err(Error::Stale)
    ));
    assert_eq!(
        replica
            .database
            .execute(Command::Set {
                key: Bytes::from_static(b"after"),
                value: Bytes::from_static(b"promotion")
            })
            .await
            .unwrap(),
        Reply::Ok
    );
    assert_eq!(replica.context.aof.flush().await.unwrap(), 22);
    replica.finish().await;
}

#[tokio::test]
async fn repeated_full_installations_never_expose_mixed_shard_generations() {
    let replica = Harness::new(Role::Replica).await;
    let generation = replica.context.runtime.begin_session();
    let database = replica.database.clone();
    let observer = tokio::spawn(async move {
        for _ in 0..24 {
            let image = database.snapshot(None).await.unwrap();
            if !image.mutations.is_empty() {
                assert_eq!(image.mutations.len(), 4);
                let values: Vec<_> = image
                    .mutations
                    .iter()
                    .map(|mutation| match mutation {
                        Mutation::Put { value, .. } => value,
                        _ => panic!("snapshot com remoção"),
                    })
                    .collect();
                assert!(values.iter().all(|value| *value == values[0]));
            }
            tokio::task::yield_now().await;
        }
    });
    for sequence in 1..=8 {
        let mutations = (0..4)
            .map(|index| Mutation::Put {
                key: Bytes::from(format!("{{{index}}}:key")),
                value: Bytes::from(sequence.to_string()).into(),
                expires_at_unix_ms: None,
            })
            .collect();
        replica
            .database
            .install_replica(
                replica.context.clone(),
                generation,
                Cursor {
                    epoch: [6; 16],
                    sequence,
                },
                mutations,
            )
            .await
            .unwrap();
    }
    observer.await.unwrap();
    assert_eq!(replica.context.aof.flush().await.unwrap(), 8);
    replica.finish().await;
}
