//! Full sync and whole-batch apply proof for the internal composed replica.

use std::future::{Future, poll_fn};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::watch;

use super::*;
use crate::command::{Command, Reply};
use crate::persistence::{self, AofConfig, FaultInjector, NoFaults};
use crate::storage::{MutationOrigin, SystemClock, Value, worker};

const EPOCH: [u8; 16] = [3; 16];
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct Harness {
    directory: PathBuf,
    config: AofConfig,
    quota: StoreConfig,
    db: DbHandle,
    context: Context,
    generation: u64,
    stop: watch::Sender<bool>,
    workers: Vec<tokio::task::JoinHandle<()>>,
    writer: tokio::task::JoinHandle<Result<(), persistence::AofError>>,
}

impl Harness {
    async fn new(quota_bytes: usize, faults: Arc<dyn FaultInjector>) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "sider-composed-replica-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&directory).unwrap();
        let mut config = AofConfig::new(directory.clone());
        config.layout.shard_count = 2;
        let quota = StoreConfig {
            max_dataset_bytes: quota_bytes,
        };
        let recovered = persistence::recover_composed_legacy_mixed(
            config.clone(),
            quota,
            Arc::new(SystemClock),
            faults,
        )
        .unwrap();
        let (_empty, aof, writer) = recovered.start();
        aof.install_snapshot(
            Vec::new(),
            0,
            ReplicationMetadata {
                role: Role::Replica,
                epoch: EPOCH,
            },
        )
        .await
        .unwrap();
        let cursor = Cursor {
            epoch: EPOCH,
            sequence: 0,
        };
        let runtime = Runtime::new(Role::Replica, cursor);
        let generation = runtime.begin_session();
        let stores = (0..2)
            .map(|owner| {
                Store::with_config(
                    StoreConfig {
                        max_dataset_bytes: config.layout.quota(quota_bytes, owner).unwrap(),
                    },
                    Arc::new(SystemClock),
                )
                .unwrap()
            })
            .collect();
        let (stop, shutdown) = watch::channel(false);
        let (db, owners) =
            worker::channel_with_stores(4, Duration::from_secs(5), shutdown, stores).unwrap();
        let db = db.with_replication(runtime.clone());
        let workers = owners
            .into_iter()
            .map(|owner| {
                tokio::spawn(
                    owner
                        .with_aof(aof.clone(), 0)
                        .with_replication(runtime.clone())
                        .run(),
                )
            })
            .collect();
        let context = Context {
            aof,
            runtime,
            layout: config.layout,
            store_config: quota,
            clock: Arc::new(SystemClock),
            journal_limits: Limits {
                max_bytes: 4096,
                max_batches: 16,
                max_frame_bytes: 1024,
            },
            aof_limits: persistence::format::Limits::default(),
        };
        Self {
            directory,
            config,
            quota,
            db,
            context,
            generation,
            stop,
            workers,
            writer,
        }
    }

    async fn stop(
        self,
    ) -> (
        PathBuf,
        AofConfig,
        StoreConfig,
        Result<(), persistence::AofError>,
    ) {
        self.stop.send_replace(true);
        for worker in self.workers {
            worker.await.unwrap();
        }
        drop(self.db);
        drop(self.context);
        let writer = self.writer.await.unwrap();
        (self.directory, self.config, self.quota, writer)
    }
}

fn batch(left: &[u8], right: &[u8]) -> ResolvedBatch {
    ResolvedBatch {
        origin: MutationOrigin::Client,
        mutations: [(b"a".as_slice(), left), (b"b".as_slice(), right)]
            .into_iter()
            .map(|(key, value)| Mutation::Put {
                key: Bytes::copy_from_slice(key),
                value: Value::String(Bytes::copy_from_slice(value)),
                expires_at_unix_ms: None,
            })
            .collect(),
    }
}

fn get(store: &mut Store, key: &'static [u8]) -> Reply {
    store.execute(Command::Get {
        key: Bytes::from_static(key),
    })
}

fn cleanup(directory: &Path) {
    let parent = std::env::temp_dir().canonicalize().unwrap();
    let target = directory.canonicalize().unwrap();
    assert_eq!(target.parent(), Some(parent.as_path()));
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("sider-composed-replica-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[tokio::test]
async fn full_sync_then_one_mixed_batch_advances_cursor_after_both_owners() {
    let harness = Harness::new(4096, Arc::new(NoFaults)).await;
    let installed = Cursor {
        epoch: EPOCH,
        sequence: 4,
    };
    harness
        .db
        .install_replica(
            harness.context.clone(),
            harness.generation,
            installed,
            batch(b"old-left", b"old-right").mutations,
        )
        .await
        .unwrap();
    assert_eq!(harness.context.runtime.status().applied, installed);
    let cursor = Cursor {
        epoch: EPOCH,
        sequence: 5,
    };
    harness
        .db
        .apply_replica(
            harness.context.clone(),
            harness.generation,
            cursor,
            batch(b"left", b"right"),
        )
        .await
        .unwrap();
    assert_eq!(harness.context.runtime.status().applied, cursor);
    assert_eq!(harness.context.aof.status().await.unwrap().0, 5);
    let snapshot = harness
        .db
        .snapshot(Some(&harness.context.aof))
        .await
        .unwrap();
    assert_eq!(snapshot.mutations.len(), 2);
    assert_eq!(snapshot.sequence, Some(5));
    let (directory, config, quota, writer) = harness.stop().await;
    writer.unwrap();
    assert!(persistence::recover(config.clone(), quota, Arc::new(SystemClock)).is_err());
    let restarted = persistence::recover_composed_legacy_mixed(
        config,
        quota,
        Arc::new(SystemClock),
        Arc::new(NoFaults),
    )
    .unwrap();
    assert_eq!(restarted.metadata.sequence, 5);
    assert_eq!(
        restarted.metadata.replication,
        Some(ReplicationMetadata {
            role: Role::Replica,
            epoch: EPOCH,
        })
    );
    let mut store = restarted.store;
    assert_eq!(
        get(&mut store, b"a"),
        Reply::Bulk(Some(Bytes::from_static(b"left")))
    );
    assert_eq!(
        get(&mut store, b"b"),
        Reply::Bulk(Some(Bytes::from_static(b"right")))
    );
    drop(store);
    cleanup(&directory);
}

#[tokio::test]
async fn last_owner_quota_rejects_before_append_and_keeps_replica_available() {
    let harness = Harness::new(512, Arc::new(NoFaults)).await;
    let cursor = Cursor {
        epoch: EPOCH,
        sequence: 1,
    };
    assert!(matches!(
        harness
            .db
            .apply_replica(
                harness.context.clone(),
                harness.generation,
                cursor,
                batch(b"small", &[b'x'; 200]),
            )
            .await,
        Err(Error::Replay(_))
    ));
    assert_eq!(harness.context.aof.status().await.unwrap().0, 0);
    assert_eq!(harness.context.runtime.status().applied.sequence, 0);
    assert_eq!(harness.db.check_admission(), Ok(()));
    harness
        .db
        .apply_replica(
            harness.context.clone(),
            harness.generation,
            cursor,
            batch(b"small", b"small"),
        )
        .await
        .unwrap();
    let (directory, config, quota, writer) = harness.stop().await;
    writer.unwrap();
    let restarted = persistence::recover_composed_legacy_mixed(
        config,
        quota,
        Arc::new(SystemClock),
        Arc::new(NoFaults),
    )
    .unwrap();
    assert_eq!(restarted.metadata.sequence, 1);
    assert_eq!(restarted.store.len(), 2);
    drop(restarted);
    cleanup(&directory);
}

struct FailAfterFirstApply;
impl FaultInjector for FailAfterFirstApply {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if point == "replica_mixed_after_first_apply" {
            Err(std::io::Error::other("injected after-first-apply failure"))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn failed_second_owner_publication_closes_admission_and_restart_restores_both() {
    let harness = Harness::new(4096, Arc::new(FailAfterFirstApply)).await;
    let cursor = Cursor {
        epoch: EPOCH,
        sequence: 1,
    };
    assert!(matches!(
        harness
            .db
            .apply_replica(
                harness.context.clone(),
                harness.generation,
                cursor,
                batch(b"left", b"right"),
            )
            .await,
        Err(Error::Persistence(_))
    ));
    assert_eq!(harness.db.check_admission(), Err(DbError::Unavailable));
    assert_eq!(harness.context.runtime.status().applied.sequence, 0);
    assert_eq!(harness.context.aof.status().await.unwrap().0, 1);
    let (directory, config, quota, writer) = harness.stop().await;
    writer.unwrap();
    let restarted = persistence::recover_composed_legacy_mixed(
        config,
        quota,
        Arc::new(SystemClock),
        Arc::new(NoFaults),
    )
    .unwrap();
    assert_eq!(restarted.metadata.sequence, 1);
    let mut store = restarted.store;
    assert_eq!(
        get(&mut store, b"a"),
        Reply::Bulk(Some(Bytes::from_static(b"left")))
    );
    assert_eq!(
        get(&mut store, b"b"),
        Reply::Bulk(Some(Bytes::from_static(b"right")))
    );
    drop(store);
    cleanup(&directory);
}

struct FailAfterAppend;
impl FaultInjector for FailAfterAppend {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if point == "after_append" {
            Err(std::io::Error::other("injected after-append failure"))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn uncertain_replica_append_closes_admission_without_advancing_cursor() {
    let harness = Harness::new(4096, Arc::new(FailAfterAppend)).await;
    assert!(matches!(
        harness
            .db
            .apply_replica(
                harness.context.clone(),
                harness.generation,
                Cursor {
                    epoch: EPOCH,
                    sequence: 1,
                },
                batch(b"left", b"right"),
            )
            .await,
        Err(Error::Persistence(_))
    ));
    assert_eq!(harness.db.check_admission(), Err(DbError::Unavailable));
    assert_eq!(harness.context.runtime.status().applied.sequence, 0);
    let (directory, config, quota, writer) = harness.stop().await;
    assert!(writer.is_err());
    let restarted = persistence::recover_composed_legacy_mixed(
        config,
        quota,
        Arc::new(SystemClock),
        Arc::new(NoFaults),
    )
    .unwrap();
    assert_eq!(restarted.metadata.sequence, 1);
    let mut store = restarted.store;
    assert_eq!(
        get(&mut store, b"a"),
        Reply::Bulk(Some(Bytes::from_static(b"left")))
    );
    assert_eq!(
        get(&mut store, b"b"),
        Reply::Bulk(Some(Bytes::from_static(b"right")))
    );
    drop(store);
    cleanup(&directory);
}

struct PauseBeforeApply {
    reached: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl FaultInjector for PauseBeforeApply {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if point == "replica_mixed_before_first_apply" {
            self.reached
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(())
                .unwrap();
            self.resume.lock().unwrap().recv().unwrap();
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnected_caller_cannot_cancel_durable_replica_publication() {
    let (reached, arrived) = std::sync::mpsc::channel();
    let (resume, resumed) = std::sync::mpsc::channel();
    let harness = Harness::new(
        4096,
        Arc::new(PauseBeforeApply {
            reached: std::sync::Mutex::new(Some(reached)),
            resume: std::sync::Mutex::new(resumed),
        }),
    )
    .await;
    let db = harness.db.clone();
    let context = harness.context.clone();
    let generation = harness.generation;
    let client = tokio::spawn(async move {
        db.apply_replica(
            context,
            generation,
            Cursor {
                epoch: EPOCH,
                sequence: 1,
            },
            batch(b"left", b"right"),
        )
        .await
    });
    tokio::task::spawn_blocking(move || arrived.recv_timeout(Duration::from_secs(5)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(harness.context.aof.status().await.unwrap().0, 1);
    assert_eq!(harness.context.runtime.status().applied.sequence, 0);
    client.abort();
    assert!(client.await.unwrap_err().is_cancelled());
    let mut snapshot = Box::pin(harness.db.snapshot(Some(&harness.context.aof)));
    poll_fn(|cx| {
        assert!(snapshot.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    resume.send(()).unwrap();
    let snapshot = snapshot.await.unwrap();
    assert_eq!(snapshot.sequence, Some(1));
    assert_eq!(snapshot.mutations.len(), 2);
    assert_eq!(harness.context.runtime.status().applied.sequence, 1);
    let (directory, _, _, writer) = harness.stop().await;
    writer.unwrap();
    cleanup(&directory);
}
