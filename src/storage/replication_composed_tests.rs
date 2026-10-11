//! Full sync and whole-batch apply proof for the internal composed replica.

use std::future::{Future, poll_fn};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
        Self::new_at(directory, quota_bytes, faults).await
    }

    async fn new_at(
        directory: PathBuf,
        quota_bytes: usize,
        faults: Arc<dyn FaultInjector>,
    ) -> Self {
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

async fn wait_for_cursor(runtime: &Runtime, expected: Cursor) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if runtime.status().applied == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn wait_for_syncs(runtime: &Runtime, full: u64, partial: u64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = runtime.status();
            if status.full_syncs == full && status.partial_syncs == partial {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composed_transport3_full_and_continue_apply_whole_mixed_batches() {
    let source = Harness::new(4096, Arc::new(NoFaults)).await;
    let primary = source
        .db
        .promote_replica(source.context.clone(), [9; 16])
        .await
        .unwrap();
    let follower = Harness::new(4096, Arc::new(NoFaults)).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let source_config = crate::replication::config::Config::default();
    let (source_stop, source_shutdown) = watch::channel(false);
    let source_task = tokio::spawn(crate::replication::session::serve(
        listener,
        source.db.clone(),
        source.context.clone(),
        source_config,
        source_shutdown,
    ));
    let mut legacy = tokio::net::TcpStream::connect(address).await.unwrap();
    crate::replication::protocol::write(
        &mut legacy,
        &crate::replication::protocol::Message::Hello(crate::replication::protocol::Hello {
            sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
            record_version: 1,
            shard_count: 2,
            routing_version: 1,
            max_record_bytes: source.config.limits.max_record_bytes as u32,
            max_mutations: source.config.limits.max_mutations as u32,
            max_snapshot_bytes: 4096,
            cursor: None,
        }),
        crate::replication::protocol::Limits::default(),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(
        crate::replication::protocol::read_composed_authorized(
            &mut legacy,
            crate::replication::protocol::Limits::default(),
            Duration::from_secs(5),
            |_| true,
        )
        .await
        .is_err()
    );
    let follow_config = crate::replication::config::Config {
        upstream: Some(address),
        ..Default::default()
    };
    let (follow_stop, follow_shutdown) = watch::channel(false);
    let follow_task = tokio::spawn(crate::replication::session::follow(
        follower.db.clone(),
        follower.context.clone(),
        follow_config.clone(),
        follow_shutdown,
    ));
    wait_for_cursor(&follower.context.runtime, primary).await;
    wait_for_syncs(&follower.context.runtime, 1, 0).await;
    assert_eq!(follower.context.runtime.status().full_syncs, 1);

    let first = Cursor {
        sequence: 1,
        ..primary
    };
    assert_eq!(
        source
            .context
            .aof
            .append(batch(b"first-left", b"first-right"))
            .await
            .unwrap(),
        first.sequence
    );
    source.context.runtime.applied(first);
    wait_for_cursor(&follower.context.runtime, first).await;
    assert_eq!(follower.context.aof.status().await.unwrap().0, 1);

    follow_stop.send_replace(true);
    follow_task.await.unwrap().unwrap();
    let second = Cursor {
        sequence: 2,
        ..primary
    };
    assert_eq!(
        source
            .context
            .aof
            .append(batch(b"second-left", b"second-right"))
            .await
            .unwrap(),
        second.sequence
    );
    source.context.runtime.applied(second);
    let (_resume_stop, resume_shutdown) = watch::channel(false);
    let resume_task = tokio::spawn(crate::replication::session::follow(
        follower.db.clone(),
        follower.context.clone(),
        follow_config,
        resume_shutdown,
    ));
    wait_for_cursor(&follower.context.runtime, second).await;
    wait_for_syncs(&follower.context.runtime, 1, 1).await;
    let status = follower.context.runtime.status();
    assert_eq!(status.full_syncs, 1);
    assert_eq!(status.partial_syncs, 1);
    let snapshot = follower
        .db
        .snapshot(Some(&follower.context.aof))
        .await
        .unwrap();
    assert_eq!(snapshot.sequence, Some(2));
    assert_eq!(
        snapshot.mutations,
        batch(b"second-left", b"second-right").mutations
    );

    resume_task.abort();
    resume_task.await.unwrap_err();
    source_stop.send_replace(true);
    source_task.await.unwrap().unwrap();
    let (source_dir, _, _, source_writer) = source.stop().await;
    source_writer.unwrap();
    cleanup(&source_dir);
    let (follower_dir, follower_config, quota, follower_writer) = follower.stop().await;
    follower_writer.unwrap();
    let recovered = persistence::recover_composed_legacy_mixed(
        follower_config,
        quota,
        Arc::new(SystemClock),
        Arc::new(NoFaults),
    )
    .unwrap();
    assert_eq!(recovered.metadata.sequence, 2);
    assert_eq!(
        recovered.metadata.replication,
        Some(ReplicationMetadata {
            role: Role::Replica,
            epoch: primary.epoch,
        })
    );
    drop(recovered);
    cleanup(&follower_dir);
}

#[tokio::test]
async fn promoted_composed_writer_publishes_one_transport3_frame_for_mixed_batch() {
    let harness = Harness::new(4096, Arc::new(NoFaults)).await;
    let promoted = harness
        .db
        .promote_replica(harness.context.clone(), [9; 16])
        .await
        .unwrap();
    assert_eq!(promoted.sequence, 0);
    let journal = harness.context.runtime.journal().unwrap();
    let mut subscriber = journal.subscribe(promoted).unwrap();
    let mixed = batch(b"left", b"right");
    assert_eq!(harness.context.aof.append(mixed.clone()).await.unwrap(), 1);
    let entry = tokio::time::timeout(Duration::from_secs(5), subscriber.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.cursor.epoch, promoted.epoch);
    assert_eq!(entry.cursor.sequence, 1);
    let limits = crate::replication::protocol::Limits::default();
    assert!(crate::replication::protocol::decode(entry.frame.clone(), limits).is_err());
    assert_eq!(
        crate::replication::protocol::decode_composed(entry.frame, limits).unwrap(),
        crate::replication::protocol::Message::Batch {
            sequence: 1,
            batch: mixed,
        }
    );
    assert_eq!(journal.status().unwrap().batches, 1);
    let (directory, config, quota, writer) = harness.stop().await;
    writer.unwrap();
    let standalone = crate::ServerConfig {
        aof: Some(config.clone()),
        composed_aof: true,
        shards: 2,
        max_dataset_bytes: quota.max_dataset_bytes,
        ..crate::ServerConfig::default()
    };
    assert!(matches!(
        crate::server::prepare(&standalone).await,
        Err(crate::server::ServerError::Config(
            crate::ConfigError::InvalidServerLimits {
                reason: "persisted composed replication role requires an explicit replication configuration"
            }
        ))
    ));
    let restarted = persistence::recover_composed_legacy_mixed(
        config,
        quota,
        Arc::new(SystemClock),
        Arc::new(NoFaults),
    )
    .unwrap();
    assert_eq!(restarted.metadata.sequence, 1);
    assert_eq!(
        restarted.metadata.replication,
        Some(ReplicationMetadata {
            role: Role::Primary,
            epoch: promoted.epoch,
        })
    );
    drop(restarted);
    cleanup(&directory);
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

struct OneShotFault {
    point: &'static str,
    armed: AtomicBool,
}

impl FaultInjector for OneShotFault {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if point == self.point && self.armed.swap(false, Ordering::SeqCst) {
            Err(std::io::Error::other("injected composed replica boundary"))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn composed_replica_append_apply_and_sync_faults_recover_only_complete_batches() {
    for (point, recovered_sequence, writer_fails) in [
        ("before_append", 0, true),
        ("after_append", 1, true),
        ("before_sync", 1, true),
        ("after_sync", 1, true),
        ("before_reply", 1, true),
        ("replica_mixed_before_first_apply", 1, false),
        ("replica_mixed_after_first_apply", 1, false),
        ("replica_mixed_after_all_apply", 1, false),
    ] {
        let fault = Arc::new(OneShotFault {
            point,
            armed: AtomicBool::new(false),
        });
        let harness = Harness::new(4096, fault.clone()).await;
        fault.armed.store(true, Ordering::SeqCst);
        let result = harness
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
            .await;
        assert!(
            matches!(result, Err(Error::Persistence(_))),
            "{point}: {result:?}"
        );
        assert_eq!(
            harness.db.check_admission(),
            Err(DbError::Unavailable),
            "{point}"
        );
        assert_eq!(
            harness.context.runtime.status().applied.sequence,
            0,
            "{point}"
        );
        let (directory, config, quota, writer) = harness.stop().await;
        assert_eq!(writer.is_err(), writer_fails, "{point}");
        let recovered = persistence::recover_composed_legacy_mixed(
            config,
            quota,
            Arc::new(SystemClock),
            Arc::new(NoFaults),
        )
        .unwrap();
        assert_eq!(recovered.metadata.sequence, recovered_sequence, "{point}");
        let mut store = recovered.store;
        let expected = if recovered_sequence == 0 {
            None
        } else {
            Some(Bytes::from_static(b"left"))
        };
        assert_eq!(get(&mut store, b"a"), Reply::Bulk(expected), "{point}");
        let expected = if recovered_sequence == 0 {
            None
        } else {
            Some(Bytes::from_static(b"right"))
        };
        assert_eq!(get(&mut store, b"b"), Reply::Bulk(expected), "{point}");
        drop(store);
        cleanup(&directory);
    }
}

struct PauseAtReplicaBoundary {
    point: String,
    directory: PathBuf,
    armed: AtomicBool,
}

impl FaultInjector for PauseAtReplicaBoundary {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if self.armed.load(Ordering::SeqCst) && point == self.point {
            use std::io::Write;
            let mut signal = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.directory.join("paused"))?;
            signal.write_all(point.as_bytes())?;
            signal.sync_all()?;
            loop {
                std::thread::park_timeout(Duration::from_secs(1));
            }
        }
        Ok(())
    }
}

#[test]
#[ignore = "crash helper executed only by the parent process"]
fn replica_crash_child() {
    let directory = PathBuf::from(std::env::var_os("SIDER_TEST_REPLICA_AOF_DIR").unwrap());
    let fault = Arc::new(PauseAtReplicaBoundary {
        point: std::env::var("SIDER_TEST_REPLICA_POINT").unwrap(),
        directory: directory.clone(),
        armed: AtomicBool::new(false),
    });
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let harness = Harness::new_at(directory, 4096, fault.clone()).await;
            fault.armed.store(true, Ordering::SeqCst);
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
                .await
                .unwrap();
            panic!("replica apply returned before the child was terminated");
        });
}

struct CrashChild(std::process::Child);

impl Drop for CrashChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[test]
fn composed_replica_process_crashes_recover_whole_batches_at_each_boundary() {
    use std::process::{Command, Stdio};
    use std::time::Instant;

    for point in [
        "before_append",
        "after_append",
        "before_sync",
        "after_sync",
        "before_reply",
        "replica_mixed_before_first_apply",
        "replica_mixed_after_first_apply",
        "replica_mixed_after_all_apply",
    ] {
        let directory = std::env::temp_dir().join(format!(
            "sider-composed-replica-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed),
        ));
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--ignored", "replica_crash_child", "--nocapture"])
            .env("SIDER_TEST_REPLICA_AOF_DIR", &directory)
            .env("SIDER_TEST_REPLICA_POINT", point)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = CrashChild(command.spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(15);
        while !directory.join("paused").is_file() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before {point}"
            );
            assert!(Instant::now() < deadline, "point not reached: {point}");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            std::fs::read(directory.join("paused")).unwrap(),
            point.as_bytes()
        );
        child.0.kill().unwrap();
        child.0.wait().unwrap();

        let mut config = AofConfig::new(directory.clone());
        config.layout.shard_count = 2;
        let recovered = persistence::recover_composed_legacy_mixed(
            config,
            StoreConfig {
                max_dataset_bytes: 4096,
            },
            Arc::new(SystemClock),
            Arc::new(NoFaults),
        )
        .unwrap();
        assert_eq!(
            recovered.metadata.replication,
            Some(ReplicationMetadata {
                role: Role::Replica,
                epoch: EPOCH,
            }),
            "{point}"
        );
        let sequence = recovered.metadata.sequence;
        assert!(sequence <= 1, "{point}: sequence {sequence}");
        if point == "before_append" {
            assert_eq!(sequence, 0, "{point}");
        }
        if matches!(
            point,
            "after_sync"
                | "before_reply"
                | "replica_mixed_before_first_apply"
                | "replica_mixed_after_first_apply"
                | "replica_mixed_after_all_apply"
        ) {
            assert_eq!(sequence, 1, "{point}");
        }
        let mut store = recovered.store;
        let expected_left = (sequence == 1).then(|| Bytes::from_static(b"left"));
        let expected_right = (sequence == 1).then(|| Bytes::from_static(b"right"));
        assert_eq!(get(&mut store, b"a"), Reply::Bulk(expected_left), "{point}");
        assert_eq!(
            get(&mut store, b"b"),
            Reply::Bulk(expected_right),
            "{point}"
        );
        drop(store);
        cleanup(&directory);
    }
}

struct PauseDuringTcpDisconnect {
    point: &'static str,
    armed: AtomicBool,
    reached: std::sync::mpsc::Sender<&'static str>,
    resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl FaultInjector for PauseDuringTcpDisconnect {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if point == self.point && self.armed.swap(false, Ordering::SeqCst) {
            self.reached
                .send(point)
                .map_err(|_| std::io::Error::other("disconnect observer lost"))?;
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .map_err(|_| std::io::Error::other("disconnect pause not resumed"))?;
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composed_replica_disconnect_during_commit_reconnects_after_whole_batch() {
    use crate::replication::protocol::{self, Message};

    let limits = protocol::Limits::default();
    let deadline = Duration::from_secs(5);
    let base = Cursor {
        epoch: EPOCH,
        sequence: 0,
    };
    let next = Cursor {
        sequence: 1,
        ..base
    };
    for point in [
        "before_append",
        "after_append",
        "before_sync",
        "after_sync",
        "before_reply",
        "replica_mixed_before_first_apply",
        "replica_mixed_after_first_apply",
        "replica_mixed_after_all_apply",
    ] {
        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let fault = Arc::new(PauseDuringTcpDisconnect {
            point,
            armed: AtomicBool::new(false),
            reached: reached_tx,
            resume: std::sync::Mutex::new(resume_rx),
        });
        let follower = Harness::new(4096, fault.clone()).await;
        fault.armed.store(true, Ordering::SeqCst);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = crate::replication::config::Config {
            upstream: Some(listener.local_addr().unwrap()),
            ..Default::default()
        };
        let (stop, shutdown) = watch::channel(false);
        let following = tokio::spawn(crate::replication::session::follow(
            follower.db.clone(),
            follower.context.clone(),
            config,
            shutdown,
        ));
        let (mut socket, _) = tokio::time::timeout(deadline, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let (_, Message::ComposedHello(mut peer)) =
            protocol::read_composed_authorized(&mut socket, limits, deadline, |_| true)
                .await
                .unwrap()
        else {
            panic!("{point}: expected composed hello");
        };
        assert_eq!(peer.base.cursor, Some(base), "{point}");
        peer.base.cursor = Some(base);
        protocol::write_composed(&mut socket, &Message::ComposedHello(peer), limits, deadline)
            .await
            .unwrap();
        protocol::write_composed(&mut socket, &Message::Continue(base), limits, deadline)
            .await
            .unwrap();
        assert_eq!(
            protocol::read_composed_authorized(&mut socket, limits, deadline, |_| true)
                .await
                .unwrap()
                .1,
            Message::Ack(base),
            "{point}"
        );
        protocol::write_composed(
            &mut socket,
            &Message::Batch {
                sequence: 1,
                batch: batch(b"left", b"right"),
            },
            limits,
            deadline,
        )
        .await
        .unwrap();
        let reached = tokio::task::spawn_blocking(move || reached_rx.recv_timeout(deadline))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reached, point);
        drop(socket);
        resume_tx.send(()).unwrap();
        wait_for_cursor(&follower.context.runtime, next).await;

        let (mut reconnected, _) = tokio::time::timeout(deadline, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let (_, Message::ComposedHello(peer)) =
            protocol::read_composed_authorized(&mut reconnected, limits, deadline, |_| true)
                .await
                .unwrap()
        else {
            panic!("{point}: expected reconnect hello");
        };
        assert_eq!(peer.base.cursor, Some(next), "{point}");
        stop.send_replace(true);
        following.await.unwrap().unwrap();
        assert_eq!(follower.context.aof.status().await.unwrap().0, 1);
        let snapshot = follower
            .db
            .snapshot(Some(&follower.context.aof))
            .await
            .unwrap();
        assert_eq!(snapshot.mutations, batch(b"left", b"right").mutations);
        let (directory, config, quota, writer) = follower.stop().await;
        writer.unwrap();
        let recovered = persistence::recover_composed_legacy_mixed(
            config,
            quota,
            Arc::new(SystemClock),
            Arc::new(NoFaults),
        )
        .unwrap();
        assert_eq!(recovered.metadata.sequence, 1, "{point}");
        assert_eq!(recovered.store.len(), 2, "{point}");
        drop(recovered);
        cleanup(&directory);
    }
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
