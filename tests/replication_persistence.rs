//! AOF com papel/época atômicos e journal alimentado pelo escritor real.

#[path = "common/process.rs"]
mod process;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use sider::command::{Command, parse};
use sider::persistence::{
    self, AofConfig, AofError, FaultInjector, ReplicationMetadata, Role, format,
};
use sider::replication::{Cursor, journal, protocol};
use sider::resp::Frame;
use sider::storage::{Clock, Mutation, Store, StoreConfig};

struct ClockFixed(tokio::time::Instant);
impl Clock for ClockFixed {
    fn now(&self) -> tokio::time::Instant {
        self.0
    }
    fn unix_millis(&self) -> i64 {
        1000
    }
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(std::env::temp_dir().join(format!(
            "sider-repl-aof-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
    fn config(&self) -> AofConfig {
        let mut config = AofConfig::new(self.0.clone());
        config.compact_after_bytes = 0;
        config.limits.max_record_bytes = 4096;
        config
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn clock() -> Arc<dyn Clock> {
    Arc::new(ClockFixed(tokio::time::Instant::now()))
}
fn set(value: &'static [u8]) -> Command {
    Command::Set {
        key: Bytes::from_static(b"{a}:string"),
        value: Bytes::from_static(value),
    }
}
fn dataset() -> Vec<Mutation> {
    let mut store = Store::with_clock(clock());
    for arguments in [
        vec![b"SET".as_slice(), b"{a}:string", b"\xff"],
        vec![b"HSET", b"{b}:hash", b"\0", b"\xff"],
        vec![b"RPUSH", b"{c}:list", b"\0", b"\xff"],
        vec![b"SADD", b"{d}:set", b"\xff", b"\0"],
        vec![b"ZADD", b"{a}:zset", b"-inf", b"\xff", b"1e-7", b"\0"],
        vec![b"PEXPIRE", b"{a}:zset", b"5000"],
    ] {
        store.execute(
            parse(Frame::Array(Some(
                arguments
                    .into_iter()
                    .map(|arg| Frame::Bulk(Some(Bytes::copy_from_slice(arg))))
                    .collect(),
            )))
            .unwrap(),
        );
    }
    store.snapshot()
}

#[test]
fn role_header_validates_every_prefix_byte_and_reserved_fields() {
    let metadata = ReplicationMetadata {
        role: Role::Replica,
        epoch: [9; 16],
    };
    let mut header = Vec::new();
    format::write_header_with_replication(
        &mut header,
        42,
        persistence::DurableLayout::default(),
        Some(metadata),
    )
    .unwrap();
    assert_eq!(header.len(), format::REPLICATION_HEADER_BYTES);
    let decoded = format::read_header_with_layout(header.as_slice()).unwrap();
    assert_eq!(decoded.sequence, 42);
    assert_eq!(decoded.replication, Some(metadata));
    for length in 0..header.len() {
        assert!(format::read_header_with_layout(&header[..length]).is_err());
    }
    for position in 0..header.len() {
        let mut corrupt = header.clone();
        corrupt[position] ^= 0x80;
        assert!(format::read_header_with_layout(corrupt.as_slice()).is_err());
    }
    for (position, value) in [(28, 9), (29, 1)] {
        let mut corrupt = header.clone();
        corrupt[position] = value;
        let digest = format::checksum(&corrupt[..48]);
        corrupt[48..].copy_from_slice(&digest.to_le_bytes());
        assert!(format::read_header_with_layout(corrupt.as_slice()).is_err());
    }
}

#[tokio::test]
async fn install_compaction_and_promotion_recover_one_role_epoch_and_complete_dataset() {
    let directory = Directory::new();
    let mut config = directory.config();
    config.layout.shard_count = 4;
    let recovered = persistence::recover(config.clone(), StoreConfig::default(), clock()).unwrap();
    let (_, aof, writer) = recovered.start();
    let snapshot = dataset();
    let replica = ReplicationMetadata {
        role: Role::Replica,
        epoch: [3; 16],
    };
    aof.install_snapshot(snapshot.clone(), 40, replica)
        .await
        .unwrap();
    let mut expected = Store::with_clock(clock());
    expected.replay(&snapshot).unwrap();
    let prepared = expected.prepare(set(b"updated"));
    assert_eq!(aof.append(prepared.batch.clone()).await.unwrap(), 41);
    expected.apply(prepared);
    aof.compact(expected.snapshot()).await.unwrap();
    drop(aof);
    writer.await.unwrap().unwrap();
    let recovered = persistence::recover(config.clone(), StoreConfig::default(), clock()).unwrap();
    assert_eq!(recovered.metadata.replication, Some(replica));
    assert_eq!(recovered.metadata.sequence, 41);
    assert_eq!(recovered.store.snapshot(), expected.snapshot());
    let (_, aof, writer) = recovered.start();
    let primary = ReplicationMetadata {
        role: Role::Primary,
        epoch: [4; 16],
    };
    aof.install_snapshot(expected.snapshot(), 41, primary)
        .await
        .unwrap();
    drop(aof);
    writer.await.unwrap().unwrap();
    let recovered = persistence::recover(config, StoreConfig::default(), clock()).unwrap();
    assert_eq!(recovered.metadata.replication, Some(primary));
    assert_eq!(recovered.metadata.sequence, 41);
    assert_eq!(recovered.store.snapshot(), expected.snapshot());
}

#[tokio::test]
async fn writer_publishes_exact_committed_records_and_rejects_unaligned_journal() {
    let directory = Directory::new();
    let recovered =
        persistence::recover(directory.config(), StoreConfig::default(), clock()).unwrap();
    let (_, aof, writer) = recovered.start();
    let cursor = Cursor {
        epoch: [3; 16],
        sequence: 20,
    };
    let metadata = ReplicationMetadata {
        role: Role::Primary,
        epoch: cursor.epoch,
    };
    aof.install_snapshot(Vec::new(), 20, metadata)
        .await
        .unwrap();
    let limits = journal::Limits {
        max_bytes: 8192,
        max_batches: 32,
        max_frame_bytes: 8192,
    };
    assert!(matches!(
        aof.attach_journal(
            journal::Journal::new(
                Cursor {
                    sequence: 21,
                    ..cursor
                },
                limits
            )
            .unwrap()
        )
        .await,
        Err(AofError::Sequence)
    ));
    let journal = journal::Journal::new(cursor, limits).unwrap();
    let mut subscription = journal.subscribe(cursor).unwrap();
    aof.attach_journal(journal.clone()).await.unwrap();
    let prepared = Store::new().prepare(set(b"committed"));
    assert_eq!(aof.append(prepared.batch.clone()).await.unwrap(), 21);
    let entry = subscription.next().await.unwrap();
    assert_eq!(
        protocol::decode(entry.frame, protocol::Limits::default()).unwrap(),
        protocol::Message::Batch {
            sequence: 21,
            batch: prepared.batch
        }
    );
    assert_eq!(aof.flush().await.unwrap(), 21);
    let mut invalid = dataset();
    invalid.reverse();
    assert!(matches!(
        aof.install_snapshot(invalid, 90, metadata).await,
        Err(AofError::Sequence)
    ));
    assert_eq!(aof.flush().await.unwrap(), 21);
    drop(aof);
    writer.await.unwrap().unwrap();
    assert!(matches!(journal.status(), Err(journal::Error::Closed)));
}

struct FailOnce {
    point: &'static str,
    armed: AtomicBool,
}
impl FaultInjector for FailOnce {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if self.point == point && self.armed.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("falha de instalação injetada"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn install_failure_before_publish_keeps_old_generation_after_publish_requires_recovery() {
    for point in [
        "replication_before_install",
        "replication_before_publish",
        "replication_after_publish",
    ] {
        let directory = Directory::new();
        let recovered = persistence::recover_with_faults(
            directory.config(),
            StoreConfig::default(),
            clock(),
            Arc::new(FailOnce {
                point,
                armed: AtomicBool::new(true),
            }),
        )
        .unwrap();
        let (_, aof, writer) = recovered.start();
        aof.append(Store::new().prepare(set(b"old")).batch)
            .await
            .unwrap();
        let snapshot = dataset();
        let metadata = ReplicationMetadata {
            role: Role::Replica,
            epoch: [8; 16],
        };
        assert!(
            aof.install_snapshot(snapshot.clone(), 30, metadata)
                .await
                .is_err()
        );
        if point != "replication_after_publish" {
            assert_eq!(aof.flush().await.unwrap(), 1);
        }
        drop(aof);
        let failed = writer.await.unwrap().is_err();
        assert_eq!(failed, point == "replication_after_publish");
        let recovered =
            persistence::recover(directory.config(), StoreConfig::default(), clock()).unwrap();
        if point == "replication_after_publish" {
            assert_eq!(recovered.store.snapshot(), snapshot);
            assert_eq!(recovered.metadata.replication, Some(metadata));
            assert_eq!(recovered.metadata.sequence, 30);
        } else {
            assert_eq!(recovered.store.len(), 1);
            assert_eq!(recovered.metadata.replication, None);
            assert_eq!(recovered.metadata.sequence, 1);
        }
    }
}

struct PauseInstall {
    point: String,
    directory: PathBuf,
}
impl FaultInjector for PauseInstall {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if point == self.point {
            use std::io::Write;
            let mut marker = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(self.directory.join("paused"))?;
            marker.write_all(point.as_bytes())?;
            marker.sync_all()?;
            loop {
                std::thread::park_timeout(std::time::Duration::from_secs(1));
            }
        }
        Ok(())
    }
}

#[test]
#[ignore = "helper de crash chamado exclusivamente pelo teste pai"]
fn replication_install_process_child() {
    let directory = PathBuf::from(std::env::var_os("SIDER_TEST_REPLICATION_DIRECTORY").unwrap());
    let point = std::env::var("SIDER_TEST_REPLICATION_POINT").unwrap();
    let role = match std::env::var("SIDER_TEST_REPLICATION_ROLE")
        .unwrap()
        .as_str()
    {
        "primary" => Role::Primary,
        "replica" => Role::Replica,
        _ => panic!("papel do teste"),
    };
    let mut config = AofConfig::new(directory.clone());
    config.layout.shard_count = 4;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let recovered = persistence::recover_with_faults(
            config,
            StoreConfig::default(),
            clock(),
            Arc::new(PauseInstall { point, directory }),
        )
        .unwrap();
        let (_, aof, writer) = recovered.start();
        aof.install_snapshot(
            dataset(),
            30,
            ReplicationMetadata {
                role,
                epoch: [8; 16],
            },
        )
        .await
        .unwrap();
        drop(aof);
        writer.await.unwrap().unwrap();
    });
}

#[test]
fn replication_real_process_kill_installs_data_and_role_as_one_generation() {
    let timeout = std::time::Duration::from_secs(10);
    for role in [Role::Replica, Role::Primary] {
        for point in [
            "replication_before_install",
            "replication_before_publish",
            "replication_after_publish",
        ] {
            let directory = Directory::new();
            let mut config = directory.config();
            config.layout.shard_count = 4;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let mut old = Store::with_clock(clock());
            old.execute(set(b"old"));
            let old = old.snapshot();
            let before = ReplicationMetadata {
                role: Role::Replica,
                epoch: [3; 16],
            };
            runtime.block_on(async {
                let recovered =
                    persistence::recover(config.clone(), StoreConfig::default(), clock()).unwrap();
                let (_, aof, writer) = recovered.start();
                aof.install_snapshot(old.clone(), 1, before).await.unwrap();
                drop(aof);
                writer.await.unwrap().unwrap();
            });
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--ignored",
                    "--exact",
                    "replication_install_process_child",
                    "--nocapture",
                ])
                .env("SIDER_TEST_REPLICATION_DIRECTORY", &directory.0)
                .env("SIDER_TEST_REPLICATION_POINT", point)
                .env(
                    "SIDER_TEST_REPLICATION_ROLE",
                    if role == Role::Primary {
                        "primary"
                    } else {
                        "replica"
                    },
                );
            let mut child = process::OwnedChild::spawn(&mut command).unwrap();
            let deadline = std::time::Instant::now() + timeout;
            while !directory.0.join("paused").is_file() {
                child.assert_alive().unwrap();
                assert!(std::time::Instant::now() < deadline, "crash em {point}");
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            child.terminate(timeout).unwrap();
            let recovered = persistence::recover(config, StoreConfig::default(), clock()).unwrap();
            if point == "replication_after_publish" {
                assert_eq!(recovered.store.snapshot(), dataset());
                assert_eq!(recovered.metadata.sequence, 30);
                assert_eq!(
                    recovered.metadata.replication,
                    Some(ReplicationMetadata {
                        role,
                        epoch: [8; 16]
                    })
                );
            } else {
                assert_eq!(recovered.store.snapshot(), old);
                assert_eq!(recovered.metadata.sequence, 1);
                assert_eq!(recovered.metadata.replication, Some(before));
            }
            assert!(!std::fs::read_dir(&directory.0).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("replication-install-")
            }));
        }
    }
}
