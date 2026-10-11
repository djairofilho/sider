//! Explicit schema-3 archive export, verification, and isolated restore.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sider::command::{Command, Reply};
use sider::persistence::{self, AofConfig, DurableLayout, backup, composed_format, format};
use sider::replication::{
    Cursor, composed,
    protocol::{self, ComposedHello, Hello, Message},
};
use sider::storage::{Clock, Mutation, Store, StoreConfig, SystemClock, worker};
use tokio::io::{AsyncWriteExt, duplex};
use tokio::sync::watch;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
const CURSOR: Cursor = Cursor {
    epoch: [7; 16],
    sequence: 12,
};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "sider-composed-backup-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&root).unwrap();
        Self(root)
    }
    fn child(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        assert_eq!(
            self.0.canonicalize().unwrap().parent(),
            Some(std::env::temp_dir().canonicalize().unwrap().as_path()),
        );
        fs::remove_dir_all(&self.0).unwrap();
    }
}

struct FixedClock(i64, tokio::time::Instant);
impl Clock for FixedClock {
    fn now(&self) -> tokio::time::Instant {
        self.1
    }
    fn unix_millis(&self) -> i64 {
        self.0
    }
}
fn clock() -> Arc<dyn Clock> {
    Arc::new(FixedClock(1000, tokio::time::Instant::now()))
}

fn layout() -> DurableLayout {
    DurableLayout {
        shard_count: 4,
        routing_version: 1,
    }
}

fn hello(cursor: Option<Cursor>, limits: backup::Limits) -> ComposedHello {
    ComposedHello {
        base: Hello {
            sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
            record_version: 1,
            shard_count: layout().shard_count,
            routing_version: layout().routing_version,
            max_record_bytes: limits.max_record_bytes as u32,
            max_mutations: limits.max_mutations as u32,
            max_snapshot_bytes: limits.max_snapshot_bytes,
            cursor,
        },
        aof_header_version: composed_format::HEADER_VERSION,
        required_capabilities: composed_format::MIXED_OWNER_BATCH,
    }
}

fn options(destination: &Path, limits: backup::Limits) -> backup::ExportOptions {
    backup::ExportOptions {
        source: "127.0.0.1:1".parse().unwrap(),
        destination: destination.to_owned(),
        source_sha: "a".repeat(40),
        limits,
    }
}

fn images() -> Vec<Mutation> {
    vec![
        Mutation::Put {
            key: Bytes::from_static(b"\0a"),
            value: Bytes::from_static(b"\xff\0first").into(),
            expires_at_unix_ms: Some(2000),
        },
        Mutation::Put {
            key: Bytes::from_static(b"z\0"),
            value: Bytes::from_static(b"second").into(),
            expires_at_unix_ms: None,
        },
    ]
}

async fn export_to(target: &Path, limits: backup::Limits) -> backup::Manifest {
    let (mut producer, mut consumer) = duplex(128);
    let source = tokio::spawn(async move {
        let admission = composed::negotiate_export(
            &mut producer,
            hello(None, limits),
            protocol::Limits::default(),
            Duration::from_secs(5),
        )
        .await
        .unwrap()
        .unwrap();
        admission
            .send_snapshot(
                &mut producer,
                hello(Some(CURSOR), limits),
                CURSOR,
                &images(),
            )
            .await
            .unwrap();
    });
    let manifest = backup::export_composed_stream(&mut consumer, options(target, limits), clock())
        .await
        .unwrap();
    source.await.unwrap();
    manifest
}

#[tokio::test]
async fn schema3_export_verify_and_restore_use_header7_exactly() {
    let directory = Directory::new();
    let target = directory.child("archive");
    let restored = directory.child("restored");
    let limits = backup::Limits::default();
    let manifest = export_to(&target, limits).await;
    assert_eq!(manifest.profile, backup::Profile::Composed);
    assert_eq!(
        manifest.required_capabilities,
        Some(composed_format::MIXED_OWNER_BATCH)
    );
    assert_eq!(manifest.cursor, CURSOR);
    let header =
        composed_format::read_header(fs::File::open(target.join(backup::SNAPSHOT)).unwrap())
            .unwrap();
    assert_eq!(
        header.record_version,
        composed_format::RecordVersion::Legacy
    );
    assert!(header.mixed_owner_batch);
    assert_eq!(header.replication, None);
    backup::verify_composed(&target, layout(), limits, clock()).unwrap();
    assert!(backup::verify(&target, layout(), limits, clock()).is_err());
    assert!(backup::verify_memory(&target, layout(), limits, clock()).is_err());
    backup::restore_composed(
        backup::RestoreOptions {
            source: target,
            destination: restored.clone(),
            layout: layout(),
            limits,
        },
        clock(),
    )
    .unwrap();
    let restored_header = composed_format::read_header(
        fs::File::open(restored.join("generation-00000000000000000000.aof")).unwrap(),
    )
    .unwrap();
    assert_eq!(restored_header, header);
    let snapshot_path = restored.join("generation-00000000000000000000.aof");
    let before = fs::read(&snapshot_path).unwrap();
    let mut config = AofConfig::new(restored);
    config.layout = layout();
    assert!(persistence::recover(config.clone(), StoreConfig::default(), clock()).is_err());
    assert!(
        persistence::recover(
            config.clone().with_memory_profile(),
            StoreConfig::default(),
            clock(),
        )
        .is_err()
    );
    assert_eq!(fs::read(&snapshot_path).unwrap(), before);
    let recovered = persistence::recover_composed(config, StoreConfig::default(), clock()).unwrap();
    assert_eq!(
        recovered.metadata.format_version,
        composed_format::HEADER_VERSION
    );
    assert_eq!(recovered.metadata.sequence, CURSOR.sequence);
    assert_eq!(recovered.store.snapshot(), images());
}

#[tokio::test]
async fn live_mixed_owner_exec_exports_one_durable_cut() {
    let directory = Directory::new();
    let mut config = AofConfig::new(directory.child("live-aof"));
    config.layout = layout();
    let store_config = StoreConfig::default();
    let recovered =
        persistence::recover_composed(config.clone(), store_config, Arc::new(SystemClock)).unwrap();
    let (_empty, aof, writer) = recovered.start();
    let (stop, shutdown) = watch::channel(false);
    let stores = (0..layout().shard_count)
        .map(|_| Store::with_config(store_config, Arc::new(SystemClock)).unwrap())
        .collect();
    let (database, owners) =
        worker::channel_with_stores(8, Duration::from_secs(5), shutdown, stores).unwrap();
    assert_ne!(
        layout().shard_for(b"a").unwrap(),
        layout().shard_for(b"b").unwrap()
    );
    let database = database.with_composed_aof(aof.clone()).unwrap();
    let tasks: Vec<_> = owners
        .into_iter()
        .map(|owner| tokio::spawn(owner.with_aof(aof.clone(), 0).run()))
        .collect();
    assert_eq!(
        database
            .execute_batch(
                vec![
                    Command::Set {
                        key: Bytes::from_static(b"a"),
                        value: Bytes::from_static(b"first"),
                    },
                    Command::Set {
                        key: Bytes::from_static(b"b"),
                        value: Bytes::from_static(b"second"),
                    },
                ],
                Vec::new(),
            )
            .await
            .unwrap(),
        Reply::Array(vec![Reply::Ok, Reply::Ok])
    );
    assert_eq!(aof.status().await.unwrap().0, 1);
    let limits = backup::Limits::default();
    let (mut invalid_producer, mut invalid_consumer) = duplex(128);
    let unbound = database.clone().with_unpersisted_multi_owner();
    let invalid_aof = aof.clone();
    let rejected = tokio::spawn(async move {
        composed::export_database(
            &mut invalid_producer,
            &unbound,
            &invalid_aof,
            hello(None, limits),
            [9; 16],
            protocol::Limits::default(),
            Duration::from_secs(5),
        )
        .await
    });
    let rejected_archive = directory.child("unbound-archive");
    assert!(
        backup::export_composed_stream(
            &mut invalid_consumer,
            options(&rejected_archive, limits),
            clock(),
        )
        .await
        .is_err()
    );
    assert!(rejected.await.unwrap().is_err());
    assert!(!rejected_archive.exists());
    let (mut producer, mut consumer) = duplex(128);
    let source_database = database.clone();
    let source_aof = aof.clone();
    let source = tokio::spawn(async move {
        composed::export_database(
            &mut producer,
            &source_database,
            &source_aof,
            hello(None, limits),
            [9; 16],
            protocol::Limits::default(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    });
    let archive = directory.child("live-archive");
    let manifest =
        backup::export_composed_stream(&mut consumer, options(&archive, limits), clock())
            .await
            .unwrap();
    source.await.unwrap();
    assert_eq!(manifest.cursor.sequence, 1);
    backup::verify_composed(&archive, layout(), limits, clock()).unwrap();
    let restored = directory.child("live-restored");
    backup::restore_composed(
        backup::RestoreOptions {
            source: archive,
            destination: restored.clone(),
            layout: layout(),
            limits,
        },
        clock(),
    )
    .unwrap();
    let mut restored_config = AofConfig::new(restored);
    restored_config.layout = layout();
    let restored =
        persistence::recover_composed(restored_config, store_config, Arc::new(SystemClock))
            .unwrap();
    assert_eq!(restored.metadata.sequence, 1);
    assert_eq!(
        restored.store.snapshot(),
        vec![
            Mutation::Put {
                key: Bytes::from_static(b"a"),
                value: Bytes::from_static(b"first").into(),
                expires_at_unix_ms: None,
            },
            Mutation::Put {
                key: Bytes::from_static(b"b"),
                value: Bytes::from_static(b"second").into(),
                expires_at_unix_ms: None,
            },
        ]
    );
    drop(restored);
    drop(database);
    stop.send_replace(true);
    for task in tasks {
        task.await.unwrap();
    }
    drop(aof);
    writer.await.unwrap().unwrap();
}

#[tokio::test]
async fn composed_export_refuses_an_owner_without_the_shared_writer() {
    let directory = Directory::new();
    let mut config = AofConfig::new(directory.child("misbound-aof"));
    config.layout = layout();
    let recovered =
        persistence::recover_composed(config, StoreConfig::default(), Arc::new(SystemClock))
            .unwrap();
    let (_empty, aof, writer) = recovered.start();
    let (stop, shutdown) = watch::channel(false);
    let stores = (0..layout().shard_count)
        .map(|_| Store::with_config(StoreConfig::default(), Arc::new(SystemClock)).unwrap())
        .collect();
    let (database, owners) =
        worker::channel_with_stores(8, Duration::from_secs(5), shutdown, stores).unwrap();
    let database = database.with_composed_aof(aof.clone()).unwrap();
    let tasks: Vec<_> = owners
        .into_iter()
        .enumerate()
        .map(|(index, owner)| {
            let owner = if index == 0 {
                owner
            } else {
                owner.with_aof(aof.clone(), 0)
            };
            tokio::spawn(owner.run())
        })
        .collect();
    let (mut producer, mut consumer) = duplex(128);
    let source_aof = aof.clone();
    let source = tokio::spawn(async move {
        composed::export_database(
            &mut producer,
            &database,
            &source_aof,
            hello(None, backup::Limits::default()),
            [9; 16],
            protocol::Limits::default(),
            Duration::from_secs(5),
        )
        .await
    });
    let archive = directory.child("misbound-archive");
    assert!(
        backup::export_composed_stream(
            &mut consumer,
            options(&archive, backup::Limits::default()),
            clock(),
        )
        .await
        .is_err()
    );
    assert!(source.await.unwrap().is_err());
    assert!(!archive.exists());
    assert_eq!(aof.status().await.unwrap().0, 0);
    stop.send_replace(true);
    for task in tasks {
        task.await.unwrap();
    }
    drop(aof);
    writer.await.unwrap().unwrap();
}

#[tokio::test]
async fn incompatible_source_hello_creates_no_archive() {
    let directory = Directory::new();
    let target = directory.child("rejected");
    let limits = backup::Limits::default();
    let (mut producer, mut consumer) = duplex(128);
    let source = tokio::spawn(async move {
        let _ = protocol::read_composed_authorized(
            &mut producer,
            protocol::Limits::default(),
            Duration::from_secs(5),
            |kind| kind == 10,
        )
        .await
        .unwrap();
        let mut incompatible = hello(Some(CURSOR), limits);
        incompatible.base.sider_version = Bytes::from_static(b"other");
        protocol::write_composed(
            &mut producer,
            &Message::ComposedHello(incompatible),
            protocol::Limits::default(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    });
    assert!(
        backup::export_composed_stream(&mut consumer, options(&target, limits), clock(),)
            .await
            .is_err()
    );
    source.await.unwrap();
    assert!(!target.exists());
}

#[tokio::test]
async fn truncated_snapshot_removes_unpublished_archive() {
    let directory = Directory::new();
    let target = directory.child("truncated");
    let limits = backup::Limits::default();
    let (mut producer, mut consumer) = duplex(128);
    let source = tokio::spawn(async move {
        let _ = protocol::read_composed_authorized(
            &mut producer,
            protocol::Limits::default(),
            Duration::from_secs(5),
            |kind| kind == 10,
        )
        .await
        .unwrap();
        for message in [
            Message::ComposedHello(hello(Some(CURSOR), limits)),
            Message::FullStart {
                cursor: CURSOR,
                entries: 1,
            },
        ] {
            protocol::write_composed(
                &mut producer,
                &message,
                protocol::Limits::default(),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        }
        producer.shutdown().await.unwrap();
    });
    assert!(
        backup::export_composed_stream(&mut consumer, options(&target, limits), clock(),)
            .await
            .is_err()
    );
    source.await.unwrap();
    assert!(!target.exists());
}

fn write_manifest_with_checksums(target: &Path, value: &Value) {
    let mut encoded = serde_json::to_vec_pretty(value).unwrap();
    encoded.push(b'\n');
    fs::write(target.join(backup::MANIFEST), &encoded).unwrap();
    let snapshot_hash = value["snapshot"]["sha256"].as_str().unwrap();
    let sums = format!(
        "{:x}  {}\n{}  {}\n",
        Sha256::digest(&encoded),
        backup::MANIFEST,
        snapshot_hash,
        backup::SNAPSHOT,
    );
    fs::write(target.join(backup::CHECKSUMS), sums).unwrap();
}

#[tokio::test]
async fn recomputed_checksums_cannot_hide_profile_mismatches() {
    let directory = Directory::new();
    let limits = backup::Limits::default();
    for case in [
        "missing_capability",
        "wrong_capability",
        "record_2",
        "header_4",
        "header_without_mixed",
    ] {
        let target = directory.child(case);
        let manifest = export_to(&target, limits).await;
        let mut value = manifest.json();
        match case {
            "missing_capability" => {
                value
                    .as_object_mut()
                    .unwrap()
                    .remove("required_capabilities");
            }
            "wrong_capability" => {
                value["required_capabilities"] = Value::from(2);
            }
            "record_2" => {
                value["record_version"] = Value::from(2);
            }
            "header_4" => {
                value["aof_header_version"] = Value::from(4);
            }
            "header_without_mixed" => {
                let snapshot = target.join(backup::SNAPSHOT);
                let mut bytes = fs::read(&snapshot).unwrap();
                bytes[36..40].copy_from_slice(&0u32.to_le_bytes());
                let checksum = format::checksum(&bytes[..60]);
                bytes[60..64].copy_from_slice(&checksum.to_le_bytes());
                fs::write(snapshot, &bytes).unwrap();
                value["snapshot"]["sha256"] = Value::from(format!("{:x}", Sha256::digest(&bytes)));
            }
            _ => unreachable!(),
        }
        write_manifest_with_checksums(&target, &value);
        assert!(
            backup::verify_composed(&target, layout(), limits, clock()).is_err(),
            "{case}"
        );
    }
}
