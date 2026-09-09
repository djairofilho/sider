//! Identidade de shards e migração offline com origem legada preservada.
#![forbid(unsafe_code)]

#[path = "common/process.rs"]
mod process;

use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use sider::command::{Command as DbCommand, Reply};
use sider::persistence::format::{self, FormatError, Limits, Record};
use sider::persistence::migration::{MigrationOptions, migrate_offline, options_from_args};
use sider::persistence::{self, AofConfig, AofError, DurableLayout};
use sider::storage::{Clock, Mutation, MutationOrigin, ResolvedBatch, StoreConfig};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-migrate-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn child(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
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
fn layout(count: u32) -> DurableLayout {
    DurableLayout {
        shard_count: count,
        routing_version: 1,
    }
}
fn put(key: &'static [u8], value: &'static [u8], ttl: Option<i64>) -> Mutation {
    Mutation::Put {
        key: Bytes::from_static(key),
        value: Bytes::from_static(value),
        expires_at_unix_ms: ttl,
    }
}
fn aof_path(directory: &Path) -> PathBuf {
    directory.join("generation-00000000000000000000.aof")
}

fn seed(directory: &Path, header_layout: Option<DurableLayout>, mutations: Vec<Mutation>) {
    fs::create_dir(directory).unwrap();
    fs::write(directory.join("writer.lock"), []).unwrap();
    let mut file = File::create(aof_path(directory)).unwrap();
    match header_layout {
        Some(layout) => format::write_header_with_layout(&mut file, 0, layout).unwrap(),
        None => format::write_header(&mut file, 0).unwrap(),
    }
    file.write_all(
        &format::encode(
            &Record::Seal {
                sequence: 0,
                entries: 0,
                digest: 0,
            },
            Limits::default(),
        )
        .unwrap(),
    )
    .unwrap();
    file.write_all(
        &format::encode(
            &Record::Batch {
                sequence: 1,
                batch: ResolvedBatch {
                    origin: MutationOrigin::Client,
                    mutations,
                },
            },
            Limits::default(),
        )
        .unwrap(),
    )
    .unwrap();
    file.sync_all().unwrap();
}

fn options(parent: &Directory, count: u32) -> MigrationOptions {
    let source = AofConfig::new(parent.child("source"));
    let mut destination = AofConfig::new(parent.child("destination"));
    destination.layout = layout(count);
    MigrationOptions {
        source,
        destination,
        source_store: StoreConfig::default(),
        destination_store: StoreConfig::default(),
    }
}

#[test]
fn new_header_roundtrips_all_prefixes_corruption_and_legacy_identity() {
    let mut bytes = Vec::new();
    format::write_header_with_layout(&mut bytes, 42, layout(16)).unwrap();
    assert_eq!(bytes.len(), 32);
    let header = format::read_header_with_layout(bytes.as_slice()).unwrap();
    assert_eq!(
        (
            header.sequence,
            header.layout,
            header.format_version,
            header.bytes
        ),
        (42, layout(16), 2, 32)
    );
    for prefix in 0..bytes.len() {
        assert!(format::read_header_with_layout(&bytes[..prefix]).is_err());
    }
    for index in 0..bytes.len() {
        let mut bad = bytes.clone();
        bad[index] ^= 0x80;
        assert!(format::read_header_with_layout(bad.as_slice()).is_err());
    }
    let mut legacy = Vec::new();
    format::write_header(&mut legacy, 7).unwrap();
    let header = format::read_header_with_layout(legacy.as_slice()).unwrap();
    assert_eq!(
        (
            header.sequence,
            header.layout,
            header.format_version,
            header.bytes
        ),
        (7, layout(1), 1, 24)
    );
}

#[test]
fn invalid_layout_is_rejected_before_creating_or_modifying_files() {
    let parent = Directory::new();
    for candidate in [
        layout(0),
        layout(257),
        DurableLayout {
            shard_count: 1,
            routing_version: 99,
        },
    ] {
        let mut config = AofConfig::new(parent.child("absent"));
        config.layout = candidate;
        assert!(persistence::recover(config.clone(), StoreConfig::default(), clock()).is_err());
        assert!(!config.directory.exists());
    }
    assert_eq!(layout(3).quota(5, 0).unwrap(), 2);
    assert_eq!(layout(3).quota(5, 2).unwrap(), 1);
}

#[test]
fn legacy_layout_mismatch_precedes_truncated_tail_repair() {
    let parent = Directory::new();
    let mut request = options(&parent, 2);
    seed(
        &request.source.directory,
        None,
        vec![put(b"a", b"one", None)],
    );
    let path = aof_path(&request.source.directory);
    let mut original = fs::read(&path).unwrap();
    original.extend_from_slice(&[1, 2]);
    fs::write(&path, &original).unwrap();
    request.source.layout = layout(2);
    assert!(matches!(
        persistence::recover(request.source.clone(), request.source_store, clock()),
        Err(AofError::LayoutMismatch { .. })
    ));
    assert_eq!(fs::read(path).unwrap(), original);
    assert_eq!(fs::read_dir(request.source.directory).unwrap().count(), 2);
}

#[test]
fn replay_rejects_cross_shard_batch_and_hot_shard_quota() {
    let parent = Directory::new();
    let mut request = options(&parent, 2);
    request.source.layout = layout(2);
    assert_ne!(
        layout(2).shard_for(b"a").unwrap(),
        layout(2).shard_for(b"b").unwrap()
    );
    seed(
        &request.source.directory,
        Some(layout(2)),
        vec![put(b"a", b"one", None), put(b"b", b"two", None)],
    );
    let original = fs::read(aof_path(&request.source.directory)).unwrap();
    assert!(matches!(
        persistence::recover(request.source.clone(), request.source_store, clock()),
        Err(AofError::CrossShard)
    ));
    assert_eq!(
        fs::read(aof_path(&request.source.directory)).unwrap(),
        original
    );
    let hot = parent.child("hot");
    seed(
        &hot,
        Some(layout(2)),
        vec![put(b"{x}a", b"one", None), put(b"{x}b", b"two", None)],
    );
    let mut config = AofConfig::new(hot);
    config.layout = layout(2);
    assert!(matches!(
        persistence::recover(
            config,
            StoreConfig {
                max_dataset_bytes: 400
            },
            clock()
        ),
        Err(AofError::ShardQuota { .. })
    ));
}

#[test]
fn migration_preserves_sequence_values_ttl_and_source_bytes() {
    let parent = Directory::new();
    let request = options(&parent, 4);
    seed(
        &request.source.directory,
        None,
        vec![
            put(b"a", b"one", None),
            put(b"b", b"two", Some(5000)),
            put(b"expired", b"gone", Some(900)),
        ],
    );
    let original = fs::read(aof_path(&request.source.directory)).unwrap();
    let report = migrate_offline(request.clone(), clock()).unwrap();
    assert_eq!(
        (
            report.source.format_version,
            report.sequence,
            report.entries
        ),
        (1, 1, 2)
    );
    assert_eq!(report.destination_layout, layout(4));
    assert_eq!(report.destination_shard_usage.iter().sum::<usize>(), 264);
    assert_eq!(
        fs::read(aof_path(&request.source.directory)).unwrap(),
        original
    );
    let mut recovered =
        persistence::recover(request.destination, request.destination_store, clock()).unwrap();
    assert_eq!(recovered.metadata.layout, layout(4));
    assert_eq!(recovered.metadata.format_version, 2);
    assert_eq!(recovered.metadata.sequence, 1);
    assert_eq!(
        recovered.store.execute(DbCommand::Get {
            key: Bytes::from_static(b"a")
        }),
        Reply::Bulk(Some(Bytes::from_static(b"one")))
    );
    assert_eq!(
        recovered.store.execute(DbCommand::Ttl {
            key: Bytes::from_static(b"b"),
            milliseconds: true
        }),
        Reply::Integer(4000)
    );
    assert_eq!(
        recovered.store.execute(DbCommand::Get {
            key: Bytes::from_static(b"expired")
        }),
        Reply::Bulk(None)
    );
}

#[test]
fn migration_keeps_source_tail_and_checks_target_quota_before_creating_destination() {
    let parent = Directory::new();
    let mut request = options(&parent, 2);
    seed(
        &request.source.directory,
        None,
        vec![put(b"{x}a", b"one", None), put(b"{x}b", b"two", None)],
    );
    let path = aof_path(&request.source.directory);
    let mut original = fs::read(&path).unwrap();
    original.extend_from_slice(&[1, 2]);
    fs::write(&path, &original).unwrap();
    request.destination_store.max_dataset_bytes = 400;
    assert!(matches!(
        migrate_offline(request.clone(), clock()),
        Err(AofError::ShardQuota { .. })
    ));
    assert!(!request.destination.directory.exists());
    assert_eq!(fs::read(&path).unwrap(), original);
    request.destination_store = StoreConfig::default();
    let report = migrate_offline(request.clone(), clock()).unwrap();
    assert_eq!(report.source.incomplete_tail_bytes, 2);
    assert_eq!(fs::read(path).unwrap(), original);
    assert_eq!(fs::read_dir(request.source.directory).unwrap().count(), 2);
}

#[test]
fn migration_refuses_live_source_existing_destination_nested_path_and_corruption() {
    let parent = Directory::new();
    let request = options(&parent, 2);
    seed(
        &request.source.directory,
        None,
        vec![put(b"a", b"one", None)],
    );
    let active =
        persistence::recover(request.source.clone(), request.source_store, clock()).unwrap();
    assert!(matches!(
        migrate_offline(request.clone(), clock()),
        Err(AofError::Locked)
    ));
    drop(active);
    let mut nested = request.clone();
    nested.destination.directory = nested.source.directory.join("nested");
    assert!(migrate_offline(nested, clock()).is_err());
    fs::create_dir(&request.destination.directory).unwrap();
    fs::write(
        request.destination.directory.join("human.txt"),
        b"preserved",
    )
    .unwrap();
    assert!(migrate_offline(request.clone(), clock()).is_err());
    assert_eq!(
        fs::read(request.destination.directory.join("human.txt")).unwrap(),
        b"preserved"
    );
    let mut corrupt = request;
    corrupt.destination.directory = parent.child("other");
    let path = aof_path(&corrupt.source.directory);
    let mut bytes = fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(&path, &bytes).unwrap();
    assert!(migrate_offline(corrupt.clone(), clock()).is_err());
    assert!(!corrupt.destination.directory.exists());
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn destination_write_failure_cleans_only_its_new_files() {
    let parent = Directory::new();
    let mut request = options(&parent, 2);
    seed(
        &request.source.directory,
        None,
        vec![put(
            b"a",
            b"value-too-large-for-the-small-destination-record-limit-because-it-has-many-bytes",
            None,
        )],
    );
    let original = fs::read(aof_path(&request.source.directory)).unwrap();
    request.destination.limits.max_record_bytes = 64;
    assert!(matches!(
        migrate_offline(request.clone(), clock()),
        Err(AofError::Format(FormatError::Limit))
    ));
    assert!(!request.destination.directory.exists());
    assert_eq!(
        fs::read(aof_path(&request.source.directory)).unwrap(),
        original
    );
}

fn cli_arguments(request: &MigrationOptions) -> Vec<OsString> {
    vec![
        "--source".into(),
        request.source.directory.clone().into(),
        "--source-shards".into(),
        request.source.layout.shard_count.to_string().into(),
        "--source-routing".into(),
        "1".into(),
        "--destination".into(),
        request.destination.directory.clone().into(),
        "--shards".into(),
        request.destination.layout.shard_count.to_string().into(),
        "--routing".into(),
        "1".into(),
    ]
}

#[test]
fn cli_parser_requires_explicit_identity_and_preserves_native_paths() {
    let parent = Directory::new();
    let request = options(&parent, 3);
    let args = cli_arguments(&request);
    let parsed = options_from_args(args.clone()).unwrap();
    assert_eq!(parsed.destination.layout, layout(3));
    assert_eq!(parsed.source.directory, request.source.directory);
    assert!(options_from_args(Vec::<OsString>::new()).is_err());
    for extra in [
        ["--routing", "2"],
        ["--max-dataset-bytes", "-1"],
        ["--unknown", "1"],
    ] {
        let mut bad = args.clone();
        bad.extend(extra.map(OsString::from));
        assert!(options_from_args(bad).is_err());
    }
}

#[test]
fn real_cli_migrates_fixed_legacy_fixture_and_refuses_overwrite() {
    let parent = Directory::new();
    let request = options(&parent, 3);
    fs::create_dir(&request.source.directory).unwrap();
    fs::write(request.source.directory.join("writer.lock"), []).unwrap();
    let fixture: Vec<u8> = include_str!("fixtures/aof-v1.hex")
        .trim()
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    fs::write(aof_path(&request.source.directory), &fixture).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_sider-aof-migrate"));
    command.args(cli_arguments(&request));
    let output = process::run(&mut command, Duration::from_secs(10)).unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["source_format"], 1);
    assert_eq!(report["target_shards"], 3);
    assert_eq!(report["entries"], 1);
    let mut recovered = persistence::recover(
        request.destination.clone(),
        request.destination_store,
        clock(),
    )
    .unwrap();
    assert_eq!(
        recovered.store.execute(DbCommand::Get {
            key: Bytes::from_static(b"k")
        }),
        Reply::Bulk(Some(Bytes::from_static(b"v1")))
    );
    drop(recovered);
    let output = process::run(&mut command, Duration::from_secs(10)).unwrap();
    assert!(!output.status.success());
    assert_eq!(
        fs::read(aof_path(&request.source.directory)).unwrap(),
        fixture
    );
}
