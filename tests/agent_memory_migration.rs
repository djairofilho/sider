//! Explicit offline migration to record 2, without changing the legacy CLI.
#![forbid(unsafe_code)]

use bytes::Bytes;
use sha2::{Digest, Sha256};
use sider::command::{Command, Reply, SetCondition};
use sider::persistence::migration::{
    MigrationOptions, migrate_memory_offline, migrate_memory_offline_with_faults, migrate_offline,
};
use sider::persistence::{
    self, AofConfig, AofError, DurableLayout, FaultInjector, ReplicationMetadata, Role, format,
    memory_format,
};
use sider::storage::{
    Clock, Store, StoreConfig,
    memory::{Descriptor, Member, Operation, Scalar, Ttl},
};
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);
const GENERATION: &str = "generation-00000000000000000000.aof";
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-memory-migrate-{}-{}-{}",
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
        assert_eq!(
            self.0.canonicalize().unwrap().parent(),
            Some(std::env::temp_dir().canonicalize().unwrap().as_path())
        );
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
fn clock(ms: i64) -> Arc<dyn Clock> {
    Arc::new(FixedClock(ms, tokio::time::Instant::now()))
}
fn config(path: &Path, memory: bool) -> AofConfig {
    let config = AofConfig::new(path.to_owned());
    if memory {
        config.with_memory_profile()
    } else {
        config
    }
}
fn options(source: &Path, destination: PathBuf, memory: bool) -> MigrationOptions {
    MigrationOptions {
        source: config(source, memory),
        destination: config(&destination, true),
        source_store: StoreConfig::default(),
        destination_store: StoreConfig::default(),
    }
}
fn files(path: &Path) -> BTreeMap<std::ffi::OsString, Vec<u8>> {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let size = entry.metadata().unwrap().len();
            (
                entry.file_name(),
                if size == 0 {
                    Vec::new()
                } else {
                    fs::read(entry.path()).unwrap()
                },
            )
        })
        .collect()
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn command(store: &mut Store, args: &[&[u8]]) {
    let command = sider::command::parse(sider::resp::Frame::Array(Some(
        args.iter()
            .map(|arg| sider::resp::Frame::Bulk(Some(Bytes::copy_from_slice(arg))))
            .collect(),
    )))
    .unwrap();
    assert!(!matches!(store.execute(command), Reply::Error(_)));
}
fn legacy_store() -> Store {
    let mut store = Store::with_clock(clock(1000));
    for args in [
        vec![b"SET".as_slice(), b"\xff\0", b"\0\xff\r\n"],
        vec![b"HSET", b"h", b"\xff", b""],
        vec![b"RPUSH", b"l", b"\xff", b""],
        vec![b"SADD", b"s", b"\0", b"\xff"],
        vec![b"ZADD", b"z", b"-inf", b"\xff", b"1e-7", b"\0"],
        vec![b"SET", b"ttl", b"retained"],
        vec![b"PEXPIRE", b"ttl", b"2000"],
        vec![b"SET", b"expired", b"omitted"],
        vec![b"PEXPIRE", b"expired", b"10"],
    ] {
        command(&mut store, &args);
    }
    store
}
fn seed_legacy(path: &Path, version: u32) {
    fs::create_dir(path).unwrap();
    fs::write(path.join("writer.lock"), []).unwrap();
    fs::write(
        path.join("replication-install-unowned.tmp"),
        b"preserved residue",
    )
    .unwrap();
    let snapshot = legacy_store().snapshot();
    let mut file = fs::File::create(path.join(GENERATION)).unwrap();
    match version {
        1 => format::write_header(&mut file, 9).unwrap(),
        2 => format::write_header_with_layout(&mut file, 9, DurableLayout::default()).unwrap(),
        3 => format::write_header_with_replication(
            &mut file,
            9,
            DurableLayout::default(),
            Some(ReplicationMetadata {
                role: Role::Replica,
                epoch: [7; 16],
            }),
        )
        .unwrap(),
        _ => unreachable!(),
    }
    let mut digest = 0;
    for image in &snapshot {
        let bytes = format::encode(
            &format::Record::Snapshot(image.clone()),
            format::Limits::default(),
        )
        .unwrap();
        digest = format::snapshot_digest(digest, &bytes);
        file.write_all(&bytes).unwrap();
    }
    file.write_all(
        &format::encode(
            &format::Record::Seal {
                sequence: 9,
                entries: snapshot.len() as u64,
                digest,
            },
            format::Limits::default(),
        )
        .unwrap(),
    )
    .unwrap();
    file.sync_all().unwrap();
}
fn descriptor() -> Descriptor {
    Descriptor {
        dimension: 2,
        provider: Bytes::from_static(b"p"),
        model: Bytes::from_static(b"m"),
        revision: Bytes::from_static(b"r"),
        transform: Bytes::from_static(b"t"),
    }
}
fn member(id: &'static [u8]) -> Member {
    Member {
        id: Bytes::from_static(id),
        namespace: Bytes::from_static(b"\xff\0"),
        content: Bytes::from_static(b"\0\r\n\xff"),
        vector: Bytes::from([(-0.0f32).to_le_bytes(), f32::from_bits(1).to_le_bytes()].concat()),
        deadline: None,
        metadata: BTreeMap::from([
            (Bytes::from_static(b"b"), Scalar::Bool(true)),
            (Bytes::from_static(b"i"), Scalar::Integer(i64::MIN)),
            (
                Bytes::from_static(b"x"),
                Scalar::Bytes(Bytes::from_static(b"\0\xff")),
            ),
        ]),
        provenance: BTreeMap::from([(
            Bytes::from_static(b"source"),
            Bytes::from_static(b"\xff\0"),
        )]),
    }
}
fn memory(store: &mut Store, key: &'static [u8], operation: Operation) -> Reply {
    store.execute(Command::Memory {
        key: Bytes::from_static(key),
        operation: Box::new(operation),
    })
}
fn seed_memory(path: &Path, version: u32) -> Vec<memory_format::Mutation> {
    fs::create_dir(path).unwrap();
    fs::write(path.join("writer.lock"), []).unwrap();
    let mut store = legacy_store();
    for key in [b"c".as_slice(), b"empty", b"outer"] {
        assert_eq!(
            memory(&mut store, key, Operation::Create(descriptor())),
            Reply::Ok
        );
    }
    command(&mut store, &[b"PEXPIRE", b"outer", b"10"]);
    for id in [b"expired".as_slice(), b"\xff\0"] {
        assert_eq!(
            memory(
                &mut store,
                b"c",
                Operation::Put {
                    descriptor: descriptor(),
                    member: Box::new(member(id)),
                    ttl: Ttl::After(20),
                    condition: SetCondition::Always
                }
            ),
            Reply::Integer(1)
        );
    }
    let images = store.snapshot_memory();
    let prepared = store.prepare_memory(
        Bytes::from_static(b"c"),
        Operation::Persist {
            id: Bytes::from_static(b"\xff\0"),
        },
    );
    let delta = prepared.resolved().unwrap().clone();
    assert_eq!(store.apply_memory(prepared), Reply::Integer(1));
    let mut file = fs::File::create(path.join(GENERATION)).unwrap();
    file.write_all(
        &memory_format::encode_header(memory_format::Header {
            sequence: 9,
            layout: DurableLayout::default(),
            replication: if version == 5 {
                Some(ReplicationMetadata {
                    role: Role::Replica,
                    epoch: [7; 16],
                })
            } else {
                None
            },
        })
        .unwrap(),
    )
    .unwrap();
    let mut digest = 0;
    for image in &images {
        let bytes = memory_format::encode(
            &memory_format::Record::Snapshot(image.clone()),
            format::Limits::default(),
        )
        .unwrap();
        digest = format::snapshot_digest(digest, &bytes);
        file.write_all(&bytes).unwrap();
    }
    file.write_all(
        &memory_format::encode(
            &memory_format::Record::Seal {
                sequence: 9,
                entries: images.len() as u64,
                digest,
            },
            format::Limits::default(),
        )
        .unwrap(),
    )
    .unwrap();
    file.write_all(
        &memory_format::encode(
            &memory_format::Record::Batch {
                sequence: 10,
                origin: sider::storage::MutationOrigin::Client,
                mutations: vec![delta],
            },
            format::Limits::default(),
        )
        .unwrap(),
    )
    .unwrap();
    file.sync_all().unwrap();
    store
        .snapshot_memory()
        .into_iter()
        .filter(|image| image.key() != b"expired".as_slice())
        .collect()
}

#[test]
fn memory_migration_hashes_actual_recovered_generation_and_preserves_older_files_and_residues() {
    let directory = Directory::new();
    let source = directory.child("source");
    seed_legacy(&source, 2);
    let selected = "generation-00000000000000000009.aof";
    let bytes = fs::read(source.join(GENERATION)).unwrap();
    fs::write(source.join(selected), &bytes).unwrap();
    let mut old = bytes.clone();
    *old.last_mut().unwrap() ^= 1;
    fs::write(source.join(GENERATION), &old).unwrap();
    fs::write(
        source.join("replication-install-00000000000000000099.tmp"),
        b"ignored install residue",
    )
    .unwrap();
    let original = files(&source);
    let report = migrate_memory_offline(
        options(&source, directory.child("target"), false),
        clock(2000),
    )
    .unwrap();
    assert_eq!(report.source_generation, std::ffi::OsString::from(selected));
    assert_eq!(report.source_sha256, digest(&bytes));
    assert_ne!(report.source_sha256, digest(&old));
    assert_eq!(files(&source), original);
}

#[test]
fn memory_migration_upgrades_legacy_headers_and_preserves_binary_types_ttl_sequence_and_hashes() {
    for version in [1, 2, 3] {
        let directory = Directory::new();
        let source = directory.child("source");
        seed_legacy(&source, version);
        let original = files(&source);
        let mut request = options(&source, directory.child("memory"), false);
        request.destination.layout.shard_count = 4;
        let report = migrate_memory_offline(request.clone(), clock(2000)).unwrap();
        assert_eq!(report.migration.source.format_version, version);
        assert_eq!(
            (
                report.source_record_version,
                report.destination_header_version,
                report.destination_record_version
            ),
            (1, 4, 2)
        );
        assert_eq!(
            (
                report.migration.sequence,
                report.migration.entries,
                report.memory_collections,
                report.memory_members
            ),
            (9, 6, 0, 0)
        );
        assert_eq!(
            report.source_sha256,
            digest(&original[&std::ffi::OsString::from(GENERATION)])
        );
        assert_eq!(
            report.destination_sha256,
            digest(&fs::read(request.destination.directory.join(GENERATION)).unwrap())
        );
        assert_eq!(files(&source), original);
        let recovered =
            persistence::recover(request.destination, request.destination_store, clock(2500))
                .unwrap();
        assert_eq!(recovered.metadata.replication, None);
        assert_eq!(recovered.metadata.sequence, 9);
        assert_eq!(recovered.metadata.layout.shard_count, 4);
        let snapshot = recovered.store.snapshot();
        assert_eq!(snapshot.len(), 6);
        assert!(snapshot.iter().any(|image| matches!(image, sider::storage::Mutation::Put { key, expires_at_unix_ms: Some(3000), .. } if key.as_ref() == b"ttl")));
        let mut expected = legacy_store();
        command(&mut expected, &[b"DEL", b"expired"]);
        assert_eq!(snapshot, expected.snapshot());
    }
}

#[test]
fn memory_migration_rehashes_record2_images_and_preserves_retained_expiry_and_guarded_history() {
    for version in [4, 5] {
        let directory = Directory::new();
        let source = directory.child("source");
        let expected = seed_memory(&source, version);
        let original = files(&source);
        let mut request = options(&source, directory.child("new-layout"), true);
        request.destination.layout.shard_count = 4;
        let report = migrate_memory_offline(request.clone(), clock(2000)).unwrap();
        assert_eq!(
            (
                report.migration.sequence,
                report.source_record_version,
                report.memory_collections,
                report.memory_members
            ),
            (10, 2, 3, 2)
        );
        assert_eq!(files(&source), original);
        let recovered = persistence::recover(
            request.destination.clone(),
            request.destination_store,
            clock(2500),
        )
        .unwrap();
        assert_eq!(recovered.metadata.replication, None);
        assert_eq!(recovered.store.snapshot_memory(), expected);
        assert_eq!(
            recovered.metadata.shard_usage,
            report.migration.destination_shard_usage
        );
        drop(recovered);
        let bytes = fs::read(request.destination.directory.join(GENERATION)).unwrap();
        assert!(matches!(
            format::read_header_with_layout(bytes.as_slice()),
            Err(format::FormatError::Version(4))
        ));
    }
}

#[test]
fn memory_migration_requires_explicit_profiles_and_keeps_default_migration_legacy() {
    let directory = Directory::new();
    let absent = directory.child("absent");
    let mut request = options(&absent, directory.child("target"), true);
    request.destination = config(&request.destination.directory, false);
    assert!(matches!(
        migrate_memory_offline(request.clone(), clock(1000)),
        Err(AofError::Migration(_))
    ));
    assert!(!request.destination.directory.exists());
    let source = directory.child("legacy");
    seed_legacy(&source, 2);
    request = options(&source, directory.child("default"), false);
    assert!(matches!(
        migrate_offline(request.clone(), clock(1000)),
        Err(AofError::Migration(_))
    ));
    request.destination = config(&request.destination.directory, false);
    migrate_offline(request.clone(), clock(1000)).unwrap();
    assert_eq!(
        format::read_header_with_layout(
            fs::read(request.destination.directory.join(GENERATION))
                .unwrap()
                .as_slice()
        )
        .unwrap()
        .format_version,
        2
    );
    let wrong_profile = options(&source, directory.child("wrong"), true);
    let original = files(&source);
    assert!(matches!(
        migrate_memory_offline(wrong_profile.clone(), clock(1000)),
        Err(AofError::Migration(_))
    ));
    assert!(!wrong_profile.destination.directory.exists());
    assert_eq!(files(&source), original);
}

#[test]
fn memory_migration_preserves_incomplete_tail_and_refuses_lock_corruption_and_unknown_versions() {
    let directory = Directory::new();
    let source = directory.child("source");
    seed_legacy(&source, 2);
    let mut bytes = fs::read(source.join(GENERATION)).unwrap();
    bytes.extend_from_slice(&[1, 2]);
    fs::write(source.join(GENERATION), &bytes).unwrap();
    let original = files(&source);
    let request = options(&source, directory.child("target"), false);
    // Active recovery must use a complete copy so it cannot repair the test source.
    let active_dir = directory.child("active");
    seed_legacy(&active_dir, 2);
    let active_config = config(&active_dir, false);
    let active =
        persistence::recover(active_config.clone(), StoreConfig::default(), clock(1000)).unwrap();
    let locked = options(&active_dir, directory.child("locked-target"), false);
    assert!(matches!(
        migrate_memory_offline(locked.clone(), clock(1000)),
        Err(AofError::Locked)
    ));
    assert!(!locked.destination.directory.exists());
    drop(active);
    let report = migrate_memory_offline(request, clock(2000)).unwrap();
    assert_eq!(report.migration.source.incomplete_tail_bytes, 2);
    assert_eq!(files(&source), original);
    bytes[32 + 12] ^= 1;
    fs::write(source.join(GENERATION), &bytes).unwrap();
    let bad = options(&source, directory.child("bad"), false);
    assert!(migrate_memory_offline(bad.clone(), clock(1000)).is_err());
    assert!(!bad.destination.directory.exists());
    assert_eq!(fs::read(source.join(GENERATION)).unwrap(), bytes);
    for version in [6u32, 7, 99] {
        let mut prefix = format::MAGIC.to_vec();
        prefix.extend_from_slice(&version.to_le_bytes());
        fs::write(source.join(GENERATION), &prefix).unwrap();
        let bad = options(&source, directory.child(&format!("v{version}")), false);
        assert!(
            matches!(migrate_memory_offline(bad.clone(), clock(1000)), Err(AofError::Format(format::FormatError::Version(found))) if found == version)
        );
        assert!(!bad.destination.directory.exists());
        assert_eq!(fs::read(source.join(GENERATION)).unwrap(), prefix);
    }
}

#[test]
fn memory_migration_preflights_record_limit_and_local_quota_without_output_or_source_effects() {
    let directory = Directory::new();
    let source = directory.child("source");
    seed_memory(&source, 4);
    let original = files(&source);
    let mut request = options(&source, directory.child("small-record"), true);
    request.destination.limits.max_record_bytes = 64;
    assert!(matches!(
        migrate_memory_offline(request.clone(), clock(2000)),
        Err(AofError::Format(format::FormatError::Limit))
    ));
    assert!(!request.destination.directory.exists());
    request.destination = config(&directory.child("small-quota"), true);
    request.destination.layout.shard_count = 4;
    request.destination_store.max_dataset_bytes = 1000;
    assert!(matches!(
        migrate_memory_offline(request.clone(), clock(2000)),
        Err(AofError::ShardQuota { .. })
    ));
    assert!(!request.destination.directory.exists());
    assert_eq!(files(&source), original);
}

struct Fail(&'static str);
impl FaultInjector for Fail {
    fn hit(&self, point: &'static str) -> io::Result<()> {
        if self.0 == point {
            Err(io::Error::other("injected"))
        } else {
            Ok(())
        }
    }
}
struct Partial;
impl FaultInjector for Partial {
    fn hit(&self, _: &'static str) -> io::Result<()> {
        Ok(())
    }
    fn write_append(&self, file: &mut fs::File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(&bytes[..bytes.len() / 2])?;
        Err(io::Error::other("partial write"))
    }
}
struct Corrupt;
impl FaultInjector for Corrupt {
    fn hit(&self, _: &'static str) -> io::Result<()> {
        Ok(())
    }
    fn write_append(&self, file: &mut fs::File, bytes: &[u8]) -> io::Result<()> {
        let mut bytes = bytes.to_vec();
        *bytes.last_mut().unwrap() ^= 1;
        file.write_all(&bytes)
    }
}
#[test]
fn memory_migration_failures_clean_only_owned_outputs_before_and_after_publication() {
    let directory = Directory::new();
    let source = directory.child("source");
    seed_legacy(&source, 3);
    let original = files(&source);
    let mut faults: Vec<Arc<dyn FaultInjector>> = [
        "memory_migration_before_append",
        "memory_migration_before_sync",
        "memory_migration_after_sync",
        "memory_migration_before_publication",
        "memory_migration_after_publication",
        "memory_migration_after_directory_sync",
    ]
    .into_iter()
    .map(|point| Arc::new(Fail(point)) as Arc<dyn FaultInjector>)
    .collect();
    faults.push(Arc::new(Partial));
    faults.push(Arc::new(Corrupt));
    for (index, faults) in faults.into_iter().enumerate() {
        let request = options(&source, directory.child(&format!("fault{index}")), false);
        assert!(migrate_memory_offline_with_faults(request.clone(), clock(2000), faults).is_err());
        assert!(!request.destination.directory.exists());
        assert_eq!(files(&source), original);
    }
    let existing = directory.child("existing");
    fs::create_dir(&existing).unwrap();
    fs::write(existing.join("human.txt"), b"untouched").unwrap();
    assert!(
        migrate_memory_offline(options(&source, existing.clone(), false), clock(2000)).is_err()
    );
    assert_eq!(fs::read(existing.join("human.txt")).unwrap(), b"untouched");
    let nested = source.join("nested");
    assert!(migrate_memory_offline(options(&source, nested.clone(), false), clock(2000)).is_err());
    assert!(!nested.exists());
}

struct AssertLock {
    source: AofConfig,
    checked: AtomicBool,
}
impl FaultInjector for AssertLock {
    fn hit(&self, point: &'static str) -> io::Result<()> {
        if point == "memory_migration_before_append"
            || point == "memory_migration_after_directory_sync"
        {
            assert!(matches!(
                persistence::recover(self.source.clone(), StoreConfig::default(), clock(2000)),
                Err(AofError::Locked)
            ));
            self.checked.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}
#[test]
fn memory_migration_keeps_source_lock_through_final_publication() {
    let directory = Directory::new();
    let source = directory.child("source");
    seed_legacy(&source, 2);
    let request = options(&source, directory.child("target"), false);
    let original = files(&source);
    let faults = Arc::new(AssertLock {
        source: request.source.clone(),
        checked: AtomicBool::new(false),
    });
    migrate_memory_offline_with_faults(request.clone(), clock(2000), faults.clone()).unwrap();
    assert!(faults.checked.load(Ordering::SeqCst));
    assert_eq!(files(&source), original);
    let recovered =
        persistence::recover(request.source, StoreConfig::default(), clock(2000)).unwrap();
    drop(recovered);
}

#[test]
fn memory_migration_old_reader_refuses_every_first_record_prefix_without_repair_or_residue_cleanup()
{
    let directory = Directory::new();
    let source = directory.child("source");
    seed_memory(&source, 4);
    let request = options(&source, directory.child("target"), true);
    migrate_memory_offline(request.clone(), clock(2000)).unwrap();
    let bytes = fs::read(request.destination.directory.join(GENERATION)).unwrap();
    let header = memory_format::HEADER_BYTES;
    let payload = u32::from_le_bytes(bytes[header..header + 4].try_into().unwrap()) as usize;
    let first_record_end = header + 12 + payload;
    let refusal = directory.child("old-reader");
    fs::create_dir(&refusal).unwrap();
    fs::write(refusal.join("writer.lock"), []).unwrap();
    fs::write(refusal.join("replication-install-kept.tmp"), b"kept").unwrap();
    let legacy = config(&refusal, false);
    for end in header..=first_record_end {
        fs::write(refusal.join(GENERATION), &bytes[..end]).unwrap();
        let original = files(&refusal);
        assert!(matches!(
            persistence::recover(legacy.clone(), StoreConfig::default(), clock(2000)),
            Err(AofError::Format(format::FormatError::Version(4)))
        ));
        assert_eq!(files(&refusal), original);
    }
    assert!(matches!(
        migrate_offline(
            MigrationOptions {
                source: request.destination,
                destination: config(&directory.child("downgrade"), false),
                source_store: StoreConfig::default(),
                destination_store: StoreConfig::default()
            },
            clock(2000)
        ),
        Err(AofError::Migration(_))
    ));
    assert!(!directory.child("downgrade").exists());
}
