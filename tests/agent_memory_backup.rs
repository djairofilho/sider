//! Explicit record-2 offline export, archive verification and atomic restoration.
#![forbid(unsafe_code)]

#[path = "common/process.rs"]
mod process;
#[path = "common/wire.rs"]
mod wire;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use sider::command::{Command, Reply, SetCondition};
use sider::persistence::{
    self, AofConfig, DurableLayout, ReplicationMetadata, Role, backup, format, memory_format,
};
use sider::storage::{
    Clock, Store, StoreConfig,
    memory::{Descriptor, Member, Operation, Scalar, Ttl},
};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-memory-backup-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
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
            (
                Bytes::from_static(b"bytes"),
                Scalar::Bytes(Bytes::from_static(b"\0\xff")),
            ),
            (Bytes::from_static(b"int"), Scalar::Integer(i64::MIN)),
            (Bytes::from_static(b"bool"), Scalar::Bool(true)),
        ]),
        provenance: BTreeMap::from([(
            Bytes::from_static(b"source"),
            Bytes::from_static(b"\xff\0"),
        )]),
    }
}
fn execute(store: &mut Store, key: &'static [u8], operation: Operation) -> Reply {
    store.execute(Command::Memory {
        key: Bytes::from_static(key),
        operation: Box::new(operation),
    })
}
fn config(path: &Path) -> AofConfig {
    AofConfig::new(path.to_owned()).with_memory_profile()
}
const GENERATION: &str = "generation-00000000000000000000.aof";

fn seed(path: &Path) -> Vec<memory_format::Mutation> {
    fs::create_dir(path).unwrap();
    fs::write(path.join("writer.lock"), []).unwrap();
    let mut store = Store::with_clock(clock(1000));
    for key in [b"c".as_slice(), b"empty", b"outer"] {
        assert_eq!(
            execute(&mut store, key, Operation::Create(descriptor())),
            Reply::Ok
        );
    }
    for (id, ttl) in [
        (b"elapsed".as_slice(), Ttl::After(20)),
        (b"\xff\0", Ttl::After(1000)),
    ] {
        assert_eq!(
            execute(
                &mut store,
                b"c",
                Operation::Put {
                    descriptor: descriptor(),
                    member: Box::new(member(id)),
                    ttl,
                    condition: SetCondition::Always
                }
            ),
            Reply::Integer(1)
        );
    }
    for args in [
        vec![b"SET".as_slice(), b"s", b"\0\xff"],
        vec![b"HSET", b"h", b"\xff", b""],
        vec![b"RPUSH", b"l", b"\xff", b""],
        vec![b"SADD", b"t", b"\0", b"\xff"],
        vec![b"ZADD", b"z", b"-inf", b"\xff", b"1e-7", b"\0"],
        vec![b"PEXPIRE", b"outer", b"10"],
    ] {
        let command = sider::command::parse(sider::resp::Frame::Array(Some(
            args.into_iter()
                .map(|arg| sider::resp::Frame::Bulk(Some(Bytes::copy_from_slice(arg))))
                .collect(),
        )))
        .unwrap();
        assert!(!matches!(store.execute(command), Reply::Error(_)));
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
            sequence: 11,
            layout: DurableLayout::default(),
            replication: Some(ReplicationMetadata {
                role: Role::Replica,
                epoch: [7; 16],
            }),
        })
        .unwrap(),
    )
    .unwrap();
    let mut digest = 0;
    for image in &images {
        let record = memory_format::encode(
            &memory_format::Record::Snapshot(image.clone()),
            format::Limits::default(),
        )
        .unwrap();
        digest = format::snapshot_digest(digest, &record);
        file.write_all(&record).unwrap();
    }
    file.write_all(
        &memory_format::encode(
            &memory_format::Record::Seal {
                sequence: 11,
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
                sequence: 12,
                origin: sider::storage::MutationOrigin::Client,
                mutations: vec![delta],
            },
            format::Limits::default(),
        )
        .unwrap(),
    )
    .unwrap();
    file.sync_all().unwrap();
    store.snapshot_memory()
}
fn options(source: &Path, destination: PathBuf) -> backup::OfflineMemoryExportOptions {
    backup::OfflineMemoryExportOptions {
        source: config(source),
        destination,
        source_sha: "0123456789012345678901234567890123456789".into(),
        limits: backup::Limits::default(),
    }
}
fn restore_options(source: PathBuf, destination: PathBuf) -> backup::RestoreOptions {
    backup::RestoreOptions {
        source,
        destination,
        layout: DurableLayout::default(),
        limits: backup::Limits::default(),
    }
}

#[test]
fn memory_backup_offline_roundtrip_preserves_images_bits_deadlines_and_clears_role() {
    let directory = Directory::new();
    let source = directory.child("source");
    let expected = seed(&source);
    let original = fs::read(source.join(GENERATION)).unwrap();
    let archive = directory.child("archive");
    let manifest =
        backup::export_memory_offline(options(&source, archive.clone()), clock(1100)).unwrap();
    assert_eq!(manifest.profile, backup::Profile::Memory);
    let json = manifest.json();
    assert_eq!(json["schema_version"], 2);
    assert_eq!(json["aof_header_version"], 4);
    assert_eq!(json["record_version"], 2);
    assert_eq!(manifest.cursor.sequence, 12);
    assert_eq!(manifest.cursor.epoch, [7; 16]);
    assert_eq!(fs::read(source.join(GENERATION)).unwrap(), original);
    let verified = backup::verify_memory(
        &archive,
        DurableLayout::default(),
        backup::Limits::default(),
        clock(2000),
    )
    .unwrap();
    assert_eq!(verified.live_entries, expected.len() as u64 - 1);
    assert!(
        backup::verify(
            &archive,
            DurableLayout::default(),
            backup::Limits::default(),
            clock(1100)
        )
        .is_err()
    );
    let destination = directory.child("restored");
    backup::restore_memory(
        restore_options(archive.clone(), destination.clone()),
        clock(2000),
    )
    .unwrap();
    let recovered =
        persistence::recover(config(&destination), StoreConfig::default(), clock(2000)).unwrap();
    assert_eq!(recovered.metadata.format_version, 4);
    assert_eq!(recovered.metadata.replication, None);
    assert_eq!(recovered.metadata.sequence, 12);
    assert_eq!(recovered.store.snapshot_memory(), expected);
    let mut store = recovered.store;
    assert_eq!(
        execute(
            &mut store,
            b"c",
            Operation::Get {
                id: Bytes::from_static(b"elapsed")
            }
        ),
        Reply::Bulk(None)
    );
    assert!(matches!(
        execute(
            &mut store,
            b"c",
            Operation::Get {
                id: Bytes::from_static(b"\xff\0")
            }
        ),
        Reply::Array(_)
    ));
    assert_eq!(
        execute(&mut store, b"outer", Operation::Info),
        Reply::Bulk(None)
    );
    assert!(matches!(
        execute(&mut store, b"empty", Operation::Info),
        Reply::Array(_)
    ));
}

fn archive(directory: &Directory) -> PathBuf {
    let source = directory.child("source");
    seed(&source);
    let archive = directory.child("archive");
    backup::export_memory_offline(options(&source, archive.clone()), clock(1100)).unwrap();
    archive
}

fn refresh_checksums(archive: &Path, value: &serde_json::Value) {
    let manifest = serde_json::to_vec_pretty(value).unwrap();
    fs::write(archive.join(backup::MANIFEST), &manifest).unwrap();
    fs::write(
        archive.join(backup::CHECKSUMS),
        format!(
            "{:x}  {}\n{}  {}\n",
            Sha256::digest(&manifest),
            backup::MANIFEST,
            value["snapshot"]["sha256"].as_str().unwrap(),
            backup::SNAPSHOT
        ),
    )
    .unwrap();
}
fn rewrite_snapshot(archive: &Path, bytes: &[u8]) {
    fs::write(archive.join(backup::SNAPSHOT), bytes).unwrap();
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(archive.join(backup::MANIFEST)).unwrap()).unwrap();
    manifest["snapshot"]["bytes"] = (bytes.len() as u64).into();
    manifest["snapshot"]["sha256"] = format!("{:x}", Sha256::digest(bytes)).into();
    refresh_checksums(archive, &manifest);
}
fn file_set(path: &Path) -> BTreeMap<std::ffi::OsString, Vec<u8>> {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let bytes = if entry.metadata().unwrap().len() == 0 {
                // Windows mandatory writer locks reject even an empty ReadFile.
                Vec::new()
            } else {
                fs::read(entry.path()).unwrap()
            };
            (entry.file_name(), bytes)
        })
        .collect()
}

#[test]
fn memory_backup_manifest_allowlist_rejects_before_snapshot_io_or_destination_creation() {
    let directory = Directory::new();
    let archive = archive(&directory);
    let original: serde_json::Value =
        serde_json::from_slice(&fs::read(archive.join(backup::MANIFEST)).unwrap()).unwrap();
    fs::remove_file(archive.join(backup::SNAPSHOT)).unwrap();
    fs::remove_file(archive.join(backup::CHECKSUMS)).unwrap();
    for (schema, header, record) in [(1, 2, 1), (2, 5, 2), (2, 4, 1), (3, 7, 2), (99, 4, 2)] {
        let mut manifest = original.clone();
        manifest["schema_version"] = schema.into();
        manifest["aof_header_version"] = header.into();
        manifest["record_version"] = record.into();
        fs::write(
            archive.join(backup::MANIFEST),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            backup::verify_memory(
                &archive,
                DurableLayout::default(),
                backup::Limits::default(),
                clock(1100)
            ),
            Err(backup::Error::Invalid("manifest schema"))
        ));
        let target = directory.child("rejected");
        assert!(
            backup::restore_memory(
                restore_options(archive.clone(), target.clone()),
                clock(1100)
            )
            .is_err()
        );
        assert!(!target.exists());
    }
    let mut manifest = original;
    manifest["sider_version"] = "incompatible".into();
    fs::write(
        archive.join(backup::MANIFEST),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        backup::verify_memory(
            &archive,
            DurableLayout::default(),
            backup::Limits::default(),
            clock(1100)
        ),
        Err(backup::Error::Invalid("manifest version or limits"))
    ));
}

#[test]
fn memory_backup_actual_header_preflight_precedes_hash_and_refuses_roles() {
    let directory = Directory::new();
    let archive = archive(&directory);
    let original = fs::read(archive.join(backup::SNAPSHOT)).unwrap();
    for version in [1u32, 2, 3, 6, 7, 99] {
        let mut prefix = original[..12].to_vec();
        prefix[8..12].copy_from_slice(&version.to_le_bytes());
        fs::write(archive.join(backup::SNAPSHOT), &prefix).unwrap();
        assert!(
            matches!(backup::verify_memory(&archive, DurableLayout::default(), backup::Limits::default(), clock(1100)), Err(backup::Error::Format(format::FormatError::Version(actual))) if actual == version)
        );
        assert_eq!(fs::read(archive.join(backup::SNAPSHOT)).unwrap(), prefix);
    }
    let mut bytes = memory_format::encode_header(memory_format::Header {
        sequence: 12,
        layout: DurableLayout::default(),
        replication: Some(ReplicationMetadata {
            role: Role::Replica,
            epoch: [7; 16],
        }),
    })
    .unwrap();
    bytes.extend_from_slice(&original[memory_format::HEADER_BYTES..]);
    rewrite_snapshot(&archive, &bytes);
    assert!(matches!(
        backup::verify_memory(
            &archive,
            DurableLayout::default(),
            backup::Limits::default(),
            clock(1100)
        ),
        Err(backup::Error::Invalid("snapshot header identity"))
    ));
}

#[test]
fn memory_backup_rejects_corruption_truncation_bad_seal_order_and_transport_digest() {
    let directory = Directory::new();
    let archive = archive(&directory);
    let original = fs::read(archive.join(backup::SNAPSHOT)).unwrap();
    let original_files = file_set(&archive);
    // Prefixes include incomplete header, image, and seal, plus a missing final byte.
    for length in [1, 12, 35, 36, 40, original.len() - 34, original.len() - 1] {
        rewrite_snapshot(&archive, &original[..length]);
        assert!(
            backup::verify_memory(
                &archive,
                DurableLayout::default(),
                backup::Limits::default(),
                clock(1100)
            )
            .is_err()
        );
    }
    let mut corrupt = original.clone();
    corrupt[45] ^= 1;
    fs::write(archive.join(backup::SNAPSHOT), &corrupt).unwrap();
    assert!(matches!(
        backup::verify_memory(
            &archive,
            DurableLayout::default(),
            backup::Limits::default(),
            clock(1100)
        ),
        Err(backup::Error::Invalid("snapshot checksum or size"))
    ));
    rewrite_snapshot(&archive, &corrupt);
    assert!(
        backup::verify_memory(
            &archive,
            DurableLayout::default(),
            backup::Limits::default(),
            clock(1100)
        )
        .is_err()
    );
    let mut bytes = original[..original.len() - 33].to_vec();
    bytes.extend_from_slice(
        &memory_format::encode(
            &memory_format::Record::Seal {
                sequence: 12,
                entries: 8,
                digest: 0xdeadbeef,
            },
            format::Limits::default(),
        )
        .unwrap(),
    );
    rewrite_snapshot(&archive, &bytes);
    assert!(
        backup::verify_memory(
            &archive,
            DurableLayout::default(),
            backup::Limits::default(),
            clock(1100)
        )
        .is_err()
    );
    let mut extra = original.clone();
    extra.push(0);
    rewrite_snapshot(&archive, &extra);
    assert!(
        backup::verify_memory(
            &archive,
            DurableLayout::default(),
            backup::Limits::default(),
            clock(1100)
        )
        .is_err()
    );
    let mut reader = std::io::Cursor::new(&original[memory_format::HEADER_BYTES..]);
    let mut images = Vec::new();
    while let memory_format::Next::Record(record) =
        memory_format::read_record(&mut reader, format::Limits::default()).unwrap()
    {
        if let memory_format::Record::Snapshot(image) = *record {
            images.push(image);
        } else {
            break;
        }
    }
    images.swap(0, 1);
    let mut bytes = original[..memory_format::HEADER_BYTES].to_vec();
    let mut digest = 0;
    for image in images {
        let encoded = memory_format::encode(
            &memory_format::Record::Snapshot(image),
            format::Limits::default(),
        )
        .unwrap();
        digest = format::snapshot_digest(digest, &encoded);
        bytes.extend_from_slice(&encoded);
    }
    bytes.extend_from_slice(
        &memory_format::encode(
            &memory_format::Record::Seal {
                sequence: 12,
                entries: 8,
                digest,
            },
            format::Limits::default(),
        )
        .unwrap(),
    );
    rewrite_snapshot(&archive, &bytes);
    assert!(matches!(
        backup::verify_memory(
            &archive,
            DurableLayout::default(),
            backup::Limits::default(),
            clock(1100)
        ),
        Err(backup::Error::Invalid("key order or duplicate"))
    ));
    for (name, bytes) in &original_files {
        fs::write(archive.join(name), bytes).unwrap();
    }
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&original_files[std::ffi::OsStr::new(backup::MANIFEST)]).unwrap();
    manifest["transport_digest_crc32"] = 0.into();
    refresh_checksums(&archive, &manifest);
    assert!(
        backup::verify_memory(
            &archive,
            DurableLayout::default(),
            backup::Limits::default(),
            clock(1100)
        )
        .is_err()
    );
    manifest["transport_digest_crc32"] = serde_json::from_slice::<serde_json::Value>(
        &original_files[std::ffi::OsStr::new(backup::MANIFEST)],
    )
    .unwrap()["transport_digest_crc32"]
        .clone();
    refresh_checksums(&archive, &manifest);
    fs::write(archive.join(backup::CHECKSUMS), b"bad\n").unwrap();
    assert!(
        backup::verify_memory(
            &archive,
            DurableLayout::default(),
            backup::Limits::default(),
            clock(1100)
        )
        .is_err()
    );
}

#[test]
fn memory_backup_source_lock_incomplete_tail_and_unsupported_profile_preserve_all_source_files() {
    let directory = Directory::new();
    let source = directory.child("source");
    seed(&source);
    fs::write(source.join("replication-install-unowned.tmp"), b"keep").unwrap();
    let original = file_set(&source);
    let held = persistence::recover(config(&source), StoreConfig::default(), clock(1100)).unwrap();
    let locked_files = file_set(&source);
    let target = directory.child("locked");
    assert!(matches!(
        backup::export_memory_offline(options(&source, target.clone()), clock(1100)),
        Err(backup::Error::Persistence(persistence::AofError::Locked))
    ));
    assert!(!target.exists());
    assert_eq!(file_set(&source), locked_files);
    drop(held);
    // Regular recovery may clean an installation residue; add it back before read-only checks.
    fs::write(source.join("replication-install-unowned.tmp"), b"keep").unwrap();
    let bytes = &original[std::ffi::OsStr::new(GENERATION)];
    fs::write(source.join(GENERATION), &bytes[..bytes.len() - 1]).unwrap();
    let incomplete = file_set(&source);
    assert!(matches!(
        backup::export_memory_offline(options(&source, directory.child("tail")), clock(1100)),
        Err(backup::Error::Invalid("source has incomplete AOF tail"))
    ));
    assert_eq!(file_set(&source), incomplete);
    assert!(!directory.child("tail").exists());
    let mut prefix = bytes[..12].to_vec();
    prefix[8..12].copy_from_slice(&99u32.to_le_bytes());
    fs::write(source.join(GENERATION), &prefix).unwrap();
    let unsupported = file_set(&source);
    assert!(
        backup::export_memory_offline(options(&source, directory.child("version")), clock(1100))
            .is_err()
    );
    assert_eq!(file_set(&source), unsupported);
    assert!(!directory.child("version").exists());
    let mut wrong = options(
        &directory.child("absent-source"),
        directory.child("profile"),
    );
    wrong.source = AofConfig::new(directory.child("absent-source"));
    assert!(matches!(
        backup::export_memory_offline(wrong, clock(1100)),
        Err(backup::Error::Invalid("memory export profile or capacity"))
    ));
}

struct FailAt(&'static str);
impl persistence::FaultInjector for FailAt {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if point == self.0 {
            Err(std::io::Error::other("injected backup boundary"))
        } else {
            Ok(())
        }
    }
}
struct PartialWrite;
impl persistence::FaultInjector for PartialWrite {
    fn hit(&self, _: &'static str) -> std::io::Result<()> {
        Ok(())
    }
    fn write_append(&self, file: &mut fs::File, bytes: &[u8]) -> std::io::Result<()> {
        file.write_all(&bytes[..bytes.len() / 2])?;
        Err(std::io::Error::other("injected partial backup write"))
    }
}
struct CorruptWrite;
impl persistence::FaultInjector for CorruptWrite {
    fn hit(&self, _: &'static str) -> std::io::Result<()> {
        Ok(())
    }
    fn write_append(&self, file: &mut fs::File, bytes: &[u8]) -> std::io::Result<()> {
        let mut changed = bytes.to_vec();
        changed[0] ^= 1;
        file.write_all(&changed)
    }
}

#[test]
fn memory_backup_export_and_restore_failures_remove_only_owned_partial_destinations() {
    let directory = Directory::new();
    let archive = archive(&directory);
    let source = directory.child("source");
    let source_files = file_set(&source);
    for point in [
        "backup_export_before_snapshot_sync",
        "backup_export_after_snapshot_sync",
        "backup_export_after_manifest_sync",
        "backup_export_before_publication",
    ] {
        let target = directory.child(point);
        assert!(
            backup::export_memory_offline_with_faults(
                options(&source, target.clone()),
                clock(1100),
                Arc::new(FailAt(point))
            )
            .is_err()
        );
        assert!(!target.exists(), "{point}");
        assert_eq!(file_set(&source), source_files);
    }
    let target = directory.child("partial-export");
    assert!(
        backup::export_memory_offline_with_faults(
            options(&source, target.clone()),
            clock(1100),
            Arc::new(PartialWrite)
        )
        .is_err()
    );
    assert!(!target.exists());
    let archive_files = file_set(&archive);
    for point in [
        "backup_restore_after_copy_sync",
        "backup_restore_after_generation_publish",
    ] {
        let target = directory.child(point);
        assert!(
            backup::restore_memory_with_faults(
                restore_options(archive.clone(), target.clone()),
                clock(1100),
                Arc::new(FailAt(point))
            )
            .is_err()
        );
        assert!(!target.exists(), "{point}");
        assert_eq!(file_set(&archive), archive_files);
    }
    let target = directory.child("partial-restore");
    assert!(
        backup::restore_memory_with_faults(
            restore_options(archive.clone(), target.clone()),
            clock(1100),
            Arc::new(PartialWrite)
        )
        .is_err()
    );
    assert!(!target.exists());
    let target = directory.child("corrupt-copy");
    assert!(
        backup::restore_memory_with_faults(
            restore_options(archive.clone(), target.clone()),
            clock(1100),
            Arc::new(CorruptWrite)
        )
        .is_err()
    );
    assert!(
        !target.exists(),
        "copy validation must refuse a write that reports success with corrupt bytes"
    );
    let existing = directory.child("existing");
    fs::create_dir(&existing).unwrap();
    fs::write(existing.join("unowned"), b"never delete").unwrap();
    assert!(
        backup::restore_memory(
            restore_options(archive.clone(), existing.clone()),
            clock(1100)
        )
        .is_err()
    );
    assert!(
        backup::export_memory_offline(options(&source, existing.clone()), clock(1100)).is_err()
    );
    assert_eq!(fs::read(existing.join("unowned")).unwrap(), b"never delete");
    assert!(
        backup::restore_memory(
            restore_options(archive.clone(), archive.join("inside")),
            clock(1100)
        )
        .is_err()
    );
    assert!(
        backup::export_memory_offline(options(&source, source.join("inside")), clock(1100))
            .is_err()
    );
    assert_eq!(file_set(&source), source_files);
    assert_eq!(file_set(&archive), archive_files);
}

#[test]
fn memory_backup_limits_layout_and_retained_elapsed_member_charge_are_checked_before_restore() {
    let directory = Directory::new();
    let archive = archive(&directory);
    let original = file_set(&archive);
    let verified = backup::verify_memory(
        &archive,
        DurableLayout::default(),
        backup::Limits::default(),
        clock(1100),
    )
    .unwrap();
    let usage = verified.shard_usage[0];
    for limits in [
        backup::Limits {
            max_dataset_bytes: usage - 1,
            ..backup::Limits::default()
        },
        backup::Limits {
            max_record_bytes: 64,
            ..backup::Limits::default()
        },
        backup::Limits {
            max_snapshot_bytes: 128,
            ..backup::Limits::default()
        },
    ] {
        let mut request = restore_options(archive.clone(), directory.child("too-small"));
        request.limits = limits;
        assert!(backup::restore_memory(request, clock(1100)).is_err());
        assert!(!directory.child("too-small").exists());
    }
    let exact = backup::Limits {
        max_dataset_bytes: usage,
        ..backup::Limits::default()
    };
    assert_eq!(
        backup::verify_memory(&archive, DurableLayout::default(), exact, clock(1100))
            .unwrap()
            .shard_usage[0],
        usage
    );
    let mut request = restore_options(archive.clone(), directory.child("layout"));
    request.layout.shard_count = 2;
    assert!(backup::restore_memory(request, clock(1100)).is_err());
    assert!(!directory.child("layout").exists());
    let source = directory.child("source");
    for limits in [
        backup::Limits {
            max_dataset_bytes: usage - 1,
            ..backup::Limits::default()
        },
        backup::Limits {
            max_snapshot_bytes: 128,
            ..backup::Limits::default()
        },
    ] {
        let target = directory.child("export-too-small");
        let mut request = options(&source, target.clone());
        request.limits = limits;
        assert!(backup::export_memory_offline(request, clock(1100)).is_err());
        assert!(!target.exists());
    }
    let mut request = options(&source, directory.child("record-too-small"));
    request.source.limits.max_record_bytes = 64;
    request.limits.max_record_bytes = 64;
    assert!(backup::export_memory_offline(request, clock(1100)).is_err());
    assert!(!directory.child("record-too-small").exists());
    assert_eq!(file_set(&archive), original);
}

#[test]
fn memory_backup_multishard_restore_checks_each_shards_retained_quota() {
    let directory = Directory::new();
    let source = directory.child("source");
    seed(&source);
    let layout = DurableLayout {
        shard_count: 2,
        routing_version: 1,
    };
    let bytes = fs::read(source.join(GENERATION)).unwrap();
    let mut replaced = memory_format::encode_header(memory_format::Header {
        sequence: 11,
        layout,
        replication: Some(ReplicationMetadata {
            role: Role::Replica,
            epoch: [7; 16],
        }),
    })
    .unwrap();
    replaced.extend_from_slice(&bytes[memory_format::REPLICATION_HEADER_BYTES..]);
    fs::write(source.join(GENERATION), replaced).unwrap();
    let archive = directory.child("archive");
    let mut request = options(&source, archive.clone());
    request.source.layout = layout;
    backup::export_memory_offline(request, clock(1100)).unwrap();
    let verified =
        backup::verify_memory(&archive, layout, backup::Limits::default(), clock(1100)).unwrap();
    let maximum = *verified.shard_usage.iter().max().unwrap();
    let limits = backup::Limits {
        max_dataset_bytes: maximum * 2 - 2,
        ..backup::Limits::default()
    };
    let target = directory.child("quota");
    assert!(matches!(
        backup::restore_memory(
            backup::RestoreOptions {
                source: archive,
                destination: target.clone(),
                layout,
                limits
            },
            clock(1100)
        ),
        Err(backup::Error::Invalid("snapshot exceeds one shard's quota"))
    ));
    assert!(!target.exists());
}

fn oracle_crc(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xedb88320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}
fn oracle_digest(previous: u32, bytes: &[u8]) -> u32 {
    oracle_crc(&[previous.to_le_bytes(), oracle_crc(bytes).to_le_bytes()].concat())
}

#[test]
fn memory_backup_manifest_digest_matches_independent_transport2_envelope_oracle() {
    let directory = Directory::new();
    let archive = archive(&directory);
    let snapshot = fs::read(archive.join(backup::SNAPSHOT)).unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(archive.join(backup::MANIFEST)).unwrap()).unwrap();
    let mut offset = 36;
    let mut digest = 0;
    for _ in 0..manifest["snapshot"]["entries"].as_u64().unwrap() {
        let payload = u32::from_le_bytes(snapshot[offset..offset + 4].try_into().unwrap()) as usize;
        let record = &snapshot[offset..offset + 12 + payload];
        let mut frame = b"SIDERREP\x02\x00\x04\x00".to_vec();
        let length = record.len() as u32;
        frame.extend_from_slice(&length.to_le_bytes());
        frame.extend_from_slice(&(!length).to_le_bytes());
        frame.extend_from_slice(&oracle_digest(oracle_crc(&frame), record).to_le_bytes());
        frame.extend_from_slice(record);
        digest = oracle_digest(digest, &frame);
        offset += record.len();
    }
    assert_eq!(
        manifest["transport_digest_crc32"].as_u64().unwrap(),
        u64::from(digest)
    );
}

#[tokio::test]
async fn live_memory_transport_export_round_trips_mixed_snapshot() {
    use sider::replication::{
        Cursor,
        protocol::{self, Hello},
    };
    let directory = Directory::new();
    let images = seed(&directory.child("source"));
    let cursor = Cursor {
        epoch: [7; 16],
        sequence: 12,
    };
    let limits = backup::Limits::default();
    let hello = Hello {
        sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
        record_version: 2,
        shard_count: 1,
        routing_version: 1,
        max_record_bytes: limits.max_record_bytes as u32,
        max_mutations: limits.max_mutations as u32,
        max_snapshot_bytes: limits.max_snapshot_bytes,
        cursor: Some(cursor),
    };
    let (mut producer, mut consumer) = tokio::io::duplex(128);
    let source = tokio::spawn(async move {
        sider::replication::memory::export_snapshot(
            &mut producer,
            hello,
            cursor,
            &images,
            protocol::Limits::default(),
            std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
    });
    let destination = directory.child("live");
    let manifest = backup::export_memory_stream(
        &mut consumer,
        backup::ExportOptions {
            source: "127.0.0.1:1".parse().unwrap(),
            destination: destination.clone(),
            source_sha: "a".repeat(40),
            limits,
        },
        clock(1000),
    )
    .await
    .unwrap();
    source.await.unwrap();
    assert_eq!(manifest.profile, backup::Profile::Memory);
    assert_eq!(manifest.cursor, cursor);
    backup::verify_memory(&destination, DurableLayout::default(), limits, clock(1000)).unwrap();
    assert!(backup::verify(&destination, DurableLayout::default(), limits, clock(1000)).is_err());
    backup::restore_memory(
        backup::RestoreOptions {
            source: destination,
            destination: directory.child("restored"),
            layout: DurableLayout::default(),
            limits,
        },
        clock(1000),
    )
    .unwrap();
}

#[tokio::test]
async fn live_memory_export_rejects_capacity_before_snapshot_or_destination() {
    use sider::replication::{
        Cursor,
        protocol::{self, Hello},
    };
    let directory = Directory::new();
    let cursor = Cursor {
        epoch: [7; 16],
        sequence: 12,
    };
    let limits = backup::Limits::default();
    let source = Hello {
        sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
        record_version: 2,
        shard_count: 1,
        routing_version: 1,
        max_record_bytes: limits.max_record_bytes as u32,
        max_mutations: limits.max_mutations as u32,
        max_snapshot_bytes: limits.max_snapshot_bytes,
        cursor: Some(cursor),
    };
    let (mut producer, mut consumer) = tokio::io::duplex(128);
    let task = tokio::spawn(async move {
        sider::replication::memory::export_snapshot(
            &mut producer,
            source,
            cursor,
            &[],
            protocol::Limits::default(),
            std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
    });
    let destination = directory.child("rejected");
    let smaller = backup::Limits {
        max_record_bytes: limits.max_record_bytes - 1,
        ..limits
    };
    assert!(
        backup::export_memory_stream(
            &mut consumer,
            backup::ExportOptions {
                source: "127.0.0.1:1".parse().unwrap(),
                destination: destination.clone(),
                source_sha: "a".repeat(40),
                limits: smaller
            },
            clock(1000)
        )
        .await
        .is_err()
    );
    task.await.unwrap();
    assert!(!destination.exists());
}

#[tokio::test]
async fn live_record2_server_exports_worker_snapshot_and_refuses_legacy_or_small_receiver() {
    use std::net::SocketAddr;
    use std::time::Duration;

    use sider::ServerConfig;
    use sider::replication::config::Config as ReplicationConfig;

    let directory = Directory::new();
    let internal_ready = directory.child("internal.json");
    let aof_config = AofConfig::new(directory.child("aof")).with_memory_profile();
    let config = ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        aof: Some(aof_config),
        replication: Some(ReplicationConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            ready_file: Some(internal_ready.clone()),
            ..ReplicationConfig::default()
        }),
        ..ServerConfig::default()
    };
    config.validate().unwrap();
    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .unwrap();
    let client_addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(sider::server::serve(listener, config, async {
        let _ = stopped.await;
    }));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !internal_ready.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "internal listener readiness"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let ready: serde_json::Value =
        serde_json::from_slice(&fs::read(&internal_ready).unwrap()).unwrap();
    let source: SocketAddr = format!(
        "{}:{}",
        ready["host"].as_str().unwrap(),
        ready["port"].as_u64().unwrap()
    )
    .parse()
    .unwrap();
    tokio::task::spawn_blocking(move || {
        let mut client = std::net::TcpStream::connect(client_addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let send = |client: &mut std::net::TcpStream, args: Vec<Vec<u8>>| {
            client.write_all(&wire::request(&args)).unwrap();
            wire::read_response(client).unwrap().bytes
        };
        assert_eq!(
            send(
                &mut client,
                [
                    b"MEM.CREATE".as_slice(),
                    b"c",
                    b"1",
                    b"2",
                    b"L2SQ",
                    b"p",
                    b"m",
                    b"r",
                    b"t",
                ]
                .into_iter()
                .map(<[u8]>::to_vec)
                .collect()
            ),
            b"+OK\r\n"
        );
        let vector = [1.0f32.to_le_bytes(), 0.0f32.to_le_bytes()].concat();
        assert_eq!(
            send(
                &mut client,
                vec![
                    b"MEM.PUT".to_vec(),
                    b"c".to_vec(),
                    b"\xff\0".to_vec(),
                    b"n".to_vec(),
                    b"content\0\xff".to_vec(),
                    b"p".to_vec(),
                    b"m".to_vec(),
                    b"r".to_vec(),
                    b"t".to_vec(),
                    vector,
                    b"-1".to_vec(),
                    b"UPSERT".to_vec(),
                    b"0".to_vec(),
                    b"0".to_vec(),
                ]
            ),
            b":1\r\n"
        );
    })
    .await
    .unwrap();
    let options = |destination: PathBuf, limits: backup::Limits| backup::ExportOptions {
        source,
        destination,
        source_sha: "a".repeat(40),
        limits,
    };
    let legacy = directory.child("legacy-rejected");
    assert!(
        backup::export(
            options(legacy.clone(), backup::Limits::default()),
            Arc::new(sider::storage::SystemClock)
        )
        .await
        .is_err()
    );
    assert!(!legacy.exists());
    let small = directory.child("small-rejected");
    let small_limits = backup::Limits {
        max_record_bytes: backup::Limits::default().max_record_bytes - 1,
        ..backup::Limits::default()
    };
    assert!(
        backup::export_memory(
            options(small.clone(), small_limits),
            Arc::new(sider::storage::SystemClock)
        )
        .await
        .is_err()
    );
    assert!(!small.exists());
    let archive = directory.child("live-archive");
    let manifest = backup::export_memory(
        options(archive.clone(), backup::Limits::default()),
        Arc::new(sider::storage::SystemClock),
    )
    .await
    .unwrap();
    assert_eq!(manifest.profile, backup::Profile::Memory);
    assert_eq!(manifest.entries, 1);
    assert_eq!(manifest.cursor.epoch, [0; 16]);
    assert_eq!(manifest.cursor.sequence, 2);
    backup::verify_memory(
        &archive,
        DurableLayout::default(),
        backup::Limits::default(),
        Arc::new(sider::storage::SystemClock),
    )
    .unwrap();
    let restored = directory.child("restored-live");
    backup::restore_memory(
        backup::RestoreOptions {
            source: archive,
            destination: restored.clone(),
            layout: DurableLayout::default(),
            limits: backup::Limits::default(),
        },
        Arc::new(sider::storage::SystemClock),
    )
    .unwrap();
    let mut recovered = persistence::recover(
        AofConfig::new(restored).with_memory_profile(),
        StoreConfig::default(),
        Arc::new(sider::storage::SystemClock),
    )
    .unwrap();
    let images = recovered.store.snapshot_memory();
    assert_eq!(images.len(), 1);
    assert_eq!(images[0].key().as_ref(), b"c");
    let restored_search = execute(
        &mut recovered.store,
        b"c",
        Operation::Search {
            descriptor: descriptor(),
            vector: Bytes::from([1.0_f32.to_le_bytes(), 0.0_f32.to_le_bytes()].concat()),
            k: 1,
            namespace: Bytes::from_static(b"n"),
            metadata: BTreeMap::new(),
        },
    );
    assert_eq!(
        restored_search,
        Reply::Array(vec![Reply::Array(vec![
            Reply::Bulk(Some(Bytes::from_static(b"\xff\0"))),
            Reply::Bulk(Some(Bytes::copy_from_slice(&0.0_f64.to_le_bytes()))),
        ])])
    );
    drop(recovered);
    fn run_cli(args: Vec<std::ffi::OsString>) -> std::process::Output {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_sider-backup"));
        command
            .env("SIDER_SECURITY_PROFILE", "legacy-loopback")
            .env_remove("SIDER_SECURITY_DIR")
            .args(args);
        process::OwnedChild::spawn(&mut command)
            .unwrap()
            .wait(Duration::from_secs(10))
            .unwrap()
    }
    let cli_archive = directory.child("cli-live-archive");
    let cli_source = source.to_string();
    let cli_destination = cli_archive.clone();
    let exported = tokio::task::spawn_blocking(move || {
        run_cli(vec![
            "export".into(),
            "--record-version".into(),
            "2".into(),
            "--source".into(),
            cli_source.into(),
            "--destination".into(),
            cli_destination.into_os_string(),
            "--source-sha".into(),
            "a".repeat(40).into(),
        ])
    })
    .await
    .unwrap();
    assert!(
        exported.status.success(),
        "{}",
        String::from_utf8_lossy(&exported.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&exported.stdout).unwrap();
    assert_eq!(report["record_version"], 2);
    let verify_args = |memory: bool| {
        let mut args = vec![
            "verify".into(),
            "--source".into(),
            cli_archive.clone().into_os_string(),
            "--shards".into(),
            "1".into(),
            "--routing".into(),
            "1".into(),
        ];
        if memory {
            args.extend(["--record-version".into(), "2".into()]);
        }
        args
    };
    assert!(!run_cli(verify_args(false)).status.success());
    let mut unsupported = verify_args(false);
    unsupported.extend(["--record-version".into(), "3".into()]);
    let rejected = run_cli(unsupported);
    assert!(!rejected.status.success());
    assert!(rejected.stdout.is_empty());
    let verified = run_cli(verify_args(true));
    assert!(
        verified.status.success(),
        "{}",
        String::from_utf8_lossy(&verified.stderr)
    );
    let cli_restored = directory.child("cli-restored");
    let legacy_restored = directory.child("legacy-cli-restored");
    let legacy_result = run_cli(vec![
        "restore".into(),
        "--source".into(),
        cli_archive.clone().into_os_string(),
        "--destination".into(),
        legacy_restored.clone().into_os_string(),
        "--shards".into(),
        "1".into(),
        "--routing".into(),
        "1".into(),
    ]);
    assert!(!legacy_result.status.success());
    assert!(!legacy_restored.exists());
    let destination = cli_restored.clone();
    let archive = cli_archive.clone();
    let restored_result = tokio::task::spawn_blocking(move || {
        run_cli(vec![
            "restore".into(),
            "--record-version".into(),
            "2".into(),
            "--source".into(),
            archive.into_os_string(),
            "--destination".into(),
            destination.into_os_string(),
            "--shards".into(),
            "1".into(),
            "--routing".into(),
            "1".into(),
        ])
    })
    .await
    .unwrap();
    assert!(
        restored_result.status.success(),
        "{}",
        String::from_utf8_lossy(&restored_result.stderr)
    );
    let mut cli_image = persistence::recover(
        AofConfig::new(cli_restored).with_memory_profile(),
        StoreConfig::default(),
        Arc::new(sider::storage::SystemClock),
    )
    .unwrap();
    assert_eq!(cli_image.store.snapshot_memory(), images);
    assert_eq!(
        execute(
            &mut cli_image.store,
            b"c",
            Operation::Search {
                descriptor: descriptor(),
                vector: Bytes::from([1.0_f32.to_le_bytes(), 0.0_f32.to_le_bytes()].concat()),
                k: 1,
                namespace: Bytes::from_static(b"n"),
                metadata: BTreeMap::new(),
            },
        ),
        restored_search
    );
    drop(cli_image);
    {
        use sider::replication::protocol::{self, Hello, Message};
        let limits = protocol::Limits::default();
        let peer = Hello {
            sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
            record_version: memory_format::RECORD_VERSION,
            shard_count: 1,
            routing_version: 1,
            max_record_bytes: limits.record.max_record_bytes as u32,
            max_mutations: limits.record.max_mutations as u32,
            max_snapshot_bytes: backup::MAX_BACKUP_BYTES,
            cursor: None,
        };
        let mut replica = tokio::net::TcpStream::connect(source).await.unwrap();
        protocol::write_memory(
            &mut replica,
            &Message::Hello(peer.clone()),
            limits,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        async fn read(socket: &mut tokio::net::TcpStream, limits: protocol::Limits) -> Message {
            protocol::read_memory_authorized(socket, limits, Duration::from_secs(5), |_| true)
                .await
                .unwrap()
                .1
        }
        let Message::Hello(source_hello) = read(&mut replica, limits).await else {
            panic!("replica source hello");
        };
        let cursor = source_hello.cursor.unwrap();
        assert_ne!(cursor.epoch, [0; 16]);
        assert_eq!(cursor.sequence, 2);
        assert_eq!(
            read(&mut replica, limits).await,
            Message::FullStart { cursor, entries: 1 }
        );
        assert!(matches!(
            read(&mut replica, limits).await,
            Message::MemorySnapshotEntry(_)
        ));
        assert!(matches!(
            read(&mut replica, limits).await,
            Message::FullEnd { cursor: end, entries: 1, .. } if end == cursor
        ));
        protocol::write_memory(
            &mut replica,
            &Message::Ack(cursor),
            limits,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        tokio::task::spawn_blocking(move || {
            let mut client = std::net::TcpStream::connect(client_addr).unwrap();
            client
                .write_all(&wire::request(&[
                    b"MEM.DEL".to_vec(),
                    b"c".to_vec(),
                    b"\xff\0".to_vec(),
                ]))
                .unwrap();
            assert_eq!(wire::read_response(&mut client).unwrap().bytes, b":1\r\n");
        })
        .await
        .unwrap();
        let next = loop {
            match read(&mut replica, limits).await {
                Message::Heartbeat(_) => {
                    protocol::write_memory(
                        &mut replica,
                        &Message::Ack(cursor),
                        limits,
                        Duration::from_secs(5),
                    )
                    .await
                    .unwrap();
                }
                Message::MemoryBatch { sequence, .. } => break sequence,
                message => panic!("unexpected replica frame: {message:?}"),
            }
        };
        assert_eq!(next, 3);
        let next_cursor = sider::replication::Cursor {
            sequence: next,
            ..cursor
        };
        protocol::write_memory(
            &mut replica,
            &Message::Ack(next_cursor),
            limits,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        drop(replica);
        let mut resumed = tokio::net::TcpStream::connect(source).await.unwrap();
        protocol::write_memory(
            &mut resumed,
            &Message::Hello(Hello {
                cursor: Some(next_cursor),
                ..peer.clone()
            }),
            limits,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let mut stale = tokio::net::TcpStream::connect(source).await.unwrap();
        protocol::write_memory(
            &mut stale,
            &Message::Hello(Hello {
                cursor: Some(sider::replication::Cursor {
                    epoch: [0x99; 16],
                    sequence: 3,
                }),
                ..peer
            }),
            limits,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let Message::Hello(stale_hello) = read(&mut stale, limits).await else {
            panic!("stale replica source hello");
        };
        assert_eq!(stale_hello.cursor, Some(next_cursor));
        assert_eq!(
            read(&mut stale, limits).await,
            Message::FullStart {
                cursor: next_cursor,
                entries: 1,
            }
        );
        assert!(matches!(
            read(&mut stale, limits).await,
            Message::MemorySnapshotEntry(_)
        ));
        assert!(matches!(
            read(&mut stale, limits).await,
            Message::FullEnd { cursor, entries: 1, .. } if cursor == next_cursor
        ));
        protocol::write_memory(
            &mut stale,
            &Message::Ack(next_cursor),
            limits,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert!(matches!(
            read(&mut resumed, limits).await,
            Message::Hello(_)
        ));
        assert_eq!(
            read(&mut resumed, limits).await,
            Message::Continue(next_cursor)
        );
        protocol::write_memory(
            &mut resumed,
            &Message::Ack(next_cursor),
            limits,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        drop(resumed);
    }
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[test]
fn memory_transport_delta_replay_preserves_historical_expired_state() {
    use sider::replication::protocol::{self, Message};
    let mut primary = Store::with_clock(clock(1000));
    assert_eq!(
        execute(&mut primary, b"c", Operation::Create(descriptor())),
        Reply::Ok
    );
    let image = primary.snapshot_memory().remove(0);
    let mut replica = Store::with_clock(clock(9000));
    replica.replay_memory(&[image]).unwrap();
    for operation in [
        Operation::Put {
            descriptor: descriptor(),
            member: Box::new(member(b"id")),
            ttl: Ttl::After(1),
            condition: SetCondition::Always,
        },
        Operation::Persist {
            id: Bytes::from_static(b"id"),
        },
    ] {
        let prepared = primary.prepare_memory(Bytes::from_static(b"c"), operation);
        let delta = prepared.resolved().unwrap().clone();
        assert_eq!(primary.apply_memory(prepared), Reply::Integer(1));
        let message = Message::MemoryBatch {
            sequence: 1,
            origin: sider::storage::MutationOrigin::Client,
            mutations: vec![delta],
        };
        let frame = protocol::encode_memory(&message, protocol::Limits::default()).unwrap();
        let Message::MemoryBatch { mutations, .. } =
            protocol::decode_memory(frame, protocol::Limits::default()).unwrap()
        else {
            panic!("batch")
        };
        replica.replay_memory(&mutations).unwrap();
    }
    assert_eq!(replica.snapshot_memory(), primary.snapshot_memory());
}
