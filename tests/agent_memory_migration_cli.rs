//! Real CLI profile upgrades and refusal invariants; child processes run serially.
#![forbid(unsafe_code)]
#[path = "common/process.rs"]
mod process;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use sider::command::{Command, Reply, SetCondition};
use sider::persistence::migration::options_from_args;
use sider::persistence::{
    self, AofConfig, DurableLayout, ReplicationMetadata, Role, format, memory_format,
};
use sider::storage::{
    Clock, Store, StoreConfig, SystemClock,
    memory::{Descriptor, Member, Operation, Ttl},
};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command as Process,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

static NEXT: AtomicU64 = AtomicU64::new(0);
const GENERATION: &str = "generation-00000000000000000000.aof";
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-memory-cli-{}-{}-{}",
            std::process::id(),
            SystemClock.unix_millis(),
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
struct FixedClock(i64);
impl Clock for FixedClock {
    fn now(&self) -> tokio::time::Instant {
        tokio::time::Instant::now()
    }
    fn unix_millis(&self) -> i64 {
        self.0
    }
}
fn files(path: &Path) -> BTreeMap<OsString, Vec<u8>> {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name(),
                if entry.metadata().unwrap().len() == 0 {
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
        content: Bytes::from_static(b"\0\xff\r\n"),
        vector: Bytes::from([(-0.0f32).to_le_bytes(), f32::from_bits(1).to_le_bytes()].concat()),
        deadline: None,
        metadata: BTreeMap::from([
            (
                Bytes::from_static(b"typed"),
                sider::storage::memory::Scalar::Integer(i64::MIN),
            ),
            (
                Bytes::from_static(b"bool"),
                sider::storage::memory::Scalar::Bool(true),
            ),
            (
                Bytes::from_static(b"bytes"),
                sider::storage::memory::Scalar::Bytes(Bytes::from_static(b"\xff\0")),
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
fn seed(path: &Path, version: u32) -> Vec<memory_format::Mutation> {
    fs::create_dir(path).unwrap();
    fs::write(path.join("writer.lock"), []).unwrap();
    fs::write(
        path.join("replication-install-preserved.tmp"),
        b"unowned residue",
    )
    .unwrap();
    let mut store = Store::with_clock(Arc::new(FixedClock(SystemClock.unix_millis() - 5000)));
    for args in [
        vec![b"SET".as_slice(), b"\xff\0", b"\0\xff"],
        vec![b"HSET", b"h", b"\xff", b""],
        vec![b"RPUSH", b"l", b"\xff", b""],
        vec![b"SADD", b"s", b"\0", b"\xff"],
        vec![b"ZADD", b"z", b"-inf", b"\xff"],
        vec![b"SET", b"ttl", b"retained"],
        vec![b"PEXPIRE", b"ttl", b"120000"],
        vec![b"SET", b"expired", b"omitted"],
        vec![b"PEXPIRE", b"expired", b"1000"],
    ] {
        command(&mut store, &args);
    }
    let mut delta = None;
    if version >= 4 {
        for key in [b"c".as_slice(), b"empty", b"outer"] {
            assert_eq!(
                memory(&mut store, key, Operation::Create(descriptor())),
                Reply::Ok
            );
        }
        command(&mut store, &[b"PEXPIRE", b"outer", b"1000"]);
        for id in [b"elapsed".as_slice(), b"\xff\0"] {
            assert_eq!(
                memory(
                    &mut store,
                    b"c",
                    Operation::Put {
                        descriptor: descriptor(),
                        member: Box::new(member(id)),
                        ttl: Ttl::After(1000),
                        condition: SetCondition::Always
                    }
                ),
                Reply::Integer(1)
            );
        }
        let prepared = store.prepare_memory(
            Bytes::from_static(b"c"),
            Operation::Persist {
                id: Bytes::from_static(b"\xff\0"),
            },
        );
        delta = Some(prepared);
    }
    let images = store.snapshot_memory();
    let resolved = delta.map(|prepared| {
        let resolved = prepared.resolved().unwrap().clone();
        assert_eq!(store.apply_memory(prepared), Reply::Integer(1));
        resolved
    });
    let mut file = fs::File::create(path.join(GENERATION)).unwrap();
    if version < 4 {
        if version == 1 {
            format::write_header(&mut file, 9).unwrap();
        } else {
            format::write_header_with_replication(
                &mut file,
                9,
                DurableLayout::default(),
                if version == 3 {
                    Some(ReplicationMetadata {
                        role: Role::Replica,
                        epoch: [7; 16],
                    })
                } else {
                    None
                },
            )
            .unwrap();
        }
    } else {
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
    }
    let mut digest = 0;
    for image in &images {
        let bytes = if version < 4 {
            let memory_format::Mutation::Legacy(image) = image else {
                unreachable!()
            };
            format::encode(
                &format::Record::Snapshot(image.clone()),
                format::Limits::default(),
            )
            .unwrap()
        } else {
            memory_format::encode(
                &memory_format::Record::Snapshot(image.clone()),
                format::Limits::default(),
            )
            .unwrap()
        };
        digest = format::snapshot_digest(digest, &bytes);
        file.write_all(&bytes).unwrap();
    }
    let bytes = if version < 4 {
        format::encode(
            &format::Record::Seal {
                sequence: 9,
                entries: images.len() as u64,
                digest,
            },
            format::Limits::default(),
        )
        .unwrap()
    } else {
        memory_format::encode(
            &memory_format::Record::Seal {
                sequence: 9,
                entries: images.len() as u64,
                digest,
            },
            format::Limits::default(),
        )
        .unwrap()
    };
    file.write_all(&bytes).unwrap();
    if let Some(delta) = resolved {
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
    }
    file.sync_all().unwrap();
    store
        .snapshot_memory()
        .into_iter()
        .filter(|image| image.key().as_ref() != b"expired")
        .collect()
}
fn args(source: &Path, target: &Path, source_record: u32, target_record: u32) -> Vec<OsString> {
    let mut args = vec![
        "--source".into(),
        source.into(),
        "--source-shards".into(),
        "1".into(),
        "--source-routing".into(),
        "1".into(),
        "--destination".into(),
        target.into(),
        "--shards".into(),
        "1".into(),
        "--routing".into(),
        "1".into(),
    ];
    if source_record != 1 {
        args.extend([
            "--source-record-version".into(),
            source_record.to_string().into(),
        ]);
    }
    if target_record != 1 {
        args.extend(["--record-version".into(), target_record.to_string().into()]);
    }
    args
}
fn run(args: Vec<OsString>) -> std::process::Output {
    let mut command = Process::new(env!("CARGO_BIN_EXE_sider-aof-migrate"));
    command.args(args);
    process::run(&mut command, Duration::from_secs(15)).unwrap()
}
fn success(output: std::process::Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn refused(output: std::process::Output) -> String {
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    String::from_utf8(output.stderr).unwrap()
}

#[test]
fn memory_cli_upgrades_legacy123_and_rewrites_memory45_preserving_exact_images_hashes_and_source() {
    for version in [1, 2, 3, 4, 5] {
        let directory = Directory::new();
        let source = directory.child("source");
        let expected = seed(&source, version);
        let original = files(&source);
        let target = directory.child("target");
        let mut arguments = args(&source, &target, if version >= 4 { 2 } else { 1 }, 2);
        arguments[9] = "4".into();
        let report = success(run(arguments));
        assert_eq!(report["source_format"], version);
        assert_eq!(report["target_format"], 4);
        assert_eq!(report["target_record_version"], 2);
        assert_eq!(report["source_shards"], 1);
        assert_eq!(report["target_shards"], 4);
        assert_eq!(
            report["source_record_version"],
            if version >= 4 { 2 } else { 1 }
        );
        assert_eq!(
            report["source_sha256"],
            digest(&original[&OsString::from(GENERATION)])
        );
        assert_eq!(
            report["target_sha256"],
            digest(&fs::read(target.join(GENERATION)).unwrap())
        );
        assert_eq!(report["sequence"], if version >= 4 { 10 } else { 9 });
        assert_eq!(
            report["retained_memory_collections"],
            if version >= 4 { 3 } else { 0 }
        );
        assert_eq!(
            report["retained_memory_members"],
            if version >= 4 { 2 } else { 0 }
        );
        assert_eq!(files(&source), original);
        let mut configuration = AofConfig::new(target).with_memory_profile();
        configuration.layout.shard_count = 4;
        let recovered =
            persistence::recover(configuration, StoreConfig::default(), Arc::new(SystemClock))
                .unwrap();
        assert_eq!(recovered.metadata.replication, None);
        assert_eq!(recovered.store.snapshot_memory(), expected);
    }
}

#[test]
fn memory_cli_no_flags_retains_exact_legacy_report_and_refuses_memory_default_or_downgrade() {
    let directory = Directory::new();
    let source = directory.child("source");
    seed(&source, 2);
    let target = directory.child("legacy");
    let output = run(args(&source, &target, 1, 1));
    let expected_usage: usize = persistence::recover(
        AofConfig::new(target.clone()),
        StoreConfig::default(),
        Arc::new(SystemClock),
    )
    .unwrap()
    .metadata
    .shard_usage[0];
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!(
            "{{\"source_format\":2,\"sequence\":9,\"entries\":6,\"source_shards\":1,\"target_shards\":1,\"routing_version\":1,\"shard_usage\":[{expected_usage}],\"incomplete_source_tail_bytes\":0}}\n"
        )
    );
    let memory_source = directory.child("memory");
    seed(&memory_source, 4);
    let original = files(&memory_source);
    for source_record in [1, 2] {
        let target = directory.child(&format!("refused{source_record}"));
        let error = refused(run(args(&memory_source, &target, source_record, 1)));
        assert!(error.contains(if source_record == 2 {
            "downgrade refused"
        } else {
            "unsupported AOF version: 4"
        }));
        assert!(!target.exists());
        assert_eq!(files(&memory_source), original);
    }
    let missing = directory.child("absent");
    let error = refused(run(args(&missing, &directory.child("no-io"), 2, 1)));
    assert!(error.contains("downgrade refused"));
    assert!(!missing.exists());
    assert!(!directory.child("no-io").exists());
    let explicit_target = directory.child("explicit-downgrade");
    let mut arguments = args(&missing, &explicit_target, 2, 1);
    arguments.extend(["--record-version".into(), "1".into()]);
    assert!(refused(run(arguments)).contains("downgrade refused"));
    assert!(!missing.exists());
    assert!(!explicit_target.exists());
}

#[test]
fn memory_cli_parser_validates_versions_duplicates_missing_values_and_help_without_io() {
    let directory = Directory::new();
    let base = args(&directory.child("absent"), &directory.child("target"), 1, 1);
    let parsed = options_from_args(base.clone()).unwrap();
    assert!(!parsed.source.is_memory_profile());
    assert!(!parsed.destination.is_memory_profile());
    for flag in ["--source-record-version", "--record-version"] {
        for bad in ["0", "3", "-1", "abc", "184467440737095516160"] {
            let mut args = base.clone();
            args.extend([flag.into(), bad.into()]);
            assert!(refused(run(args)).contains(flag));
        }
        let mut args = base.clone();
        args.push(flag.into());
        assert!(refused(run(args)).contains("requires a value"));
        let mut args = base.clone();
        args.extend([flag.into(), "1".into(), flag.into(), "1".into()]);
        assert!(refused(run(args)).contains("repeated option"));
    }
    let help = run(vec!["--help".into()]);
    assert!(help.status.success());
    assert!(
        String::from_utf8(help.stdout)
            .unwrap()
            .contains("Record versions default to 1")
    );
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 0);
}

#[test]
fn memory_cli_refuses_lock_overwrite_quota_record_limit_profile_and_corruption_preserving_all_files()
 {
    let directory = Directory::new();
    let source = directory.child("source");
    seed(&source, 5);
    let original = files(&source);
    let active = persistence::recover(
        AofConfig::new(source.clone()).with_memory_profile(),
        StoreConfig::default(),
        Arc::new(SystemClock),
    )
    .unwrap();
    let locked_original = files(&source);
    let locked = directory.child("locked");
    assert!(refused(run(args(&source, &locked, 2, 2))).contains("another process"));
    assert!(!locked.exists());
    assert_eq!(files(&source), locked_original);
    drop(active);
    // Writable active recovery above cleans install residues; restore the original fixture.
    for (name, bytes) in &original {
        fs::write(source.join(name), bytes).unwrap();
    }
    for (name, extra) in [
        ("quota", vec!["--max-dataset-bytes", "100"]),
        ("record", vec!["--max-record-bytes", "64"]),
    ] {
        let target = directory.child(name);
        let mut arguments = args(&source, &target, 2, 2);
        arguments.extend(extra.into_iter().map(OsString::from));
        refused(run(arguments));
        assert!(!target.exists());
        assert_eq!(files(&source), original);
    }
    let target = directory.child("existing");
    fs::create_dir(&target).unwrap();
    fs::write(target.join("human.txt"), b"keep").unwrap();
    assert!(refused(run(args(&source, &target, 2, 2))).contains("already exists"));
    assert_eq!(fs::read(target.join("human.txt")).unwrap(), b"keep");
    assert_eq!(files(&source), original);
    let mismatch = directory.child("mismatch");
    assert!(refused(run(args(&source, &mismatch, 1, 2))).contains("unsupported AOF version: 5"));
    assert!(!mismatch.exists());
    assert_eq!(files(&source), original);
    let mut bad = fs::read(source.join(GENERATION)).unwrap();
    *bad.last_mut().unwrap() ^= 1;
    fs::write(source.join(GENERATION), &bad).unwrap();
    let corrupt = files(&source);
    let target = directory.child("corrupt");
    refused(run(args(&source, &target, 2, 2)));
    assert!(!target.exists());
    assert_eq!(files(&source), corrupt);
}

#[test]
fn memory_cli_reports_incomplete_tail_without_repair_and_never_restores_relative_ttl() {
    let directory = Directory::new();
    let source = directory.child("source");
    let expected = seed(&source, 5);
    let mut bytes = fs::read(source.join(GENERATION)).unwrap();
    bytes.extend_from_slice(&[1, 2]);
    fs::write(source.join(GENERATION), &bytes).unwrap();
    let original = files(&source);
    let target = directory.child("target");
    let report = success(run(args(&source, &target, 2, 2)));
    assert_eq!(report["incomplete_source_tail_bytes"], 2);
    assert_eq!(report["source_sha256"], digest(&bytes));
    assert_eq!(files(&source), original);
    let recovered = persistence::recover(
        AofConfig::new(target).with_memory_profile(),
        StoreConfig::default(),
        Arc::new(SystemClock),
    )
    .unwrap();
    assert_eq!(recovered.store.snapshot_memory(), expected);
}

#[cfg(windows)]
#[test]
#[ignore = "requires checksum-pinned published v1.0.1 Windows binaries"]
fn published_old_binaries_refuse_all_first_record_prefixes_preserving_aof_and_characterizing_residue_cleanup()
 {
    let package = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/legacy-memory-cli/package/sider-v1.0.1-x86_64-pc-windows-msvc");
    let old_migrator = package.join("sider-aof-migrate.exe");
    let old_server = package.join("sider.exe");
    assert_eq!(
        digest(&fs::read(&old_migrator).expect("missing pinned old migrator")),
        "c3dde28d0543014ba9a88cc028833c37cdaad160867e7fcb2b757a894e3a3e7f"
    );
    assert_eq!(
        digest(&fs::read(&old_server).expect("missing pinned old server")),
        "2ffe4aa76fd42e450bce2dfac49139c5ab9caff0b1608219298a4640b3d4068a"
    );
    let directory = Directory::new();
    let source = directory.child("source");
    seed(&source, 5);
    let target = directory.child("new");
    success(run(args(&source, &target, 2, 2)));
    fs::write(target.join("unowned-sentinel.bin"), b"must remain").unwrap();
    let bytes = fs::read(target.join(GENERATION)).unwrap();
    let header = memory_format::HEADER_BYTES;
    let first_end =
        header + 12 + u32::from_le_bytes(bytes[header..header + 4].try_into().unwrap()) as usize;
    let mut checked = 0;
    for end in (header..=first_end).chain(std::iter::once(bytes.len())) {
        fs::write(target.join(GENERATION), &bytes[..end]).unwrap();
        fs::write(target.join("replication-install-preserved.tmp"), b"keep").unwrap();
        let original = files(&target);
        let destination = directory.child("old-output");
        let mut old = Process::new(&old_migrator);
        old.args(args(&target, &destination, 1, 1));
        assert!(
            refused(process::run(&mut old, Duration::from_secs(10)).unwrap())
                .contains("unsupported AOF version: 4")
        );
        assert!(!destination.exists());
        assert_eq!(files(&target), original);
        let mut old = Process::new(&old_server);
        old.env_clear()
            .env(
                "SystemRoot",
                std::env::var_os("SystemRoot").expect("Windows system root"),
            )
            .env("TOKIO_WORKER_THREADS", "1");
        old.env("SIDER_AOF_DIR", &target)
            .env("SIDER_ADDR", "127.0.0.1:0")
            .env("SIDER_SHARDS", "1");
        let output = process::run(&mut old, Duration::from_secs(10)).unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("unsupported AOF version: 4"));
        // Original v1.0.1 writable startup deletes its known install residues before
        // reading the header. This is a historical limitation, not full directory
        // preservation; require every generation, lock and unowned file unchanged.
        let mut expected_after_old_server = original;
        assert_eq!(
            expected_after_old_server.remove(&OsString::from("replication-install-preserved.tmp")),
            Some(b"keep".to_vec())
        );
        assert_eq!(files(&target), expected_after_old_server);
        checked += 1;
    }
    assert!(checked > 1);
    println!(
        "published v1.0.1 server/migrator checked {checked} prefix/full-file refusals; old server removed its install residue each time while preserving AOF, lock and unowned files"
    );
}
