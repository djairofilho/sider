//! R10-03: TCP export, verifiable archive, and restore without overwriting.
#![forbid(unsafe_code)]

#[path = "common/process.rs"]
mod process;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use sider::command::parse;
use sider::persistence::backup::{self, ExportOptions, Limits, RestoreOptions};
use sider::persistence::{self, AofConfig, DurableLayout, format};
use sider::replication::Cursor;
use sider::replication::protocol::{self, Hello, Message};
use sider::resp::Frame;
use sider::storage::{Clock, Mutation, Store, StoreConfig};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, duplex};
use tokio::net::TcpListener;

const DEADLINE: Duration = Duration::from_secs(10);
const SHA: &str = "0123456789012345678901234567890123456789";
const CURSOR: Cursor = Cursor {
    epoch: [7; 16],
    sequence: 11,
};
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-backup-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed),
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
fn clock(unix_ms: i64) -> Arc<dyn Clock> {
    Arc::new(FixedClock(unix_ms, tokio::time::Instant::now()))
}
fn layout() -> DurableLayout {
    DurableLayout {
        shard_count: 4,
        routing_version: 1,
    }
}
fn options(destination: PathBuf) -> ExportOptions {
    ExportOptions {
        source: "127.0.0.1:12345".parse().unwrap(),
        destination,
        source_sha: SHA.into(),
        limits: Limits {
            timeout: DEADLINE,
            ..Limits::default()
        },
    }
}
fn hello() -> Hello {
    Hello {
        sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
        record_version: format::VERSION,
        shard_count: 4,
        routing_version: 1,
        max_record_bytes: 4096,
        max_mutations: 100_000,
        max_snapshot_bytes: 65536,
        cursor: Some(CURSOR),
    }
}
fn dataset() -> Vec<Mutation> {
    let mut store = Store::with_clock(clock(1000));
    for args in [
        vec![b"SET".as_slice(), b"s", b"\0\xff"],
        vec![b"HSET", b"h", b"\xff", b""],
        vec![b"RPUSH", b"l", b"\xff", b""],
        vec![b"SADD", b"t", b"\0", b"\xff"],
        vec![b"ZADD", b"z", b"-inf", b"\xff", b"1e-7", b"\0"],
        vec![b"SET", b"ttl", b"deadline", b"PX", b"1500"],
    ] {
        store.execute(
            parse(Frame::Array(Some(
                args.into_iter()
                    .map(|arg| Frame::Bulk(Some(Bytes::copy_from_slice(arg))))
                    .collect(),
            )))
            .unwrap(),
        );
    }
    store.snapshot()
}
fn messages(mutations: Vec<Mutation>) -> Vec<Message> {
    let entries = mutations.len() as u64;
    let mut result = vec![
        Message::Hello(hello()),
        Message::FullStart {
            cursor: CURSOR,
            entries,
        },
    ];
    let mut digest = 0;
    for mutation in mutations {
        let message = Message::SnapshotEntry(mutation);
        digest = format::snapshot_digest(
            digest,
            &protocol::encode(&message, protocol::Limits::default()).unwrap(),
        );
        result.push(message);
    }
    result.push(Message::FullEnd {
        cursor: CURSOR,
        entries,
        digest,
    });
    result
}
fn encode(messages: &[Message]) -> Vec<u8> {
    messages
        .iter()
        .flat_map(|message| protocol::encode(message, protocol::Limits::default()).unwrap())
        .collect()
}
async fn serve(stream: &mut (impl AsyncRead + AsyncWrite + Unpin), payload: &[u8]) {
    assert!(matches!(
        protocol::read(stream, protocol::Limits::default(), DEADLINE)
            .await
            .unwrap(),
        Message::Export { .. }
    ));
    // TCP fragmentation does not define record or snapshot boundaries.
    for bytes in payload.chunks(17) {
        if stream.write_all(bytes).await.is_err() {
            return;
        }
    }
    let _ = stream.shutdown().await;
}
async fn fixture(
    destination: PathBuf,
    payload: Vec<u8>,
    limits: Option<Limits>,
) -> Result<backup::Manifest, backup::Error> {
    let (mut client, mut peer) = duplex(4096);
    let mut request = options(destination);
    if let Some(limits) = limits {
        request.limits = limits;
    }
    let task = tokio::spawn(async move { serve(&mut peer, &payload).await });
    let result = backup::export_stream(&mut client, request, clock(1000)).await;
    drop(client);
    task.await.unwrap();
    result
}
fn restore_options(parent: &Directory, destination: &str) -> RestoreOptions {
    RestoreOptions {
        source: parent.child("backup"),
        destination: parent.child(destination),
        layout: layout(),
        limits: Limits::default(),
    }
}

#[tokio::test]
async fn roundtrip_preserves_all_types_binary_values_absolute_ttl_and_layout() {
    let parent = Directory::new();
    let expected = dataset();
    assert_eq!(expected.len(), 6);
    let manifest = fixture(
        parent.child("backup"),
        encode(&messages(expected.clone())),
        None,
    )
    .await
    .unwrap();
    assert_eq!(manifest.cursor, CURSOR);
    assert_eq!(manifest.source_sha, SHA);
    assert_eq!(manifest.entries, 6);
    let original = fs::read(parent.child("backup").join(backup::SNAPSHOT)).unwrap();
    let before = backup::verify(
        &parent.child("backup"),
        layout(),
        Limits::default(),
        clock(1000),
    )
    .unwrap();
    assert_eq!(before.live_entries, 6);
    assert_eq!(before.shard_usage.len(), 4);
    let restored = backup::restore(restore_options(&parent, "restored"), clock(1000)).unwrap();
    assert_eq!(restored.shard_usage, before.shard_usage);
    let mut config = AofConfig::new(parent.child("restored"));
    config.layout = layout();
    let recovered = persistence::recover(config, StoreConfig::default(), clock(1000)).unwrap();
    assert_eq!(recovered.store.snapshot(), expected);
    assert_eq!(recovered.metadata.sequence, CURSOR.sequence);
    drop(recovered);
    let expired = backup::restore(restore_options(&parent, "later"), clock(3000)).unwrap();
    assert_eq!(expired.live_entries, 5);
    let mut config = AofConfig::new(parent.child("later"));
    config.layout = layout();
    let recovered = persistence::recover(config, StoreConfig::default(), clock(3000)).unwrap();
    assert_eq!(recovered.store.snapshot().len(), 5);
    assert!(
        !recovered
            .store
            .snapshot()
            .iter()
            .any(|entry| entry.key().as_ref() == b"ttl")
    );
    assert_eq!(
        fs::read(parent.child("backup").join(backup::SNAPSHOT)).unwrap(),
        original
    );
}

#[tokio::test]
async fn malformed_streams_never_publish_a_backup() {
    let parent = Directory::new();
    let normal = messages(dataset());
    let mut cases = Vec::new();
    let mut changed = normal.clone();
    if let Message::Hello(hello) = &mut changed[0] {
        hello.cursor = None;
    }
    cases.push(encode(&changed));
    let mut changed = normal.clone();
    if let Message::Hello(hello) = &mut changed[0] {
        hello.record_version += 1;
    }
    cases.push(encode(&changed));
    let mut changed = normal.clone();
    if let Message::Hello(hello) = &mut changed[0] {
        hello.routing_version += 1;
    }
    cases.push(encode(&changed));
    let mut changed = normal.clone();
    changed.swap(2, 3);
    cases.push(encode(&changed));
    let mut changed = normal.clone();
    changed[3] = changed[2].clone();
    cases.push(encode(&changed));
    let mut changed = normal.clone();
    if let Some(Message::FullEnd { digest, .. }) = changed.last_mut() {
        *digest ^= 1;
    }
    cases.push(encode(&changed));
    let mut changed = normal.clone();
    if let Some(Message::FullEnd { cursor, .. }) = changed.last_mut() {
        cursor.sequence += 1;
    }
    cases.push(encode(&changed));
    let mut changed = normal.clone();
    if let Some(Message::FullEnd { entries, .. }) = changed.last_mut() {
        *entries += 1;
    }
    cases.push(encode(&changed));
    let mut changed = normal.clone();
    changed.push(Message::Ack(CURSOR));
    cases.push(encode(&changed));
    let bytes = encode(&normal);
    cases.push(bytes[..bytes.len() - 1].to_vec());
    cases.push(encode(&normal[..3]));
    for (index, payload) in cases.into_iter().enumerate() {
        let destination = parent.child(&format!("bad-{index}"));
        assert!(
            fixture(destination.clone(), payload, None).await.is_err(),
            "case {index}"
        );
        assert!(!destination.exists(), "partial destination in case {index}");
    }
}

#[tokio::test(start_paused = true)]
async fn missing_eof_obeys_total_deadline_and_removes_partial_files() {
    let parent = Directory::new();
    let (mut client, mut peer) = duplex(8192);
    let task = tokio::spawn(async move {
        protocol::read(&mut peer, protocol::Limits::default(), DEADLINE)
            .await
            .unwrap();
        peer.write_all(&encode(&messages(dataset()))).await.unwrap();
        std::future::pending::<()>().await;
    });
    let result =
        backup::export_stream(&mut client, options(parent.child("backup")), clock(1000)).await;
    assert!(matches!(result, Err(backup::Error::Timeout)));
    assert!(!parent.child("backup").exists());
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn cancellation_removes_owned_files_and_empty_snapshot_is_restorable() {
    let parent = Directory::new();
    let path = parent.child("cancelled");
    let request = options(path.clone());
    let (mut client, mut peer) = duplex(8192);
    let receiver =
        tokio::spawn(async move { backup::export_stream(&mut client, request, clock(1000)).await });
    protocol::read(&mut peer, protocol::Limits::default(), DEADLINE)
        .await
        .unwrap();
    peer.write_all(&encode(&messages(dataset())[..2]))
        .await
        .unwrap();
    tokio::time::timeout(DEADLINE, async {
        while !path.join(backup::SNAPSHOT).exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    receiver.abort();
    assert!(receiver.await.unwrap_err().is_cancelled());
    assert!(!path.exists());

    fixture(parent.child("backup"), encode(&messages(Vec::new())), None)
        .await
        .unwrap();
    let restored = backup::restore(restore_options(&parent, "empty"), clock(1000)).unwrap();
    assert_eq!(restored.manifest.entries, 0);
    assert_eq!(restored.live_entries, 0);
    let mut config = AofConfig::new(parent.child("empty"));
    config.layout = layout();
    let recovered = persistence::recover(config, StoreConfig::default(), clock(1000)).unwrap();
    assert_eq!(recovered.metadata.sequence, CURSOR.sequence);
    assert!(recovered.store.is_empty());
}

#[tokio::test]
async fn resource_limits_and_existing_destinations_are_enforced() {
    let parent = Directory::new();
    let payload = encode(&messages(dataset()));
    for (name, limits) in [
        (
            "quota",
            Limits {
                max_dataset_bytes: 4,
                ..Limits::default()
            },
        ),
        (
            "record",
            Limits {
                max_record_bytes: 1024,
                ..Limits::default()
            },
        ),
        (
            "snapshot",
            Limits {
                max_snapshot_bytes: 1024,
                ..Limits::default()
            },
        ),
        (
            "mutations",
            Limits {
                max_mutations: 1,
                ..Limits::default()
            },
        ),
    ] {
        let path = parent.child(name);
        assert!(
            fixture(path.clone(), payload.clone(), Some(limits))
                .await
                .is_err()
        );
        assert!(!path.exists());
    }
    let existing = parent.child("existing");
    fs::create_dir(&existing).unwrap();
    fs::write(existing.join("keep"), b"original").unwrap();
    assert!(
        fixture(existing.clone(), payload.clone(), None)
            .await
            .is_err()
    );
    assert_eq!(fs::read(existing.join("keep")).unwrap(), b"original");
    fixture(parent.child("backup"), payload, None)
        .await
        .unwrap();
    assert!(backup::restore(restore_options(&parent, "existing"), clock(1000)).is_err());
    assert_eq!(fs::read(existing.join("keep")).unwrap(), b"original");
    let mut nested = restore_options(&parent, "unused");
    nested.destination = parent.child("backup/nested");
    assert!(backup::restore(nested, clock(1000)).is_err());
    assert!(!parent.child("backup/nested").exists());
}

fn rewrite_checksums(path: &Path) {
    let manifest = fs::read(path.join(backup::MANIFEST)).unwrap();
    let json: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
    fs::write(
        path.join(backup::CHECKSUMS),
        format!(
            "{:x}  {}\n{}  {}\n",
            Sha256::digest(&manifest),
            backup::MANIFEST,
            json["snapshot"]["sha256"].as_str().unwrap(),
            backup::SNAPSHOT
        ),
    )
    .unwrap();
}

#[tokio::test]
async fn corruption_layout_and_quota_are_rejected_before_destination_creation() {
    let parent = Directory::new();
    fixture(parent.child("backup"), encode(&messages(dataset())), None)
        .await
        .unwrap();
    let backup_path = parent.child("backup");
    let manifest_path = backup_path.join(backup::MANIFEST);
    let snapshot_path = backup_path.join(backup::SNAPSHOT);
    let original_manifest = fs::read(&manifest_path).unwrap();
    let original_snapshot = fs::read(&snapshot_path).unwrap();
    let mut request = restore_options(&parent, "restored");
    request.layout.shard_count = 2;
    assert!(backup::restore(request, clock(1000)).is_err());
    let mut request = restore_options(&parent, "restored");
    request.limits.max_dataset_bytes = 4;
    assert!(backup::restore(request, clock(1000)).is_err());
    assert!(!parent.child("restored").exists());
    for position in [0, 31, 48, original_snapshot.len() - 1] {
        let mut bytes = original_snapshot.clone();
        bytes[position] ^= 0x80;
        fs::write(&snapshot_path, bytes).unwrap();
        assert!(backup::restore(restore_options(&parent, "restored"), clock(1000)).is_err());
        assert!(!parent.child("restored").exists());
    }
    fs::write(&snapshot_path, &original_snapshot).unwrap();
    let mut changed: serde_json::Value = serde_json::from_slice(&original_manifest).unwrap();
    changed["layout"]["shard_count"] = 2.into();
    fs::write(&manifest_path, serde_json::to_vec(&changed).unwrap()).unwrap();
    // Even updating the manifest checksum does not change the AOF identity.
    rewrite_checksums(&backup_path);
    let mut request = restore_options(&parent, "restored");
    request.layout.shard_count = 2;
    assert!(backup::restore(request, clock(1000)).is_err());
    changed = serde_json::from_slice(&original_manifest).unwrap();
    changed["transport_digest_crc32"] =
        (changed["transport_digest_crc32"].as_u64().unwrap() ^ 1).into();
    fs::write(&manifest_path, serde_json::to_vec(&changed).unwrap()).unwrap();
    rewrite_checksums(&backup_path);
    assert!(backup::restore(restore_options(&parent, "restored"), clock(1000)).is_err());
    assert!(!parent.child("restored").exists());
}

fn run_cli(args: Vec<std::ffi::OsString>) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sider-backup"));
    command.args(args);
    process::OwnedChild::spawn(&mut command)
        .unwrap()
        .wait(DEADLINE)
        .unwrap()
}

#[tokio::test]
async fn real_cli_exports_tcp_verifies_restores_and_redacts_invalid_values() {
    let parent = Directory::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        serve(&mut stream, &encode(&messages(dataset()))).await;
    });
    let output_path = parent.child("backup");
    let result = tokio::task::spawn_blocking(move || {
        run_cli(vec![
            "export".into(),
            "--source".into(),
            address.to_string().into(),
            "--destination".into(),
            output_path.into_os_string(),
            "--source-sha".into(),
            SHA.into(),
        ])
    })
    .await
    .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    peer.await.unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(manifest["snapshot"]["entries"], 6);
    for action in ["verify", "restore"] {
        let mut args = vec![
            action.into(),
            "--source".into(),
            parent.child("backup").into_os_string(),
            "--shards".into(),
            "4".into(),
            "--routing".into(),
            "1".into(),
        ];
        if action == "restore" {
            args.extend([
                "--destination".into(),
                parent.child("restored").into_os_string(),
            ]);
        }
        let result = tokio::task::spawn_blocking(move || run_cli(args))
            .await
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(report["live_entries"], 5); // The absolute TTL from 1970 is not restarted.
    }
    let result = run_cli(vec![
        "export".into(),
        "--source".into(),
        "private-secret.invalid:999".into(),
    ]);
    assert_eq!(result.status.code(), Some(1));
    assert!(result.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&result.stderr).contains("private-secret"));
}
