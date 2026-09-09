//! Sessões entre processos reais, export consistente, reconexão e promoção durável.

#![forbid(unsafe_code)]

#[path = "common/gate_receipt.rs"]
mod gate_receipt;
#[path = "common/process.rs"]
mod process;
#[path = "common/wire.rs"]
mod wire;

use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sider::persistence::format;
use sider::replication::{
    Cursor,
    protocol::{self, Hello, Limits, Message, Reject},
};
use sider::storage::{Mutation, MutationOrigin, ResolvedBatch};
use tokio::net::{TcpListener, TcpStream};
use wire::Response;

const DEADLINE: Duration = Duration::from_secs(10);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "sider-replication-network-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        Self(directory)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Node {
    child: Option<process::OwnedChild>,
    directory: PathBuf,
    resp: SocketAddr,
    internal: SocketAddr,
}
impl Node {
    async fn start(directory: &Path, upstream: Option<SocketAddr>) -> Self {
        Self::start_at(directory, upstream, None).await
    }
    async fn start_at(
        directory: &Path,
        upstream: Option<SocketAddr>,
        listen: Option<SocketAddr>,
    ) -> Self {
        fs::create_dir_all(directory).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_sider"));
        for (name, _) in std::env::vars_os() {
            if name
                .to_string_lossy()
                .to_ascii_uppercase()
                .starts_with("SIDER_")
            {
                command.env_remove(name);
            }
        }
        command
            .env("SIDER_ADDR", "127.0.0.1:0")
            .env("SIDER_READY_FILE", directory.join("resp.json"))
            .env(
                "SIDER_REPLICATION_ADDR",
                listen.map_or_else(|| "127.0.0.1:0".to_owned(), |address| address.to_string()),
            )
            .env(
                "SIDER_REPLICATION_READY_FILE",
                directory.join("internal.json"),
            )
            .env("SIDER_AOF_DIR", directory.join("aof"))
            .env("SIDER_AOF_MAX_RECORD_BYTES", "8192")
            .env("SIDER_AOF_COMPACT_AFTER_BYTES", "0")
            .env("SIDER_AOF_SYNC", "everysec")
            .env("SIDER_MAX_DATASET_BYTES", "262144")
            .env("SIDER_SHARDS", "4")
            .env("SIDER_REPLICATION_BACKLOG_BYTES", "32768")
            .env("SIDER_REPLICATION_BACKLOG_BATCHES", "128")
            .env("SIDER_REPLICATION_FRAME_TIMEOUT_MS", "2000")
            .env("SIDER_REPLICATION_RECONNECT_MIN_MS", "20")
            .env("SIDER_REPLICATION_RECONNECT_MAX_MS", "100");
        if let Some(upstream) = upstream {
            command.env("SIDER_REPLICA_OF", upstream.to_string());
        }
        let mut child = process::OwnedChild::spawn(&mut command).unwrap();
        let deadline = Instant::now() + DEADLINE;
        loop {
            if let Err(error) = child.assert_alive() {
                let output = child.wait(DEADLINE).unwrap();
                panic!(
                    "{error}: {} {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            if directory.join("resp.json").exists() {
                break;
            }
            assert!(Instant::now() < deadline, "prontidão do processo");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let read = |name| {
            let value: serde_json::Value =
                serde_json::from_slice(&fs::read(directory.join(name)).unwrap()).unwrap();
            assert_eq!(value.as_object().unwrap().len(), 3);
            assert_eq!(value["pid"], child.id());
            let address: SocketAddr =
                format!("{}:{}", value["host"].as_str().unwrap(), value["port"])
                    .parse()
                    .unwrap();
            assert!(address.ip().is_loopback());
            assert_ne!(address.port(), 0);
            address
        };
        let resp = read("resp.json");
        let internal = read("internal.json");
        Self {
            child: Some(child),
            directory: directory.to_owned(),
            resp,
            internal,
        }
    }

    fn connect(&self) -> std::net::TcpStream {
        let stream = std::net::TcpStream::connect_timeout(&self.resp, DEADLINE).unwrap();
        stream.set_read_timeout(Some(DEADLINE)).unwrap();
        stream.set_write_timeout(Some(DEADLINE)).unwrap();
        stream
    }
    fn command(&self, arguments: &[&[u8]]) -> Response {
        request(&mut self.connect(), arguments)
    }
    async fn admin(&self, message: Message) -> Message {
        let mut socket = TcpStream::connect(self.internal).await.unwrap();
        protocol::write(&mut socket, &message, Limits::default(), DEADLINE)
            .await
            .unwrap();
        protocol::read(&mut socket, Limits::default(), DEADLINE)
            .await
            .unwrap()
    }
    async fn status(&self) -> Message {
        self.admin(Message::StatusRequest).await
    }
    async fn cursor(&self) -> Cursor {
        let Message::Status { cursor, .. } = self.status().await else {
            panic!("status");
        };
        cursor
    }
    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let output = child.terminate(DEADLINE).unwrap();
            if std::thread::panicking() {
                eprintln!(
                    "{} {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            for file in ["resp.json", "internal.json"] {
                fs::remove_file(self.directory.join(file)).unwrap();
            }
        }
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

fn request(stream: &mut std::net::TcpStream, arguments: &[&[u8]]) -> Response {
    stream
        .write_all(&wire::request(
            &arguments.iter().map(|s| s.to_vec()).collect::<Vec<_>>(),
        ))
        .unwrap();
    wire::read_response(stream).unwrap().value
}
fn ok(response: Response) {
    assert!(!matches!(response, Response::Error(_)), "{response:?}");
}

fn dataset_metrics(node: &Node) -> Vec<(String, String)> {
    let Response::Bulk(Some(bytes)) = node.command(&[b"INFO", b"memory"]) else {
        panic!("INFO memory ausente");
    };
    let text = String::from_utf8(bytes).unwrap();
    let fields: Vec<_> = text
        .lines()
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.starts_with("dataset_")
                .then(|| (name.to_owned(), value.to_owned()))
        })
        .collect();
    assert_eq!(fields.len(), 4);
    fields
}

async fn caught_up(primary: &Node, replica: &Node) -> Message {
    let expected = primary.cursor().await;
    let deadline = Instant::now() + DEADLINE;
    loop {
        let status = replica.status().await;
        if matches!(&status, Message::Status { cursor, connected: true, .. } if *cursor == expected)
        {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "réplica não alcançou {expected:?}: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_processes_full_delta_export_continue_and_durable_promotion() {
    let directory = Directory::new();
    let primary = Node::start(&directory.0.join("primary"), None).await;
    for args in [
        vec![b"SET".as_slice(), b"{s}:string", b"\0\xffvalue"],
        vec![b"HSET", b"{h}:hash", b"\xff", b"\0"],
        vec![b"RPUSH", b"{l}:list", b"a", b"b"],
        vec![b"SADD", b"{t}:set", b"b", b"a"],
        vec![b"ZADD", b"{z}:sorted", b"1e-7", b"\xff", b"-inf", b"a"],
        vec![b"SET", b"{s}:ttl", b"expires", b"PX", b"30000"],
    ] {
        ok(primary.command(&args));
    }
    let mut replica = Node::start(&directory.0.join("replica"), Some(primary.internal)).await;
    assert!(matches!(
        caught_up(&primary, &replica).await,
        Message::Status {
            full_syncs: 1,
            readonly: true,
            ..
        }
    ));
    let queries: Vec<Vec<&[u8]>> = vec![
        vec![b"GET", b"{s}:string"],
        vec![b"HGETALL", b"{h}:hash"],
        vec![b"LRANGE", b"{l}:list", b"0", b"-1"],
        vec![b"SMEMBERS", b"{t}:set"],
        vec![b"ZRANGE", b"{z}:sorted", b"0", b"-1", b"WITHSCORES"],
    ];
    for query in &queries {
        assert_eq!(primary.command(query), replica.command(query));
    }
    assert_eq!(
        dataset_metrics(&primary),
        dataset_metrics(&replica),
        "métricas após FULL"
    );
    assert!(
        matches!(replica.command(&[b"SET", b"{s}:forbidden", b"x"]), Response::Error(error) if error.starts_with(b"READONLY"))
    );
    let mut tx = primary.connect();
    for command in [
        vec![b"MULTI".as_slice()],
        vec![b"SET", b"{s}:string", b"new"],
        vec![b"HSET", b"{s}:string", b"field", b"value"],
        vec![b"SET", b"{s}:other", b"same-batch"],
    ] {
        ok(request(&mut tx, &command));
    }
    assert!(
        matches!(request(&mut tx, &[b"EXEC"]), Response::Array(Some(values)) if values.len() == 3 && matches!(values[1], Response::Error(_)))
    );
    ok(primary.command(&[b"PEXPIRE", b"{s}:ttl", b"1"]));
    let deadline = Instant::now() + DEADLINE;
    while primary.command(&[b"GET", b"{s}:ttl"]) != Response::Bulk(None) {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    caught_up(&primary, &replica).await;
    assert_eq!(
        dataset_metrics(&primary),
        dataset_metrics(&replica),
        "métricas após lote e TTL"
    );
    assert_eq!(
        replica.command(&[b"MGET", b"{s}:string", b"{s}:other", b"{s}:ttl"]),
        primary.command(&[b"MGET", b"{s}:string", b"{s}:other", b"{s}:ttl"])
    );

    let mut export = TcpStream::connect(primary.internal).await.unwrap();
    protocol::write(
        &mut export,
        &Message::Export {
            sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
            max_record_bytes: 8192,
            max_snapshot_bytes: 262144,
        },
        Limits::default(),
        DEADLINE,
    )
    .await
    .unwrap();
    let Message::Hello(hello) = protocol::read(&mut export, Limits::default(), DEADLINE)
        .await
        .unwrap()
    else {
        panic!("hello export");
    };
    let Message::FullStart { cursor, entries } =
        protocol::read(&mut export, Limits::default(), DEADLINE)
            .await
            .unwrap()
    else {
        panic!("full export");
    };
    assert_eq!(hello.cursor, Some(cursor));
    let mut digest = 0;
    let mut previous = None;
    for _ in 0..entries {
        let (frame, Message::SnapshotEntry(mutation)) =
            protocol::read_with_frame(&mut export, Limits::default(), DEADLINE)
                .await
                .unwrap()
        else {
            panic!("snapshot entry");
        };
        assert!(previous.as_ref().is_none_or(|key| key < mutation.key()));
        previous = Some(mutation.key().clone());
        digest = format::snapshot_digest(digest, &frame);
    }
    assert_eq!(
        protocol::read(&mut export, Limits::default(), DEADLINE)
            .await
            .unwrap(),
        Message::FullEnd {
            cursor,
            entries,
            digest
        }
    );
    let mut byte = [0];
    assert_eq!(
        tokio::io::AsyncReadExt::read(&mut export, &mut byte)
            .await
            .unwrap(),
        0
    );

    let replica_path = replica.directory.clone();
    replica.stop();
    ok(primary.command(&[b"SET", b"{s}:offline", b"delta"]));
    let mut replica = Node::start(&replica_path, Some(primary.internal)).await;
    assert!(matches!(
        caught_up(&primary, &replica).await,
        Message::Status {
            partial_syncs: 1,
            full_syncs: 0,
            ..
        }
    ));
    assert_eq!(
        replica.command(&[b"GET", b"{s}:offline"]),
        Response::Bulk(Some(b"delta".to_vec()))
    );
    let old = replica.cursor().await;
    let Message::Promoted(promoted) = replica.admin(Message::Promote).await else {
        panic!("promoted");
    };
    assert_ne!(old.epoch, promoted.epoch);
    assert_eq!(old.sequence, promoted.sequence);
    ok(replica.command(&[b"SET", b"{s}:owned", b"new-primary"]));
    ok(primary.command(&[b"SET", b"{s}:owned", b"old-primary"]));
    replica.stop();
    let replica = Node::start(&replica_path, Some(primary.internal)).await;
    assert!(matches!(
        replica.status().await,
        Message::Status {
            readonly: false,
            connected: false,
            ..
        }
    ));
    assert_eq!(
        replica.command(&[b"GET", b"{s}:owned"]),
        Response::Bulk(Some(b"new-primary".to_vec()))
    );
    ok(replica.command(&[b"SET", b"{s}:restart", b"writable"]));
}

fn hello(cursor: Option<Cursor>) -> Hello {
    Hello {
        sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
        record_version: format::VERSION,
        shard_count: 4,
        routing_version: 1,
        max_record_bytes: 8192,
        max_mutations: 100_000,
        max_snapshot_bytes: 262144,
        cursor,
    }
}

async fn send(socket: &mut TcpStream, message: Message) {
    protocol::write(socket, &message, Limits::default(), DEADLINE)
        .await
        .unwrap();
}
async fn read(socket: &mut TcpStream) -> Message {
    protocol::read(socket, Limits::default(), DEADLINE)
        .await
        .unwrap()
}
async fn offer(listener: &TcpListener, expected: Option<Cursor>, head: Cursor) -> TcpStream {
    let (mut socket, _) = tokio::time::timeout(DEADLINE, listener.accept())
        .await
        .unwrap()
        .unwrap();
    let Message::Hello(peer) = read(&mut socket).await else {
        panic!("hello da réplica");
    };
    assert_eq!(peer.cursor, expected);
    send(&mut socket, Message::Hello(hello(Some(head)))).await;
    socket
}
fn put(value: &'static [u8]) -> Mutation {
    Mutation::Put {
        key: Bytes::from_static(b"{s}:key"),
        value: Bytes::from_static(value).into(),
        expires_at_unix_ms: None,
    }
}
async fn full(socket: &mut TcpStream, cursor: Cursor, value: &'static [u8], valid: bool) {
    send(socket, Message::FullStart { cursor, entries: 1 }).await;
    let mutation = Message::SnapshotEntry(put(value));
    let digest =
        format::snapshot_digest(0, &protocol::encode(&mutation, Limits::default()).unwrap());
    send(socket, mutation).await;
    send(
        socket,
        Message::FullEnd {
            cursor,
            entries: 1,
            digest: if valid { digest } else { digest ^ 1 },
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_process_rejects_bad_full_gaps_and_changed_duplicates_ack_survives_kill() {
    let directory = Directory::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = listener.local_addr().unwrap();
    let path = directory.0.join("replica");
    let mut replica = Node::start(&path, Some(upstream)).await;
    let base = Cursor {
        epoch: [5; 16],
        sequence: 10,
    };
    let mut socket = offer(&listener, None, base).await;
    full(&mut socket, base, b"original", true).await;
    assert_eq!(read(&mut socket).await, Message::Ack(base));
    assert_eq!(
        replica.command(&[b"GET", b"{s}:key"]),
        Response::Bulk(Some(b"original".to_vec()))
    );
    drop(socket);

    let invalid = Cursor {
        epoch: [6; 16],
        sequence: 40,
    };
    let mut socket = offer(&listener, Some(base), invalid).await;
    send(
        &mut socket,
        Message::FullStart {
            cursor: invalid,
            entries: 1,
        },
    )
    .await;
    send(
        &mut socket,
        Message::SnapshotEntry(put(b"partial-transfer")),
    )
    .await;
    drop(socket);
    assert_eq!(replica.cursor().await, base);
    assert_eq!(
        replica.command(&[b"GET", b"{s}:key"]),
        Response::Bulk(Some(b"original".to_vec()))
    );
    let mut socket = offer(&listener, Some(base), invalid).await;
    full(&mut socket, invalid, b"must-not-be-visible", false).await;
    assert!(
        protocol::read(&mut socket, Limits::default(), DEADLINE)
            .await
            .is_err()
    );
    assert_eq!(replica.cursor().await, base);
    assert_eq!(
        replica.command(&[b"GET", b"{s}:key"]),
        Response::Bulk(Some(b"original".to_vec()))
    );

    let mut socket = offer(&listener, Some(base), base).await;
    send(&mut socket, Message::Continue(base)).await;
    assert_eq!(read(&mut socket).await, Message::Ack(base));
    let next = Cursor {
        sequence: 11,
        ..base
    };
    let batch = ResolvedBatch {
        origin: MutationOrigin::Client,
        mutations: vec![put(b"persisted-before-ack")],
    };
    for _ in 0..2 {
        send(
            &mut socket,
            Message::Batch {
                sequence: next.sequence,
                batch: batch.clone(),
            },
        )
        .await;
        assert_eq!(read(&mut socket).await, Message::Ack(next));
    }
    send(
        &mut socket,
        Message::Batch {
            sequence: 13,
            batch,
        },
    )
    .await;
    assert!(
        protocol::read(&mut socket, Limits::default(), DEADLINE)
            .await
            .is_err()
    );
    assert_eq!(replica.cursor().await, next);
    // A política é everysec; matar assim que recebemos ACK exige flush da replicação.
    replica.stop();
    let replica = Node::start(&path, Some(upstream)).await;
    let mut socket = offer(&listener, Some(next), next).await;
    send(&mut socket, Message::Continue(next)).await;
    assert_eq!(read(&mut socket).await, Message::Ack(next));
    assert_eq!(
        replica.command(&[b"GET", b"{s}:key"]),
        Response::Bulk(Some(b"persisted-before-ack".to_vec()))
    );
    send(
        &mut socket,
        Message::Batch {
            sequence: next.sequence,
            batch: ResolvedBatch {
                origin: MutationOrigin::Client,
                mutations: vec![put(b"forged-duplicate")],
            },
        },
    )
    .await;
    assert!(
        protocol::read(&mut socket, Limits::default(), DEADLINE)
            .await
            .is_err()
    );
    assert_eq!(replica.cursor().await, next);
    assert_eq!(
        replica.command(&[b"GET", b"{s}:key"]),
        Response::Bulk(Some(b"persisted-before-ack".to_vec()))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_slow_peer_does_not_block_healthy_replica_and_lost_history_forces_full() {
    let directory = Directory::new();
    let primary = Node::start(&directory.0.join("primary"), None).await;
    let healthy = Node::start(&directory.0.join("healthy"), Some(primary.internal)).await;
    let path = directory.0.join("offline");
    let mut offline = Node::start(&path, Some(primary.internal)).await;
    caught_up(&primary, &healthy).await;
    caught_up(&primary, &offline).await;
    let baseline = primary.cursor().await;
    offline.stop();
    let mut slow = TcpStream::connect(primary.internal).await.unwrap();
    send(&mut slow, Message::Hello(hello(Some(baseline)))).await;
    assert!(matches!(read(&mut slow).await, Message::Hello(_)));
    assert_eq!(read(&mut slow).await, Message::Continue(baseline));
    send(&mut slow, Message::Ack(baseline)).await;
    for index in 0..200 {
        let value = format!("{index:04}{}", "x".repeat(1024));
        ok(primary.command(&[b"SET", b"{s}:bounded", value.as_bytes()]));
    }
    caught_up(&primary, &healthy).await;
    assert_eq!(
        healthy.command(&[b"GET", b"{s}:bounded"]),
        primary.command(&[b"GET", b"{s}:bounded"])
    );
    assert!(
        matches!(primary.status().await, Message::Status { backlog_bytes, .. } if backlog_bytes <= 32768)
    );
    // O peer reteve no máximo um lote fora do journal e ficou esperando ACK.
    assert!(matches!(read(&mut slow).await, Message::Heartbeat(_)));
    let Message::Batch { sequence, .. } = read(&mut slow).await else {
        panic!("primeiro lote");
    };
    send(&mut slow, Message::Ack(baseline)).await;
    send(
        &mut slow,
        Message::Ack(Cursor {
            sequence,
            ..baseline
        }),
    )
    .await;
    assert_eq!(read(&mut slow).await, Message::Reject(Reject::FullRequired));
    let offline = Node::start(&path, Some(primary.internal)).await;
    assert!(matches!(
        caught_up(&primary, &offline).await,
        Message::Status {
            full_syncs: 1,
            partial_syncs: 0,
            ..
        }
    ));
    assert_eq!(
        offline.command(&[b"GET", b"{s}:bounded"]),
        primary.command(&[b"GET", b"{s}:bounded"])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_primary_restart_changes_epoch_and_forces_full_online() {
    let directory = Directory::new();
    let path = directory.0.join("primary");
    let mut primary = Node::start(&path, None).await;
    ok(primary.command(&[b"SET", b"{s}:before", b"durable"]));
    let replica = Node::start(&directory.0.join("replica"), Some(primary.internal)).await;
    caught_up(&primary, &replica).await;
    let previous = primary.cursor().await;
    let listen = primary.internal;
    primary.stop();
    // Reabrir o endereço real do serviço após sua parada, sem reservar porta livre.
    let primary = Node::start_at(&path, None, Some(listen)).await;
    assert_ne!(primary.cursor().await.epoch, previous.epoch);
    ok(primary.command(&[b"SET", b"{s}:after", b"new-epoch"]));
    assert!(matches!(
        caught_up(&primary, &replica).await,
        Message::Status { full_syncs: 2, .. }
    ));
    assert_eq!(
        replica.command(&[b"MGET", b"{s}:before", b"{s}:after"]),
        primary.command(&[b"MGET", b"{s}:before", b"{s}:after"])
    );
}

struct PausedProxy {
    address: SocketAddr,
    paused: tokio::sync::watch::Sender<bool>,
    blocked: std::sync::Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl PausedProxy {
    async fn new(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (paused, receiver) = tokio::sync::watch::channel(false);
        let blocked = std::sync::Arc::new(tokio::sync::Notify::new());
        let signal = blocked.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((downstream, _)) = listener.accept().await else {
                    return;
                };
                let Ok(source) = TcpStream::connect(upstream).await else {
                    continue;
                };
                let (mut down_read, mut down_write) = downstream.into_split();
                let (mut source_read, mut source_write) = source.into_split();
                let mut paused = receiver.clone();
                let to_replica = async {
                    loop {
                        let Ok((frame, message)) = protocol::read_with_frame(
                            &mut source_read,
                            Limits::default(),
                            DEADLINE,
                        )
                        .await
                        else {
                            break;
                        };
                        if matches!(message, Message::Batch { .. }) && *paused.borrow() {
                            signal.notify_one();
                            while *paused.borrow_and_update() {
                                if paused.changed().await.is_err() {
                                    return;
                                }
                            }
                        }
                        if tokio::io::AsyncWriteExt::write_all(&mut down_write, &frame)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                };
                tokio::select! {
                    () = to_replica => {},
                    _ = tokio::io::copy(&mut down_read, &mut source_write) => {},
                }
            }
        });
        Self {
            address,
            paused,
            blocked,
            task,
        }
    }
}
impl Drop for PausedProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn delayed_promotion_case() -> serde_json::Value {
    let directory = Directory::new();
    let primary = Node::start(&directory.0.join("primary"), None).await;
    let proxy = PausedProxy::new(primary.internal).await;
    let path = directory.0.join("replica");
    let mut replica = Node::start(&path, Some(proxy.address)).await;
    let start = Instant::now();
    caught_up(&primary, &replica).await;
    let catchup_ms = start.elapsed().as_millis() as u64;
    let converged = replica.cursor().await;
    proxy.paused.send_replace(true);
    ok(primary.command(&[b"SET", b"{s}:unreplicated", b"only-primary"]));
    tokio::time::timeout(DEADLINE, proxy.blocked.notified())
        .await
        .unwrap();
    let deadline = Instant::now() + DEADLINE;
    let lag = loop {
        if let Message::Status {
            connected: true,
            upstream_sequence: Some(head),
            cursor,
            ..
        } = replica.status().await
            && head > cursor.sequence
        {
            break head - cursor.sequence;
        }
        assert!(Instant::now() < deadline, "atraso observado pelo heartbeat");
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_eq!(
        replica.command(&[b"GET", b"{s}:unreplicated"]),
        Response::Bulk(None)
    );
    let output = process::run(
        Command::new(env!("CARGO_BIN_EXE_sider-replica")).args([
            "--addr",
            &replica.internal.to_string(),
            "--promote",
        ]),
        DEADLINE,
    )
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["role"], "primary");
    assert_eq!(value["sequence"], converged.sequence);
    proxy.paused.send_replace(false);
    ok(replica.command(&[b"SET", b"{s}:owned", b"promoted"]));
    ok(primary.command(&[b"SET", b"{s}:owned", b"old-upstream"]));
    replica.stop();
    let replica = Node::start(&path, Some(proxy.address)).await;
    assert_eq!(
        replica.command(&[b"GET", b"{s}:unreplicated"]),
        Response::Bulk(None)
    );
    assert_eq!(
        replica.command(&[b"GET", b"{s}:owned"]),
        Response::Bulk(Some(b"promoted".to_vec()))
    );
    assert!(matches!(
        replica.status().await,
        Message::Status {
            readonly: false,
            connected: false,
            ..
        }
    ));
    serde_json::json!({ "initial_catchup_ms": catchup_ms, "converged_sequence": converged.sequence, "lag_before_manual_promotion_batches": lag, "unreplicated_keys_absent_after_promotion_and_restart": 1 })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_delayed_manual_promotion_preserves_applied_prefix_and_cuts_old_upstream() {
    delayed_promotion_case().await;
}

#[test]
#[ignore = "gate de release exige contexto exato; suíte interna roda diretamente"]
fn release_replication_gate() {
    let context = gate_receipt::GateContext::from_env("replication").unwrap();
    let began = Instant::now();
    replication_processes_full_delta_export_continue_and_durable_promotion();
    replication_process_rejects_bad_full_gaps_and_changed_duplicates_ack_survives_kill();
    replication_slow_peer_does_not_block_healthy_replica_and_lost_history_forces_full();
    replication_primary_restart_changes_epoch_and_forces_full_online();
    let observed = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
        .block_on(delayed_promotion_case());
    context.publish(5, began.elapsed(), serde_json::json!({
        "scope": "real_sider_processes", "scenarios": 5, "shards": 4, "value_types": 5,
        "snapshot_modes": ["full", "export"], "resume_modes": ["continue", "full_after_history_loss", "full_after_epoch_change"],
        "replica_sync_policy": "everysec_with_ack_flush", "observed": observed
    })).unwrap();
}
