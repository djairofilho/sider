//! R10-03: primário e CLI reais, rede lenta, tráfego transacional e instância restaurada.
#![forbid(unsafe_code)]

#[path = "common/process.rs"]
mod process;
#[path = "common/sider_process.rs"]
mod sider_process;
#[path = "common/wire.rs"]
mod wire;

use std::fs;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sider::persistence::{DurableLayout, backup};
use sider::replication::protocol::{self, Message};
use sider::storage::SystemClock;
use sider_process::SiderProcess;
use tokio::net::TcpSocket;
use wire::Response;

const TIMEOUT: Duration = Duration::from_secs(20);
const SHA: &str = "0123456789012345678901234567890123456789";

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-backup-process-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
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
fn connect(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, TIMEOUT).unwrap();
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(TIMEOUT)).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
}
fn call(stream: &mut TcpStream, args: &[&[u8]]) -> Response {
    stream
        .write_all(&wire::request(
            &args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>(),
        ))
        .unwrap();
    wire::read_response(stream).unwrap().value
}
fn start(binary: &Path, data: PathBuf, replication: Option<PathBuf>) -> SiderProcess {
    let mut config = vec![
        ("SIDER_AOF_DIR", data.into_os_string()),
        ("SIDER_AOF_SYNC", "always".into()),
        ("SIDER_SHARDS", "4".into()),
    ];
    if let Some(ready) = replication {
        config.extend([
            ("SIDER_REPLICATION_ADDR", "127.0.0.1:0".into()),
            ("SIDER_REPLICATION_READY_FILE", ready.into_os_string()),
            ("SIDER_REPLICATION_FRAME_TIMEOUT_MS", "20000".into()),
        ]);
    }
    SiderProcess::try_start_configured(binary, env!("CARGO_PKG_VERSION"), &config).unwrap()
}
fn replication_address(path: &Path, pid: u32) -> SocketAddr {
    let ready: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(ready["pid"], pid);
    assert_eq!(ready["host"], "127.0.0.1");
    let port = u16::try_from(ready["port"].as_u64().unwrap()).unwrap();
    assert_ne!(port, 0);
    SocketAddr::from(([127, 0, 0, 1], port))
}
fn cli(binary: &Path, args: &[&std::ffi::OsStr]) -> std::process::Output {
    let mut command = Command::new(binary);
    command.args(args);
    let result = process::run(&mut command, TIMEOUT).unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    result
}

struct Traffic {
    stop: Arc<AtomicBool>,
    count: Arc<AtomicU64>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Traffic {
    fn start(address: SocketAddr) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let count = Arc::new(AtomicU64::new(0));
        let thread_stop = stop.clone();
        let thread_count = count.clone();
        let thread = std::thread::spawn(move || {
            let mut stream = connect(address);
            while !thread_stop.load(Ordering::SeqCst) {
                assert_eq!(
                    call(&mut stream, &[b"MULTI"]),
                    Response::Simple(b"OK".to_vec())
                );
                for key in [b"{traffic}:a".as_slice(), b"{traffic}:b"] {
                    assert_eq!(
                        call(&mut stream, &[b"INCR", key]),
                        Response::Simple(b"QUEUED".to_vec())
                    );
                }
                let sequence = thread_count.load(Ordering::SeqCst) as i64 + 1;
                assert_eq!(
                    call(&mut stream, &[b"EXEC"]),
                    Response::Array(Some(vec![
                        Response::Integer(sequence),
                        Response::Integer(sequence)
                    ]))
                );
                thread_count.store(sequence as u64, Ordering::SeqCst);
            }
        });
        Self {
            stop,
            count,
            thread: Some(thread),
        }
    }
    async fn advance(&self, count: u64) {
        let wanted = self.count.load(Ordering::SeqCst) + count;
        let deadline = Instant::now() + TIMEOUT;
        while self.count.load(Ordering::SeqCst) < wanted {
            assert!(
                Instant::now() < deadline,
                "tráfego bloqueado durante exportação"
            );
            tokio::task::yield_now().await;
        }
    }
}
impl Drop for Traffic {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let result = thread.join();
            if !std::thread::panicking() {
                result.unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_backup_preserves_transaction_cut_while_slow_export_and_writes_continue() {
    exercise(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        Path::new(env!("CARGO_BIN_EXE_sider-backup")),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "exige SIDER_BACKUP_PACKAGE_DIR com pacote realmente extraído; não produz recibo de release"]
async fn extracted_package_backup_roundtrip_under_traffic() {
    let directory =
        PathBuf::from(std::env::var_os("SIDER_BACKUP_PACKAGE_DIR").expect("pacote explícito"));
    assert!(directory.is_absolute());
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let sider = directory.join(format!("sider{suffix}"));
    let backup = directory.join(format!("sider-backup{suffix}"));
    for path in [&directory, &sider, &backup] {
        let metadata = fs::symlink_metadata(path).unwrap();
        assert!(!metadata.file_type().is_symlink());
        if path != &directory {
            assert!(metadata.is_file() && metadata.len() > 0);
        }
    }
    exercise(&sider, &backup).await;
}

async fn exercise(sider_binary: &Path, backup_binary: &Path) {
    let parent = Directory::new();
    let primary = start(
        sider_binary,
        parent.child("primary"),
        Some(parent.child("replication-ready.json")),
    );
    let address = replication_address(&parent.child("replication-ready.json"), primary.id());
    let mut client = connect(primary.address());
    assert_eq!(
        call(&mut client, &[b"MULTI"]),
        Response::Simple(b"OK".to_vec())
    );
    for args in [
        vec![b"SET".as_slice(), b"{types}:string", b"\0\xff"],
        vec![b"HSET", b"{types}:hash", b"f\xff", b"v\0"],
        vec![b"RPUSH", b"{types}:list", b"\xff", b""],
        vec![b"SADD", b"{types}:set", b"\xff", b""],
        vec![b"ZADD", b"{types}:zset", b"1", b"\xff", b"2", b""],
        vec![b"SET", b"{types}:ttl", b"live", b"PX", b"120000"],
    ] {
        assert_eq!(
            call(&mut client, &args),
            Response::Simple(b"QUEUED".to_vec())
        );
    }
    assert_eq!(
        call(&mut client, &[b"EXEC"]),
        Response::Array(Some(vec![
            Response::Simple(b"OK".to_vec()),
            Response::Integer(1),
            Response::Integer(2),
            Response::Integer(2),
            Response::Integer(2),
            Response::Simple(b"OK".to_vec()),
        ]))
    );
    let layout = DurableLayout {
        shard_count: 4,
        routing_version: 1,
    };
    let value = vec![b'x'; 512 * 1024];
    let mut per_shard = [0usize; 4];
    // 8 MiB distribuídos igualmente: excede buffers do peer lento sem esgotar quota.
    for index in 0..1000 {
        let key = format!("bulk:{index}");
        let shard = layout.shard_for(key.as_bytes()).unwrap();
        if per_shard[shard] == 4 {
            continue;
        }
        assert_eq!(
            call(&mut client, &[b"SET", key.as_bytes(), &value]),
            Response::Simple(b"OK".to_vec())
        );
        per_shard[shard] += 1;
        if per_shard == [4; 4] {
            break;
        }
    }
    assert_eq!(per_shard, [4; 4]);
    let traffic = Traffic::start(primary.address());
    traffic.advance(5).await;
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(1024).unwrap();
    assert!(socket.recv_buffer_size().unwrap() <= 64 * 1024);
    let mut slow = socket.connect(address).await.unwrap();
    protocol::write(
        &mut slow,
        &Message::Export {
            sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
            max_record_bytes: backup::Limits::default().max_record_bytes as u32,
            max_snapshot_bytes: backup::Limits::default().max_snapshot_bytes,
        },
        protocol::Limits::default(),
        TIMEOUT,
    )
    .await
    .unwrap();
    assert!(matches!(
        protocol::read(&mut slow, protocol::Limits::default(), TIMEOUT)
            .await
            .unwrap(),
        Message::Hello(_)
    ));
    assert!(
        matches!(protocol::read(&mut slow, protocol::Limits::default(), TIMEOUT).await.unwrap(), Message::FullStart { entries, .. } if entries >= 24)
    );
    // Nenhuma entrada do snapshot é lida; o servidor fica sem espaço de envio.
    traffic.advance(20).await;
    let before = traffic.count.load(Ordering::SeqCst);
    let output = cli(
        backup_binary,
        &[
            "export".as_ref(),
            "--source".as_ref(),
            address.to_string().as_ref(),
            "--destination".as_ref(),
            parent.child("backup").as_os_str(),
            "--source-sha".as_ref(),
            SHA.as_ref(),
        ],
    );
    let after = traffic.count.load(Ordering::SeqCst);
    assert!(after > before);
    let manifest: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(manifest["layout"]["shard_count"], 4);
    assert!(manifest["snapshot"]["bytes"].as_u64().unwrap() > 8 * 1024 * 1024);
    drop(slow);
    drop(traffic);
    let ttl_before = call(&mut client, &[b"PTTL", b"{types}:ttl"]);
    cli(
        backup_binary,
        &[
            "restore".as_ref(),
            "--source".as_ref(),
            parent.child("backup").as_os_str(),
            "--destination".as_ref(),
            parent.child("restored").as_os_str(),
            "--shards".as_ref(),
            "4".as_ref(),
            "--routing".as_ref(),
            "1".as_ref(),
        ],
    );
    let verified = backup::verify(
        &parent.child("backup"),
        layout,
        backup::Limits::default(),
        Arc::new(SystemClock),
    )
    .unwrap();
    assert!(
        verified
            .shard_usage
            .iter()
            .all(|bytes| *bytes > 2 * 1024 * 1024)
    );
    let restored = start(sider_binary, parent.child("restored"), None);
    let mut recovered = connect(restored.address());
    for args in [
        vec![b"GET".as_slice(), b"{types}:string"],
        vec![b"HGETALL", b"{types}:hash"],
        vec![b"LRANGE", b"{types}:list", b"0", b"-1"],
        vec![b"SMEMBERS", b"{types}:set"],
        vec![b"ZRANGE", b"{types}:zset", b"0", b"-1", b"WITHSCORES"],
    ] {
        assert_eq!(call(&mut recovered, &args), call(&mut client, &args));
    }
    let a = call(&mut recovered, &[b"GET", b"{traffic}:a"]);
    let b = call(&mut recovered, &[b"GET", b"{traffic}:b"]);
    assert_eq!(a, b, "snapshot não pode capturar metade de EXEC");
    assert!(
        matches!(a, Response::Bulk(Some(value)) if std::str::from_utf8(&value).unwrap().parse::<u64>().unwrap() >= before)
    );
    let Response::Integer(ttl_before) = ttl_before else {
        panic!("PTTL da origem")
    };
    let Response::Integer(ttl_after) = call(&mut recovered, &[b"PTTL", b"{types}:ttl"]) else {
        panic!("PTTL restaurado")
    };
    assert!(
        ttl_after > 0 && ttl_after <= ttl_before,
        "TTL não deve recomeçar"
    );
    restored.finish();
    primary.finish();
    println!(
        "{}",
        serde_json::json!({
            "task":"R10-03", "status":"success", "snapshot_bytes":manifest["snapshot"]["bytes"],
            "cursor":manifest["cursor"], "transactions_before_export":before,
            "transactions_after_export":after, "slow_receiver_buffer":1024,
            "shards":4,"types":5,"ttl_before_restore_ms":ttl_before,"ttl_after_restore_ms":ttl_after,
            "binaries":{"sider":sider_binary,"backup":backup_binary},
            "temporary_fixture":true
        })
    );
}
