//! Explicit local composed profile through the executable and RESP transaction path.
#![forbid(unsafe_code)]

#[path = "common/process.rs"]
mod process;
#[path = "common/sider_process.rs"]
mod sider_process;
#[path = "common/wire.rs"]
mod wire;

use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sider::persistence::composed_format;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "sider-composed-server-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        Self(root)
    }
    fn aof(&self) -> PathBuf {
        self.0.join("aof")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let resolved = self.0.canonicalize().unwrap();
        let root = std::env::temp_dir().canonicalize().unwrap();
        assert_eq!(resolved.parent(), Some(root.as_path()));
        assert!(
            resolved
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("sider-composed-server-")
        );
        fs::remove_dir_all(resolved).unwrap();
    }
}

fn client(address: SocketAddr) -> TcpStream {
    let client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client
}

fn send(client: &mut TcpStream, args: &[&[u8]], expected: &[u8]) {
    client
        .write_all(&wire::request(
            &args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>(),
        ))
        .unwrap();
    let response = wire::read_response(client).unwrap();
    assert_eq!(response.bytes, expected, "command {args:?}");
}

#[test]
fn composed_exec_survives_restart_and_other_readers_refuse_header7() {
    let directory = Directory::new();
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_sider"));
    let version = env!("CARGO_PKG_VERSION");
    let aof = directory.aof().into_os_string();
    let configured = [
        ("SIDER_AOF_DIR", aof.clone()),
        ("SIDER_SHARDS", OsString::from("2")),
        ("SIDER_AOF_COMPOSED", OsString::from("1")),
    ];
    let server =
        sider_process::SiderProcess::try_start_configured(&binary, version, &configured).unwrap();
    let mut first = client(server.address());
    let mut second = client(server.address());
    send(&mut first, &[b"MULTI"], b"+OK\r\n");
    send(&mut first, &[b"SET", b"a", b"left"], b"+QUEUED\r\n");
    send(&mut first, &[b"SET", b"b", b"right"], b"+QUEUED\r\n");
    send(&mut first, &[b"EXEC"], b"*2\r\n+OK\r\n+OK\r\n");
    send(&mut first, &[b"GET", b"a"], b"$4\r\nleft\r\n");
    send(&mut first, &[b"GET", b"b"], b"$5\r\nright\r\n");
    send(&mut first, &[b"MULTI"], b"+OK\r\n");
    send(&mut first, &[b"MGET", b"b", b"a", b"b"], b"+QUEUED\r\n");
    send(
        &mut first,
        &[b"EXEC"],
        b"*1\r\n*3\r\n$5\r\nright\r\n$4\r\nleft\r\n$5\r\nright\r\n",
    );

    send(&mut first, &[b"WATCH", b"a"], b"+OK\r\n");
    send(&mut first, &[b"MULTI"], b"+OK\r\n");
    send(&mut first, &[b"SET", b"b", b"discarded"], b"+QUEUED\r\n");
    send(&mut second, &[b"SET", b"a", b"new"], b"+OK\r\n");
    send(&mut first, &[b"EXEC"], b"*-1\r\n");
    send(&mut first, &[b"GET", b"b"], b"$5\r\nright\r\n");

    send(&mut first, &[b"MULTI"], b"+OK\r\n");
    send(
        &mut first,
        &[b"PUBLISH", b"channel", b"message"],
        b"+QUEUED\r\n",
    );
    send(&mut first, &[b"SET", b"a", b"bad"], b"+QUEUED\r\n");
    send(
        &mut first,
        &[b"SET", b"b", b"bad"],
        b"-ERR unsupported cross-shard transaction command\r\n",
    );
    send(
        &mut first,
        &[b"EXEC"],
        b"-EXECABORT Transaction discarded because of previous errors.\r\n",
    );
    drop(first);
    drop(second);
    server.finish();

    let generation = directory.aof().join("generation-00000000000000000000.aof");
    let header = composed_format::read_header(fs::File::open(&generation).unwrap()).unwrap();
    assert!(header.mixed_owner_batch);
    let server =
        sider_process::SiderProcess::try_start_configured(&binary, version, &configured).unwrap();
    let mut first = client(server.address());
    send(&mut first, &[b"GET", b"a"], b"$3\r\nnew\r\n");
    send(&mut first, &[b"GET", b"b"], b"$5\r\nright\r\n");
    drop(first);
    server.finish();

    let before = fs::read(&generation).unwrap();
    let legacy = [
        ("SIDER_AOF_DIR", aof.clone()),
        ("SIDER_SHARDS", OsString::from("2")),
    ];
    assert!(sider_process::SiderProcess::try_start_configured(&binary, version, &legacy).is_err());
    let memory = [
        ("SIDER_AOF_DIR", aof),
        ("SIDER_SHARDS", OsString::from("2")),
        ("SIDER_AOF_RECORD_VERSION", OsString::from("2")),
    ];
    assert!(sider_process::SiderProcess::try_start_configured(&binary, version, &memory).is_err());
    assert_eq!(fs::read(generation).unwrap(), before);
}
