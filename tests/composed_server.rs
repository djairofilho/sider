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
use std::process::Command;
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
fn composed_backup_cli_exports_verifies_restores_and_restarts() {
    let directory = Directory::new();
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_sider"));
    let backup_binary = PathBuf::from(env!("CARGO_BIN_EXE_sider-backup"));
    let export_ready = directory.0.join("export-ready.json");
    let configured = [
        ("SIDER_AOF_DIR", directory.aof().into_os_string()),
        ("SIDER_SHARDS", OsString::from("2")),
        ("SIDER_AOF_COMPOSED", OsString::from("1")),
        ("SIDER_COMPOSED_EXPORT_ADDR", OsString::from("127.0.0.1:0")),
        (
            "SIDER_COMPOSED_EXPORT_READY_FILE",
            export_ready.clone().into_os_string(),
        ),
    ];
    let server = sider_process::SiderProcess::try_start_configured(
        &binary,
        env!("CARGO_PKG_VERSION"),
        &configured,
    )
    .unwrap();
    let ready: serde_json::Value =
        serde_json::from_slice(&fs::read(&export_ready).unwrap()).unwrap();
    assert_eq!(ready["pid"], server.id());
    let export_addr = SocketAddr::from((
        [127, 0, 0, 1],
        u16::try_from(ready["port"].as_u64().unwrap()).unwrap(),
    ));
    let mut first_client = client(server.address());
    send(&mut first_client, &[b"MULTI"], b"+OK\r\n");
    send(&mut first_client, &[b"SET", b"a", b"left"], b"+QUEUED\r\n");
    send(&mut first_client, &[b"SET", b"b", b"right"], b"+QUEUED\r\n");
    send(&mut first_client, &[b"EXEC"], b"*2\r\n+OK\r\n+OK\r\n");

    let run = |args: Vec<OsString>| {
        let mut command = Command::new(&backup_binary);
        command
            .env("SIDER_SECURITY_PROFILE", "legacy-loopback")
            .env_remove("SIDER_SECURITY_DIR")
            .args(args);
        process::run(&mut command, Duration::from_secs(20)).unwrap()
    };
    let archive = directory.0.join("archive");
    let exported = run(vec![
        "export".into(),
        "--profile".into(),
        "composed".into(),
        "--source".into(),
        export_addr.to_string().into(),
        "--destination".into(),
        archive.clone().into_os_string(),
        "--source-sha".into(),
        "0123456789012345678901234567890123456789".into(),
    ]);
    assert!(
        exported.status.success(),
        "{}",
        String::from_utf8_lossy(&exported.stderr)
    );
    let manifest: serde_json::Value = serde_json::from_slice(&exported.stdout).unwrap();
    assert_eq!(manifest["schema_version"], 3);
    assert_eq!(manifest["required_capabilities"], 1);
    let verification = vec![
        "--source".into(),
        archive.clone().into_os_string(),
        "--shards".into(),
        "2".into(),
        "--routing".into(),
        "1".into(),
    ];
    let mut verify = vec!["verify".into(), "--profile".into(), "composed".into()];
    verify.extend(verification.clone());
    assert!(run(verify).status.success());
    let mut wrong_profile = vec!["verify".into()];
    wrong_profile.extend(verification.clone());
    assert!(!run(wrong_profile).status.success());
    let restored = directory.0.join("restored");
    let mut restore = vec!["restore".into(), "--profile".into(), "composed".into()];
    restore.extend(verification);
    restore.extend(["--destination".into(), restored.clone().into_os_string()]);
    assert!(run(restore).status.success());
    drop(first_client);
    server.finish();
    let restarted = sider_process::SiderProcess::try_start_configured(
        &binary,
        env!("CARGO_PKG_VERSION"),
        &[
            ("SIDER_AOF_DIR", restored.into_os_string()),
            ("SIDER_SHARDS", "2".into()),
            ("SIDER_AOF_COMPOSED", "1".into()),
        ],
    )
    .unwrap();
    let mut client = client(restarted.address());
    send(&mut client, &[b"GET", b"a"], b"$4\r\nleft\r\n");
    send(&mut client, &[b"GET", b"b"], b"$5\r\nright\r\n");
    drop(client);
    restarted.finish();
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

    send(&mut first, &[b"WATCH", b"a", b"b"], b"+OK\r\n");
    send(&mut first, &[b"MULTI"], b"+OK\r\n");
    send(&mut first, &[b"SET", b"b", b"discarded"], b"+QUEUED\r\n");
    send(&mut second, &[b"SET", b"a", b"new"], b"+OK\r\n");
    send(&mut first, &[b"EXEC"], b"*-1\r\n");
    send(&mut first, &[b"GET", b"b"], b"$5\r\nright\r\n");

    send(&mut first, &[b"WATCH", b"a", b"b"], b"+OK\r\n");
    send(&mut first, &[b"MULTI"], b"+OK\r\n");
    send(&mut first, &[b"SET", b"a", b"discarded"], b"+QUEUED\r\n");
    send(&mut second, &[b"SET", b"b", b"right"], b"+OK\r\n");
    send(&mut first, &[b"EXEC"], b"*-1\r\n");
    send(&mut first, &[b"GET", b"a"], b"$3\r\nnew\r\n");

    send(&mut first, &[b"WATCH", b"a"], b"+OK\r\n");
    send(&mut first, &[b"WATCH", b"b"], b"+OK\r\n");
    send(&mut first, &[b"MULTI"], b"+OK\r\n");
    send(
        &mut first,
        &[b"PUBLISH", b"channel", b"message"],
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
