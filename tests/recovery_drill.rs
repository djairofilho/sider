//! Opt-in operator exercise against a verified extracted release, never a release receipt.
#![forbid(unsafe_code)]

#[path = "common/process.rs"]
mod process;
#[path = "common/wire.rs"]
mod wire;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sider::persistence::DurableLayout;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use wire::Response;

const TIMEOUT: Duration = Duration::from_secs(120);
const SEED: u64 = 107;
const PAYLOAD_BYTES: usize = 520_000;
const BULK_KEYS: usize = 128; // 63.48 MiB, balanced below every shard's quota.

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn file_hash(path: &Path) -> String {
    hash(&fs::read(path).unwrap())
}
fn save(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}
fn unix_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}
fn env_path(name: &str) -> PathBuf {
    let path = PathBuf::from(std::env::var_os(name).unwrap_or_else(|| panic!("required {name}")));
    assert!(path.is_absolute(), "{name} must be absolute");
    path
}

struct Evidence {
    root: PathBuf,
    began: Instant,
    complete: bool,
}
impl Evidence {
    fn new(root: PathBuf) -> Self {
        fs::create_dir(&root).expect("evidence destination must be new, with an existing parent");
        save(
            &root.join("result.json"),
            &json!({"schema":1,"status":"running","issue":107}),
        );
        Self {
            root,
            began: Instant::now(),
            complete: false,
        }
    }
    fn event(&self, name: &str, began: Instant, detail: Value) {
        let mut log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("phases.jsonl"))
            .unwrap();
        writeln!(log, "{}", json!({"phase":name,"start_ns":began.duration_since(self.began).as_nanos().to_string(),
            "end_ns":self.began.elapsed().as_nanos().to_string(),"elapsed_ms":began.elapsed().as_secs_f64()*1000.0,"detail":detail})).unwrap();
        log.sync_all().unwrap();
    }
    fn success(&mut self, detail: Value) {
        save(
            &self.root.join("result.json"),
            &json!({"schema":1,"status":"success","issue":107,"platform":std::env::consts::OS,"detail":detail}),
        );
        self.complete = true;
    }
}
impl Drop for Evidence {
    fn drop(&mut self) {
        if !self.complete {
            let _ = fs::write(
                self.root.join("result.json"),
                b"{\"schema\":1,\"status\":\"failed\",\"issue\":107}\n",
            );
        }
    }
}

fn checksum(text: &str, name: &str) -> String {
    let values: Vec<_> = text
        .lines()
        .filter_map(|line| line.split_once("  "))
        .filter(|(_, path)| *path == name)
        .collect();
    assert_eq!(values.len(), 1, "unique checksum for {name}");
    let digest = values[0].0;
    assert!(digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()));
    digest.to_ascii_lowercase()
}

fn verify_package(evidence: &Evidence) -> (PathBuf, PathBuf, String) {
    let began = Instant::now();
    let package = env_path("SIDER_DRILL_PACKAGE_DIR");
    let archive = env_path("SIDER_DRILL_ARCHIVE");
    let manifest = env_path("SIDER_DRILL_MANIFEST");
    let sums = env_path("SIDER_DRILL_SHA256SUMS");
    for path in [&package, &archive, &manifest, &sums] {
        assert!(!fs::symlink_metadata(path).unwrap().file_type().is_symlink());
    }
    let sums_text = fs::read_to_string(&sums).unwrap();
    assert_eq!(
        file_hash(&manifest),
        checksum(&sums_text, "release-manifest.json")
    );
    let name = archive.file_name().unwrap().to_str().unwrap();
    assert_eq!(file_hash(&archive), checksum(&sums_text, name));
    let release: Value = serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
    assert_eq!(release["schema_version"], 2);
    assert_eq!(release["artifact_version"], env!("CARGO_PKG_VERSION"));
    let sha = std::env::var("SIDER_DRILL_SOURCE_SHA").expect("declared full source SHA");
    assert!(sha.len() == 40 && sha.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(release["sha"], sha);
    let target = if cfg!(windows) {
        "x86_64-pc-windows-msvc"
    } else {
        "x86_64-unknown-linux-gnu"
    };
    assert_eq!(std::env::consts::ARCH, "x86_64");
    assert!(["windows", "linux"].contains(&std::env::consts::OS));
    let stem = format!("sider-v{}-{target}", env!("CARGO_PKG_VERSION"));
    assert_eq!(
        name,
        format!("{stem}.{}", if cfg!(windows) { "zip" } else { "tar.gz" })
    );
    let artifact = release["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == name)
        .unwrap();
    assert_eq!(artifact["sha256"], file_hash(&archive));
    assert_eq!(
        artifact["size"].as_u64(),
        Some(fs::metadata(&archive).unwrap().len())
    );
    let mut binaries = Vec::new();
    for executable in [
        "sider",
        "sider-backup",
        "sider-replica",
        "sider-aof-migrate",
    ] {
        let filename = format!("{executable}{}", if cfg!(windows) { ".exe" } else { "" });
        let path = package.join(&filename);
        assert!(fs::symlink_metadata(&path).unwrap().is_file());
        assert!(
            !fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        let archived = process::run_with_stdout_limit(
            Command::new("tar")
                .arg("-xOf")
                .arg(&archive)
                .arg(format!("{stem}/{filename}")),
            TIMEOUT,
            16 * 1024 * 1024,
        )
        .unwrap();
        assert!(archived.status.success() && !archived.stdout.is_empty());
        assert_eq!(
            file_hash(&path),
            hash(&archived.stdout),
            "extracted binary differs from checked archive"
        );
        binaries.push(json!({"path":path,"bytes":archived.stdout.len(),"sha256":file_hash(&path)}));
    }
    let context_path = env_path("SIDER_DRILL_CONTEXT");
    let context: Value = serde_json::from_slice(&fs::read(&context_path).unwrap()).unwrap();
    for field in [
        "os",
        "cpu",
        "ram_bytes",
        "storage",
        "filesystem",
        "workload_isolation",
    ] {
        assert!(!context[field].is_null(), "context requires {field}");
    }
    assert_eq!(
        context["workload_isolation"],
        "no competing benchmark, build, or load test"
    );
    fs::copy(&context_path, evidence.root.join("context.json")).unwrap();
    fs::copy(&manifest, evidence.root.join("release-manifest.json")).unwrap();
    fs::copy(&sums, evidence.root.join("release-SHA256SUMS")).unwrap();
    save(
        &evidence.root.join("package.json"),
        &json!({"source_sha":sha,"target":target,"archive":archive,
        "archive_sha256":file_hash(&archive),"manifest_sha256":file_hash(&manifest),"checksums_sha256":file_hash(&sums),"binaries":binaries}),
    );
    evidence.event(
        "package_identity",
        began,
        json!({"target":target,"source_sha":sha}),
    );
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    (
        package.join(format!("sider{suffix}")),
        package.join(format!("sider-backup{suffix}")),
        sha,
    )
}

fn command(binary: &Path) -> Command {
    let mut cmd = Command::new(binary);
    for (name, _) in std::env::vars_os() {
        if name
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("SIDER_")
        {
            cmd.env_remove(name);
        }
    }
    cmd
}
struct Server {
    child: Child,
    ready: PathBuf,
    internal: PathBuf,
}
impl Server {
    fn spawn(binary: &Path, root: &Path, label: &str, data: &Path, shards: usize) -> Self {
        let ready = root.join(format!("{label}-ready.json"));
        let internal = root.join(format!("{label}-internal.json"));
        let mut cmd = command(binary);
        cmd.env("SIDER_ADDR", "127.0.0.1:0")
            .env("SIDER_READY_FILE", &ready)
            .env("SIDER_REPLICATION_ADDR", "127.0.0.1:0")
            .env("SIDER_REPLICATION_READY_FILE", &internal)
            .env("SIDER_AOF_DIR", data)
            .env("SIDER_AOF_SYNC", "always")
            .env("SIDER_SHARDS", shards.to_string())
            .env("SIDER_AOF_COMPACT_AFTER_BYTES", "0")
            .stdout(Stdio::from(
                File::create(root.join(format!("{label}.stdout"))).unwrap(),
            ))
            .stderr(Stdio::from(
                File::create(root.join(format!("{label}.stderr"))).unwrap(),
            ))
            .stdin(Stdio::null());
        let child = cmd.spawn().unwrap();
        save(
            &root.join(format!("{label}-command.json")),
            &json!({"binary":binary,"pid":child.id(),"data":data,
            "SIDER_ADDR":"127.0.0.1:0","SIDER_REPLICATION_ADDR":"127.0.0.1:0","SIDER_AOF_SYNC":"always",
            "SIDER_SHARDS":shards,"SIDER_AOF_COMPACT_AFTER_BYTES":0,"ready":ready,"internal_ready":internal}),
        );
        Self {
            child,
            ready,
            internal,
        }
    }
    fn address(&self, internal: bool) -> SocketAddr {
        let ready: Value = serde_json::from_slice(
            &fs::read(if internal {
                &self.internal
            } else {
                &self.ready
            })
            .unwrap(),
        )
        .unwrap();
        assert_eq!(ready["pid"], self.child.id());
        assert_eq!(ready["host"], "127.0.0.1");
        let port = u16::try_from(ready["port"].as_u64().unwrap()).unwrap();
        assert_ne!(port, 0);
        SocketAddr::from(([127, 0, 0, 1], port))
    }
    fn await_ready(&mut self) -> SocketAddr {
        let deadline = Instant::now() + TIMEOUT;
        while !self.ready.exists() {
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "startup failed; inspect retained stderr"
            );
            assert!(Instant::now() < deadline, "readiness deadline");
            std::thread::sleep(Duration::from_millis(10));
        }
        let addr = self.address(false);
        assert_eq!(
            call(&mut connect(addr), &[b"PING"]),
            Response::Simple(b"PONG".to_vec())
        );
        addr
    }
    fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
    fn rejected(&mut self) {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            assert!(!self.ready.exists(), "invalid AOF published readiness");
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(!status.success());
                break;
            }
            assert!(Instant::now() < deadline, "failed startup deadline");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
fn connect(addr: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&addr, TIMEOUT).unwrap();
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(TIMEOUT)).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
}
fn observed(stream: &mut TcpStream, args: &[&[u8]]) -> wire::Observed {
    stream
        .write_all(&wire::request(
            &args.iter().map(|a| a.to_vec()).collect::<Vec<_>>(),
        ))
        .unwrap();
    wire::read_response(stream).unwrap()
}
fn call(stream: &mut TcpStream, args: &[&[u8]]) -> Response {
    observed(stream, args).value
}
fn ok(stream: &mut TcpStream, args: &[&[u8]]) {
    assert_eq!(call(stream, args), Response::Simple(b"OK".to_vec()));
}
fn payload(index: usize) -> Vec<u8> {
    (0..PAYLOAD_BYTES)
        .map(|i| ((i as u64 * 31 + index as u64 * 17 + SEED) % 256) as u8)
        .collect()
}
fn keys(shards: usize) -> Vec<Vec<u8>> {
    let layout = DurableLayout {
        shard_count: shards as u32,
        routing_version: 1,
    };
    let mut counts = vec![0; shards];
    let mut keys = Vec::new();
    for candidate in 0..10000 {
        let key = format!("bulk:{candidate}").into_bytes();
        let shard = layout.shard_for(&key).unwrap();
        let wanted = BULK_KEYS / shards + usize::from(shard < BULK_KEYS % shards);
        if counts[shard] < wanted {
            counts[shard] += 1;
            keys.push(key);
        }
        if keys.len() == BULK_KEYS {
            break;
        }
    }
    assert_eq!(keys.len(), BULK_KEYS);
    keys
}
fn load(stream: &mut TcpStream, keys: &[Vec<u8>]) {
    for (index, key) in keys.iter().enumerate() {
        ok(stream, &[b"SET", key, &payload(index)]);
    }
    ok(stream, &[b"MULTI"]);
    for args in [
        vec![b"SET".as_slice(), b"{types}:string\xff", b"\0\xff\r\n"],
        vec![b"HSET", b"{types}:hash", b"f\xff", b"v\0"],
        vec![b"RPUSH", b"{types}:list", b"\xff", b""],
        vec![b"SADD", b"{types}:set", b"\xff"],
        vec![b"ZADD", b"{types}:zset", b"1", b"\xff", b"2", b""],
        vec![b"SET", b"{types}:long", b"live", b"PX", b"3600000"],
    ] {
        assert_eq!(call(stream, &args), Response::Simple(b"QUEUED".to_vec()));
    }
    assert_eq!(
        call(stream, &[b"EXEC"]),
        Response::Array(Some(vec![
            Response::Simple(b"OK".to_vec()),
            Response::Integer(1),
            Response::Integer(2),
            Response::Integer(1),
            Response::Integer(2),
            Response::Simple(b"OK".to_vec())
        ]))
    );
}
fn compare(stream: &mut TcpStream, keys: &[Vec<u8>]) -> String {
    let mut digest = Sha256::new();
    for (index, key) in keys.iter().enumerate() {
        let reply = observed(stream, &[b"GET", key]);
        assert_eq!(reply.value, Response::Bulk(Some(payload(index))));
        digest.update((key.len() as u64).to_le_bytes());
        digest.update(key);
        digest.update(reply.bytes);
    }
    for (args, expected) in [
        (
            vec![b"GET".as_slice(), b"{types}:string\xff"],
            Response::Bulk(Some(b"\0\xff\r\n".to_vec())),
        ),
        (
            vec![b"HGETALL", b"{types}:hash"],
            Response::Array(Some(vec![
                Response::Bulk(Some(b"f\xff".to_vec())),
                Response::Bulk(Some(b"v\0".to_vec())),
            ])),
        ),
        (
            vec![b"LRANGE", b"{types}:list", b"0", b"-1"],
            Response::Array(Some(vec![
                Response::Bulk(Some(vec![255])),
                Response::Bulk(Some(vec![])),
            ])),
        ),
        (
            vec![b"SMEMBERS", b"{types}:set"],
            Response::Array(Some(vec![Response::Bulk(Some(vec![255]))])),
        ),
        (
            vec![b"ZRANGE", b"{types}:zset", b"0", b"-1", b"WITHSCORES"],
            Response::Array(Some(vec![
                Response::Bulk(Some(vec![255])),
                Response::Bulk(Some(b"1".to_vec())),
                Response::Bulk(Some(vec![])),
                Response::Bulk(Some(b"2".to_vec())),
            ])),
        ),
        (
            vec![b"GET", b"{types}:long"],
            Response::Bulk(Some(b"live".to_vec())),
        ),
        (vec![b"GET", b"{types}:short"], Response::Bulk(None)),
    ] {
        let reply = observed(stream, &args);
        assert_eq!(reply.value, expected);
        digest.update(reply.bytes);
    }
    format!("{:x}", digest.finalize())
}
fn integer(stream: &mut TcpStream, args: &[&[u8]]) -> i64 {
    let Response::Integer(n) = call(stream, args) else {
        panic!("expected integer")
    };
    n
}
#[derive(Clone, Copy, Debug)]
struct TtlSample {
    remaining_ms: i64,
    wall_before_ms: i64,
    wall_after_ms: i64,
    query_monotonic_ms: i64,
}
impl TtlSample {
    fn valid(self) -> bool {
        self.remaining_ms > 0
            && self.wall_after_ms >= self.wall_before_ms
            && self.query_monotonic_ms <= 250
            && ((self.wall_after_ms - self.wall_before_ms) - self.query_monotonic_ms).abs() <= 250
    }
    fn deadline(self) -> (i64, i64) {
        (
            self.wall_before_ms + self.remaining_ms,
            self.wall_after_ms + self.remaining_ms,
        )
    }
    fn gap(self, other: Self) -> i64 {
        let (lo, hi) = self.deadline();
        let (other_lo, other_hi) = other.deadline();
        (lo.max(other_lo) - hi.min(other_hi)).max(0)
    }
}
fn sample_ttl(
    evidence: &Evidence,
    stream: &mut TcpStream,
    shards: usize,
    stage: &str,
) -> TtlSample {
    let began = Instant::now();
    let wall_before_ms = unix_ms();
    let remaining_ms = integer(stream, &[b"PTTL", b"{types}:long"]);
    let wall_after_ms = unix_ms();
    let query_monotonic_ms = i64::try_from(began.elapsed().as_millis()).unwrap();
    let sample = TtlSample {
        remaining_ms,
        wall_before_ms,
        wall_after_ms,
        query_monotonic_ms,
    };
    let civil_delta_ms = wall_after_ms - wall_before_ms;
    evidence.event("ttl_sample", began, json!({"shards":shards,"stage":stage,"remaining_ms":remaining_ms,
        "wall_before_ms":wall_before_ms,"wall_after_ms":wall_after_ms,"query_monotonic_ms":query_monotonic_ms,
        "civil_minus_monotonic_ms":civil_delta_ms-query_monotonic_ms,"inferred_deadline_ms":sample.deadline(),
        "sample_valid":sample.valid(),"tolerance_ms":250}));
    assert!(
        sample.valid(),
        "invalid TTL observation (expired, delayed query, or civil clock jump): {sample:?}"
    );
    sample
}
fn same_deadline(
    evidence: &Evidence,
    anchor: TtlSample,
    sample: TtlSample,
    shards: usize,
    stage: &str,
) {
    let began = Instant::now();
    let gap = anchor.gap(sample);
    evidence.event(
        "ttl_deadline_check",
        began,
        json!({"shards":shards,"stage":stage,
        "anchor_deadline_ms":anchor.deadline(),"sample_deadline_ms":sample.deadline(),
        "interval_gap_ms":gap,"tolerance_ms":250,"oracle":"absolute_deadline_brackets_v1"}),
    );
    assert!(
        gap <= 250,
        "{stage} changed absolute TTL deadline: anchor={anchor:?}, sample={sample:?}, gap={gap} ms"
    );
}
fn count(stream: &mut TcpStream, key: &[u8]) -> u64 {
    let Response::Bulk(Some(n)) = call(stream, &[b"GET", key]) else {
        panic!("expected counter")
    };
    std::str::from_utf8(&n).unwrap().parse().unwrap()
}

struct Traffic {
    stop: Arc<AtomicBool>,
    count: Arc<AtomicU64>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Traffic {
    fn start(addr: SocketAddr) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let count = Arc::new(AtomicU64::new(0));
        let s = stop.clone();
        let c = count.clone();
        let thread = std::thread::spawn(move || {
            let mut stream = connect(addr);
            while !s.load(Ordering::SeqCst) {
                ok(&mut stream, &[b"MULTI"]);
                for key in [b"{traffic}:a".as_slice(), b"{traffic}:b"] {
                    assert_eq!(
                        call(&mut stream, &[b"INCR", key]),
                        Response::Simple(b"QUEUED".to_vec())
                    );
                }
                let n = c.load(Ordering::SeqCst) + 1;
                assert_eq!(
                    call(&mut stream, &[b"EXEC"]),
                    Response::Array(Some(vec![
                        Response::Integer(n as i64),
                        Response::Integer(n as i64)
                    ]))
                );
                c.store(n, Ordering::SeqCst);
            }
        });
        Self {
            stop,
            count,
            thread: Some(thread),
        }
    }
    fn advance(&self) {
        let wanted = self.count.load(Ordering::SeqCst) + 5;
        let deadline = Instant::now() + TIMEOUT;
        while self.count.load(Ordering::SeqCst) < wanted {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    fn stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
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

struct CliChild(Child);
impl Drop for CliChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
fn cli(binary: &Path, root: &Path, label: &str, args: &[String], success: bool) -> Value {
    save(
        &root.join(format!("{label}-command.json")),
        &json!({"binary":binary,"args":args,"expected_success":success}),
    );
    // Files retain even partial output when the CLI times out or the harness panics.
    let stdout = root.join(format!("{label}.stdout"));
    let mut cmd = command(binary);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(File::create(&stdout).unwrap()))
        .stderr(Stdio::from(
            File::create(root.join(format!("{label}.stderr"))).unwrap(),
        ));
    let mut child = CliChild(cmd.spawn().unwrap());
    let deadline = Instant::now() + TIMEOUT;
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            save(
                &root.join(format!("{label}-exit.json")),
                &json!({"success":false,"timed_out":true}),
            );
            panic!("{label} exceeded deadline; raw output retained");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    save(
        &root.join(format!("{label}-exit.json")),
        &json!({"success":status.success(),"code":status.code(),"timed_out":false}),
    );
    assert_eq!(
        status.success(),
        success,
        "{label}; inspect retained stderr"
    );
    if success {
        assert!(fs::metadata(&stdout).unwrap().len() <= process::OUTPUT_LIMIT as u64);
        serde_json::from_slice(&fs::read(stdout).unwrap()).unwrap()
    } else {
        Value::Null
    }
}
fn backup_args(verb: &str, source: &Path, shards: usize) -> Vec<String> {
    vec![
        verb.into(),
        "--source".into(),
        source.display().to_string(),
        "--shards".into(),
        shards.to_string(),
        "--routing".into(),
        "1".into(),
    ]
}
fn copy_directory(source: &Path, dest: &Path) {
    fs::create_dir(dest).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        assert!(entry.file_type().unwrap().is_file());
        fs::copy(entry.path(), dest.join(entry.file_name())).unwrap();
    }
}
fn generations(path: &Path) -> Value {
    let mut files = fs::read_dir(path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "aof"))
        .collect::<Vec<_>>();
    files.sort();
    assert!(!files.is_empty());
    json!(files.iter().map(|p|json!({"name":p.file_name().unwrap().to_str().unwrap(),"sha256":file_hash(p),"bytes":fs::metadata(p).unwrap().len()})).collect::<Vec<_>>())
}

fn exercise(
    evidence: &Evidence,
    server_binary: &Path,
    backup_binary: &Path,
    sha: &str,
    shards: usize,
) -> Value {
    let root = evidence.root.join(format!("shards-{shards}"));
    fs::create_dir(&root).unwrap();
    let primary_dir = root.join("primary");
    let backup_dir = root.join("backup");
    let restore_dir = root.join("restored");
    let began = Instant::now();
    let mut primary = Server::spawn(server_binary, &root, "primary", &primary_dir, shards);
    let addr = primary.await_ready();
    evidence.event("primary_ready", began, json!({"shards":shards}));
    let keys = keys(shards);
    save(
        &root.join("dataset.json"),
        &json!({"seed":SEED,"payload_bytes_per_key":PAYLOAD_BYTES,
        "payload_formula":"(byte_index * 31 + key_index * 17 + 107) % 256",
        "keys":keys.iter().map(|k|String::from_utf8(k.clone()).unwrap()).collect::<Vec<_>>(),
        "quota_bytes":64*1024*1024,"routing":1,"shards":shards,"short_ttl_ms":5000,"long_ttl_ms":3600000}),
    );
    let mut client = connect(addr);
    let began = Instant::now();
    load(&mut client, &keys);
    evidence.event("load",began,json!({"shards":shards,"seed":SEED,"bulk_payload_bytes":BULK_KEYS*PAYLOAD_BYTES,"bulk_keys":keys.len()}));
    ok(
        &mut client,
        &[b"SET", b"{types}:short", b"expires", b"PX", b"5000"],
    );
    assert!(integer(&mut client, &[b"PTTL", b"{types}:short"]) > 0);
    let ttl_began = Instant::now();
    let ttl_anchor = sample_ttl(evidence, &mut client, shards, "anchor");
    let long_before = ttl_anchor.remaining_ms;
    evidence.event("ttl_anchor", ttl_began, json!({"shards":shards,"long_pttl_ms":long_before,"short_pttl_positive":true,"tolerance_ms":250,"oracle":"absolute_deadline_brackets_v1"}));
    let mut traffic = Traffic::start(addr);
    traffic.advance();
    let before = traffic.count.load(Ordering::SeqCst);
    let began = Instant::now();
    let exported = cli(
        backup_binary,
        &root,
        "export",
        &[
            "export".into(),
            "--source".into(),
            primary.address(true).to_string(),
            "--destination".into(),
            backup_dir.display().to_string(),
            "--source-sha".into(),
            sha.into(),
        ],
        true,
    );
    let after = traffic.count.load(Ordering::SeqCst);
    assert!(after > before, "writes must progress during export");
    assert_eq!(
        exported["snapshot"]["entries"],
        BULK_KEYS + 9,
        "short TTL must exist in capture"
    );
    evidence.event(
        "export",
        began,
        json!({"shards":shards,"before":before,"after":after,"manifest":exported}),
    );
    traffic.stop();
    let final_count = traffic.count.load(Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(10);
    while integer(&mut client, &[b"PTTL", b"{types}:short"]) != -2 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    let began = Instant::now();
    let source_digest = compare(&mut client, &keys);
    evidence.event(
        "source_compare",
        began,
        json!({"sha256":source_digest,"shards":shards}),
    );
    let verify_recovery_began = Instant::now();
    let began = verify_recovery_began;
    let verified = cli(
        backup_binary,
        &root,
        "verify",
        &backup_args("verify", &backup_dir, shards),
        true,
    );
    let shard_usage = verified["shard_usage"].as_array().unwrap();
    assert_eq!(shard_usage.len(), shards);
    let logical_bytes: u64 = shard_usage.iter().map(|n| n.as_u64().unwrap()).sum();
    assert!((63 * 1024 * 1024..64 * 1024 * 1024).contains(&logical_bytes));
    assert!(
        shard_usage
            .iter()
            .all(|n| n.as_u64().unwrap() < (64 * 1024 * 1024 / shards) as u64)
    );
    evidence.event("verify", began, json!({"shards":shards,"report":verified}));
    let recovery_began = Instant::now();
    let began = Instant::now();
    let mut args = backup_args("restore", &backup_dir, shards);
    args.extend(["--destination".into(), restore_dir.display().to_string()]);
    cli(backup_binary, &root, "restore", &args, true);
    evidence.event("restore", began, json!({"shards":shards}));
    let began = Instant::now();
    let mut restored = Server::spawn(server_binary, &root, "restored", &restore_dir, shards);
    let restored_addr = restored.await_ready();
    evidence.event("restored_ready", began, json!({"shards":shards}));
    let mut recovered = connect(restored_addr);
    // Sample immediately after replay/readiness. Runtime TTL advances monotonically;
    // a later civil clock adjustment during the full data read is a separate event.
    let restored_ttl = sample_ttl(evidence, &mut recovered, shards, "restore");
    same_deadline(evidence, ttl_anchor, restored_ttl, shards, "restore");
    let long_after = restored_ttl.remaining_ms;
    let began = Instant::now();
    let restored_digest = compare(&mut recovered, &keys);
    assert_eq!(restored_digest, source_digest);
    let cut = count(&mut recovered, b"{traffic}:a");
    assert_eq!(cut, count(&mut recovered, b"{traffic}:b"));
    assert!((before..=final_count).contains(&cut));
    evidence.event("restored_compare",began,json!({"shards":shards,"sha256":restored_digest,"transaction_cut":cut,"long_ttl_ms":long_after,"short_ttl":-2}));
    evidence.event(
        "restore_to_data_validated",
        recovery_began,
        json!({"shards":shards}),
    );
    evidence.event(
        "verify_to_data_validated",
        verify_recovery_began,
        json!({"shards":shards}),
    );
    restored.kill();
    primary.kill();
    save(
        &root.join("primary-before-restart.json"),
        &generations(&primary_dir),
    );
    let recovery_began = Instant::now();
    let began = Instant::now();
    let mut restarted = Server::spawn(server_binary, &root, "aof-restart", &primary_dir, shards);
    let restart_addr = restarted.await_ready();
    evidence.event("aof_restart_ready", began, json!({"shards":shards}));
    let began = Instant::now();
    let mut client = connect(restart_addr);
    let restarted_ttl = sample_ttl(evidence, &mut client, shards, "aof_restart");
    same_deadline(evidence, ttl_anchor, restarted_ttl, shards, "aof_restart");
    assert_eq!(compare(&mut client, &keys), source_digest);
    for key in [b"{traffic}:a".as_slice(), b"{traffic}:b"] {
        assert_eq!(count(&mut client, key), final_count);
    }
    evidence.event(
        "aof_restart_compare",
        began,
        json!({"shards":shards,"sha256":source_digest,"acknowledged_transactions":final_count}),
    );
    evidence.event(
        "aof_restart_to_data_validated",
        recovery_began,
        json!({"shards":shards}),
    );
    restarted.kill();
    let original = generations(&primary_dir);
    save(&root.join("primary-after-restart.json"), &original);
    // Only a stopped copy receives an incomplete record. Recovery must preserve
    // the full pre-repair generation in a tail backup before truncating it.
    let tail = root.join("incomplete-tail");
    copy_directory(&primary_dir, &tail);
    let generation = fs::read_dir(&tail)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "aof"))
        .max()
        .unwrap();
    OpenOptions::new()
        .append(true)
        .open(&generation)
        .unwrap()
        .write_all(&[128, 0, 0])
        .unwrap();
    let damaged_hash = file_hash(&generation);
    let began = Instant::now();
    let mut repaired = Server::spawn(server_binary, &root, "tail-recovery", &tail, shards);
    let repaired_addr = repaired.await_ready();
    evidence.event("tail_recovery_ready", began, json!({"shards":shards}));
    let mut repaired_client = connect(repaired_addr);
    let tail_ttl = sample_ttl(evidence, &mut repaired_client, shards, "tail_recovery");
    same_deadline(evidence, ttl_anchor, tail_ttl, shards, "tail_recovery");
    assert_eq!(compare(&mut repaired_client, &keys), source_digest);
    for key in [b"{traffic}:a".as_slice(), b"{traffic}:b"] {
        assert_eq!(count(&mut connect(repaired_addr), key), final_count);
    }
    repaired.kill();
    let tail_backup = fs::read_dir(&tail)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("tail-")
                && p.extension().is_some_and(|e| e == "bak")
        })
        .expect("preserved pre-repair AOF");
    assert_eq!(file_hash(&tail_backup), damaged_hash);
    evidence.event(
        "tail_recovery_to_data_validated",
        began,
        json!({"shards":shards,"pre_repair_sha256":damaged_hash,"preserved":tail_backup}),
    );
    let backup_hash = file_hash(&backup_dir.join("snapshot.aof"));
    let corrupt = root.join("corrupt-backup");
    copy_directory(&backup_dir, &corrupt);
    let mut bytes = fs::read(corrupt.join("snapshot.aof")).unwrap();
    bytes[64] ^= 1;
    fs::write(corrupt.join("snapshot.aof"), bytes).unwrap();
    let began = Instant::now();
    cli(
        backup_binary,
        &root,
        "corrupt-verify",
        &backup_args("verify", &corrupt, shards),
        false,
    );
    let incompatible_destination = root.join("incompatible-restore");
    let mut args = backup_args("restore", &backup_dir, if shards == 1 { 4 } else { 1 });
    args.extend([
        "--destination".into(),
        incompatible_destination.display().to_string(),
    ]);
    cli(backup_binary, &root, "incompatible-restore", &args, false);
    assert!(!incompatible_destination.exists());
    let mut args = backup_args("restore", &corrupt, shards);
    let rejected = root.join("rejected-restore");
    args.extend(["--destination".into(), rejected.display().to_string()]);
    cli(backup_binary, &root, "corrupt-restore", &args, false);
    assert!(!rejected.exists());
    cli(
        backup_binary,
        &root,
        "incompatible-verify",
        &backup_args("verify", &backup_dir, if shards == 1 { 4 } else { 1 }),
        false,
    );
    let mut args = backup_args("restore", &backup_dir, shards);
    args.extend(["--destination".into(), restore_dir.display().to_string()]);
    let restored_original = generations(&restore_dir);
    cli(backup_binary, &root, "existing-destination", &args, false);
    assert_eq!(generations(&restore_dir), restored_original);
    for (label, offset) in [
        ("corrupt-aof", Some(64)),
        ("unknown-version-aof", Some(8)),
        ("incompatible-aof", None),
    ] {
        let copied = root.join(label);
        copy_directory(&primary_dir, &copied);
        if let Some(offset) = offset {
            let path = fs::read_dir(&copied)
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| p.extension().is_some_and(|e| e == "aof"))
                .max()
                .unwrap();
            let mut bytes = fs::read(&path).unwrap();
            bytes[offset] ^= 128;
            fs::write(path, bytes).unwrap();
        }
        let previous = generations(&copied);
        let mut failed = Server::spawn(
            server_binary,
            &root,
            label,
            &copied,
            if offset.is_some() {
                shards
            } else if shards == 1 {
                4
            } else {
                1
            },
        );
        failed.rejected();
        assert_eq!(
            generations(&copied),
            previous,
            "failed startup changed original AOF copy"
        );
    }
    assert_eq!(file_hash(&backup_dir.join("snapshot.aof")), backup_hash);
    assert_eq!(generations(&primary_dir), original);
    evidence.event(
        "negative_cases",
        began,
        json!({"shards":shards,"backup_original_preserved":true,"aof_original_preserved":true}),
    );
    json!({"shards":shards,"bulk_payload_bytes":BULK_KEYS*PAYLOAD_BYTES,"logical_bytes":logical_bytes,"shard_usage":shard_usage,"digest_sha256":source_digest,"cut":cut,
        "transactions_before":before,"transactions_after":after,"transactions_final":final_count,"long_ttl_before_ms":long_before,"long_ttl_restored_ms":long_after})
}

#[test]
#[ignore = "requires explicit verified release inputs and a new retained evidence directory; see docs/recovery-drill.md"]
fn extracted_package_operator_recovery_drill() {
    let mut evidence = Evidence::new(env_path("SIDER_DRILL_EVIDENCE"));
    let runner_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/recovery_drill.rs");
    fs::copy(&runner_path, evidence.root.join("recovery_drill.rs")).unwrap();
    let checkout = process::run(Command::new("git").args(["rev-parse", "HEAD"]), TIMEOUT).unwrap();
    assert!(checkout.status.success());
    save(
        &evidence.root.join("runner.json"),
        &json!({"runner_checkout_sha":String::from_utf8(checkout.stdout).unwrap().trim(),
        "runner_test_sha256":file_hash(&runner_path),"debug_assertions":cfg!(debug_assertions),"started_unix_ms":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis(),
        "command":"cargo test --locked --test recovery_drill -- --ignored --exact extracted_package_operator_recovery_drill --nocapture"}),
    );
    let (server, backup, sha) = verify_package(&evidence);
    for binary in [&server, &backup] {
        let result = process::run(command(binary).arg("--version"), TIMEOUT).unwrap();
        let name = binary.file_stem().unwrap().to_str().unwrap();
        fs::write(
            evidence.root.join(format!("{name}-version.stdout")),
            &result.stdout,
        )
        .unwrap();
        fs::write(
            evidence.root.join(format!("{name}-version.stderr")),
            &result.stderr,
        )
        .unwrap();
        assert!(result.status.success());
        assert_eq!(
            result.stdout,
            format!("{name} {}\n", env!("CARGO_PKG_VERSION")).as_bytes()
        );
    }
    let results = [1, 4].map(|shards| exercise(&evidence, &server, &backup, &sha, shards));
    evidence.success(json!({"source_sha":sha,"results":results,"replica_promotion":"pending optional separate drill","linux":if cfg!(windows) {"pending"} else {"executed"}}));
    println!("Recovery evidence retained at {}", evidence.root.display());
}

#[test]
fn deterministic_dataset_balances_near_quota_and_preserves_binary_bytes() {
    const {
        assert!(
            BULK_KEYS * PAYLOAD_BYTES >= 63 * 1024 * 1024
                && BULK_KEYS * PAYLOAD_BYTES < 64 * 1024 * 1024
        );
    }
    for shards in [1, 4] {
        let layout = DurableLayout {
            shard_count: shards as u32,
            routing_version: 1,
        };
        let mut counts = vec![0; shards];
        for key in keys(shards) {
            counts[layout.shard_for(&key).unwrap()] += 1;
        }
        assert!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1);
    }
    assert_eq!(payload(0), payload(0));
    assert_ne!(payload(0), payload(1));
    assert!(payload(0).contains(&0) && payload(0).contains(&255));
}
#[test]
fn absolute_ttl_intervals_reject_relative_reset_and_civil_clock_jump() {
    let anchor = TtlSample {
        remaining_ms: 10_000,
        wall_before_ms: 1000,
        wall_after_ms: 1001,
        query_monotonic_ms: 1,
    };
    let restored = TtlSample {
        remaining_ms: 8000,
        wall_before_ms: 3000,
        wall_after_ms: 3002,
        query_monotonic_ms: 2,
    };
    assert!(anchor.valid() && restored.valid());
    assert_eq!(anchor.gap(restored), 0);
    assert!(
        anchor.gap(TtlSample {
            remaining_ms: 10_000,
            ..restored
        }) > 250
    );
    assert!(
        !TtlSample {
            wall_after_ms: 3300,
            ..restored
        }
        .valid()
    );
    assert!(
        !TtlSample {
            wall_after_ms: 2999,
            ..restored
        }
        .valid()
    );
    assert!(
        !TtlSample {
            query_monotonic_ms: 251,
            ..restored
        }
        .valid()
    );
}
#[test]
fn failed_or_incomplete_evidence_cannot_claim_success() {
    let path = std::env::temp_dir().join(format!(
        "sider-drill-contract-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    {
        let evidence = Evidence::new(path.clone());
        let running: Value =
            serde_json::from_slice(&fs::read(evidence.root.join("result.json")).unwrap()).unwrap();
        assert_eq!(running["status"], "running");
    }
    let failed: Value =
        serde_json::from_slice(&fs::read(path.join("result.json")).unwrap()).unwrap();
    assert_eq!(failed["status"], "failed");
    fs::remove_dir_all(path).unwrap();
}
