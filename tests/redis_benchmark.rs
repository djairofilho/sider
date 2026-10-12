//! Independent descriptive Sider/Redis comparison; never a release gate.
#![forbid(unsafe_code)]

#[path = "common/gate_receipt.rs"]
mod gate_receipt;
#[path = "common/process.rs"]
mod process;
#[path = "common/release_input.rs"]
mod release_input;
#[path = "common/sider_process.rs"]
mod sider_process;
#[path = "common/wire.rs"]
mod wire;

use serde_json::{Value, json};
use sider_process::SiderProcess;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    io::{BufReader, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    sync::mpsc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use wire::Response;

const SEED: u64 = 0x5240_4005;
const CLIENTS: usize = 4;
const OPERATIONS: usize = 2048;
const WARMUP: usize = 128;
const REPETITIONS: usize = 3;
const TIMEOUT: Duration = Duration::from_secs(30);
const PHASE_TIMEOUT: Duration = Duration::from_secs(120);
const RSS_INTERVAL: Duration = Duration::from_millis(1);
const MAX_RSS_SAMPLES: usize = 500_000;
const MAX_RAW_BYTES: usize = 64 * 1024 * 1024;

enum ComparisonServer {
    Sider(SiderProcess),
    Redis(process::OwnedChild, SocketAddr),
}
impl ComparisonServer {
    fn start(
        redis: Option<&Path>,
        sider: &Path,
        version: &str,
        overrides: &[(&str, OsString)],
        scenario: Scenario,
        data: &Path,
    ) -> Result<Self, String> {
        let Some(redis) = redis else {
            return SiderProcess::try_start_configured(sider, version, overrides).map(Self::Sider);
        };
        let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        let address = listener.local_addr().map_err(|e| e.to_string())?;
        drop(listener);
        let mut command = Command::new(redis);
        command
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &address.port().to_string(),
                "--save",
                "",
                "--appendonly",
                if scenario.durable { "yes" } else { "no" },
                "--appendfsync",
                "always",
                "--auto-aof-rewrite-percentage",
                "0",
                "--maxmemory",
                "0",
                "--protected-mode",
                "yes",
                "--daemonize",
                "no",
            ])
            .arg("--dir")
            .arg(data);
        let mut child = process::OwnedChild::spawn(&mut command)?;
        let deadline = Instant::now() + TIMEOUT;
        loop {
            child.assert_alive()?;
            if let Ok(mut stream) = connection(address) {
                stream
                    .get_mut()
                    .write_all(&wire::request(&[b"PING".to_vec()]))
                    .map_err(|e| e.to_string())?;
                if wire::read_response(&mut stream)
                    .map_err(|e| e.to_string())?
                    .value
                    != Response::Simple(b"PONG".to_vec())
                {
                    return Err("Redis readiness response mismatch".into());
                }
                return Ok(Self::Redis(child, address));
            }
            if Instant::now() >= deadline {
                return Err("Redis readiness timeout".into());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
    fn id(&self) -> u32 {
        match self {
            Self::Sider(s) => s.id(),
            Self::Redis(s, _) => s.id(),
        }
    }
    fn address(&self) -> SocketAddr {
        match self {
            Self::Sider(s) => s.address(),
            Self::Redis(_, a) => *a,
        }
    }
    fn finish(self) {
        match self {
            Self::Sider(s) => s.finish(),
            Self::Redis(mut s, _) => {
                s.terminate(TIMEOUT).expect("reap Redis");
            }
        }
    }
}
fn clock_ticks() -> Result<u64, String> {
    output(Command::new("getconf").arg("CLK_TCK"))?
        .parse::<u64>()
        .map_err(|e| e.to_string())
        .and_then(|n| {
            if n > 0 {
                Ok(n)
            } else {
                Err("zero clock ticks".into())
            }
        })
}
fn parse_cpu_ticks(stat: &str) -> Result<u64, String> {
    let (_, fields) = stat.rsplit_once(") ").ok_or("invalid /proc stat")?;
    let fields: Vec<_> = fields.split_whitespace().collect();
    let user = fields
        .get(11)
        .ok_or("missing utime")?
        .parse::<u64>()
        .map_err(|e| e.to_string())?;
    let system = fields
        .get(12)
        .ok_or("missing stime")?
        .parse::<u64>()
        .map_err(|e| e.to_string())?;
    user.checked_add(system)
        .ok_or_else(|| "CPU ticks overflow".into())
}
fn cpu_ticks(pid: u32) -> Result<u64, String> {
    parse_cpu_ticks(&fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|e| e.to_string())?)
}
fn write_new(path: &Path, value: &Value) -> Result<(), String> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    file.write_all(&serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())
}
struct Container(String);
impl Drop for Container {
    fn drop(&mut self) {
        let _ = process::run(
            Command::new("docker").args(["rm", "--force", &self.0]),
            TIMEOUT,
        );
    }
}
fn verify_redis(binary: &Path) -> Result<Value, String> {
    if !cfg!(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_env = "gnu"
    )) || !binary.is_absolute()
    {
        return Err(
            "comparison requires native Linux GNU x86_64 and an absolute Redis executable path"
                .into(),
        );
    }
    let plan: Value =
        serde_json::from_str(include_str!("../releases/plan.json")).map_err(|e| e.to_string())?;
    let image = plan["reference"]["image"].as_str().ok_or("missing image")?;
    let version = plan["reference"]["redis_version"]
        .as_str()
        .ok_or("missing Redis version")?;
    let digest = image.split_once('@').ok_or("unpinned Redis image")?.1;
    let inspected: Value = serde_json::from_str(&output(
        Command::new("docker").args(["image", "inspect", image]),
    )?)
    .map_err(|e| e.to_string())?;
    let inspected = &inspected[0];
    if inspected["Os"] != "linux"
        || inspected["Architecture"] != "amd64"
        || !inspected["RepoDigests"].as_array().is_some_and(|items| {
            items.iter().any(|v| {
                v.as_str()
                    .is_some_and(|s| s.ends_with(&format!("@{digest}")))
            })
        })
    {
        return Err("Redis image platform/digest mismatch".into());
    }
    let container = Container(output(Command::new("docker").args([
        "create",
        "--pull",
        "never",
        "--platform",
        "linux/amd64",
        image,
    ]))?);
    if container.0.len() != 64 || !container.0.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("invalid container ID".into());
    }
    let directory = Directory::new(999, 999)?;
    let extracted = directory.0.join("redis-server");
    output(
        Command::new("docker")
            .arg("cp")
            .arg(format!("{}:/usr/local/bin/redis-server", container.0))
            .arg(&extracted),
    )?;
    let hash = release_input::sha256(binary)?;
    if hash != release_input::sha256(&extracted)? {
        return Err("native Redis binary differs from pinned image".into());
    }
    let observed = output(Command::new(binary).arg("--version"))?;
    if !observed
        .split_whitespace()
        .any(|field| field == format!("v={version}"))
    {
        return Err("Redis version mismatch".into());
    }
    Ok(
        json!({"image":image,"image_id":inspected["Id"],"platform":"linux/amd64","binary_sha256":hash,
        "version":observed,"native_linkage":output(Command::new("ldd").arg(binary))?.lines().map(|line|line.split(" (0x").next().unwrap().trim()).collect::<Vec<_>>(),"execution":"native on the same OS image as Sider"}),
    )
}
fn comparison_report(receipt: &Value) -> String {
    let mut report = format!(
        "# Sider versus Redis descriptive comparison\n\nSource SHA: `{}`. Raw samples: `samples.json` (SHA-256 `{}`).\n\n",
        receipt["sha"].as_str().unwrap(),
        receipt["raw_samples"]["sha256"].as_str().unwrap()
    );
    report.push_str("| Scenario | Repeat | Server | ops/s | batch p50 ms | p95 ms | p99 ms | CPU seconds | sampled RSS bytes | startup s |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n");
    for (index, scenario) in receipt["scenarios"].as_array().unwrap().iter().enumerate() {
        for pair in scenario["pairs"].as_array().unwrap() {
            for row in pair.as_array().unwrap() {
                report.push_str(&format!(
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                    index,
                    row["repetition"],
                    row["server"].as_str().unwrap(),
                    row["operations_per_second"],
                    row["batch_rtt_ms"]["p50"],
                    row["batch_rtt_ms"]["p95"],
                    row["batch_rtt_ms"]["p99"],
                    row["cpu_seconds"],
                    row["rss_sampled_max_bytes"],
                    row["startup_seconds"]
                ));
            }
        }
    }
    report.push_str("\nThree repetitions are descriptive; no superiority test or outlier removal. Redis uses one execution thread while Sider shard count varies. Batch RTT includes client decoding and is not per-command latency. CPU includes sampler synchronization around the measured window. RSS is sampled, not a guaranteed peak. Startup includes Sider version validation. See docs/redis-benchmarks.md for configuration and recovery scope.\n");
    report
}
fn validate_comparison(scenarios: &[Value]) -> Result<(), String> {
    if scenarios.len() != matrix().len() {
        return Err("incomplete scenario matrix".into());
    }
    for (expected, scenario) in matrix().into_iter().zip(scenarios) {
        if scenario["scenario"] != expected.details() {
            return Err("non-equivalent scenario".into());
        }
        let pairs = scenario["pairs"].as_array().ok_or("missing paired runs")?;
        if pairs.len() != REPETITIONS {
            return Err("incomplete repetitions".into());
        }
        for (iteration, pair) in pairs.iter().enumerate() {
            let pair = pair.as_array().ok_or("invalid pair")?;
            if pair.len() != 2 {
                return Err("incomplete pair".into());
            }
            for server in ["sider", "redis"] {
                let rows: Vec<_> = pair.iter().filter(|row| row["server"] == server).collect();
                if rows.len() != 1 {
                    return Err("missing or repeated server".into());
                }
                let row = rows[0];
                if row["repetition"] != iteration + 1
                    || row["operations"] != CLIENTS * OPERATIONS
                    || row["warmup_operations"] != CLIENTS * WARMUP
                    || row["batch_samples"] != CLIENTS * OPERATIONS / expected.pipeline
                    || row["rss_samples"].as_u64().is_none_or(|n| n == 0)
                    || row["verified_keys"].as_u64().is_none_or(|n| n == 0)
                    || row["operations_per_second"]
                        .as_f64()
                        .is_none_or(|n| !n.is_finite() || n <= 0.0)
                    || row["cpu_ticks_after"]
                        .as_u64()
                        .zip(row["cpu_ticks_before"].as_u64())
                        .is_none_or(|(after, before)| after < before)
                    || (expected.durable
                        && row["recovery"]["verified_keys"] != row["verified_keys"])
                {
                    return Err("invalid or incomplete observations".into());
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Scenario {
    shards: usize,
    hot: bool,
    pipeline: usize,
    durable: bool,
}
fn matrix() -> Vec<Scenario> {
    let mut scenarios = Vec::new();
    for shards in [1, 4] {
        for hot in [false, true] {
            for pipeline in [1, 16] {
                for durable in [false, true] {
                    scenarios.push(Scenario {
                        shards,
                        hot,
                        pipeline,
                        durable,
                    });
                }
            }
        }
    }
    scenarios
}
impl Scenario {
    fn details(self) -> Value {
        json!({"shards":self.shards, "key_distribution":if self.hot {"single_hot_key"} else {"per_client_256_keys"},
            "pipeline":self.pipeline, "aof":if self.durable {"always"} else {"off"}})
    }
}

fn output(command: &mut Command) -> Result<String, String> {
    let result = process::run(command, TIMEOUT)?;
    if !result.status.success() {
        return Err(format!(
            "observation failed: {}",
            String::from_utf8_lossy(&result.stderr)
        ));
    }
    String::from_utf8(result.stdout)
        .map(|text| text.trim().to_owned())
        .map_err(|e| e.to_string())
}
fn hardware() -> Result<Value, String> {
    let cpu = fs::read_to_string("/proc/cpuinfo").map_err(|e| e.to_string())?;
    let memory = fs::read_to_string("/proc/meminfo").map_err(|e| e.to_string())?;
    Ok(json!({
        "cpu":cpu.lines().filter(|line| line.starts_with("model name") || line.starts_with("cpu cores") || line.starts_with("siblings")).take(3).collect::<Vec<_>>(),
        "logical_processors":cpu.lines().filter(|line| line.starts_with("processor")).count(),
        "available_parallelism":thread::available_parallelism().map_err(|e| e.to_string())?.get(),
        "memory":memory.lines().find(|line| line.starts_with("MemTotal:")).ok_or("missing MemTotal")?,
        "os_release":fs::read_to_string("/etc/os-release").map_err(|e| e.to_string())?,
        "kernel":output(Command::new("uname").arg("-a"))?,
        "temporary_filesystem":output(Command::new("findmnt").args(["--json","--output","SOURCE,FSTYPE,OPTIONS,TARGET","--target"]).arg(std::env::temp_dir()))?,
        "clock_ticks_per_second":clock_ticks()?,
        "loadavg_before":fs::read_to_string("/proc/loadavg").map_err(|e| e.to_string())?,
        "runner_compiler":output(Command::new("rustc").arg("-Vv"))?,
        "cargo":output(Command::new("cargo").arg("--version"))?,
        "toolchain_file":include_str!("../rust-toolchain.toml"),
        "architecture":std::env::consts::ARCH
    }))
}
fn parse_rss(status: &str) -> Result<u64, String> {
    let mut fields = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .ok_or("missing VmRSS")?
        .split_whitespace();
    let kib = fields
        .next()
        .ok_or("VmRSS has no value")?
        .parse::<u64>()
        .map_err(|e| e.to_string())?;
    if fields.next() != Some("kB") || fields.next().is_some() {
        return Err("unexpected VmRSS unit".into());
    }
    kib.checked_mul(1024)
        .ok_or_else(|| "VmRSS excede u64".into())
}
fn rss(pid: u32) -> Result<u64, String> {
    parse_rss(&fs::read_to_string(format!("/proc/{pid}/status")).map_err(|e| e.to_string())?)
}

struct RssSample {
    at: Instant,
    bytes: u64,
}
struct RssSampler {
    stop: mpsc::Sender<()>,
    task: Option<JoinHandle<Result<Vec<RssSample>, String>>>,
}
impl RssSampler {
    fn start(pid: u32) -> Result<Self, String> {
        let (stop, wait) = mpsc::channel();
        let (ready, started) = mpsc::channel();
        let task = thread::spawn(move || {
            let mut samples = Vec::new();
            loop {
                let bytes = rss(pid)?;
                samples.push(RssSample {
                    at: Instant::now(),
                    bytes,
                });
                if samples.len() == 1 {
                    let _ = ready.send(());
                }
                if samples.len() >= MAX_RSS_SAMPLES {
                    return Err("RSS samples exceeded the limit".into());
                }
                match wait.recv_timeout(RSS_INTERVAL) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(samples),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
        });
        let sampler = Self {
            stop,
            task: Some(task),
        };
        started
            .recv_timeout(TIMEOUT)
            .map_err(|e| format!("start RSS sampling: {e}"))?;
        Ok(sampler)
    }
    fn finish(mut self) -> Result<Vec<RssSample>, String> {
        let _ = self.stop.send(());
        self.task
            .take()
            .unwrap()
            .join()
            .map_err(|_| "RSS sampler panicked")?
    }
}
impl Drop for RssSampler {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
    }
}

struct Directory(PathBuf);
impl Directory {
    fn new(scenario: usize, repetition: usize) -> Result<Self, String> {
        let path = std::env::temp_dir().join(format!(
            "sider-release-benchmark-{}-{scenario}-{repetition}",
            std::process::id()
        ));
        fs::create_dir(&path).map_err(|e| e.to_string())?;
        Ok(Self(path))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        // Dedicated path created by this guard; never a reused data source.
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn connection(address: SocketAddr) -> Result<BufReader<TcpStream>, String> {
    let stream = TcpStream::connect_timeout(&address, TIMEOUT).map_err(|e| e.to_string())?;
    stream.set_nodelay(true).map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(TIMEOUT))
        .map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(Some(TIMEOUT))
        .map_err(|e| e.to_string())?;
    Ok(BufReader::new(stream))
}
type Counts = BTreeMap<Vec<u8>, i64>;
struct Plan {
    warmup: Vec<Vec<u8>>,
    measured: Vec<Vec<u8>>,
    counts: Counts,
}
fn plan(scenario: Scenario, client: usize) -> Plan {
    let mut random = SEED ^ client as u64;
    let mut counts = Counts::new();
    let mut build = |operations| {
        (0..operations / scenario.pipeline)
            .map(|_| {
                let mut bytes = Vec::new();
                for _ in 0..scenario.pipeline {
                    random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let key = if scenario.hot {
                        b"hot".to_vec()
                    } else {
                        format!("client-{client}:key-{}", random % 256).into_bytes()
                    };
                    *counts.entry(key.clone()).or_default() += 1;
                    bytes.extend(wire::request(&[b"INCR".to_vec(), key]));
                }
                bytes
            })
            .collect()
    };
    let warmup = build(WARMUP);
    let measured = build(OPERATIONS);
    Plan {
        warmup,
        measured,
        counts,
    }
}
fn batch(
    stream: &mut BufReader<TcpStream>,
    bytes: &[u8],
    pipeline: usize,
    deadline: Instant,
) -> Result<(), String> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or("phase deadline exceeded")?;
    stream
        .get_ref()
        .set_write_timeout(Some(remaining.min(TIMEOUT)))
        .map_err(|e| e.to_string())?;
    stream
        .get_mut()
        .write_all(bytes)
        .map_err(|e| e.to_string())?;
    for _ in 0..pipeline {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or("phase deadline exceeded")?;
        stream
            .get_ref()
            .set_read_timeout(Some(remaining.min(TIMEOUT)))
            .map_err(|e| e.to_string())?;
        if !matches!(wire::read_response(stream).map_err(|e| e.to_string())?.value, Response::Integer(value) if value > 0)
        {
            return Err("INCR did not return a positive integer".into());
        }
    }
    Ok(())
}
struct Sample {
    at: Instant,
    rtt_ns: u64,
}
struct ClientResult {
    counts: Counts,
    samples: Vec<Sample>,
    began: Instant,
    ended: Instant,
}

fn client_run(
    address: SocketAddr,
    scenario: Scenario,
    workload: Plan,
    ready: mpsc::Sender<Result<(), String>>,
    start: mpsc::Receiver<()>,
) -> Result<ClientResult, String> {
    let preparation = (|| {
        let mut stream = connection(address)?;
        let deadline = Instant::now() + PHASE_TIMEOUT;
        for bytes in &workload.warmup {
            batch(&mut stream, bytes, scenario.pipeline, deadline)?;
        }
        Ok::<_, String>(stream)
    })();
    ready
        .send(preparation.as_ref().map(|_| ()).map_err(Clone::clone))
        .map_err(|e| e.to_string())?;
    let mut stream = preparation?;
    start
        .recv_timeout(PHASE_TIMEOUT)
        .map_err(|e| format!("wait for measured phase: {e}"))?;
    let mut samples = Vec::with_capacity(workload.measured.len());
    let began = Instant::now();
    let deadline = began + PHASE_TIMEOUT;
    for bytes in workload.measured {
        let at = Instant::now();
        batch(&mut stream, &bytes, scenario.pipeline, deadline)?;
        let rtt_ns = u64::try_from(at.elapsed().as_nanos()).map_err(|e| e.to_string())?;
        samples.push(Sample { at, rtt_ns });
    }
    Ok(ClientResult {
        counts: workload.counts,
        samples,
        began,
        ended: Instant::now(),
    })
}

fn percentiles(samples: &[u64]) -> Value {
    assert!(!samples.is_empty());
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let percentile = |q: usize| ordered[(ordered.len() * q).div_ceil(100) - 1] as f64 / 1_000_000.0;
    json!({"p50":percentile(50), "p95":percentile(95), "p99":percentile(99)})
}
fn variation(values: &[f64]) -> Value {
    assert!(!values.is_empty() && values.iter().all(|v| v.is_finite() && *v > 0.0));
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let deviation = (values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64)
        .sqrt();
    json!({"min":sorted[0], "median":sorted[sorted.len()/2], "max":sorted[sorted.len()-1],
        "mean":mean, "population_stddev":deviation, "coefficient_of_variation":deviation/mean})
}
fn diagnose(binary: &Path, overrides: &[(&str, OsString)]) -> Result<String, String> {
    let mut command = Command::new(binary);
    for (name, _) in std::env::vars_os() {
        if name
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("SIDER_")
        {
            command.env_remove(name);
        }
    }
    command.env("SIDER_SECURITY_PROFILE", "legacy-loopback");
    for (name, value) in overrides {
        command.env(name, value);
    }
    output(command.env("SIDER_ADDR", "127.0.0.1:0").arg("--diagnose"))
}

fn repetition(
    redis: Option<&Path>,
    package: &release_input::ReleaseInput,
    version: &str,
    scenario: Scenario,
    index: usize,
    iteration: usize,
) -> Result<(Value, Value, f64), String> {
    let directory = Directory::new(index, iteration)?;
    let mut overrides = vec![("SIDER_SHARDS", OsString::from(scenario.shards.to_string()))];
    if scenario.durable {
        overrides.extend([
            ("SIDER_AOF_DIR", directory.0.as_os_str().to_owned()),
            ("SIDER_AOF_SYNC", "always".into()),
            ("SIDER_AOF_COMPACT_AFTER_BYTES", "0".into()),
        ]);
    }
    let configuration = if redis.is_some() {
        format!(
            "bind=127.0.0.1;save=;appendonly={};appendfsync=always;auto-aof-rewrite-percentage=0;maxmemory=0;protected-mode=yes;daemonize=no;dir={}",
            if scenario.durable { "yes" } else { "no" },
            directory.0.display()
        )
    } else {
        diagnose(package.binary(), &overrides)?
    };
    let startup = Instant::now();
    let server = ComparisonServer::start(
        redis,
        package.binary(),
        version,
        &overrides,
        scenario,
        &directory.0,
    )?;
    let startup_seconds = startup.elapsed().as_secs_f64();
    let address = server.address();
    let warmup_began = Instant::now();
    let (ready, prepared) = mpsc::channel();
    let mut starts = Vec::new();
    let mut tasks = Vec::new();
    for client in 0..CLIENTS {
        let workload = plan(scenario, client);
        let ready = ready.clone();
        let (start, wait) = mpsc::channel();
        starts.push(start);
        tasks.push(thread::spawn(move || {
            client_run(address, scenario, workload, ready, wait)
        }));
    }
    drop(ready);
    let readiness = (|| {
        for _ in 0..CLIENTS {
            prepared
                .recv_timeout(PHASE_TIMEOUT)
                .map_err(|e| format!("warmup: {e}"))??;
        }
        Ok::<_, String>(())
    })();
    if let Err(error) = readiness {
        drop(starts);
        for task in tasks {
            let _ = task.join();
        }
        return Err(error);
    }
    let warmup_seconds = warmup_began.elapsed().as_secs_f64();
    let cpu_before = cpu_ticks(server.id())?;
    let cpu_clock = Instant::now();
    let sampler = match RssSampler::start(server.id()) {
        Ok(sampler) => sampler,
        Err(error) => {
            drop(starts);
            for task in tasks {
                let _ = task.join();
            }
            return Err(error);
        }
    };
    for start in starts {
        let _ = start.send(());
    }
    let clients: Vec<_> = tasks
        .into_iter()
        .map(|task| {
            task.join()
                .map_err(|_| "client panicked".to_owned())
                .and_then(|result| result)
        })
        .collect();
    let cpu_after = cpu_ticks(server.id())?;
    let cpu_window_seconds = cpu_clock.elapsed().as_secs_f64();
    let rss_samples = sampler.finish()?;
    let clients = clients.into_iter().collect::<Result<Vec<_>, _>>()?;
    let began = clients.iter().map(|client| client.began).min().unwrap();
    let ended = clients.iter().map(|client| client.ended).max().unwrap();
    let seconds = ended.duration_since(began).as_secs_f64();
    let rates = (CLIENTS * OPERATIONS) as f64 / seconds;
    let mut expected = Counts::new();
    let mut latencies = Vec::new();
    let mut raw_clients = Vec::new();
    for (client, result) in clients.into_iter().enumerate() {
        for (key, value) in result.counts {
            *expected.entry(key).or_default() += value;
        }
        latencies.extend(result.samples.iter().map(|sample| sample.rtt_ns));
        raw_clients.push(json!({"client":client, "seed":SEED ^ client as u64,
            "start_seconds":result.began.duration_since(began).as_secs_f64(),
            "end_seconds":result.ended.duration_since(began).as_secs_f64(),
            "samples":result.samples.into_iter().map(|sample| json!({
                "start_seconds":sample.at.duration_since(began).as_secs_f64(), "rtt_nanoseconds":sample.rtt_ns
            })).collect::<Vec<_>>()
        }));
    }
    let sampled: Vec<_> = rss_samples
        .into_iter()
        .filter(|sample| sample.at >= began && sample.at <= ended)
        .collect();
    let observed_max = sampled
        .iter()
        .map(|sample| sample.bytes)
        .max()
        .ok_or("no RSS sample within the measured phase")?;
    let mut stream = connection(address)?;
    for (key, count) in &expected {
        stream
            .get_mut()
            .write_all(&wire::request(&[b"GET".to_vec(), key.clone()]))
            .map_err(|e| e.to_string())?;
        if wire::read_response(&mut stream)
            .map_err(|e| e.to_string())?
            .value
            != Response::Bulk(Some(count.to_string().into_bytes()))
        {
            return Err("final state differs from measured and warmup INCR operations".into());
        }
    }
    drop(stream);
    server.finish();
    let recovery = if scenario.durable {
        let started = Instant::now();
        let recovered = ComparisonServer::start(
            redis,
            package.binary(),
            version,
            &overrides,
            scenario,
            &directory.0,
        )?;
        let recovery_seconds = started.elapsed().as_secs_f64();
        let mut stream = connection(recovered.address())?;
        for (key, count) in &expected {
            stream
                .get_mut()
                .write_all(&wire::request(&[b"GET".to_vec(), key.clone()]))
                .map_err(|e| e.to_string())?;
            if wire::read_response(&mut stream)
                .map_err(|e| e.to_string())?
                .value
                != Response::Bulk(Some(count.to_string().into_bytes()))
            {
                return Err("durable restart lost acknowledged operations".into());
            }
        }
        drop(stream);
        recovered.finish();
        json!({"restart_ready_seconds":recovery_seconds,"verified_keys":expected.len(),"scope":"forced termination then replay; not power loss"})
    } else {
        Value::Null
    };
    let summary = json!({"recovery":recovery,"repetition":iteration+1, "operations":CLIENTS*OPERATIONS,
        "server":if redis.is_some() {"redis"} else {"sider"}, "startup_seconds":startup_seconds,
        "cpu_ticks_before":cpu_before,"cpu_ticks_after":cpu_after,"cpu_window_seconds":cpu_window_seconds,
        "cpu_seconds":(cpu_after-cpu_before) as f64 / clock_ticks()? as f64,
        "warmup_operations":CLIENTS*WARMUP, "warmup_seconds":warmup_seconds, "seconds":seconds,
        "operations_per_second":rates, "verified_keys":expected.len(),
        "batch_rtt_ms":percentiles(&latencies), "batch_samples":latencies.len(),
        "rss_sampled_max_bytes":observed_max, "rss_samples":sampled.len()});
    let raw = json!({"summary":summary, "effective_configuration":configuration,
        "runtime":{"address":address.to_string(),"ready_file_enabled":redis.is_none()},
        "clients":raw_clients, "rss_samples":sampled.into_iter().map(|sample|json!({
            "seconds":sample.at.duration_since(began).as_secs_f64(),"bytes":sample.bytes
        })).collect::<Vec<_>>()});
    Ok((summary, raw, rates))
}

// Compact JSON adds only one comma when extending a nonempty array.
// The envelope is already counted, without additional indentation from nesting.
struct RawSamples {
    document: Value,
    bytes: usize,
    limit: usize,
}
impl RawSamples {
    fn new(document: Value, limit: usize) -> Result<Self, String> {
        let bytes = serde_json::to_vec(&document)
            .map_err(|e| e.to_string())?
            .len();
        if bytes > limit {
            return Err(format!("samples exceeded the {limit}-byte limit"));
        }
        Ok(Self {
            document,
            bytes,
            limit,
        })
    }

    fn push_repetition(&mut self, scenario: usize, detail: Value) -> Result<(), String> {
        let repetitions = self.document["scenarios"][scenario]["repetitions"]
            .as_array_mut()
            .unwrap();
        let detail_bytes = serde_json::to_vec(&detail)
            .map_err(|e| e.to_string())?
            .len();
        let bytes = self
            .bytes
            .checked_add(detail_bytes)
            .and_then(|bytes| bytes.checked_add(usize::from(!repetitions.is_empty())))
            .filter(|bytes| *bytes <= self.limit)
            .ok_or_else(|| format!("samples exceeded the {}-byte limit", self.limit))?;
        repetitions.push(detail);
        self.bytes = bytes;
        Ok(())
    }

    fn finish(mut self, loadavg_after: String) -> Result<Vec<u8>, String> {
        let previous_bytes = serde_json::to_vec(&self.document["loadavg_after"])
            .map_err(|e| e.to_string())?
            .len();
        let observation = Value::String(loadavg_after);
        let observation_bytes = serde_json::to_vec(&observation)
            .map_err(|e| e.to_string())?
            .len();
        self.bytes = self
            .bytes
            .checked_sub(previous_bytes)
            .and_then(|bytes| bytes.checked_add(observation_bytes))
            .filter(|bytes| *bytes <= self.limit)
            .ok_or_else(|| format!("samples exceeded the {}-byte limit", self.limit))?;
        self.document["loadavg_after"] = observation;
        let bytes = serde_json::to_vec(&self.document).map_err(|e| e.to_string())?;
        if bytes.len() != self.bytes || bytes.len() > self.limit {
            return Err("final sample size differs from the compact budget".into());
        }
        Ok(bytes)
    }
}

#[test]
#[ignore = "requires exact Linux package, pinned Redis executable, and an idle machine"]
fn redis_performance_comparison() {
    assert_eq!(
        std::env::var("SIDER_BENCH_IDLE_MACHINE").as_deref(),
        Ok("1")
    );
    let context = gate_receipt::GateContext::from_env("benchmarks").unwrap();
    let package = release_input::ReleaseInput::from_env(&context).unwrap();
    let redis = PathBuf::from(
        std::env::var_os("SIDER_BENCH_REDIS_BINARY").expect("missing native Redis binary"),
    );
    let reference = verify_redis(&redis).unwrap();
    let out = PathBuf::from(
        std::env::var_os("SIDER_COMPARE_DIR").expect("missing comparison output directory"),
    );
    assert!(out.is_absolute());
    fs::create_dir(&out).expect("output directory must be new; retain failed runs");
    write_new(
        &out.join("status.json"),
        &json!({"status":"incomplete","sha":context.sha()}),
    )
    .unwrap();
    let hardware = hardware().unwrap();
    let mut samples = RawSamples::new(json!({"schema_version":1,"task":"issue-97","sha":context.sha(),
        "version":context.version(),"target":context.target(),"input":package.details(),"redis":reference,
        "hardware":hardware,"seed":SEED,"clients":CLIENTS,"warmup_operations_per_client":WARMUP,
        "measured_operations_per_client":OPERATIONS,"loadavg_after":null,
        "scenarios":matrix().into_iter().map(|scenario|json!({"scenario":scenario.details(),"repetitions":[]})).collect::<Vec<_>>()
    }), MAX_RAW_BYTES).unwrap();
    let mut summaries = Vec::new();
    for (index, scenario) in matrix().into_iter().enumerate() {
        let mut rates = [Vec::new(), Vec::new()];
        let mut pairs = Vec::new();
        for iteration in 0..REPETITIONS {
            let mut pair = Vec::new();
            // Alternate pair order to expose order and cache effects.
            let order = if iteration % 2 == 0 { [0, 1] } else { [1, 0] };
            for engine in order {
                let (summary, raw, rate) = match repetition(
                    if engine == 1 { Some(&redis) } else { None },
                    &package,
                    context.version(),
                    scenario,
                    index,
                    iteration,
                ) {
                    Ok(result) => result,
                    Err(error) => {
                        write_new(&out.join("failure.json"), &json!({"status":"failed","scenario":index,"repetition":iteration,"engine":engine,"error":error})).unwrap();
                        panic!("comparison failed: {error}");
                    }
                };
                rates[engine].push(rate);
                samples.push_repetition(index, raw.clone()).unwrap();
                write_new(
                    &out.join(format!(
                        "scenario-{index}-repeat-{iteration}-engine-{engine}.json"
                    )),
                    &raw,
                )
                .unwrap();
                pair.push(summary);
            }
            pairs.push(json!(pair));
        }
        summaries.push(json!({"scenario":scenario.details(),"pairs":pairs,
            "sider_throughput_variation":variation(&rates[0]),"redis_throughput_variation":variation(&rates[1])}));
    }
    validate_comparison(&summaries).unwrap();
    package.verify_again(&context).unwrap();
    assert_eq!(verify_redis(&redis).unwrap(), reference);
    let bytes = samples
        .finish(fs::read_to_string("/proc/loadavg").unwrap())
        .unwrap();
    let raw_path = out.join("samples.json");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&raw_path)
        .unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let receipt = json!({"status":"complete","release_gate":false,"statistical_scope":"descriptive",
        "sha":context.sha(),"target":context.target(),"input":package.details(),"redis":reference,
        "raw_samples":{"file":"samples.json","sha256":release_input::sha256(&raw_path).unwrap(),"bytes":bytes.len()},
        "scenarios":summaries});
    let mut report = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(out.join("report.md"))
        .unwrap();
    report
        .write_all(comparison_report(&receipt).as_bytes())
        .unwrap();
    report.sync_all().unwrap();
    package.verify_again(&context).unwrap();
    write_new(&out.join("comparison.json"), &receipt).unwrap();
}

#[test]
fn benchmark_contract_matrix_and_workloads_are_fixed_and_count_warmup_separately() {
    let scenarios = matrix();
    assert_eq!(scenarios.len(), 16);
    for scenario in scenarios {
        assert_eq!(WARMUP % scenario.pipeline, 0);
        assert_eq!(OPERATIONS % scenario.pipeline, 0);
        let work = plan(scenario, 2);
        assert_eq!(work.warmup.len(), WARMUP / scenario.pipeline);
        assert_eq!(work.measured.len(), OPERATIONS / scenario.pipeline);
        assert_eq!(
            work.counts.values().sum::<i64>(),
            (WARMUP + OPERATIONS) as i64
        );
        assert_eq!(work.counts.len(), if scenario.hot { 1 } else { 256 });
        assert_eq!(work.measured, plan(scenario, 2).measured);
    }
    assert_eq!(16 * REPETITIONS * CLIENTS * OPERATIONS, 393216);
}
#[test]
fn benchmark_contract_diagnose_uses_the_same_legacy_profile_as_the_server() {
    let output = diagnose(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        &[("SIDER_SHARDS", OsString::from("1"))],
    )
    .unwrap();
    assert!(output.contains("security_profile:legacy-loopback"));
    assert!(output.contains("transport_mode:plaintext"));
}
#[test]
fn benchmark_contract_percentiles_measure_whole_batches_and_variation_is_descriptive() {
    let samples: Vec<_> = (1..=100).map(|n| n * 1_000_000).rev().collect();
    assert_eq!(
        percentiles(&samples),
        json!({"p50":50.0,"p95":95.0,"p99":99.0})
    );
    assert_eq!(
        percentiles(&[16_000_000]),
        json!({"p50":16.0,"p95":16.0,"p99":16.0})
    );
    let result = variation(&[2.0, 4.0, 6.0]);
    assert_eq!(result["mean"], 4.0);
    assert_eq!(result["median"], 4.0);
    assert!((result["population_stddev"].as_f64().unwrap() - (8.0_f64 / 3.0).sqrt()).abs() < 1e-12);
}
#[test]
fn benchmark_contract_rss_requires_observation_and_known_units() {
    assert_eq!(parse_rss("Name: sider\nVmRSS: 123 kB\n").unwrap(), 125952);
    for invalid in [
        "",
        "VmRSS: nope kB",
        "VmRSS: 1 bytes",
        "VmRSS: 18446744073709551615 kB",
    ] {
        assert!(parse_rss(invalid).is_err());
    }
}

#[test]
fn benchmark_contract_raw_samples_preserve_nested_values_without_pretty_growth() {
    let detail = json!({
        "effective_configuration":"diagnostic café\npath=\"C:\\data\"",
        "clients":[{"client":0,"seed":SEED,"samples":(0..128).map(|index|json!({
            "start_seconds":index as f64 / 1000.0,"rtt_nanoseconds":123456
        })).collect::<Vec<_>>()}],
        "rss_samples":(0..64).map(|index|json!({
            "seconds":index as f64 / 1000.0,"bytes":12345678
        })).collect::<Vec<_>>()
    });
    let envelope = json!({"schema_version":1,"task":"R11-05",
    "hardware":{"cpu":["Test CPU"],"toolchain_file":"[toolchain]\nchannel=\"1.97.1\""},
    "loadavg_after":null,"scenarios":[
        {"scenario":{"shards":1},"repetitions":[]},
        {"scenario":{"shards":4},"repetitions":[]}
    ]});
    let observation = "0.25 0.10 0.05 2/123 456\n";
    let mut expected = envelope.clone();
    for scenario in 0..2 {
        expected["scenarios"][scenario]["repetitions"] = json!(vec![detail.clone(); 3]);
    }
    expected["loadavg_after"] = json!(observation);
    let legacy_accumulated = 6 * serde_json::to_vec_pretty(&detail).unwrap().len();
    let limit = legacy_accumulated + 1;
    assert!(serde_json::to_vec_pretty(&expected).unwrap().len() > limit);
    let compact = serde_json::to_vec(&expected).unwrap();
    assert!(compact.len() < limit);

    let mut samples = RawSamples::new(envelope, limit).unwrap();
    for scenario in 0..2 {
        for _ in 0..3 {
            samples.push_repetition(scenario, detail.clone()).unwrap();
        }
    }
    let bytes = samples.finish(observation.into()).unwrap();
    assert_eq!(bytes, compact);
    assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), expected);
}

#[test]
fn benchmark_contract_raw_samples_limit_includes_envelope_and_final_observation() {
    assert_eq!(MAX_RAW_BYTES, 64 * 1024 * 1024);
    let envelope = json!({"schema_version":1,"task":"R11-05","loadavg_after":null,
        "scenarios":[{"scenario":{"shards":4},"repetitions":[]}]});
    let envelope_bytes = serde_json::to_vec(&envelope).unwrap().len();
    assert!(RawSamples::new(envelope.clone(), envelope_bytes - 1).is_err());
    let detail = json!({"rss_samples":[{"seconds":0.001,"bytes":12345678}]});
    let mut expected = envelope.clone();
    expected["scenarios"][0]["repetitions"] = json!([detail.clone()]);
    // "é" occupies the same four bytes as null, counting quotes and UTF-8.
    expected["loadavg_after"] = json!("é");
    let exact_bytes = serde_json::to_vec(&expected).unwrap();
    let mut samples = RawSamples::new(envelope.clone(), exact_bytes.len()).unwrap();
    samples.push_repetition(0, detail.clone()).unwrap();
    assert!(samples.push_repetition(0, json!(null)).is_err());
    assert_eq!(samples.finish("é".into()).unwrap(), exact_bytes);

    let mut samples = RawSamples::new(envelope.clone(), exact_bytes.len() - 1).unwrap();
    assert!(samples.push_repetition(0, detail.clone()).is_err());
    let mut unchanged = envelope.clone();
    unchanged["loadavg_after"] = json!("é");
    assert_eq!(
        serde_json::from_slice::<Value>(&samples.finish("é".into()).unwrap()).unwrap(),
        unchanged
    );

    let observation = "0.25 0.10 0.05 2/123 456\n";
    expected["loadavg_after"] = json!(observation);
    let final_bytes = serde_json::to_vec(&expected).unwrap();
    let mut samples = RawSamples::new(envelope.clone(), final_bytes.len() - 1).unwrap();
    samples.push_repetition(0, detail.clone()).unwrap();
    assert!(samples.finish(observation.into()).is_err());
    let mut samples = RawSamples::new(envelope, final_bytes.len()).unwrap();
    samples.push_repetition(0, detail).unwrap();
    assert_eq!(samples.finish(observation.into()).unwrap(), final_bytes);
}

#[test]
fn benchmark_contract_cpu_stat_uses_final_parenthesis_and_rejects_missing_counters() {
    assert_eq!(
        parse_cpu_ticks("42 (name ) spaces) S 0 0 0 0 0 0 0 0 0 0 12 7").unwrap(),
        19
    );
    assert!(parse_cpu_ticks("42 missing").is_err());
    assert!(parse_cpu_ticks("42 (name) S").is_err());
}

#[test]
fn benchmark_contract_rejects_incomplete_or_non_equivalent_comparison() {
    let scenarios:Vec<_>=matrix().into_iter().map(|scenario|json!({"scenario":scenario.details(),
        "pairs":(0..REPETITIONS).map(|iteration|["sider","redis"].into_iter().map(|server|json!({
            "server":server,"repetition":iteration+1,"operations":CLIENTS*OPERATIONS,
            "warmup_operations":CLIENTS*WARMUP,"batch_samples":CLIENTS*OPERATIONS/scenario.pipeline,
            "rss_samples":1,"verified_keys":1,"operations_per_second":1.0,
            "cpu_ticks_before":1,"cpu_ticks_after":2,"recovery":{"verified_keys":1}
        })).collect::<Vec<_>>()).collect::<Vec<_>>()
    })).collect();
    validate_comparison(&scenarios).unwrap();
    assert!(validate_comparison(&scenarios[..15]).is_err());
    for (path, bad) in [
        ("server", json!("sider")),
        ("operations", json!(0)),
        ("rss_samples", json!(0)),
        ("cpu_ticks_after", json!(0)),
    ] {
        let mut changed = scenarios.clone();
        changed[0]["pairs"][0][1][path] = bad;
        assert!(validate_comparison(&changed).is_err(), "{path}");
    }
    let mut changed = scenarios;
    changed[0]["scenario"]["pipeline"] = json!(32);
    assert!(validate_comparison(&changed).is_err());
}
