//! Explicit, opt-in workload for recording a Sider resource envelope.
//!
//! This is a measurement harness, not a release gate. It emits no report unless
//! every declared scenario completed; raw observations remain for failed runs.
#![forbid(unsafe_code)]

#[path = "common/process.rs"]
mod process;
#[path = "common/sider_process.rs"]
mod sider_process;
#[path = "common/wire.rs"]
mod wire;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sider_process::SiderProcess;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};
use wire::Response;

const FIXTURE: &str = include_str!("fixtures/resource-envelope.json");
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_SAMPLES: usize = 1_000_000;
const MAX_SAMPLE_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Fixture {
    schema_version: u32,
    seed: u64,
    generator: String,
    repetitions: usize,
    warmup_operations: usize,
    sample_interval_ms: u64,
    shards: Vec<usize>,
    aof_policies: Vec<String>,
    quota_bytes: u64,
    worker_queue_capacity: usize,
    saturation: Saturation,
    large_values: LargeValues,
    slow_client: SlowClient,
    expiration: Expiration,
    compaction: Compaction,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Saturation {
    clients: usize,
    operations_per_client: usize,
    key_distribution: String,
    key_count: usize,
    value_bytes: usize,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
struct LargeValues {
    value_bytes: usize,
    round_trips: usize,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
struct SlowClient {
    seed: u64,
    queue_capacity: usize,
    message_bytes: usize,
    publications: usize,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Expiration {
    keys: usize,
    ttl_ms: u64,
    settle_timeout_ms: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Compaction {
    after_bytes: u64,
    value_bytes: usize,
    writes: usize,
    observe_timeout_ms: u64,
}

#[derive(Clone, Copy)]
enum AofPolicy {
    Off,
    Always,
    Everysec,
}
impl AofPolicy {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "off" => Ok(Self::Off),
            "always" => Ok(Self::Always),
            "everysec" => Ok(Self::Everysec),
            _ => Err(format!("unknown AOF policy: {value}")),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Always => "always",
            Self::Everysec => "everysec",
        }
    }
}

fn fixture() -> Fixture {
    let value: Fixture = serde_json::from_str(FIXTURE).expect("valid resource-envelope fixture");
    assert_eq!(value.schema_version, 1);
    assert!(value.repetitions > 0 && value.sample_interval_ms > 0);
    assert_eq!(value.warmup_operations, 0);
    assert!(!value.generator.is_empty());
    assert!(!value.shards.is_empty() && !value.aof_policies.is_empty());
    assert!(value.shards.iter().all(|shards| (1..=256).contains(shards)));
    assert!(
        value
            .aof_policies
            .iter()
            .all(|policy| AofPolicy::parse(policy).is_ok())
    );
    assert!(value.saturation.clients > 0 && value.saturation.operations_per_client > 0);
    assert_eq!(value.saturation.key_distribution, "shared_keyspace");
    assert!(value.saturation.key_count > 0 && value.saturation.value_bytes > 0);
    assert!(value.large_values.value_bytes > 0 && value.large_values.round_trips > 0);
    assert!(value.slow_client.queue_capacity > 0 && value.slow_client.publications > 0);
    assert!(value.expiration.keys > 0 && value.expiration.ttl_ms > 0);
    assert!(value.compaction.after_bytes > 0 && value.compaction.writes > 0);
    assert!(value.compaction.value_bytes > 0);
    assert!(
        value
            .compaction
            .value_bytes
            .checked_mul(value.compaction.writes)
            .is_some_and(|bytes| bytes <= 16 * 1024 * 1024),
        "compaction delta must fit the default 16 MiB maximum"
    );
    assert!(
        value.large_values.value_bytes <= 64 * 1024 * 1024,
        "large value must fit the default 64 MiB AOF record limit"
    );
    value
}

fn connect(address: SocketAddr) -> Result<TcpStream, String> {
    let stream =
        TcpStream::connect_timeout(&address, CONNECT_TIMEOUT).map_err(|e| e.to_string())?;
    stream.set_nodelay(true).map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(REQUEST_TIMEOUT))
        .map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(Some(REQUEST_TIMEOUT))
        .map_err(|e| e.to_string())?;
    Ok(stream)
}

fn exchange(stream: &mut TcpStream, args: &[&[u8]]) -> Result<(Response, u64), String> {
    let encoded = wire::request(&args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>());
    let began = Instant::now();
    stream.write_all(&encoded).map_err(|e| e.to_string())?;
    let response = wire::read_response(stream)
        .map_err(|e| e.to_string())?
        .value;
    let nanos = u64::try_from(began.elapsed().as_nanos()).map_err(|_| "RTT exceeds u64")?;
    Ok((response, nanos))
}

fn info(address: SocketAddr) -> Result<BTreeMap<String, String>, String> {
    let Response::Bulk(Some(bytes)) = exchange(&mut connect(address)?, &[b"INFO", b"all"])?.0
    else {
        return Err("INFO did not return a bulk string".into());
    };
    let text = std::str::from_utf8(&bytes).map_err(|e| e.to_string())?;
    let mut fields = BTreeMap::new();
    for line in text
        .split("\r\n")
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        let (key, value) = line.split_once(':').ok_or("malformed INFO line")?;
        if fields.insert(key.to_owned(), value.to_owned()).is_some() {
            return Err(format!("duplicate INFO field: {key}"));
        }
    }
    Ok(fields)
}

fn metric(fields: &BTreeMap<String, String>, name: &str) -> Result<u64, String> {
    fields
        .get(name)
        .ok_or_else(|| format!("missing metric: {name}"))?
        .parse()
        .map_err(|e| format!("invalid metric {name}: {e}"))
}

#[cfg(target_os = "linux")]
fn rss_bytes(pid: u32) -> Result<u64, String> {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).map_err(|e| e.to_string())?;
    let mut parts = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .ok_or("missing VmRSS")?
        .split_whitespace();
    let kib = parts
        .next()
        .ok_or("VmRSS has no value")?
        .parse::<u64>()
        .map_err(|e| e.to_string())?;
    if parts.next() != Some("kB") || parts.next().is_some() {
        return Err("unexpected VmRSS unit".into());
    }
    kib.checked_mul(1024)
        .ok_or_else(|| "VmRSS exceeds u64".into())
}

#[cfg(target_os = "linux")]
fn cpu_ticks(pid: u32) -> Result<u64, String> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|e| e.to_string())?;
    parse_cpu_ticks(&stat)
}

fn parse_cpu_ticks(stat: &str) -> Result<u64, String> {
    let end = stat.rfind(')').ok_or("malformed process stat")?;
    let fields: Vec<_> = stat[end + 1..].split_whitespace().collect();
    let user = fields
        .get(11)
        .ok_or("process stat lacks utime")?
        .parse::<u64>()
        .map_err(|e| e.to_string())?;
    let system = fields
        .get(12)
        .ok_or("process stat lacks stime")?
        .parse::<u64>()
        .map_err(|e| e.to_string())?;
    user.checked_add(system)
        .ok_or_else(|| "process CPU ticks overflow".into())
}

/// Return raw lifetime CPU in platform units; never reinterpret Windows ms as Linux ticks.
fn process_resources(pid: u32) -> Result<(u64, u64), String> {
    #[cfg(target_os = "linux")]
    {
        Ok((rss_bytes(pid)?, cpu_ticks(pid)?))
    }
    #[cfg(target_os = "windows")]
    {
        use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
        // A fresh snapshot cannot retain old memory when a native refresh fails.
        let mut system = System::new();
        let pid = Pid::from_u32(pid);
        if system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing().with_memory().with_cpu(),
        ) != 1
        {
            return Err("Windows process refresh did not update the requested child".into());
        }
        let process = system
            .process(pid)
            .ok_or("Windows child missing from snapshot")?;
        let rss = process.memory();
        if rss == 0 {
            return Err("Windows working-set observation is zero or unavailable".into());
        }
        Ok((rss, process.accumulated_cpu_time()))
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    {
        let _ = pid;
        Err("unsupported process observation platform".into())
    }
}

fn resource_units() -> Value {
    if cfg!(target_os = "windows") {
        json!({"memory":"bytes; Windows current working set via sysinfo::Process::memory",
            "cpu":"milliseconds; Windows lifetime user+kernel CPU via sysinfo::Process::accumulated_cpu_time",
            "cpu_units_per_second":1000,
            "cpu_api_limitation":"sysinfo discards native CPU query errors; zero is ambiguous. The runner rejects decreasing counters and a process with no positive observed CPU; this does not detect every native failure."})
    } else {
        json!({"memory":"bytes; Linux VmRSS from /proc/PID/status",
            "cpu":"clock ticks; Linux lifetime user+system CPU from /proc/PID/stat",
            "cpu_api_limitation":null})
    }
}

fn validate_cpu(previous: Option<u64>, current: u64) -> Result<(), String> {
    if previous.is_some_and(|previous| current < previous) {
        return Err("process lifetime CPU counter decreased; sample integrity failed".into());
    }
    Ok(())
}

fn sha256sum(path: &Path) -> Result<String, String> {
    let mut file = File::open(path).map_err(|e| e.to_string())?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(|e| e.to_string())?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}  {}", digest.finalize(), path.display()))
}

struct Sampler {
    stop: mpsc::Sender<()>,
    phase: Arc<Mutex<String>>,
    task: Option<thread::JoinHandle<Result<Value, String>>>,
}
impl Sampler {
    fn start(
        pid: u32,
        address: SocketAddr,
        path: &Path,
        interval: Duration,
        label: String,
    ) -> Result<Self, String> {
        let file = OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|e| e.to_string())?;
        let initial_bytes = usize::try_from(fs::metadata(path).map_err(|e| e.to_string())?.len())
            .map_err(|_| "resource observation file exceeds usize")?;
        if initial_bytes > MAX_SAMPLE_BYTES {
            return Err("resource observations exceeded the fixed byte limit".into());
        }
        let (stop, wait) = mpsc::channel();
        let phase = Arc::new(Mutex::new(label.clone()));
        let current = phase.clone();
        let started = Instant::now();
        let task = thread::Builder::new().name("resource-envelope-sampler".into()).spawn(move || {
            let mut file = file;
            let mut count = 0usize;
            let mut sample_bytes = initial_bytes;
            let mut max_rss = 0u64;
            let mut max_logical_bytes = 0u64;
            let mut max_worker_queue = 0u64;
            let mut max_aof_queue = None::<u64>;
            let mut first_cpu_ticks = None::<u64>;
            let mut previous_cpu = None::<u64>;
            loop {
                let fields = info(address)?;
                let (rss, cpu) = process_resources(pid)?;
                validate_cpu(previous_cpu, cpu)?;
                previous_cpu = Some(cpu);
                let worker_queue = metric(&fields, "worker_queue_used")?;
                max_rss = max_rss.max(rss);
                max_logical_bytes = max_logical_bytes.max(metric(&fields, "dataset_logical_bytes")?);
                max_worker_queue = max_worker_queue.max(worker_queue);
                if let Some(aof_queue) = fields.get("aof_queue_used").and_then(|value| value.parse::<u64>().ok()) {
                    max_aof_queue = Some(max_aof_queue.unwrap_or(0).max(aof_queue));
                }
                first_cpu_ticks.get_or_insert(cpu);
                let sample = json!({
                    "seconds": started.elapsed().as_secs_f64(),
                    "scenario":label,
                    "phase": current.lock().map_err(|_| "phase lock poisoned")?.clone(),
                    "rss_bytes": rss,
                    "process_cpu_ticks": if cfg!(target_os = "linux") { Some(cpu) } else { None },
                    "process_cpu_milliseconds": if cfg!(target_os = "windows") { Some(cpu) } else { None },
                    "info": fields
                });
                let encoded = serde_json::to_vec(&sample).map_err(|e| e.to_string())?;
                sample_bytes = sample_bytes.checked_add(encoded.len() + 1).ok_or("sample byte count overflow")?;
                if sample_bytes > MAX_SAMPLE_BYTES {
                    return Err("resource observations exceeded the fixed byte limit".into());
                }
                file.write_all(&encoded).map_err(|e| e.to_string())?;
                file.write_all(b"\n").map_err(|e| e.to_string())?;
                file.flush().map_err(|e| e.to_string())?;
                count += 1;
                if count >= MAX_SAMPLES { return Err("sample count exceeded the fixed limit".into()); }
                match wait.recv_timeout(interval) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                        if cfg!(target_os = "windows") && cpu == 0 {
                            return Err("Windows process has no positive CPU observation; CPU evidence unavailable".into());
                        }
                        return Ok(json!({
                        "sample_count":count,
                        "max_observed_rss_bytes":max_rss,
                        "max_observed_dataset_logical_bytes":max_logical_bytes,
                        "max_observed_worker_queue_requests":max_worker_queue,
                        "max_observed_aof_queue_requests":max_aof_queue,
                        "process_cpu_ticks_delta": if cfg!(target_os = "linux") { Some(cpu - first_cpu_ticks.unwrap_or(cpu)) } else { None },
                        "process_cpu_milliseconds_delta": if cfg!(target_os = "windows") { Some(cpu - first_cpu_ticks.unwrap_or(cpu)) } else { None }
                    })); },
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
        }).map_err(|e| e.to_string())?;
        Ok(Self {
            stop,
            phase,
            task: Some(task),
        })
    }
    fn set_phase(&self, phase: &str) -> Result<(), String> {
        *self.phase.lock().map_err(|_| "phase lock poisoned")? = phase.to_owned();
        Ok(())
    }
    fn finish(mut self) -> Result<Value, String> {
        let _ = self.stop.send(());
        self.task
            .take()
            .unwrap()
            .join()
            .map_err(|_| "sampler panicked")?
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(task) = self.task.take() {
            // On an incomplete scenario, stop and join before the child is dropped.
            let _ = task.join();
        }
    }
}

fn quantile(samples: &[u64], percentile: usize) -> Option<u64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = percentile.saturating_mul(sorted.len()).div_ceil(100).max(1);
    sorted.get(rank - 1).copied()
}

fn latency_summary(samples: &[u64], started: Instant) -> Value {
    let elapsed = started.elapsed();
    let operations = samples.len() as u64;
    json!({"operations":operations,"elapsed_ms":elapsed.as_secs_f64()*1000.0,
        "throughput_ops_per_second":operations as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
        "rtt_ns":{"p50":quantile(samples,50),"p95":quantile(samples,95),"p99":quantile(samples,99)}})
}

fn deterministic_bytes(seed: u64, size: usize) -> Vec<u8> {
    let mut state = seed;
    (0..size)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            (state >> 32) as u8
        })
        .collect()
}

fn run_saturation(address: SocketAddr, fixture: &Fixture, seed: u64) -> Result<Vec<u64>, String> {
    let mut threads = Vec::new();
    for client in 0..fixture.saturation.clients {
        let scenario_seed = seed;
        let count = fixture.saturation.operations_per_client;
        let key_count = fixture.saturation.key_count;
        let value_size = fixture.saturation.value_bytes;
        threads.push(thread::spawn(move || -> Result<Vec<u64>, String> {
            let mut stream = connect(address)?;
            let mut latencies = Vec::with_capacity(count);
            for op in 0..count {
                let offset = scenario_seed
                    .wrapping_add((client as u64) << 32)
                    .wrapping_add(op as u64);
                let key = format!(
                    "{{resource-{}}}:key-{}",
                    offset % 4,
                    offset as usize % key_count
                )
                .into_bytes();
                let value = deterministic_bytes(offset, value_size);
                let (response, rtt) = exchange(&mut stream, &[b"SET", &key, &value])?;
                if response != Response::Simple(b"OK".to_vec()) {
                    return Err("SET saturation response mismatch".into());
                }
                latencies.push(rtt);
            }
            Ok(latencies)
        }));
    }
    let mut samples = Vec::new();
    for task in threads {
        samples.extend(task.join().map_err(|_| "saturation client panicked")??);
    }
    Ok(samples)
}

fn run_large_values(address: SocketAddr, fixture: &Fixture, seed: u64) -> Result<Vec<u64>, String> {
    let mut stream = connect(address)?;
    let value = deterministic_bytes(seed, fixture.large_values.value_bytes);
    let mut samples = Vec::with_capacity(fixture.large_values.round_trips * 2);
    for index in 0..fixture.large_values.round_trips {
        let key = format!("{{large}}:value-{index}");
        let (set, rtt) = exchange(&mut stream, &[b"SET", key.as_bytes(), &value])?;
        if set != Response::Simple(b"OK".to_vec()) {
            return Err("large-value SET response mismatch".into());
        }
        samples.push(rtt);
        let (get, rtt) = exchange(&mut stream, &[b"GET", key.as_bytes()])?;
        if get != Response::Bulk(Some(value.clone())) {
            return Err("large-value GET mismatch".into());
        }
        samples.push(rtt);
    }
    Ok(samples)
}

fn run_expiration(
    address: SocketAddr,
    fixture: &Fixture,
    seed: u64,
) -> Result<(Value, Vec<u64>), String> {
    let before = info(address)?;
    let expiration_before = metric(&before, "expiration_batch_keys_removed_total")?;
    let mut stream = connect(address)?;
    let value = b"ttl";
    let mut samples = Vec::with_capacity(fixture.expiration.keys);
    let began = Instant::now();
    for index in 0..fixture.expiration.keys {
        let key = format!("{{expire}}:key-{}", seed.wrapping_add(index as u64));
        let (response, rtt) = exchange(
            &mut stream,
            &[
                b"SET",
                key.as_bytes(),
                value,
                b"PX",
                fixture.expiration.ttl_ms.to_string().as_bytes(),
            ],
        )?;
        if response != Response::Simple(b"OK".to_vec()) {
            return Err("expiration SET response mismatch".into());
        }
        samples.push(rtt);
    }
    let latency = latency_summary(&samples, began);
    let deadline = Instant::now() + Duration::from_millis(fixture.expiration.settle_timeout_ms);
    let mut after = info(address)?;
    while metric(&after, "expiration_batch_keys_removed_total")? <= expiration_before
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(50));
        after = info(address)?;
    }
    let observed = metric(&after, "expiration_batch_keys_removed_total")? > expiration_before;
    if !observed {
        return Err("expiration workload completed without observing expiration removals".into());
    }
    Ok((
        json!({"latency":latency,
        "expiration_keys_removed_before":expiration_before,
        "expiration_keys_removed_after":metric(&after,"expiration_batch_keys_removed_total")?,
        "expired_key_removals_observed":observed,
        "settle_timeout_ms":fixture.expiration.settle_timeout_ms}),
        samples,
    ))
}

fn subscribe_without_reading(address: SocketAddr) -> Result<TcpStream, String> {
    let mut stream = connect(address)?;
    let (response, _) = exchange(&mut stream, &[b"SUBSCRIBE", b"resource-slow-client"])?;
    if !matches!(response, Response::Array(Some(_))) {
        return Err("SUBSCRIBE acknowledgement mismatch".into());
    }
    Ok(stream)
}

fn run_slow_client(address: SocketAddr, fixture: &Fixture) -> Result<(Value, Vec<u64>), String> {
    let slow = subscribe_without_reading(address)?;
    let before = info(address)?;
    let evictions_before = metric(&before, "pubsub_evictions_total")?;
    let payload = deterministic_bytes(fixture.slow_client.seed, fixture.slow_client.message_bytes);
    let mut publisher = connect(address)?;
    let began = Instant::now();
    let mut samples = Vec::with_capacity(fixture.slow_client.publications);
    for _ in 0..fixture.slow_client.publications {
        let (response, rtt) = exchange(
            &mut publisher,
            &[b"PUBLISH", b"resource-slow-client", &payload],
        )?;
        if !matches!(response, Response::Integer(_)) {
            return Err("PUBLISH response mismatch".into());
        }
        samples.push(rtt);
    }
    let elapsed = began.elapsed();
    let latency = latency_summary(&samples, began);
    let after = info(address)?;
    let evictions_after = metric(&after, "pubsub_evictions_total")?;
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    let _ = slow.shutdown(std::net::Shutdown::Both);
    let evicted = evictions_after > evictions_before;
    if !evicted {
        return Err("slow-client workload completed without observing subscriber eviction".into());
    }
    Ok((
        json!({"publications":fixture.slow_client.publications,
        "message_bytes":fixture.slow_client.message_bytes,
        "elapsed_ms":elapsed_ms,
        "publication_rate_per_second":fixture.slow_client.publications as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
        "pubsub_evictions_before":evictions_before,"pubsub_evictions_after":evictions_after,
        "latency":latency,
        "slow_subscriber_evictions_observed":evicted,
        "configured_pubsub_queue_capacity":fixture.slow_client.queue_capacity}),
        samples,
    ))
}

fn run_compaction(
    address: SocketAddr,
    fixture: &Fixture,
    seed: u64,
) -> Result<(Value, Vec<u64>), String> {
    let before = info(address)?;
    if metric(&before, "aof_enabled")? == 0 {
        return Ok((
            json!({"latency":null,"compactions_before":null,"compactions_after":null,
            "completed_compaction_observed":false,"skip_reason":"AOF is disabled"}),
            Vec::new(),
        ));
    }
    let compactions_before = metric(&before, "aof_compactions_total")?;
    let began = Instant::now();
    let value = deterministic_bytes(seed, fixture.compaction.value_bytes);
    let mut stream = connect(address)?;
    let mut samples = Vec::with_capacity(fixture.compaction.writes);
    for index in 0..fixture.compaction.writes {
        let key = format!("{{compact}}:key-{index}");
        let (response, rtt) = exchange(&mut stream, &[b"SET", key.as_bytes(), &value])?;
        if response != Response::Simple(b"OK".to_vec()) {
            return Err("compaction SET response mismatch".into());
        }
        samples.push(rtt);
    }
    let write_latency = latency_summary(&samples, began);
    let deadline = Instant::now() + Duration::from_millis(fixture.compaction.observe_timeout_ms);
    let mut after = info(address)?;
    while metric(&after, "aof_compactions_total")? <= compactions_before
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(50));
        after = info(address)?;
    }
    let compactions_after = metric(&after, "aof_compactions_total")?;
    let observed = compactions_after > compactions_before;
    if !observed {
        return Err("AOF write workload completed without observing a completed compaction".into());
    }
    Ok((
        json!({"latency":write_latency,
        "compactions_before":compactions_before,"compactions_after":compactions_after,
        "completed_compaction_observed":observed,
        "automatic_threshold_bytes":fixture.compaction.after_bytes,
        "observe_timeout_ms":fixture.compaction.observe_timeout_ms}),
        samples,
    ))
}

fn overrides(
    policy: AofPolicy,
    shards: usize,
    data: &Path,
    fixture: &Fixture,
) -> Vec<(&'static str, OsString)> {
    let mut values = vec![
        ("SIDER_SHARDS", shards.to_string().into()),
        (
            "SIDER_MAX_DATASET_BYTES",
            fixture.quota_bytes.to_string().into(),
        ),
        (
            "SIDER_WORKER_QUEUE_CAPACITY",
            fixture.worker_queue_capacity.to_string().into(),
        ),
        (
            "SIDER_PUBSUB_QUEUE_CAPACITY",
            fixture.slow_client.queue_capacity.to_string().into(),
        ),
    ];
    if !matches!(policy, AofPolicy::Off) {
        values.push(("SIDER_AOF_DIR", data.as_os_str().to_owned()));
        values.push(("SIDER_AOF_SYNC", policy.name().into()));
        values.push((
            "SIDER_AOF_COMPACT_AFTER_BYTES",
            fixture.compaction.after_bytes.to_string().into(),
        ));
    }
    values
}

fn start_server(
    binary: &Path,
    policy: AofPolicy,
    shards: usize,
    data: &Path,
    fixture: &Fixture,
) -> Result<(SiderProcess, f64), String> {
    let began = Instant::now();
    let process = SiderProcess::try_start_configured(
        binary,
        env!("CARGO_PKG_VERSION"),
        &overrides(policy, shards, data, fixture),
    )?;
    // Includes the helper's --version preflight and readiness PING; this is not server-only startup time.
    Ok((process, began.elapsed().as_secs_f64() * 1000.0))
}

fn wait_synced(address: SocketAddr) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let fields = info(address)?;
        let written = metric(&fields, "aof_written_sequence")?;
        let synced = metric(&fields, "aof_synced_sequence")?;
        if written == synced {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("AOF did not confirm the written sequence".into());
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn run_scenario(
    binary: &Path,
    output: &Path,
    fixture: &Fixture,
    shards: usize,
    policy: AofPolicy,
    repetition: usize,
    latency_file: &mut File,
) -> Result<Value, String> {
    let label = format!("shards-{shards}-aof-{}-rep-{repetition}", policy.name());
    let data = output.join(format!("{label}-aof"));
    if !matches!(policy, AofPolicy::Off) {
        fs::create_dir(&data).map_err(|e| e.to_string())?;
    }
    let (process, startup_ms) = start_server(binary, policy, shards, &data, fixture)?;
    let address = process.address();
    let sampler = Sampler::start(
        process.id(),
        address,
        &output.join("resource-envelope-observations.jsonl"),
        Duration::from_millis(fixture.sample_interval_ms),
        label.clone(),
    )?;
    let mut phases = Vec::new();

    sampler.set_phase("saturation")?;
    let began = Instant::now();
    let saturation = run_saturation(
        address,
        fixture,
        fixture.seed ^ (shards as u64) ^ repetition as u64,
    )?;
    let saturation_summary = latency_summary(&saturation, began);
    write_latencies(latency_file, &label, "saturation", &saturation)?;
    phases.push(json!({"name":"saturation","metrics":saturation_summary}));

    sampler.set_phase("large_values")?;
    let began = Instant::now();
    let large = run_large_values(
        address,
        fixture,
        fixture.seed.wrapping_add(repetition as u64),
    )?;
    let large_summary = latency_summary(&large, began);
    write_latencies(latency_file, &label, "large_values", &large)?;
    phases.push(json!({"name":"large_values","value_bytes":fixture.large_values.value_bytes,"metrics":large_summary}));

    sampler.set_phase("slow_client")?;
    let (slow_client, slow_samples) = run_slow_client(address, fixture)?;
    write_latencies(latency_file, &label, "slow_client", &slow_samples)?;
    phases.push(json!({"name":"slow_client","metrics":slow_client}));

    sampler.set_phase("expiration")?;
    let (expiration, expiration_samples) = run_expiration(
        address,
        fixture,
        fixture.seed.wrapping_add(1000 * repetition as u64),
    )?;
    write_latencies(latency_file, &label, "expiration", &expiration_samples)?;
    phases.push(json!({"name":"expiration","metrics":expiration}));

    sampler.set_phase("compaction")?;
    let (compaction, compaction_samples) = run_compaction(
        address,
        fixture,
        fixture.seed.wrapping_add(2000 * repetition as u64),
    )?;
    write_latencies(latency_file, &label, "compaction", &compaction_samples)?;
    phases.push(json!({"name":"compaction","metrics":compaction}));

    let (recovery_ms, recovery_observations) = if !matches!(policy, AofPolicy::Off) {
        sampler.set_phase("pre_recovery_sync")?;
        let (set, _) = exchange(
            &mut connect(address)?,
            &[b"SET", b"{recovery}:sentinel", b"confirmed"],
        )?;
        if set != Response::Simple(b"OK".to_vec()) {
            return Err("recovery sentinel SET mismatch".into());
        }
        wait_synced(address)?;
        let observations = sampler.finish()?;
        process.finish();
        let began = Instant::now();
        let (recovered, _) = start_server(binary, policy, shards, &data, fixture)?;
        let recovery_ms = began.elapsed().as_secs_f64() * 1000.0;
        let recovery_sampler = Sampler::start(
            recovered.id(),
            recovered.address(),
            &output.join("resource-envelope-observations.jsonl"),
            Duration::from_millis(fixture.sample_interval_ms),
            format!("{label}-recovery"),
        )?;
        let (value, _) = exchange(
            &mut connect(recovered.address())?,
            &[b"GET", b"{recovery}:sentinel"],
        )?;
        if value != Response::Bulk(Some(b"confirmed".to_vec())) {
            return Err("confirmed AOF sentinel missing after restart".into());
        }
        let large_value = deterministic_bytes(
            fixture.seed.wrapping_add(repetition as u64),
            fixture.large_values.value_bytes,
        );
        let (recovered_large, _) = exchange(
            &mut connect(recovered.address())?,
            &[b"GET", b"{large}:value-0"],
        )?;
        if recovered_large != Response::Bulk(Some(large_value)) {
            return Err("confirmed large value missing or changed after restart".into());
        }
        let restart_observations = recovery_sampler.finish()?;
        recovered.finish();
        (
            Some(recovery_ms),
            json!({"restart":restart_observations,"pre_restart":observations,
                "recovered_keys_checked":2,"recovered_large_value_bytes":fixture.large_values.value_bytes}),
        )
    } else {
        let initial_observations = sampler.finish()?;
        process.finish();
        (None, json!({"initial_process":initial_observations}))
    };
    Ok(
        json!({"scenario":label,"shards":shards,"aof_policy":policy.name(),"repetition":repetition,
        "startup_harness_ms":startup_ms,"startup_timing_scope":"helper version preflight through readiness PING",
        "confirmed_aof_restart_harness_ms":recovery_ms,
        "confirmed_aof_restart_timing_scope":"helper version preflight through recovered readiness PING; sentinel checked after timer",
        "resource_observation_summaries":recovery_observations,
        "phases":phases}),
    )
}

fn write_latencies(
    file: &mut File,
    scenario: &str,
    phase: &str,
    samples: &[u64],
) -> Result<(), String> {
    for (index, rtt_ns) in samples.iter().enumerate() {
        serde_json::to_writer(
            &mut *file,
            &json!({"scenario":scenario,"phase":phase,"sample":index,"rtt_ns":rtt_ns}),
        )
        .map_err(|e| e.to_string())?;
        file.write_all(b"\n").map_err(|e| e.to_string())?;
    }
    file.flush().map_err(|e| e.to_string())
}

fn write_jsonl(file: &mut File, value: &Value) -> Result<(), String> {
    serde_json::to_writer(&mut *file, value).map_err(|e| e.to_string())?;
    file.write_all(b"\n").map_err(|e| e.to_string())?;
    file.flush().map_err(|e| e.to_string())
}

fn command_text(program: &str, args: &[&str]) -> Result<String, String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "{program} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    String::from_utf8(output.stdout)
        .map(|text| text.trim().to_owned())
        .map_err(|e| e.to_string())
}

#[cfg(target_os = "linux")]
fn machine() -> Result<Value, String> {
    let cpuinfo = fs::read_to_string("/proc/cpuinfo").map_err(|e| e.to_string())?;
    let meminfo = fs::read_to_string("/proc/meminfo").map_err(|e| e.to_string())?;
    let hz = command_text("getconf", &["CLK_TCK"])?
        .parse::<u64>()
        .map_err(|e| e.to_string())?;
    Ok(json!({"os":"linux","architecture":std::env::consts::ARCH,
        "kernel":command_text("uname", &["-a"])? ,
        "cpu":cpuinfo.lines().filter(|line| line.starts_with("model name") || line.starts_with("cpu cores") || line.starts_with("siblings")).take(3).collect::<Vec<_>>(),
        "logical_processors":cpuinfo.lines().filter(|line| line.starts_with("processor")).count(),
        "available_parallelism":thread::available_parallelism().map_err(|e| e.to_string())?.get(),
        "memory_total":meminfo.lines().find(|line| line.starts_with("MemTotal:")).ok_or("missing MemTotal")?,
        "process_clock_ticks_per_second":hz,
        "loadavg_before":fs::read_to_string("/proc/loadavg").map_err(|e| e.to_string())?,
        "rustc":command_text("rustc", &["-Vv"])? ,"cargo":command_text("cargo", &["--version"])?}))
}

#[cfg(target_os = "windows")]
fn machine() -> Result<Value, String> {
    use sysinfo::{CpuRefreshKind, MemoryRefreshKind, System};
    let mut system = System::new();
    system.refresh_cpu_list(CpuRefreshKind::nothing());
    system.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
    let os_version = System::long_os_version().ok_or("missing Windows version")?;
    if !os_version.contains("Windows 11") {
        return Err(format!(
            "declared Windows profile requires Windows 11: {os_version}"
        ));
    }
    if system.cpus().is_empty() || system.total_memory() == 0 {
        return Err("Windows hardware context unavailable".into());
    }
    Ok(
        json!({"os":"windows","os_version":os_version,"architecture":std::env::consts::ARCH,
        "kernel":System::kernel_version().ok_or("missing Windows build")?,
        "cpu":system.cpus()[0].brand(),"logical_processors":system.cpus().len(),
        "available_parallelism":thread::available_parallelism().map_err(|e| e.to_string())?.get(),
        "memory_total_bytes":system.total_memory(),"process_clock_ticks_per_second":null,
        "process_cpu_milliseconds_per_second":1000,"loadavg_before":null,
        "loadavg_limitation":"Windows has no equivalent Linux load average; idle-host declaration is not continuous monitoring",
        "process_sampler":"sysinfo 0.39.6; fresh snapshot of requested child PID each sample",
        "rustc":command_text("rustc", &["-Vv"])? ,"cargo":command_text("cargo", &["--version"])?}),
    )
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
fn machine() -> Result<Value, String> {
    Err("unsupported resource-envelope platform".into())
}

fn loadavg_after() -> Result<Option<String>, String> {
    if cfg!(target_os = "linux") {
        fs::read_to_string("/proc/loadavg")
            .map(Some)
            .map_err(|e| e.to_string())
    } else {
        Ok(None)
    }
}

fn run() -> Result<(), String> {
    if !cfg!(any(target_os = "linux", target_os = "windows")) {
        return Err("resource-envelope runner requires Linux or Windows 11".into());
    }
    if std::env::var("SIDER_RESOURCE_IDLE_MACHINE").ok().as_deref() != Some("1") {
        return Err(
            "set SIDER_RESOURCE_IDLE_MACHINE=1 only after reserving an otherwise idle machine"
                .into(),
        );
    }
    let binary = PathBuf::from(
        std::env::var_os("SIDER_RESOURCE_BINARY").ok_or("missing SIDER_RESOURCE_BINARY")?,
    );
    let output = PathBuf::from(
        std::env::var_os("SIDER_RESOURCE_OUTPUT_DIR").ok_or("missing SIDER_RESOURCE_OUTPUT_DIR")?,
    );
    if !binary.is_absolute() || !output.is_absolute() {
        return Err("binary and output paths must be absolute".into());
    }
    if !fs::metadata(&binary).map_err(|e| e.to_string())?.is_file() {
        return Err("binary must be a file".into());
    }
    let source_commit_sha = command_text("git", &["rev-parse", "HEAD"])?;
    let head_tree_sha = command_text("git", &["rev-parse", "HEAD^{tree}"])?;
    let source_tree_sha = command_text("git", &["write-tree"])?;
    let status = command_text("git", &["status", "--porcelain"])?;
    if status
        .lines()
        .any(|line| line.starts_with("??") || line.as_bytes().get(1) != Some(&b' '))
    {
        return Err("resource-envelope run requires all source inputs staged and no unstaged or untracked files".into());
    }
    fs::create_dir(&output).map_err(|e| format!("create new output directory: {e}"))?;
    let fixture = fixture();
    let harness_binary = std::env::current_exe().map_err(|e| e.to_string())?;
    let harness_sha256 = sha256sum(&harness_binary)?;
    let fixture_sha256 = format!("{:x}", Sha256::digest(FIXTURE.as_bytes()));
    let machine_before = machine()?;
    let binary_sha256 = sha256sum(&binary)?;
    let binary_version = command_text(
        binary.to_str().ok_or("binary path is not UTF-8")?,
        &["--version"],
    )?;
    if binary_version != format!("sider {}", env!("CARGO_PKG_VERSION")) {
        return Err(format!("binary version mismatch: {binary_version}"));
    }
    let mut latency_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output.join("resource-envelope-latency.jsonl"))
        .map_err(|e| e.to_string())?;
    let mut observations_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output.join("resource-envelope-observations.jsonl"))
        .map_err(|e| e.to_string())?;
    let header = json!({"record_type":"run_header","schema_version":2,"resource_units":resource_units(),
        "source_commit_sha":source_commit_sha,"source_tree_sha":source_tree_sha,"binary_sha256sum_output":binary_sha256,
        "binary_version":binary_version,"harness_binary_sha256sum_output":harness_sha256,
        "fixture_sha256":fixture_sha256,"machine":machine_before,
        "fixture":fixture.clone()});
    write_jsonl(&mut latency_file, &header)?;
    write_jsonl(&mut observations_file, &header)?;
    drop(observations_file);
    let mut scenarios = Vec::new();
    for shards in &fixture.shards {
        for policy_name in &fixture.aof_policies {
            let policy = AofPolicy::parse(policy_name)?;
            for repetition in 0..fixture.repetitions {
                scenarios.push(run_scenario(
                    &binary,
                    &output,
                    &fixture,
                    *shards,
                    policy,
                    repetition,
                    &mut latency_file,
                )?);
            }
        }
    }
    drop(latency_file);
    let binary_sha256_after = sha256sum(&binary)?;
    if binary_sha256 != binary_sha256_after {
        return Err("binary changed during the run; no report will be published".into());
    }
    if harness_sha256 != sha256sum(&harness_binary)? {
        return Err("compiled harness changed during the run".into());
    }
    if source_tree_sha != command_text("git", &["write-tree"])?
        || source_commit_sha != command_text("git", &["rev-parse", "HEAD"])?
        || status != command_text("git", &["status", "--porcelain"])?
    {
        return Err("source identity or worktree status changed during the run".into());
    }
    let observations_path = output.join("resource-envelope-observations.jsonl");
    let latency_path = output.join("resource-envelope-latency.jsonl");
    let loadavg_after = loadavg_after()?;
    let report = json!({
        "schema_version":2,
        "claim_status":"measurement_only",
        "runner_status":"all_declared_scenarios_completed",
        "source":{"git_commit_sha":source_commit_sha,"head_tree_sha":head_tree_sha,"git_tree_sha":source_tree_sha,"worktree_matches_index":true,"index_matches_head":head_tree_sha == source_tree_sha},
        "binary":{"path":binary,"sha256sum_output":binary_sha256,"version":binary_version,"provenance":"unverified_external_binary","hash_unchanged_after_run":true},
        "harness_binary":{"path":harness_binary,"sha256sum_output":harness_sha256,"hash_unchanged_after_run":true,"provenance":"compiled_harness_build_relationship_unverified"},
        "fixture_sha256":fixture_sha256,
        "machine":machine_before,
        "loadavg_after":loadavg_after,
        "fixture":fixture.clone(),
        "observations":{"file":"resource-envelope-observations.jsonl","sha256sum_output":sha256sum(&observations_path)?,"bytes":fs::metadata(&observations_path).map_err(|e| e.to_string())?.len(),"sampling_interval_ms":fixture.sample_interval_ms,
            "platform_units":resource_units(),
            "memory_unit":"bytes; platform resident memory; sampled values may miss peaks",
            "logical_bytes":"dataset_logical_bytes from INFO; not RSS or total process memory",
            "queues":"INFO request-count gauges; not byte counts",
            "cpu":"Linux: ticks / machine.process_clock_ticks_per_second. Windows: process_cpu_milliseconds / 1000. Divide CPU seconds by elapsed seconds for average core utilization."},
        "latency_samples":{"file":"resource-envelope-latency.jsonl","sha256sum_output":sha256sum(&latency_path)?,"bytes":fs::metadata(&latency_path).map_err(|e| e.to_string())?.len()},
        "limitations":[
            "Harness startup timings include a --version preflight and readiness PING, not server-only startup time.",
            "The binary hash identifies the observed executable bytes; it does not prove that executable was built from the recorded source checkout.",
            "The sampler issues INFO requests and therefore contributes a small, recorded workload.",
            "This fixture is a controlled loopback workload, not a production deployment profile or resource guarantee.",
            "A complete run means the runner finished its declared workload; it does not imply a capacity threshold or pass/fail limit."
        ],
        "scenarios":scenarios
    });
    let report_path = output.join("resource-envelope-report.json");
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&report_path)
        .map_err(|e| e.to_string())?;
    serde_json::to_writer_pretty(&mut file, &report).map_err(|e| e.to_string())?;
    file.write_all(b"\n").map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    Ok(())
}

#[test]
#[ignore = "explicit resource-envelope workload; requires SIDER_RESOURCE_IDLE_MACHINE=1 and an idle Linux or Windows 11 host"]
fn resource_envelope_workload() {
    run().unwrap_or_else(|error| {
        panic!("resource-envelope run incomplete; no report was published: {error}")
    });
}

#[test]
fn resource_envelope_fixture_is_fixed_and_covers_declared_pressure_modes() {
    let fixture = fixture();
    assert_eq!(fixture.seed, 27_688_995);
    assert_eq!(fixture.shards.as_slice(), &[1, 4]);
    assert_eq!(
        fixture
            .aof_policies
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["off", "always", "everysec"]
    );
    assert_eq!(fixture.repetitions, 3);
    assert_eq!(fixture.warmup_operations, 0);
    assert_eq!(
        fixture.generator,
        "LCG64 multiplier 6364136223846793005 increment 1; emit the low 8 bits of the high 32-bit word"
    );
    assert_eq!(fixture.saturation.key_distribution, "shared_keyspace");
    assert_eq!(fixture.slow_client.message_bytes, 65_536);
    assert_eq!(fixture.slow_client.seed, 5_311_518);
    assert_eq!(fixture.large_values.value_bytes, 262_144);
    assert_eq!(fixture.expiration.ttl_ms, 1_000);
    assert_eq!(fixture.compaction.after_bytes, 262_144);
}

#[test]
fn resource_envelope_percentiles_use_nearest_rank_without_latency_unit_conversion() {
    assert_eq!(
        quantile(&[100, 90, 80, 70, 60, 50, 40, 30, 20, 10], 50),
        Some(50)
    );
    assert_eq!(
        quantile(&[100, 90, 80, 70, 60, 50, 40, 30, 20, 10], 95),
        Some(100)
    );
    assert_eq!(quantile(&[10], 99), Some(10));
    assert_eq!(quantile(&[], 50), None);
}

#[test]
fn resource_envelope_cpu_parser_accounts_for_parenthesized_process_names() {
    // After the final ')' fields begin with field 3 (state); utime/stime are fields 14/15.
    let stat = "123 (sider (worker)) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16";
    assert_eq!(parse_cpu_ticks(stat).unwrap(), 23);
    assert!(parse_cpu_ticks("123 missing close parenthesis").is_err());
}

#[test]
fn resource_envelope_rejects_decreasing_cpu_observations() {
    assert!(validate_cpu(None, 0).is_ok());
    assert!(validate_cpu(Some(10), 10).is_ok());
    assert!(validate_cpu(Some(10), 11).is_ok());
    assert!(validate_cpu(Some(10), 0).is_err());
    assert!(validate_cpu(Some(10), 9).is_err());
}

#[cfg(target_os = "windows")]
#[test]
fn resource_envelope_windows_observes_current_process_and_rejects_absent_pid() {
    let (rss, _) = process_resources(std::process::id()).unwrap();
    assert!(rss > 0);
    assert!(process_resources(u32::MAX).is_err());
    assert_eq!(resource_units()["cpu_units_per_second"], 1000);
}

#[test]
fn resource_envelope_portable_hash_streams_bytes_and_preserves_unicode_path() {
    let directory = std::env::temp_dir().join(format!(
        "sider-resource-hash-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("evidence-é.bin");
    fs::write(&path, vec![b'a'; 1_000_000]).unwrap();
    let hash = sha256sum(&path).unwrap();
    fs::remove_file(&path).unwrap();
    fs::remove_dir(&directory).unwrap();
    assert_eq!(
        hash,
        format!(
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0  {}",
            path.display()
        )
    );
    assert!(sha256sum(&path).is_err());
}
