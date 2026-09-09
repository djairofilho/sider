//! Gate de desempenho do pacote Linux: execução explícita em máquina sem outras cargas.
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
            "observação falhou: {}",
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
        "memory":memory.lines().find(|line| line.starts_with("MemTotal:")).ok_or("MemTotal ausente")?,
        "os_release":fs::read_to_string("/etc/os-release").map_err(|e| e.to_string())?,
        "kernel":output(Command::new("uname").arg("-a"))?,
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
        .ok_or("VmRSS ausente")?
        .split_whitespace();
    let kib = fields
        .next()
        .ok_or("VmRSS sem valor")?
        .parse::<u64>()
        .map_err(|e| e.to_string())?;
    if fields.next() != Some("kB") || fields.next().is_some() {
        return Err("unidade de VmRSS inesperada".into());
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
                    return Err("amostras RSS excederam limite".into());
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
            .map_err(|e| format!("iniciar amostragem RSS: {e}"))?;
        Ok(sampler)
    }
    fn finish(mut self) -> Result<Vec<RssSample>, String> {
        let _ = self.stop.send(());
        self.task
            .take()
            .unwrap()
            .join()
            .map_err(|_| "amostrador RSS encerrou com panic")?
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
        // Caminho exclusivo criado por este guard; nunca uma fonte de dados reutilizada.
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
        .ok_or("prazo da fase excedido")?;
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
            .ok_or("prazo da fase excedido")?;
        stream
            .get_ref()
            .set_read_timeout(Some(remaining.min(TIMEOUT)))
            .map_err(|e| e.to_string())?;
        if !matches!(wire::read_response(stream).map_err(|e| e.to_string())?.value, Response::Integer(value) if value > 0)
        {
            return Err("INCR não retornou inteiro positivo".into());
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
        .map_err(|e| format!("aguardar fase medida: {e}"))?;
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
    for (name, value) in overrides {
        command.env(name, value);
    }
    output(command.env("SIDER_ADDR", "127.0.0.1:0").arg("--diagnose"))
}

fn repetition(
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
    let configuration = diagnose(package.binary(), &overrides)?;
    let server = SiderProcess::try_start_configured(package.binary(), version, &overrides)?;
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
                .map_err(|e| format!("aquecimento: {e}"))??;
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
                .map_err(|_| "cliente encerrou com panic".to_owned())
                .and_then(|result| result)
        })
        .collect();
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
        .ok_or("nenhuma amostra RSS dentro da fase medida")?;
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
            return Err("estado final difere dos INCR medidos e do aquecimento".into());
        }
    }
    let summary = json!({"repetition":iteration+1, "operations":CLIENTS*OPERATIONS,
        "warmup_operations":CLIENTS*WARMUP, "warmup_seconds":warmup_seconds, "seconds":seconds,
        "operations_per_second":rates, "verified_keys":expected.len(),
        "batch_rtt_ms":percentiles(&latencies), "batch_samples":latencies.len(),
        "rss_sampled_max_bytes":observed_max, "rss_samples":sampled.len()});
    let raw = json!({"summary":summary, "effective_configuration":configuration,
        "runtime":{"pid":server.id(),"address":address.to_string(),"ready_file_enabled":true},
        "clients":raw_clients, "rss_samples":sampled.into_iter().map(|sample|json!({
            "seconds":sample.at.duration_since(began).as_secs_f64(),"bytes":sample.bytes
        })).collect::<Vec<_>>()});
    drop(stream);
    server.finish();
    Ok((summary, raw, rates))
}

#[test]
#[ignore = "gate Linux do pacote: requer SIDER_BENCH_IDLE_MACHINE=1 e máquina sem builds ou outra carga"]
fn release_benchmarks_gate() {
    assert_eq!(
        std::env::var("SIDER_BENCH_IDLE_MACHINE").as_deref(),
        Ok("1"),
        "a carga exige confirmação explícita de máquina livre"
    );
    let context = gate_receipt::GateContext::from_env("benchmarks").unwrap();
    let package = release_input::ReleaseInput::from_env(&context).unwrap();
    let raw_path = context.release_dir().join("benchmarks-samples.json");
    assert!(!raw_path.exists(), "não substituir amostras anteriores");
    let began = Instant::now();
    let hardware = hardware().unwrap();
    let mut summaries = Vec::new();
    let mut samples = Vec::new();
    let mut sample_bytes = 0usize;
    for (index, scenario) in matrix().into_iter().enumerate() {
        let mut repetitions = Vec::new();
        let mut raw = Vec::new();
        let mut throughput = Vec::new();
        for iteration in 0..REPETITIONS {
            let (summary, detail, rate) =
                repetition(&package, context.version(), scenario, index, iteration).unwrap();
            sample_bytes += serde_json::to_vec_pretty(&detail).unwrap().len();
            assert!(
                sample_bytes < MAX_RAW_BYTES,
                "amostras acumuladas excederam 64 MiB"
            );
            repetitions.push(summary);
            raw.push(detail);
            throughput.push(rate);
        }
        summaries.push(
            json!({"scenario":scenario.details(), "repetitions":repetitions,
            "throughput_variation":variation(&throughput)}),
        );
        samples.push(json!({"scenario":scenario.details(),"repetitions":raw}));
    }
    package.verify_again(&context).unwrap();
    let methodology = json!({"scenarios":16,"repetitions":REPETITIONS,"clients":CLIENTS,
        "warmup_operations_per_client":WARMUP,"measured_operations_per_client":OPERATIONS,"seed":SEED,
        "lcg_multiplier":6364136223846793005u64,"lcg_increment":1,
        "latency_scope":"batch_round_trip","latency_unit":"nanoseconds","percentiles":"nearest_rank",
        "throughput_window":"earliest_client_start_to_latest_client_end",
        "rss_interval_requested_ms":RSS_INTERVAL.as_millis(),
        "operator_confirmed_idle_machine":true,
        "limitations":["loopback; clientes e amostrador RSS compartilham a máquina",
            "RTT de lote pipeline não é dividido em latência fictícia por comando",
            "RSS observado durante a medição, não pico garantido",
            "ordem fixa de cenários; três repetições descritivas, sem intervalo de confiança",
            "sem limiar de superioridade sobre Redis ou garantia de desempenho"]});
    let raw = json!({"schema_version":1,"task":"R11-05","sha":context.sha(),"version":context.version(),
        "target":context.target(),"input":package.details(),"hardware":hardware,"methodology":methodology,
        "loadavg_after":fs::read_to_string("/proc/loadavg").unwrap(),"scenarios":samples});
    let bytes = serde_json::to_vec_pretty(&raw).unwrap();
    assert!(bytes.len() <= MAX_RAW_BYTES, "amostras excederam 64 MiB");
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&raw_path)
        .unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let raw_hash = release_input::sha256(&raw_path).unwrap();
    package.verify_again(&context).unwrap();
    context.publish((16*REPETITIONS*CLIENTS*OPERATIONS) as u64, began.elapsed(),
        json!({"input":package.details(),"methodology":methodology,"scenarios":summaries,
            "raw_samples":{"file":"benchmarks-samples.json","sha256":raw_hash,"bytes":bytes.len()}})).unwrap();
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
