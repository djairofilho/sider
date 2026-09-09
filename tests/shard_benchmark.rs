//! Medição exploratória TCP isolada; executar sem builds ou outra carga concorrente.
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

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::net::TcpStream;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use serde_json::json;
use sider_process::SiderProcess;
use wire::Response;

const SEED: u64 = 0x5240_4005;
const CLIENTS: usize = 4;
const OPERATIONS: usize = 512;
const TIMEOUT: Duration = Duration::from_secs(30);

fn output(command: &mut Command) -> String {
    let result = process::run(command, TIMEOUT).unwrap();
    assert!(
        result.status.success(),
        "comando de observação falhou: {:?}",
        result.stderr
    );
    String::from_utf8(result.stdout).unwrap().trim().to_owned()
}

fn rss(pid: u32) -> u64 {
    if cfg!(target_os = "windows") {
        output(Command::new("powershell").args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            &format!("(Get-Process -Id {pid}).WorkingSet64"),
        ]))
        .parse()
        .unwrap()
    } else {
        let status = fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
        status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            * 1024
    }
}

fn hardware() -> String {
    if cfg!(target_os = "windows") {
        output(Command::new("powershell").args(["-NoProfile", "-NonInteractive", "-Command",
            "Get-CimInstance Win32_Processor | Select-Object Name,NumberOfCores,NumberOfLogicalProcessors | ConvertTo-Json -Compress"]))
    } else {
        fs::read_to_string("/proc/cpuinfo")
            .unwrap()
            .lines()
            .filter(|line| line.starts_with("model name") || line.starts_with("cpu cores"))
            .take(2)
            .collect::<Vec<_>>()
            .join("; ")
    }
}

fn connection(address: std::net::SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, TIMEOUT).unwrap();
    stream.set_nodelay(true).unwrap();
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(TIMEOUT)).unwrap();
    stream
}

#[test]
#[ignore = "benchmark exploratório: requer SIDER_SHARD_BENCH_OUTPUT e máquina sem carga concorrente"]
fn exploratory_shard_benchmark() {
    let context = gate_receipt::GateContext::from_env("benchmarks").unwrap();
    let package = release_input::ReleaseInput::from_env(&context).unwrap();
    let path =
        std::env::var_os("SIDER_SHARD_BENCH_OUTPUT").expect("destino de resultados obrigatório");
    let path = Path::new(&path);
    assert!(!path.exists(), "não substituir evidência anterior");
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sha = output(
        Command::new("git")
            .current_dir(root)
            .args(["rev-parse", "HEAD"]),
    );
    let status = output(Command::new("git").current_dir(root).args([
        "status",
        "--porcelain=v1",
        "--untracked-files=normal",
    ]));
    assert!(
        status.is_empty(),
        "benchmark publicado exige checkout limpo"
    );
    let compiler = output(Command::new("rustc").arg("-Vv"));
    let hardware = hardware();
    let mut rows = Vec::new();
    for shards in [1, 4] {
        for hot in [false, true] {
            for pipeline in [1, 16] {
                for durable in [false, true] {
                    let directory = std::env::temp_dir().join(format!(
                        "sider-shard-bench-{}-{}",
                        std::process::id(),
                        rows.len()
                    ));
                    fs::create_dir(&directory).unwrap();
                    let mut overrides = vec![("SIDER_SHARDS", OsString::from(shards.to_string()))];
                    if durable {
                        overrides.push(("SIDER_AOF_DIR", directory.as_os_str().to_owned()));
                        overrides.push(("SIDER_AOF_COMPACT_AFTER_BYTES", "0".into()));
                    }
                    let server = SiderProcess::try_start_configured(
                        package.binary(),
                        env!("CARGO_PKG_VERSION"),
                        &overrides,
                    )
                    .unwrap();
                    let memory_before = rss(server.id());
                    let address = server.address();
                    let barrier = Arc::new(Barrier::new(CLIENTS + 1));
                    let mut clients = Vec::new();
                    for client in 0..CLIENTS {
                        let barrier = barrier.clone();
                        clients.push(std::thread::spawn(move || {
                            let mut stream = connection(address);
                            let mut expected = BTreeMap::<Vec<u8>, i64>::new();
                            let mut random = SEED ^ client as u64;
                            let mut batches = Vec::new();
                            for _ in 0..OPERATIONS / pipeline {
                                let mut bytes = Vec::new();
                                for _ in 0..pipeline {
                                    random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                                    let key = if hot { b"hot".to_vec() } else { format!("client-{client}:key-{}", random % 256).into_bytes() };
                                    *expected.entry(key.clone()).or_default() += 1;
                                    bytes.extend(wire::request(&[b"INCR".to_vec(), key]));
                                }
                                batches.push(bytes);
                            }
                            let mut latencies = Vec::new();
                            barrier.wait();
                            let client_began = Instant::now();
                            for bytes in batches {
                                let began = Instant::now();
                                stream.write_all(&bytes).unwrap();
                                for _ in 0..pipeline {
                                    assert!(matches!(wire::read_response(&mut stream).unwrap().value, Response::Integer(value) if value > 0));
                                }
                                latencies.push(began.elapsed().as_secs_f64() * 1000.0);
                            }
                            (expected, latencies, client_began, Instant::now())
                        }));
                    }
                    barrier.wait();
                    let mut expected = BTreeMap::<Vec<u8>, i64>::new();
                    let mut latencies = Vec::new();
                    let mut began = None;
                    let mut ended = None;
                    for client in clients {
                        let (counts, samples, start, finish) = client.join().unwrap();
                        began = Some(began.map_or(start, |previous: Instant| previous.min(start)));
                        ended =
                            Some(ended.map_or(finish, |previous: Instant| previous.max(finish)));
                        for (key, count) in counts {
                            *expected.entry(key).or_default() += count;
                        }
                        latencies.extend(samples);
                    }
                    let seconds = ended.unwrap().duration_since(began.unwrap()).as_secs_f64();
                    let memory_after = rss(server.id());
                    let mut stream = connection(address);
                    for (key, count) in &expected {
                        stream
                            .write_all(&wire::request(&[b"GET".to_vec(), key.clone()]))
                            .unwrap();
                        assert_eq!(
                            wire::read_response(&mut stream).unwrap().value,
                            Response::Bulk(Some(count.to_string().into_bytes()))
                        );
                    }
                    latencies.sort_by(f64::total_cmp);
                    let percentile =
                        |q: f64| latencies[((latencies.len() - 1) as f64 * q) as usize];
                    rows.push(json!({"shards":shards,"hot_key":hot,"pipeline":pipeline,"aof":durable,"sync":"always",
                        "operations":CLIENTS*OPERATIONS,"seconds":seconds,"operations_per_second":(CLIENTS*OPERATIONS) as f64 / seconds,
                        "verified_keys":expected.len(),"batch_rtt_ms":{"p50":percentile(0.5),"p95":percentile(0.95),"p99":percentile(0.99)},
                        "rss_before_bytes":memory_before,"rss_after_bytes":memory_after,"raw_batch_rtt_ms":latencies}));
                    drop(stream);
                    server.finish();
                    // Diretório exclusivo criado acima, nunca reutilizado como fonte de dados.
                    fs::remove_dir_all(&directory).unwrap();
                }
            }
        }
    }
    assert_eq!(
        output(
            Command::new("git")
                .current_dir(root)
                .args(["rev-parse", "HEAD"])
        ),
        sha
    );
    package.verify_again(&context).unwrap();
    assert!(
        output(Command::new("git").current_dir(root).args([
            "status",
            "--porcelain=v1",
            "--untracked-files=normal"
        ]))
        .is_empty()
    );
    let result = json!({"task":"R04-05","sha":sha,"version":env!("CARGO_PKG_VERSION"),"compiler":compiler,"hardware":hardware,
        "os":std::env::consts::OS,"arch":std::env::consts::ARCH,"seed":SEED,"clients":CLIENTS,"rows":rows,
        "limitations":["loopback local; clientes compartilham a máquina", "RSS amostrado antes/depois, não pico", "RTT por lote, não latência individual de cada comando", "sem aquecimento; ensaio exploratório"]});
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    file.write_all(&serde_json::to_vec_pretty(&result).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    eprintln!(
        "R04-05: 16 cenários e {} operações verificadas; resultados {}",
        16 * CLIENTS * OPERATIONS,
        path.display()
    );
}
