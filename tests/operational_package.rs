//! Exercícios operacionais opt-in dos quatro executáveis extraídos, sem recibo de release.
#![forbid(unsafe_code)]
#[path = "common/process.rs"]
mod process;
#[path = "common/sider_process.rs"]
mod sider_process;
#[path = "common/wire.rs"]
mod wire;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sider_process::SiderProcess;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    sync::mpsc,
    time::{Duration, Instant},
};
use wire::Response;

const TIMEOUT: Duration = Duration::from_secs(15);
const TOOLS: [&str; 4] = [
    "sider",
    "sider-aof-migrate",
    "sider-backup",
    "sider-replica",
];

fn command(binary: &Path) -> Command {
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
    command
}
fn digest(path: &Path) -> String {
    let metadata = fs::symlink_metadata(path).unwrap();
    assert!(metadata.is_file() && !metadata.file_type().is_symlink());
    assert!(metadata.len() > 0 && metadata.len() <= 128 * 1024 * 1024);
    let mut file = File::open(path).unwrap();
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    format!("{:x}", hash.finalize())
}
fn inventory(directory: &Path) -> Value {
    let mut tools = Vec::new();
    for name in TOOLS {
        let path = directory.join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
        let sha256 = digest(&path);
        let help = process::run(command(&path).arg("--help"), TIMEOUT).unwrap();
        assert!(help.status.success(), "{name}: {:?}", help.stderr);
        let text = String::from_utf8(help.stdout).unwrap();
        assert!(text.contains(&format!("Uso: {name}")));
        let version = if matches!(name, "sider" | "sider-backup") {
            let result = process::run(command(&path).arg("--version"), TIMEOUT).unwrap();
            assert!(result.status.success());
            let text = String::from_utf8(result.stdout).unwrap();
            assert_eq!(text.trim(), format!("{name} {}", env!("CARGO_PKG_VERSION")));
            Some(text.trim().to_owned())
        } else {
            None
        };
        tools.push(
            json!({"name":name,"sha256":sha256,"bytes":fs::metadata(path).unwrap().len(),
            "help_checked":true,"version_output":version}),
        );
    }
    json!(tools)
}
fn connect(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, TIMEOUT).unwrap();
    stream.set_nodelay(true).unwrap();
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(TIMEOUT)).unwrap();
    stream
}
fn exchange(stream: &mut TcpStream, args: &[&[u8]]) -> Response {
    stream
        .write_all(&wire::request(
            &args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>(),
        ))
        .unwrap();
    wire::read_response(stream).unwrap().value
}
fn bulk(bytes: &[u8]) -> Response {
    Response::Bulk(Some(bytes.to_vec()))
}
fn fields(bytes: &[u8]) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    for line in std::str::from_utf8(bytes).unwrap().lines() {
        if let Some((key, value)) = line.split_once(':') {
            assert!(
                fields.insert(key.to_owned(), value.to_owned()).is_none(),
                "campo duplicado"
            );
        }
    }
    fields
}
fn info(stream: &mut TcpStream) -> BTreeMap<String, String> {
    let Response::Bulk(Some(bytes)) = exchange(stream, &[b"INFO"]) else {
        panic!("INFO ausente")
    };
    assert!(!bytes.windows(6).any(|part| part == b"secret"));
    fields(&bytes)
}
fn metric(fields: &BTreeMap<String, String>, name: &str) -> u64 {
    fields[name].parse().unwrap()
}
fn await_metric(
    stream: &mut TcpStream,
    name: &str,
    predicate: impl Fn(u64) -> bool,
) -> BTreeMap<String, String> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let current = info(stream);
        if predicate(metric(&current, name)) {
            return current;
        }
        assert!(
            Instant::now() < deadline,
            "indicador {name} não convergiu: {current:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn start(binary: &Path, overrides: &[(&str, OsString)]) -> SiderProcess {
    SiderProcess::try_start_configured(binary, env!("CARGO_PKG_VERSION"), overrides).unwrap()
}

fn configuration(binary: &Path, directory: &Path) -> Value {
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let ready = directory.join("diagnostic-ready.json");
    let aof = directory.join("secret-aof-not-created");
    let result = process::run(
        command(binary)
            .arg("--diagnose")
            .env("SIDER_ADDR", occupied.local_addr().unwrap().to_string())
            .env(
                "SIDER_REPLICATION_ADDR",
                occupied.local_addr().unwrap().to_string(),
            )
            .env("SIDER_AOF_DIR", &aof)
            .env("SIDER_READY_FILE", &ready)
            .env("SIDER_REPLICATION_READY_FILE", &ready),
        TIMEOUT,
    )
    .unwrap();
    assert!(result.status.success(), "{:?}", result.stderr);
    let effective = fields(&result.stdout);
    assert_eq!(effective["diagnostic_scope"], "configuration_only");
    assert_eq!(effective["aof_configured"], "1");
    assert_eq!(effective["replication_configured"], "1");
    assert!(!String::from_utf8_lossy(&result.stdout).contains("secret"));
    assert!(!ready.exists() && !aof.exists());
    let invalid = process::run(
        command(binary)
            .arg("--diagnose")
            .env("SIDER_MAX_CONNECTIONS", "secret-invalid-value"),
        TIMEOUT,
    )
    .unwrap();
    assert!(!invalid.status.success() && invalid.stdout.is_empty());
    let diagnostic = String::from_utf8(invalid.stderr).unwrap();
    assert!(diagnostic.contains("SIDER_MAX_CONNECTIONS") && !diagnostic.contains("secret"));
    json!({"case":"configuration","status":"passed","offline_exit_code":result.status.code(),
        "invalid_exit_code":invalid.status.code(),"invalid_diagnostic":diagnostic.trim(),
        "occupied_listener_preserved":occupied.local_addr().is_ok(),"aof_and_readiness_absent":true,
        "effective_configuration":effective})
}
fn quota(binary: &Path) -> Value {
    let server = start(
        binary,
        &[
            ("SIDER_SHARDS", "4".into()),
            ("SIDER_MAX_DATASET_BYTES", "4096".into()),
        ],
    );
    let mut client = connect(server.address());
    let key = b"{operational}:secret-key";
    let original = b"\0\xffsecret-value";
    assert_eq!(
        exchange(&mut client, &[b"SET", key, original]),
        Response::Simple(b"OK".to_vec())
    );
    let before = info(&mut client);
    assert_eq!(metric(&before, "dataset_keys"), 1);
    assert_eq!(metric(&before, "dataset_quota_bytes"), 4096);
    let Response::Error(rejection) = exchange(&mut client, &[b"SET", key, &vec![7; 8192]]) else {
        panic!("quota não recusou")
    };
    assert!(rejection.starts_with(b"OOM"));
    assert_eq!(exchange(&mut client, &[b"GET", key]), bulk(original));
    let refused = info(&mut client);
    assert_eq!(
        refused["dataset_logical_bytes"],
        before["dataset_logical_bytes"]
    );
    assert_eq!(metric(&refused, "dataset_keys"), 1);
    assert!(
        metric(&refused, "command_error_replies_total")
            > metric(&before, "command_error_replies_total")
    );
    assert_eq!(exchange(&mut client, &[b"DEL", key]), Response::Integer(1));
    let released = info(&mut client);
    assert_eq!(metric(&released, "dataset_logical_bytes"), 0);
    assert_eq!(metric(&released, "dataset_keys"), 0);
    assert_eq!(
        exchange(&mut client, &[b"SET", key, original]),
        Response::Simple(b"OK".to_vec())
    );
    assert_eq!(
        exchange(&mut client, &[b"PING"]),
        Response::Simple(b"PONG".to_vec())
    );
    drop(client);
    server.finish();
    json!({"case":"quota","status":"passed","shards":4,"quota_bytes":4096,
        "used_before":metric(&before,"dataset_logical_bytes"),"used_after_rejection":metric(&refused,"dataset_logical_bytes"),
        "used_after_delete":0,"original_preserved":true,"capacity_reused":true})
}
fn connection_limit(binary: &Path) -> Value {
    let server = start(binary, &[("SIDER_MAX_CONNECTIONS", "4".into())]);
    let mut control = connect(server.address());
    assert_eq!(
        exchange(&mut control, &[b"PING"]),
        Response::Simple(b"PONG".to_vec())
    );
    await_metric(&mut control, "connected_clients", |count| count == 1);
    let mut admitted = Vec::new();
    for _ in 0..3 {
        let mut client = connect(server.address());
        assert_eq!(
            exchange(&mut client, &[b"PING"]),
            Response::Simple(b"PONG".to_vec())
        );
        admitted.push(client);
    }
    let full = info(&mut control);
    assert_eq!(metric(&full, "connected_clients"), 4);
    let before = metric(&full, "rejected_connections");
    let mut excess = connect(server.address());
    let mut byte = [0];
    match excess.read(&mut byte) {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            ) => {}
        other => panic!("conexão excedente não foi rejeitada: {other:?}"),
    }
    let refused = await_metric(&mut control, "rejected_connections", |count| count > before);
    assert_eq!(metric(&refused, "connected_clients"), 4);
    drop(admitted.pop());
    await_metric(&mut control, "connected_clients", |count| count == 3);
    let mut replacement = connect(server.address());
    assert_eq!(
        exchange(&mut replacement, &[b"PING"]),
        Response::Simple(b"PONG".to_vec())
    );
    assert_eq!(metric(&info(&mut control), "connected_clients"), 4);
    drop((excess, replacement, admitted, control));
    server.finish();
    json!({"case":"connection_limit","status":"passed","capacity":4,
        "rejected_delta":metric(&refused,"rejected_connections")-before,"released_slot_reused":true})
}
fn slow_client(binary: &Path) -> Value {
    let server = start(
        binary,
        &[
            ("SIDER_PUBSUB_QUEUE_CAPACITY", "1".into()),
            ("SIDER_WRITE_TIMEOUT_MS", "2000".into()),
        ],
    );
    let mut publisher = connect(server.address());
    let before = metric(&info(&mut publisher), "pubsub_evictions_total");
    let channel = b"operational:\0\xff";
    let mut slow = connect(server.address());
    let mut fast = connect(server.address());
    for stream in [&mut slow, &mut fast] {
        assert_eq!(
            exchange(stream, &[b"SUBSCRIBE", channel]),
            Response::Array(Some(vec![
                bulk(b"subscribe"),
                bulk(channel),
                Response::Integer(1)
            ]))
        );
    }
    let (expect, expected) = mpsc::sync_channel::<u64>(1);
    let (acknowledge, acknowledged) = mpsc::sync_channel(1);
    let mut delivered = 0;
    let final_fields = std::thread::scope(|scope| {
        let reader = scope.spawn(move || {
            for sequence in expected {
                let Response::Array(Some(parts)) = wire::read_response(&mut fast).unwrap().value
                else {
                    panic!("notificação ausente")
                };
                assert_eq!(parts[0], bulk(b"message"));
                assert_eq!(parts[1], bulk(channel));
                let Response::Bulk(Some(payload)) = &parts[2] else {
                    panic!("payload ausente")
                };
                assert_eq!(payload.len(), 65536);
                assert_eq!(&payload[..8], &sequence.to_le_bytes());
                acknowledge.send(()).unwrap();
            }
        });
        let mut observed = None;
        for sequence in 0..512u64 {
            let mut payload = vec![0xff; 65536];
            payload[..8].copy_from_slice(&sequence.to_le_bytes());
            expect.send(sequence).unwrap();
            assert!(matches!(
                exchange(&mut publisher, &[b"PUBLISH", channel, &payload]),
                Response::Integer(1 | 2)
            ));
            acknowledged.recv_timeout(TIMEOUT).unwrap();
            delivered += 1;
            let current = info(&mut publisher);
            if metric(&current, "pubsub_evictions_total") > before {
                assert_eq!(metric(&current, "pubsub_subscribers"), 1);
                assert_eq!(
                    exchange(&mut publisher, &[b"PING"]),
                    Response::Simple(b"PONG".to_vec())
                );
                observed = Some(current);
                break;
            }
        }
        drop(expect);
        reader.join().unwrap();
        observed.expect("assinante lento não foi removido dentro da carga limitada")
    });
    drop(slow);
    await_metric(&mut publisher, "pubsub_subscriptions", |count| count == 0);
    drop(publisher);
    server.finish();
    json!({"case":"slow_client","status":"passed","messages_confirmed_by_fast_client":delivered,
        "payload_bytes":65536,"queue_capacity":1,"evictions_delta":metric(&final_fields,"pubsub_evictions_total")-before,
        "other_client_progress":true,"subscriptions_drained":true})
}
fn filesystem(binary: &Path, directory: &Path) -> Value {
    let blocked = directory.join("aof-parent-blocked");
    let sentinel = b"owned obstruction; not a database";
    fs::write(&blocked, sentinel).unwrap();
    let data = blocked.join("database");
    let ready = directory.join("blocked-ready.json");
    let failure = process::run(
        command(binary)
            .env("SIDER_ADDR", "127.0.0.1:0")
            .env("SIDER_AOF_DIR", &data)
            .env("SIDER_READY_FILE", &ready),
        TIMEOUT,
    )
    .unwrap();
    assert!(!failure.status.success() && !failure.stderr.is_empty());
    assert!(!ready.exists() && !data.exists());
    assert_eq!(fs::read(&blocked).unwrap(), sentinel);
    let diagnostic = String::from_utf8(failure.stderr).unwrap();
    fs::remove_file(&blocked).unwrap();
    fs::create_dir(&blocked).unwrap();
    let overrides = [
        ("SIDER_AOF_DIR", data.as_os_str().to_owned()),
        ("SIDER_AOF_SYNC", "always".into()),
        ("SIDER_AOF_COMPACT_AFTER_BYTES", "0".into()),
    ];
    let server = start(binary, &overrides);
    let mut client = connect(server.address());
    let key = b"operational:\0\xff";
    let value = b"confirmed:\xff\0";
    assert_eq!(
        exchange(&mut client, &[b"SET", key, value]),
        Response::Simple(b"OK".to_vec())
    );
    let before = info(&mut client);
    assert_eq!(metric(&before, "aof_failed"), 0);
    assert!(metric(&before, "aof_written_sequence") > 0);
    assert_eq!(
        before["aof_written_sequence"],
        before["aof_synced_sequence"]
    );
    let competing_ready = directory.join("competing-ready.json");
    let competing = process::run(
        command(binary)
            .env("SIDER_ADDR", "127.0.0.1:0")
            .env("SIDER_AOF_DIR", &data)
            .env("SIDER_READY_FILE", &competing_ready),
        TIMEOUT,
    )
    .unwrap();
    assert!(!competing.status.success() && !competing.stderr.is_empty());
    assert!(!competing_ready.exists());
    assert_eq!(exchange(&mut client, &[b"GET", key]), bulk(value));
    assert_eq!(
        exchange(&mut client, &[b"PING"]),
        Response::Simple(b"PONG".to_vec())
    );
    drop(client);
    server.finish();
    let recovered = start(binary, &overrides);
    let mut client = connect(recovered.address());
    assert_eq!(exchange(&mut client, &[b"GET", key]), bulk(value));
    assert_eq!(
        exchange(&mut client, &[b"PING"]),
        Response::Simple(b"PONG".to_vec())
    );
    let after = info(&mut client);
    assert_eq!(
        after["aof_written_sequence"],
        before["aof_written_sequence"]
    );
    assert_eq!(metric(&after, "aof_failed"), 0);
    drop(client);
    recovered.finish();
    json!({"case":"filesystem_open_and_recovery","status":"passed",
        "failure_boundary":"open_aof_path_with_regular_file_parent","failure_exit_code":failure.status.code(),
        "failure_diagnostic":diagnostic.trim(),"obstruction_preserved":true,"readiness_absent_on_failure":true,
        "competing_instance_exit_code":competing.status.code(),
        "competing_instance_diagnostic":String::from_utf8(competing.stderr).unwrap().trim(),
        "first_instance_remained_available":true,"confirmed_binary_value_recovered":true,
        "synced_sequence_before_restart":metric(&before,"aof_synced_sequence"),
        "sequence_after_restart":metric(&after,"aof_written_sequence")})
}

#[test]
#[ignore = "opt-in: quatro CLIs extraídas em SIDER_OPERATIONAL_PACKAGE_DIR e saída nova em SIDER_OPERATIONAL_OUTPUT_DIR"]
fn operational_extracted_package() {
    let package =
        PathBuf::from(std::env::var_os("SIDER_OPERATIONAL_PACKAGE_DIR").expect("pacote explícito"));
    let output =
        PathBuf::from(std::env::var_os("SIDER_OPERATIONAL_OUTPUT_DIR").expect("saída nova"));
    assert!(package.is_absolute() && output.is_absolute());
    let metadata = fs::symlink_metadata(&package).unwrap();
    assert!(metadata.is_dir() && !metadata.file_type().is_symlink());
    let package = package.canonicalize().unwrap();
    fs::create_dir(&output).expect("não sobrescrever evidências anteriores");
    let before = inventory(&package);
    let binary = package.join(format!("sider{}", std::env::consts::EXE_SUFFIX));
    let began = Instant::now();
    let mut cases = Vec::new();
    let mut observations = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output.join("observed-cases.jsonl"))
        .unwrap();
    let mut record = |case| {
        writeln!(observations, "{case}").unwrap();
        observations.flush().unwrap();
        cases.push(case);
    };
    record(configuration(&binary, &output));
    record(quota(&binary));
    record(connection_limit(&binary));
    record(slow_client(&binary));
    record(filesystem(&binary, &output));
    assert_eq!(
        inventory(&package),
        before,
        "executáveis mudaram durante o ensaio"
    );
    observations.sync_all().unwrap();
    let report = json!({"schema_version":1,"task":"R10-05","version":env!("CARGO_PKG_VERSION"),
        "platform":std::env::consts::OS,"architecture":std::env::consts::ARCH,
        "duration_seconds":began.elapsed().as_secs_f64(),"executables":before,"cases":cases,
        "scope":"diagnóstico, quota, limite de conexões, isolamento de cliente lento, abertura AOF e recuperação",
        "limitations":["não comprova identidade com manifesto de release; conferir pacote externamente",
            "obstrução de caminho não simula ENOSPC nem falha de write em arquivo já aberto",
            "falha de write/atomicidade permanece coberta pelas suítes nativas de injeção",
            "migração, backup/restauração e shutdown cooperativo pertencem ao ensaio de baseline"],
        "cleanup_confirmed":true,"release_gate_approved":false});
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output.join("operational-report.json"))
        .unwrap();
    serde_json::to_writer_pretty(&mut file, &report).unwrap();
    file.sync_all().unwrap();
    eprintln!("{report}");
}
