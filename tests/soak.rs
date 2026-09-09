//! Carga prolongada com modelo independente, processos reais e falhas planejadas.
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

use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::Write,
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use sider_process::SiderProcess;
use wire::Response;

const TIMEOUT: Duration = Duration::from_secs(15);
const SLOTS: usize = 8;
const SEED: u64 = 0x511e_1103;
const QUOTA: u64 = 4 * 1024 * 1024;
const RSS_ENVELOPE: u64 = 512 * 1024 * 1024;
const MESSAGES: usize = 128;

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

fn info(address: SocketAddr) -> BTreeMap<String, String> {
    let Response::Bulk(Some(bytes)) = exchange(&mut connect(address), &[b"INFO", b"all"]) else {
        panic!("INFO deve retornar bulk string");
    };
    let mut fields = BTreeMap::new();
    for line in std::str::from_utf8(&bytes).unwrap().split("\r\n") {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line.split_once(':').unwrap();
        assert!(
            fields.insert(key.to_owned(), value.to_owned()).is_none(),
            "métrica duplicada"
        );
    }
    fields
}

fn metric(fields: &BTreeMap<String, String>, key: &str) -> u64 {
    fields
        .get(key)
        .unwrap_or_else(|| panic!("métrica ausente: {key}"))
        .parse()
        .unwrap()
}

fn bulk(value: &[u8]) -> Response {
    Response::Bulk(Some(value.to_vec()))
}

fn key(slot: usize, kind: &str) -> Vec<u8> {
    format!("{{soak-{slot}}}:{kind}").into_bytes()
}

fn payload(slot: usize, value: u64) -> Vec<u8> {
    let mut bytes = vec![0, 255, slot as u8];
    bytes.extend_from_slice(&(value ^ SEED).to_le_bytes());
    bytes
}

fn mutate(stream: &mut TcpStream, slot: usize, value: u64) {
    let number = value.to_string();
    let data = payload(slot, value);
    let counter = key(slot, "counter");
    let peer = key(slot, "peer");
    let hash = key(slot, "hash");
    let list = key(slot, "list");
    let set = key(slot, "set");
    let sorted = key(slot, "sorted");
    assert_eq!(
        exchange(stream, &[b"MULTI"]),
        Response::Simple(b"OK".to_vec())
    );
    for command in [
        vec![b"SET".as_slice(), counter.as_slice(), number.as_bytes()],
        vec![b"SET".as_slice(), peer.as_slice(), number.as_bytes()],
        vec![
            b"HSET".as_slice(),
            hash.as_slice(),
            b"field",
            data.as_slice(),
        ],
        vec![b"DEL".as_slice(), list.as_slice()],
        vec![
            b"RPUSH".as_slice(),
            list.as_slice(),
            data.as_slice(),
            b"tail",
        ],
        vec![b"DEL".as_slice(), set.as_slice()],
        vec![
            b"SADD".as_slice(),
            set.as_slice(),
            data.as_slice(),
            b"fixed",
        ],
        vec![
            b"ZADD".as_slice(),
            sorted.as_slice(),
            number.as_bytes(),
            b"member",
        ],
    ] {
        assert_eq!(
            exchange(stream, &command),
            Response::Simple(b"QUEUED".to_vec())
        );
    }
    let Response::Array(Some(replies)) = exchange(stream, &[b"EXEC"]) else {
        panic!("EXEC não confirmou o lote");
    };
    assert_eq!(replies.len(), 8);
    assert!(
        replies
            .iter()
            .all(|reply| !matches!(reply, Response::Error(_)))
    );
}

fn verify_slot(stream: &mut TcpStream, slot: usize, value: u64) {
    let number = value.to_string();
    let data = payload(slot, value);
    assert_eq!(
        exchange(
            stream,
            &[b"MGET", &key(slot, "counter"), &key(slot, "peer")]
        ),
        Response::Array(Some(vec![bulk(number.as_bytes()), bulk(number.as_bytes())])),
        "meio lote ou confirmação perdida"
    );
    assert_eq!(
        exchange(stream, &[b"HGET", &key(slot, "hash"), b"field"]),
        bulk(&data)
    );
    assert_eq!(
        exchange(stream, &[b"LRANGE", &key(slot, "list"), b"0", b"-1"]),
        Response::Array(Some(vec![bulk(&data), bulk(b"tail")]))
    );
    assert_eq!(
        exchange(stream, &[b"SCARD", &key(slot, "set")]),
        Response::Integer(2)
    );
    assert_eq!(
        exchange(stream, &[b"SISMEMBER", &key(slot, "set"), &data]),
        Response::Integer(1)
    );
    assert_eq!(
        exchange(stream, &[b"SISMEMBER", &key(slot, "set"), b"fixed"]),
        Response::Integer(1)
    );
    assert_eq!(
        exchange(
            stream,
            &[b"ZRANGE", &key(slot, "sorted"), b"0", b"-1", b"WITHSCORES"]
        ),
        Response::Array(Some(vec![bulk(b"member"), bulk(number.as_bytes())]))
    );
}

fn verify_all(address: SocketAddr, model: &[u64; SLOTS]) {
    let mut stream = connect(address);
    for (slot, value) in model.iter().enumerate() {
        verify_slot(&mut stream, slot, *value);
    }
}

fn await_replica(address: SocketAddr, model: &[u64; SLOTS]) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let mut stream = connect(address);
        let matches = model.iter().enumerate().all(|(slot, value)| {
            exchange(&mut stream, &[b"GET", &key(slot, "counter")])
                == bulk(value.to_string().as_bytes())
        });
        if matches {
            verify_all(address, model);
            let fields = info(address);
            assert_eq!(
                fields.get("replication_role").map(String::as_str),
                Some("replica")
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "réplica não convergiu dentro do prazo"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn watch_abort(address: SocketAddr) {
    let mut watched = connect(address);
    let mut competitor = connect(address);
    assert_eq!(
        exchange(&mut watched, &[b"WATCH", b"{watch}observed"]),
        Response::Simple(b"OK".to_vec())
    );
    assert_eq!(
        exchange(&mut competitor, &[b"INCR", b"{watch}observed"]),
        Response::Integer(1)
    );
    assert_eq!(
        exchange(&mut watched, &[b"MULTI"]),
        Response::Simple(b"OK".to_vec())
    );
    assert_eq!(
        exchange(
            &mut watched,
            &[b"SET", b"{watch}aborted", b"must-not-exist"]
        ),
        Response::Simple(b"QUEUED".to_vec())
    );
    assert_eq!(exchange(&mut watched, &[b"EXEC"]), Response::Array(None));
    assert_eq!(
        exchange(&mut competitor, &[b"GET", b"{watch}aborted"]),
        Response::Bulk(None)
    );
    assert_eq!(
        exchange(&mut competitor, &[b"DEL", b"{watch}observed"]),
        Response::Integer(1)
    );
}

fn expiration(address: SocketAddr, replica: SocketAddr) {
    let mut stream = connect(address);
    assert_eq!(
        exchange(
            &mut stream,
            &[b"SET", b"{ttl}temporary", b"binary\0\xff", b"PX", b"50"]
        ),
        Response::Simple(b"OK".to_vec())
    );
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if exchange(&mut stream, &[b"GET", b"{ttl}temporary"]) == Response::Bulk(None)
            && exchange(&mut connect(replica), &[b"GET", b"{ttl}temporary"]) == Response::Bulk(None)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "TTL não venceu nos dois processos"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn slow_subscriber(address: SocketAddr) {
    let before = metric(&info(address), "pubsub_evictions_total");
    let channel = b"soak-channel";
    let mut slow = connect(address);
    let mut fast = connect(address);
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
    std::thread::scope(|scope| {
        let reader = scope.spawn(move || {
            for sequence in 0..MESSAGES {
                let Response::Array(Some(parts)) = wire::read_response(&mut fast).unwrap().value
                else {
                    panic!("notificação inválida");
                };
                assert_eq!(parts[0], bulk(b"message"));
                assert_eq!(parts[1], bulk(channel));
                let Response::Bulk(Some(bytes)) = &parts[2] else {
                    panic!("payload ausente")
                };
                assert_eq!(bytes.len(), 65536);
                assert_eq!(&bytes[..8], &(sequence as u64).to_le_bytes());
            }
        });
        let mut publisher = connect(address);
        for sequence in 0..MESSAGES {
            let mut bytes = vec![0xff; 65536];
            bytes[..8].copy_from_slice(&(sequence as u64).to_le_bytes());
            assert!(matches!(
                exchange(&mut publisher, &[b"PUBLISH", channel, &bytes]),
                Response::Integer(1 | 2)
            ));
            std::thread::sleep(Duration::from_millis(2));
        }
        reader.join().unwrap();
    });
    assert!(
        metric(&info(address), "pubsub_evictions_total") > before,
        "assinante lento não foi expulso pela fila limitada"
    );
    drop(slow);
}

struct Instance {
    process: SiderProcess,
    internal: SocketAddr,
}

fn start(
    binary: &Path,
    output: &Path,
    name: &str,
    epoch: u64,
    upstream: Option<SocketAddr>,
) -> Instance {
    let data = output.join(format!("{name}-data"));
    let ready = output.join(format!("{name}-internal-{epoch}.json"));
    let mut overrides = vec![
        ("SIDER_SHARDS", OsString::from("4")),
        ("SIDER_MAX_DATASET_BYTES", OsString::from(QUOTA.to_string())),
        ("SIDER_AOF_DIR", data.into_os_string()),
        ("SIDER_AOF_SYNC", OsString::from("always")),
        ("SIDER_AOF_COMPACT_AFTER_BYTES", OsString::from("131072")),
        ("SIDER_REPLICATION_ADDR", OsString::from("127.0.0.1:0")),
        (
            "SIDER_REPLICATION_READY_FILE",
            ready.clone().into_os_string(),
        ),
        ("SIDER_PUBSUB_QUEUE_CAPACITY", OsString::from("8")),
        ("SIDER_WRITE_TIMEOUT_MS", OsString::from("1000")),
    ];
    if let Some(upstream) = upstream {
        overrides.push(("SIDER_REPLICA_OF", OsString::from(upstream.to_string())));
    }
    let process =
        SiderProcess::try_start_configured(binary, env!("CARGO_PKG_VERSION"), &overrides).unwrap();
    let deadline = Instant::now() + TIMEOUT;
    let observed: Value = loop {
        if ready.exists() {
            break serde_json::from_slice(&fs::read(&ready).unwrap()).unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "listener interno não publicou prontidão"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(observed["pid"].as_u64(), Some(u64::from(process.id())));
    assert_eq!(observed["host"], "127.0.0.1");
    let port = u16::try_from(observed["port"].as_u64().unwrap()).unwrap();
    assert_ne!(port, 0);
    Instance {
        process,
        internal: SocketAddr::from(([127, 0, 0, 1], port)),
    }
}

fn rss(pid: u32) -> u64 {
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

fn observe(instance: &Instance) -> Value {
    let fields = info(instance.process.address());
    let used = metric(&fields, "dataset_logical_bytes");
    let quota = metric(&fields, "dataset_quota_bytes");
    assert_eq!(quota, QUOTA);
    assert!(used <= quota);
    assert!(metric(&fields, "worker_queue_used") <= metric(&fields, "worker_queue_capacity"));
    assert_eq!(metric(&fields, "worker_failures_total"), 0);
    assert_eq!(metric(&fields, "aof_fatal_failures_total"), 0);
    let memory = rss(instance.process.id());
    assert!(
        memory <= RSS_ENVELOPE,
        "RSS excedeu envelope explícito do ensaio"
    );
    json!({ "pid": instance.process.id(), "rss_bytes": memory, "info": fields })
}

fn exercise(binary: &Path, output: &Path, duration: Duration, rehearsal: bool) -> Value {
    if !cfg!(target_os = "linux") {
        panic!("soak exige processo Linux com /proc");
    }
    assert!(binary.is_absolute() && output.is_absolute());
    assert!(duration >= Duration::from_secs(20));
    fs::create_dir(output).expect("evidências exigem diretório novo");
    let mut samples = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output.join("soak-samples.jsonl"))
        .unwrap();
    let mut primary_epoch = 0;
    let mut replica_epoch = 0;
    let mut primary = start(binary, output, "primary", primary_epoch, None);
    let mut writer = connect(primary.process.address());
    let mut model = [0; SLOTS];
    for slot in 0..SLOTS {
        mutate(&mut writer, slot, 0);
    }
    let mut replica = start(
        binary,
        output,
        "replica",
        replica_epoch,
        Some(primary.internal),
    );
    await_replica(replica.process.address(), &model);
    let began = Instant::now();
    let deadline = began + duration;
    let mut next_sample = began;
    let event_period = if rehearsal {
        Duration::from_secs(5)
    } else {
        Duration::from_secs(60)
    };
    let restart_period = if rehearsal {
        Duration::from_secs(8)
    } else {
        Duration::from_secs(300)
    };
    let mut next_event = began;
    let mut next_restart = began + restart_period;
    let mut rounds = 0u64;
    let mut checks = 0u64;
    let mut ttl_checks = 0;
    let mut pubsub_checks = 0;
    let mut watch_checks = 0;
    let mut samples_count = 0;
    let mut primary_crashes = 0;
    let mut replica_crashes = 0;
    let mut full_syncs = 0;
    let mut partial_syncs = 0;
    let mut last_progress = began;
    while Instant::now() < deadline {
        let tick = Instant::now();
        let slot = rounds as usize % SLOTS;
        model[slot] = model[slot].checked_add(1).unwrap();
        mutate(&mut writer, slot, model[slot]);
        verify_slot(&mut writer, slot, model[slot]);
        rounds += 1;
        checks += 7;
        if tick >= next_sample {
            await_replica(replica.process.address(), &model);
            let sample = json!({"elapsed_ms": began.elapsed().as_millis(), "rounds": rounds, "primary": observe(&primary), "replica": observe(&replica)});
            writeln!(samples, "{sample}").unwrap();
            samples.flush().unwrap();
            samples_count += 1;
            next_sample = Instant::now() + Duration::from_secs(1);
        }
        if tick >= next_event {
            watch_abort(primary.process.address());
            watch_checks += 1;
            expiration(primary.process.address(), replica.process.address());
            ttl_checks += 1;
            slow_subscriber(primary.process.address());
            pubsub_checks += 1;
            // Reconexão RESP não pode preservar MULTI/WATCH ou inscrições anteriores.
            writer = connect(primary.process.address());
            next_event = Instant::now() + event_period;
        }
        if tick >= next_restart {
            let fields = info(replica.process.address());
            full_syncs += metric(&fields, "replication_full_syncs_total");
            partial_syncs += metric(&fields, "replication_partial_syncs_total");
            replica.process.finish();
            replica_crashes += 1;
            replica_epoch += 1;
            if replica_crashes % 2 == 0 {
                drop(writer);
                primary.process.finish();
                primary_crashes += 1;
                primary_epoch += 1;
                primary = start(binary, output, "primary", primary_epoch, None);
                verify_all(primary.process.address(), &model);
                writer = connect(primary.process.address());
            } else {
                // Avança durante indisponibilidade da réplica para exigir retomada real.
                model[slot] += 1;
                mutate(&mut writer, slot, model[slot]);
                rounds += 1;
            }
            replica = start(
                binary,
                output,
                "replica",
                replica_epoch,
                Some(primary.internal),
            );
            await_replica(replica.process.address(), &model);
            next_restart = Instant::now() + restart_period;
        }
        primary.process.assert_alive();
        replica.process.assert_alive();
        if last_progress.elapsed() >= Duration::from_secs(60) {
            eprintln!(
                "soak: {} s; {} lotes; {} verificações; crashes primário/réplica {primary_crashes}/{replica_crashes}",
                began.elapsed().as_secs(),
                rounds,
                checks
            );
            last_progress = Instant::now();
        }
        // Limite declarado de 20 iterações/s: carga contínua e dataset limitado.
        if let Some(remaining) = Duration::from_millis(50).checked_sub(tick.elapsed()) {
            std::thread::sleep(remaining);
        }
    }
    verify_all(primary.process.address(), &model);
    await_replica(replica.process.address(), &model);
    let final_primary = observe(&primary);
    let final_replica = observe(&replica);
    let replica_fields = info(replica.process.address());
    full_syncs += metric(&replica_fields, "replication_full_syncs_total");
    partial_syncs += metric(&replica_fields, "replication_partial_syncs_total");
    assert!(primary_crashes > 0 && replica_crashes > 1);
    assert!(
        full_syncs > 1 && partial_syncs > 0,
        "FULL e CONTINUE precisam acontecer"
    );
    assert!(ttl_checks > 0 && watch_checks > 0 && pubsub_checks > 0);
    assert!(
        rounds > duration.as_secs(),
        "progresso insuficiente no ensaio"
    );
    let elapsed = began.elapsed();
    replica.process.finish();
    primary.process.finish();
    samples.sync_all().unwrap();
    let report = json!({
        "schema_version": 1, "task": "R11-03", "rehearsal": rehearsal,
        "duration_seconds": elapsed.as_secs_f64(), "required_seconds": duration.as_secs(),
        "seed": SEED, "slots": SLOTS, "shards": 4, "quota_bytes": QUOTA,
        "rss_envelope_bytes": RSS_ENVELOPE, "target_iterations_per_second": 20,
        "durability": "always", "compaction_after_bytes": 131072,
        "rounds": rounds, "invariant_checks": checks, "samples": samples_count,
        "ttl_checks": ttl_checks, "watch_aborts": watch_checks, "slow_subscriber_checks": pubsub_checks,
        "primary_crashes": primary_crashes, "replica_crashes": replica_crashes,
        "full_syncs": full_syncs, "partial_syncs": partial_syncs,
        "final_primary": final_primary, "final_replica": final_replica,
        "scope": "modelo binário dos cinco tipos; EXEC; WATCH; TTL; clientes lentos; AOF/compactação; FULL/CONTINUE e crash/recovery",
        "crash_boundary": "processos interrompidos entre lotes confirmados; cortes durante append são cobertos pelo gate crash",
        "cleanup_confirmed": true
    });
    let mut file = File::create(output.join("soak-report.json")).unwrap();
    serde_json::to_writer_pretty(&mut file, &report).unwrap();
    file.sync_all().unwrap();
    report
}

#[test]
#[ignore = "ensaio curto explícito; exige binário com replicação e métricas, não aprova soak da release"]
fn internal_soak_rehearsal() {
    let binary = PathBuf::from(std::env::var_os("SIDER_SOAK_BINARY").expect("binário explícito"));
    let output = PathBuf::from(std::env::var_os("SIDER_SOAK_OUTPUT_DIR").expect("diretório novo"));
    let report = exercise(&binary, &output, Duration::from_secs(25), true);
    eprintln!("{report}");
}

#[test]
#[ignore = "gate exige build candidato, pacote extraído e carga real de pelo menos 3600 segundos"]
fn release_soak_gate() {
    let context = gate_receipt::GateContext::from_env("soak").expect("contexto exato da candidata");
    let input = release_input::ReleaseInput::from_env(&context).expect("pacote extraído conferido");
    let output = context.release_dir().join("soak");
    let began = Instant::now();
    let mut report = exercise(input.binary(), &output, Duration::from_secs(3600), false);
    input
        .verify_again(&context)
        .expect("pacote e executável permaneceram iguais");
    report["input"] = input.details();
    report["samples_sha256"] =
        json!(release_input::sha256(&output.join("soak-samples.jsonl")).unwrap());
    report["report_sha256"] =
        json!(release_input::sha256(&output.join("soak-report.json")).unwrap());
    assert!(report["duration_seconds"].as_f64().unwrap() >= 3600.0);
    context
        .publish(
            report["invariant_checks"].as_u64().unwrap(),
            began.elapsed(),
            report,
        )
        .expect("recibo apenas após soak completo e cleanup");
}
