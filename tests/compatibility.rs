//! Diferencial externo explícito: o codec do Sider não constrói o oráculo.

#![forbid(unsafe_code)]

#[path = "common/gate_receipt.rs"]
mod gate_receipt;
#[path = "common/process.rs"]
mod process;
#[path = "common/redis_reference.rs"]
mod redis_reference;
#[path = "common/resp_fixtures.rs"]
mod resp_fixtures;
#[path = "common/sider_process.rs"]
mod sider_process;
#[path = "common/wire.rs"]
mod wire;

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::path::Path;
use std::time::{Duration, Instant};

use gate_receipt::GateContext;
use redis_reference::RedisReference;
use serde_json::{Value, json};
use sider_process::SiderProcess;
use wire::{Observed, Response};

const SEEDS: [u64; 6] = [
    1,
    42,
    0x51de_0001,
    0xdead_beef,
    0x1234_5678_9abc_def0,
    u64::MAX,
];
const OPERATIONS: usize = 256;
const PIPELINE: usize = 16;
const IO_TIMEOUT: Duration = Duration::from_secs(5);

struct Pair {
    sider: TcpStream,
    redis: TcpStream,
    cases: u64,
}

impl Pair {
    fn new(sider: SocketAddr, redis: SocketAddr) -> Self {
        fn connect(address: SocketAddr) -> TcpStream {
            let stream = TcpStream::connect_timeout(&address, IO_TIMEOUT).unwrap();
            stream.set_read_timeout(Some(IO_TIMEOUT)).unwrap();
            stream.set_write_timeout(Some(IO_TIMEOUT)).unwrap();
            stream.set_nodelay(true).unwrap();
            stream
        }
        Self {
            sider: connect(sider),
            redis: connect(redis),
            cases: 0,
        }
    }

    fn send(&mut self, request: &[u8]) {
        self.sider.write_all(request).unwrap();
        self.redis.write_all(request).unwrap();
    }

    fn compare(&mut self, expected: Option<&[u8]>, context: &str) -> Observed {
        let sider = wire::read_response(&mut self.sider)
            .unwrap_or_else(|error| panic!("Sider {context}: {error}"));
        let redis = wire::read_response(&mut self.redis)
            .unwrap_or_else(|error| panic!("Redis {context}: {error}"));
        assert_eq!(sider.value, redis.value, "tipo/conteúdo: {context}");
        assert_eq!(sider.bytes, redis.bytes, "bytes RESP: {context}");
        if let Some(expected) = expected {
            assert_eq!(redis.bytes, expected, "referência literal: {context}");
        }
        self.cases += 1;
        sider
    }

    fn exchange(&mut self, args: &[Vec<u8>], context: &str) -> Observed {
        self.send(&wire::request(args));
        self.compare(None, context)
    }

    fn finish(mut self) -> u64 {
        for stream in [&mut self.sider, &mut self.redis] {
            stream.shutdown(Shutdown::Write).unwrap();
            assert_eq!(
                stream.read(&mut [0; 1]).unwrap(),
                0,
                "bytes extras antes de EOF"
            );
        }
        self.cases
    }
}

fn fixtures(pair: &mut Pair) {
    for case in resp_fixtures::CASES {
        for (index, (request, response)) in case.exchanges.iter().enumerate() {
            pair.send(request);
            pair.compare(Some(response), &format!("{} sequencial {index}", case.name));
        }
        let pipeline: Vec<_> = case
            .exchanges
            .iter()
            .flat_map(|(request, _)| *request)
            .copied()
            .collect();
        pair.send(&pipeline);
        for (index, (_, response)) in case.exchanges.iter().enumerate() {
            pair.compare(Some(response), &format!("{} pipeline {index}", case.name));
        }
        pair.send(b"*1\r\n$4\r\nPING\r\n");
        pair.compare(Some(b"+PONG\r\n"), "sentinela após fixture");
    }
}

// Gerador pequeno e explicitamente estável: operações wrapping fixadas em u64.
struct Sequence(u64);
impl Sequence {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 ^ (self.0 >> 29)
    }

    fn bytes(&mut self) -> Vec<u8> {
        let length = (self.next() % 129) as usize;
        (0..length).map(|_| self.next() as u8).collect()
    }
}

fn generated(seed: u64) -> (Vec<Vec<u8>>, Vec<Vec<Vec<u8>>>) {
    let mut rng = Sequence(seed);
    let keys: Vec<_> = (0..16)
        .map(|index| {
            if index == 0 {
                Vec::new()
            } else {
                let mut key = format!("r01-diff-{seed:016x}-{index}:").into_bytes();
                key.extend_from_slice(b"\0\xff\r\n");
                key
            }
        })
        .collect();
    let commands = (0..OPERATIONS)
        .map(|index| {
            let key = keys[(rng.next() as usize) % keys.len()].clone();
            match index % 8 {
                0 | 1 | 7 => vec![b"sEt".to_vec(), key, rng.bytes()],
                2 | 6 => vec![b"gEt".to_vec(), key],
                3 => vec![
                    b"dEl".to_vec(),
                    key.clone(),
                    key,
                    keys[(rng.next() as usize) % keys.len()].clone(),
                ],
                4 => vec![b"PiNg".to_vec(), rng.bytes()],
                _ => vec![b"eChO".to_vec(), rng.bytes()],
            }
        })
        .collect();
    (keys, commands)
}

fn sequences(pair: &mut Pair) {
    for seed in SEEDS {
        let (keys, commands) = generated(seed);
        for pipeline in [false, true] {
            let chunk_size = if pipeline { PIPELINE } else { 1 };
            for (chunk_index, chunk) in commands.chunks(chunk_size).enumerate() {
                let bytes: Vec<_> = chunk.iter().flat_map(|args| wire::request(args)).collect();
                pair.send(&bytes);
                for index in 0..chunk.len() {
                    pair.compare(
                        None,
                        &format!(
                            "seed={seed} pipeline={pipeline} command={}",
                            chunk_index * chunk_size + index
                        ),
                    );
                }
            }
            // O mesmo conjunto de operações deve produzir o mesmo estado observado.
            for (index, key) in keys.iter().enumerate() {
                pair.exchange(
                    &[b"GET".to_vec(), key.clone()],
                    &format!("estado seed={seed} key={index}"),
                );
            }
            let mut cleanup = vec![b"DEL".to_vec()];
            cleanup.extend(keys.clone());
            pair.exchange(&cleanup, "limpar somente chaves deste caso");
            for key in &keys {
                let absent = pair.exchange(&[b"GET".to_vec(), key.clone()], "estado após remoção");
                assert_eq!(absent.value, Response::Bulk(None));
            }
        }
    }
}

fn boundary_payloads(pair: &mut Pair) {
    let key = b"r01-diff-boundary".to_vec();
    for length in [0, 1, 127, 8192, 1024 * 1024] {
        let value: Vec<_> = (0..length).map(|index| (index % 256) as u8).collect();
        let echoed = pair.exchange(&[b"ECHO".to_vec(), value.clone()], "ECHO nas fronteiras");
        assert_eq!(echoed.value, Response::Bulk(Some(value.clone())));
        let stored = pair.exchange(
            &[b"SET".to_vec(), key.clone(), value.clone()],
            "SET nas fronteiras",
        );
        assert_eq!(stored.value, Response::Simple(b"OK".to_vec()));
        let fetched = pair.exchange(&[b"GET".to_vec(), key.clone()], "GET nas fronteiras");
        assert_eq!(fetched.value, Response::Bulk(Some(value)));
    }
    pair.exchange(&[b"DEL".to_vec(), key], "remover chave das fronteiras");
}

fn cli_suite(reference: &RedisReference, sider: SocketAddr) -> u64 {
    let cases: &[(&[&str], &[u8])] = &[
        (&["PING"], b"PONG\n"),
        (&["PING", "mensagem"], b"mensagem\n"),
        (&["ECHO", ""], b"\n"),
        (&["ECHO", "sider-cli"], b"sider-cli\n"),
        (&["GET", "r01-cli-key"], b"\n"),
        (&["SET", "r01-cli-key", "valor"], b"OK\n"),
        (&["GET", "r01-cli-key"], b"valor\n"),
        (&["DEL", "r01-cli-key", "r01-cli-key"], b"1\n"),
        (&["GET", "r01-cli-key"], b"\n"),
    ];
    for (args, expected) in cases {
        assert_eq!(
            reference.cli_at(sider, args),
            *expected,
            "redis-cli contra Sider: {args:?}"
        );
        assert_eq!(
            reference.cli(args),
            *expected,
            "redis-cli contra Redis: {args:?}"
        );
    }
    cases.len() as u64
}

fn suite(require_cli: bool) -> (u64, Value) {
    let runner = std::env::var("SIDER_TEST_RUNNER_CONTAINER").ok();
    assert!(
        !require_cli || runner.is_some(),
        "CLI/gate exigem SIDER_TEST_RUNNER_CONTAINER com o ID completo do runner Linux isolado"
    );
    let reference = match runner {
        Some(id) => RedisReference::start_shared(&id),
        None => RedisReference::start(),
    };
    let mut sider = SiderProcess::start(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        env!("CARGO_PKG_VERSION"),
    );
    let mut pair = Pair::new(sider.address(), reference.address());
    fixtures(&mut pair);
    sequences(&mut pair);
    boundary_payloads(&mut pair);
    let r01_comparisons = pair.cases;
    r02_strings(&mut pair);
    r02_set_options(&mut pair);
    let r02_temporal_observations = r02_expiration(&mut pair);
    let r02_comparisons = pair.cases - r01_comparisons - r02_temporal_observations;
    let comparisons = pair.finish();
    let cli_cases = if require_cli {
        cli_suite(&reference, sider.address())
    } else {
        0
    };
    sider.assert_alive();
    sider.finish();
    reference.finish();
    let report = json!({
        "suite": "resp2-strings-r01-r02", "seeds": SEEDS,
        "operations_per_seed": OPERATIONS, "pipeline_size": PIPELINE,
        "fixture_cases": resp_fixtures::CASES.len(), "checks": comparisons,
        "binary_comparisons": comparisons - r02_temporal_observations,
        "r01_binary_comparisons": r01_comparisons, "r02_binary_comparisons": r02_comparisons,
        "r02_temporal_observations": r02_temporal_observations,
        "r02_pttl_tolerance_ms": 100, "r02_ttl_tolerance_seconds": 1,
        "cli_cases": cli_cases, "cli_verified": require_cli,
        "maximum_bulk_bytes": 1024 * 1024, "cleanup_confirmed": true,
        "sider_version": env!("CARGO_PKG_VERSION"),
    });
    eprintln!("{report}");
    (comparisons + cli_cases, report)
}

fn r02_exchange(pair: &mut Pair, args: &[&[u8]]) -> Observed {
    pair.exchange(
        &args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>(),
        "R02 strings/options",
    )
}

fn r02_strings(pair: &mut Pair) {
    let key = b"r02:\0\xff";
    let other = b"r02:other";
    let missing = b"r02:absent";
    r02_exchange(pair, &[b"MSET", key, b"first", other, b"", key, b"last"]);
    r02_exchange(pair, &[b"EXISTS", key, key, other, missing]);
    r02_exchange(pair, &[b"MGET", key, missing, other, key]);
    for args in [
        vec![b"EXISTS".as_slice()],
        vec![b"MGET".as_slice()],
        vec![b"INCR".as_slice()],
        vec![b"DECR", key, other],
        vec![b"MSET".as_slice()],
        vec![b"MSET", key, b"new", other],
    ] {
        r02_exchange(pair, &args);
    }
    r02_exchange(pair, &[b"MGET", key, other, missing]);
    for value in [
        b"0".as_slice(),
        b"-1",
        b"9223372036854775807",
        b"-9223372036854775808",
        b"9223372036854775808",
        b"-9223372036854775809",
        b"+1",
        b"-0",
        b"01",
        b" 1",
        b"1\0",
        b"\xff",
        b"",
    ] {
        for name in [b"INCR".as_slice(), b"DECR"] {
            r02_exchange(pair, &[b"SET", key, value]);
            r02_exchange(pair, &[name, key]);
            r02_exchange(pair, &[b"GET", key]);
            r02_exchange(pair, &[b"PING"]);
        }
    }
    for name in [b"INCR".as_slice(), b"DECR"] {
        r02_exchange(pair, &[b"DEL", key]);
        r02_exchange(pair, &[name, key]);
    }
    r02_exchange(pair, &[b"DEL", key, other, missing]);
    let large = vec![0xff; 1024 * 1024];
    r02_exchange(pair, &[b"SET", key, &large]);
    r02_exchange(pair, &[b"MGET", key, key, key]);
    r02_exchange(pair, &[b"DEL", key]);
}

fn r02_set_options(pair: &mut Pair) {
    let key = b"r02:options";
    for present in [false, true] {
        for condition in [None, Some(b"NX".as_slice()), Some(b"XX".as_slice())] {
            for expiry in [
                &[][..],
                &[b"EX".as_slice(), b"60"][..],
                &[b"PX".as_slice(), b"60000"][..],
                &[b"KEEPTTL".as_slice()][..],
            ] {
                for get in [false, true] {
                    r02_exchange(pair, &[b"DEL", key]);
                    if present {
                        r02_exchange(pair, &[b"SET", key, b"old", b"PX", b"60000"]);
                    }
                    let mut args = vec![b"SET".as_slice(), key, b"new\0\xff"];
                    args.extend(condition);
                    args.extend_from_slice(expiry);
                    if get {
                        args.push(b"GET");
                    }
                    r02_exchange(pair, &args);
                    r02_exchange(pair, &[b"GET", key]);
                    r02_exchange(pair, &[b"PERSIST", key]);
                    r02_exchange(pair, &[b"TTL", key]);
                }
            }
        }
    }
    let invalid: &[&[&[u8]]] = &[
        &[b"NX", b"XX"],
        &[b"EX"],
        &[b"PX"],
        &[b"EX", b"1", b"PX", b"2"],
        &[b"KEEPTTL", b"EX", b"1"],
        &[b"EX", b"1", b"KEEPTTL"],
        &[b"EX", b"0"],
        &[b"PX", b"-1"],
        &[b"PX", b"+1"],
        &[b"PX", b"9223372036854775807"],
        &[b"EX", b"9223372036854775807"],
        &[b"EX", b"not-an-integer"],
        &[b"INVALID"],
    ];
    for options in invalid {
        r02_exchange(pair, &[b"SET", key, b"old", b"PX", b"60000"]);
        let mut args = vec![b"SET".as_slice(), key, b"new"];
        args.extend_from_slice(options);
        r02_exchange(pair, &args);
        r02_exchange(pair, &[b"GET", key]);
        r02_exchange(pair, &[b"PERSIST", key]);
    }
    r02_exchange(
        pair,
        &[
            b"SET", key, b"last", b"EX", b"invalid", b"EX", b"60", b"GET", b"GET",
        ],
    );
    r02_exchange(pair, &[b"GET", key]);
    r02_exchange(pair, &[b"DEL", key]);
}

fn r02_expiration(pair: &mut Pair) -> u64 {
    let mut temporal_observations = 0;
    let key = b"r02:expiration";
    r02_exchange(pair, &[b"TTL", key]);
    r02_exchange(pair, &[b"PTTL", key]);
    r02_exchange(pair, &[b"PERSIST", key]);
    r02_exchange(pair, &[b"SET", key, b"1"]);
    r02_exchange(pair, &[b"PEXPIRE", key, b"60000"]);
    for (command, tolerance, max) in [(b"PTTL".as_slice(), 100, 60000), (b"TTL".as_slice(), 1, 60)]
    {
        pair.send(&wire::request(&[command.to_vec(), key.to_vec()]));
        let sider = wire::read_response(&mut pair.sider).unwrap();
        let redis = wire::read_response(&mut pair.redis).unwrap();
        let (Response::Integer(sider), Response::Integer(redis)) = (sider.value, redis.value)
        else {
            panic!("TTL deve ser inteiro");
        };
        assert!((1..=max).contains(&sider) && (1..=max).contains(&redis));
        assert!(
            (sider - redis).abs() <= tolerance,
            "R02 prazo: sider={sider} redis={redis}"
        );
        pair.cases += 1;
        temporal_observations += 1;
    }
    r02_exchange(pair, &[b"INCR", key]);
    r02_exchange(pair, &[b"PERSIST", key]);
    r02_exchange(pair, &[b"TTL", key]);
    for (command, value) in [(b"EXPIRE".as_slice(), b"0".as_slice()), (b"PEXPIRE", b"-1")] {
        r02_exchange(pair, &[b"SET", key, b"v"]);
        r02_exchange(pair, &[command, key, value]);
        r02_exchange(pair, &[b"EXISTS", key]);
    }
    for name in [b"EXPIRE".as_slice(), b"PEXPIRE"] {
        r02_exchange(pair, &[name, key, b"+1"]);
        r02_exchange(pair, &[name, key, b"9223372036854775807"]);
    }
    r02_exchange(pair, &[b"SET", key, b"v", b"PX", b"1"]);
    // Expiração real observada por polling limitado; não assume ordenação de timers entre processos.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        pair.send(&wire::request(&[b"PTTL".to_vec(), key.to_vec()]));
        let sider = wire::read_response(&mut pair.sider).unwrap();
        let redis = wire::read_response(&mut pair.redis).unwrap();
        pair.cases += 1;
        temporal_observations += 1;
        if sider.value == Response::Integer(-2) && redis.value == Response::Integer(-2) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "R02 expiração não observada nos dois servidores"
        );
    }
    r02_exchange(pair, &[b"GET", key]);
    temporal_observations
}

#[test]
#[ignore = "requer Docker Linux e imagem Redis fixada; não inclui CLI contra o Sider"]
fn sider_matches_redis() {
    suite(false);
}

#[test]
#[ignore = "requer runner Linux isolado, Docker e imagem Redis fixada"]
fn sider_matches_redis_and_cli() {
    suite(true);
}

#[test]
#[ignore = "gate externo: checkout limpo, contexto de release e runner Linux isolado"]
fn release_compatibility_gate() {
    let context = GateContext::from_env("compatibility").expect("contexto real do gate");
    assert_eq!(context.version(), env!("CARGO_PKG_VERSION"));
    let started = Instant::now();
    let (cases, report) = suite(true);
    context
        .publish(cases, started.elapsed(), report)
        .expect("recibo do gate somente após sucesso e cleanup");
}

#[test]
fn generated_sequences_are_reproducible_binary_and_cover_the_subset() {
    for seed in SEEDS {
        let (keys, commands) = generated(seed);
        assert_eq!((keys.clone(), commands.clone()), generated(seed));
        assert_eq!(commands.len(), OPERATIONS);
        assert!(keys.iter().any(Vec::is_empty));
        assert!(keys.iter().any(|key| std::str::from_utf8(key).is_err()));
        let names: std::collections::HashSet<_> = commands
            .iter()
            .map(|args| args[0].to_ascii_uppercase())
            .collect();
        assert_eq!(names.len(), 5);
        assert!(
            commands
                .iter()
                .any(|args| args[0] == b"dEl" && args[1] == args[2])
        );
        assert!(
            commands
                .iter()
                .flat_map(|args| args.iter())
                .any(|arg| std::str::from_utf8(arg).is_err())
        );
    }
    assert_ne!(generated(SEEDS[0]), generated(SEEDS[1]));
}
