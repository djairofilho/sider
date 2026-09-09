//! Transações TCP, referência Redis fixada e persistência do lote.
#![forbid(unsafe_code)]

#[path = "common/gate_receipt.rs"]
mod gate_receipt;
#[path = "common/process.rs"]
mod process;
#[path = "common/redis_reference.rs"]
mod redis_reference;
#[path = "common/sider_process.rs"]
mod sider_process;
#[path = "common/wire.rs"]
mod wire;

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::path::Path;
use std::time::{Duration, Instant};

fn typed_scenario(address: SocketAddr) -> Vec<Vec<u8>> {
    let mut client = connect(address);
    let mut other = connect(address);
    let mut transcript = Vec::new();
    step(&mut client, &[b"MULTI"], b"+OK\r\n", &mut transcript);
    for args in [
        vec![b"HSET".as_slice(), b"tx:h", b"f\xff", b"v\0"],
        vec![b"GET", b"tx:h"],
        vec![b"HGET", b"tx:h", b"f\xff"],
        vec![b"RPUSH", b"tx:l", b"a", b"b"],
        vec![b"SADD", b"tx:s", b"a", b"b"],
        vec![b"ZADD", b"tx:z", b"1", b"a", b"2", b"b"],
    ] {
        step(&mut client, &args, b"+QUEUED\r\n", &mut transcript);
    }
    step(&mut client, &[b"EXEC"], b"*6\r\n:1\r\n-WRONGTYPE Operation against a key holding the wrong kind of value\r\n$2\r\nv\0\r\n:2\r\n:2\r\n:2\r\n", &mut transcript);
    for (key, mutation, result) in [
        (
            b"tx:h".as_slice(),
            vec![b"HSET".as_slice(), b"tx:h", b"f\xff", b"v\0"],
            b":0\r\n".as_slice(),
        ),
        (b"tx:l", vec![b"RPUSH", b"tx:l", b"c"], b":3\r\n"),
        (b"tx:s", vec![b"SADD", b"tx:s", b"c"], b":1\r\n"),
        (b"tx:z", vec![b"ZADD", b"tx:z", b"3", b"a"], b":0\r\n"),
    ] {
        step(&mut client, &[b"WATCH", key], b"+OK\r\n", &mut transcript);
        step(&mut other, &mutation, result, &mut transcript);
        step(&mut client, &[b"MULTI"], b"+OK\r\n", &mut transcript);
        step(&mut client, &[b"EXEC"], b"*-1\r\n", &mut transcript);
    }
    // A saída de EXEC preserva o modo resultante e o caso UNSUBSCRIBE vazio.
    step(&mut client, &[b"MULTI"], b"+OK\r\n", &mut transcript);
    step(
        &mut client,
        &[b"UNSUBSCRIBE"],
        b"+QUEUED\r\n",
        &mut transcript,
    );
    step(
        &mut client,
        &[b"EXEC"],
        b"*1\r\n*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n",
        &mut transcript,
    );
    step(&mut client, &[b"MULTI"], b"+OK\r\n", &mut transcript);
    step(
        &mut client,
        &[b"SUBSCRIBE", b"c"],
        b"+QUEUED\r\n",
        &mut transcript,
    );
    step(
        &mut client,
        &[b"EXEC"],
        b"*1\r\n*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n",
        &mut transcript,
    );
    step(
        &mut client,
        &[b"PING"],
        b"*2\r\n$4\r\npong\r\n$0\r\n\r\n",
        &mut transcript,
    );
    step(
        &mut client,
        &[b"UNSUBSCRIBE"],
        b"*3\r\n$11\r\nunsubscribe\r\n$1\r\nc\r\n:0\r\n",
        &mut transcript,
    );
    step(&mut client, &[b"PING"], b"+PONG\r\n", &mut transcript);
    transcript
}

fn connect(address: SocketAddr) -> TcpStream {
    let client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client
}

fn step(client: &mut TcpStream, args: &[&[u8]], expected: &[u8], transcript: &mut Vec<Vec<u8>>) {
    client
        .write_all(&wire::request(
            &args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>(),
        ))
        .unwrap();
    let observed = wire::read_response(client).unwrap();
    assert_eq!(observed.bytes, expected, "comando {args:?}");
    transcript.push(observed.bytes);
}

fn data_scenario(address: SocketAddr) -> Vec<Vec<u8>> {
    let mut client = connect(address);
    let mut other = connect(address);
    let mut transcript = Vec::new();
    let mut c =
        |args: &[&[u8]], expected: &[u8]| step(&mut client, args, expected, &mut transcript);
    c(&[b"EXEC"], b"-ERR EXEC without MULTI\r\n");
    c(&[b"DISCARD"], b"-ERR DISCARD without MULTI\r\n");
    c(&[b"MULTI"], b"+OK\r\n");
    c(&[b"MULTI"], b"-ERR MULTI calls can not be nested\r\n");
    c(
        &[b"WATCH", b"tx:a"],
        b"-ERR WATCH inside MULTI is not allowed\r\n",
    );
    c(&[b"SET", b"tx:a", b"bad"], b"+QUEUED\r\n");
    c(&[b"INCR", b"tx:a"], b"+QUEUED\r\n");
    c(&[b"SET", b"tx:b", b"\xff\0"], b"+QUEUED\r\n");
    c(&[b"GET", b"tx:b"], b"+QUEUED\r\n");
    c(
        &[b"EXEC"],
        b"*4\r\n+OK\r\n-ERR value is not an integer or out of range\r\n+OK\r\n$2\r\n\xff\0\r\n",
    );
    c(&[b"MULTI"], b"+OK\r\n");
    c(&[b"SET", b"tx:a", b"discarded"], b"+QUEUED\r\n");
    c(
        &[b"GET"],
        b"-ERR wrong number of arguments for 'get' command\r\n",
    );
    c(
        &[b"EXEC"],
        b"-EXECABORT Transaction discarded because of previous errors.\r\n",
    );
    c(&[b"GET", b"tx:a"], b"$3\r\nbad\r\n");
    c(&[b"MULTI"], b"+OK\r\n");
    c(&[b"SET", b"tx:a", b"discarded"], b"+QUEUED\r\n");
    c(&[b"DISCARD"], b"+OK\r\n");
    c(&[b"GET", b"tx:a"], b"$3\r\nbad\r\n");
    c(&[b"MULTI"], b"+OK\r\n");
    c(&[b"EXEC"], b"*0\r\n");
    // WATCH detecta SET idêntico, criação seguida de DEL e escrita da própria conexão.
    for own_write in [false, true] {
        step(
            &mut client,
            &[b"WATCH", b"tx:a", b"tx:a"],
            b"+OK\r\n",
            &mut transcript,
        );
        step(
            if own_write { &mut client } else { &mut other },
            &[b"SET", b"tx:a", b"bad"],
            b"+OK\r\n",
            &mut transcript,
        );
        step(&mut client, &[b"MULTI"], b"+OK\r\n", &mut transcript);
        step(
            &mut client,
            &[b"SET", b"tx:a", b"blocked"],
            b"+QUEUED\r\n",
            &mut transcript,
        );
        step(&mut client, &[b"EXEC"], b"*-1\r\n", &mut transcript);
    }
    step(
        &mut client,
        &[b"WATCH", b"tx:absent"],
        b"+OK\r\n",
        &mut transcript,
    );
    step(
        &mut other,
        &[b"SET", b"tx:absent", b"x"],
        b"+OK\r\n",
        &mut transcript,
    );
    step(
        &mut other,
        &[b"DEL", b"tx:absent"],
        b":1\r\n",
        &mut transcript,
    );
    step(&mut client, &[b"MULTI"], b"+OK\r\n", &mut transcript);
    step(&mut client, &[b"EXEC"], b"*-1\r\n", &mut transcript);
    // DEL ausente não invalida; EXEC anterior liberou as observações inválidas.
    step(
        &mut client,
        &[b"WATCH", b"tx:absent"],
        b"+OK\r\n",
        &mut transcript,
    );
    step(
        &mut other,
        &[b"DEL", b"tx:absent"],
        b":0\r\n",
        &mut transcript,
    );
    step(&mut client, &[b"MULTI"], b"+OK\r\n", &mut transcript);
    step(&mut client, &[b"PING"], b"+QUEUED\r\n", &mut transcript);
    step(&mut client, &[b"EXEC"], b"*1\r\n+PONG\r\n", &mut transcript);
    for queued in [false, true] {
        step(
            &mut client,
            &[b"WATCH", b"tx:a"],
            b"+OK\r\n",
            &mut transcript,
        );
        if queued {
            step(&mut client, &[b"MULTI"], b"+OK\r\n", &mut transcript);
        }
        step(
            &mut client,
            &[b"UNWATCH"],
            if queued { b"+QUEUED\r\n" } else { b"+OK\r\n" },
            &mut transcript,
        );
        step(
            &mut other,
            &[b"SET", b"tx:a", b"new"],
            b"+OK\r\n",
            &mut transcript,
        );
        if !queued {
            step(&mut client, &[b"MULTI"], b"+OK\r\n", &mut transcript);
        }
        step(
            &mut client,
            &[b"GET", b"tx:a"],
            b"+QUEUED\r\n",
            &mut transcript,
        );
        step(
            &mut client,
            &[b"EXEC"],
            if queued {
                b"*-1\r\n"
            } else {
                b"*1\r\n$3\r\nnew\r\n"
            },
            &mut transcript,
        );
    }
    // Desconexão antes de EXEC descarta a fila; EOF confirma cleanup da conexão.
    let mut abandoned = connect(address);
    step(
        &mut abandoned,
        &[b"WATCH", b"tx:a"],
        b"+OK\r\n",
        &mut transcript,
    );
    step(&mut abandoned, &[b"MULTI"], b"+OK\r\n", &mut transcript);
    step(
        &mut abandoned,
        &[b"SET", b"tx:a", b"abandoned"],
        b"+QUEUED\r\n",
        &mut transcript,
    );
    abandoned.shutdown(Shutdown::Write).unwrap();
    let mut trailing = Vec::new();
    abandoned.read_to_end(&mut trailing).unwrap();
    assert!(trailing.is_empty());
    step(
        &mut client,
        &[b"GET", b"tx:a"],
        b"$3\r\nnew\r\n",
        &mut transcript,
    );
    transcript
}

const PUBSUB_EXEC: &[u8] = b"+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*6\r\n*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n*2\r\n$4\r\npong\r\n$7\r\npayload\r\n+OK\r\n:1\r\n*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n*3\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:0\r\n+PONG\r\n*3\r\n$7\r\nmessage\r\n$1\r\na\r\n$11\r\nown-message\r\n";

fn pubsub_scenario(address: SocketAddr) -> Vec<u8> {
    let mut client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let commands: &[&[&[u8]]] = &[
        &[b"MULTI"],
        &[b"SUBSCRIBE", b"a", b"b"],
        &[b"PING", b"payload"],
        &[b"SET", b"tx-key", b"value"],
        &[b"PUBLISH", b"a", b"own-message"],
        &[b"UNSUBSCRIBE", b"a", b"b"],
        &[b"PING"],
        &[b"EXEC"],
    ];
    for args in commands {
        client
            .write_all(&wire::request(
                &args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>(),
            ))
            .unwrap();
    }
    client.shutdown(Shutdown::Write).unwrap();
    let mut bytes = Vec::new();
    client.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, PUBSUB_EXEC);
    bytes
}

#[test]
fn transactions_tcp_contract() {
    let sider = sider_process::SiderProcess::start(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        env!("CARGO_PKG_VERSION"),
    );
    pubsub_scenario(sider.address());
    assert!(!data_scenario(sider.address()).is_empty());
    assert!(!typed_scenario(sider.address()).is_empty());
    sider.finish();
}

#[test]
#[ignore = "requer Docker Linux e imagem Redis fixada"]
fn transactions_matches_redis() {
    differential();
}

fn differential() -> (u64, serde_json::Value) {
    let reference = match std::env::var("SIDER_TEST_RUNNER_CONTAINER") {
        Ok(id) => redis_reference::RedisReference::start_shared(&id),
        Err(_) => redis_reference::RedisReference::start(),
    };
    let sider = sider_process::SiderProcess::start(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        env!("CARGO_PKG_VERSION"),
    );
    assert_eq!(
        pubsub_scenario(sider.address()),
        pubsub_scenario(reference.address())
    );
    let actual = data_scenario(sider.address());
    assert_eq!(actual, data_scenario(reference.address()));
    let typed = typed_scenario(sider.address());
    assert_eq!(typed, typed_scenario(reference.address()));
    let cases = (actual.len() + typed.len() + 1) as u64;
    let report = serde_json::json!({ "suite": "resp2-transactions-r07", "binary_comparisons": cases, "exec_pubsub_transcript_bytes": PUBSUB_EXEC.len(), "cleanup_confirmed": true });
    eprintln!("{report}");
    sider.finish();
    reference.finish();
    (cases, report)
}

#[test]
#[ignore = "gate externo: checkout limpo e contexto de release Linux"]
fn release_transactions_gate() {
    let context =
        gate_receipt::GateContext::from_env("transactions").expect("contexto real do gate");
    let began = Instant::now();
    let mut native_outputs = Vec::new();
    for target in [vec!["--lib"], vec!["--test", "transactions_persistence"]] {
        let output = process::run(
            std::process::Command::new("cargo")
                .current_dir(env!("CARGO_MANIFEST_DIR"))
                .args(["test", "--locked"])
                .args(target)
                .args(["transactions_", "--", "--nocapture"]),
            Duration::from_secs(180),
        )
        .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(
            stdout.contains("test result: ok.") && !stdout.contains("running 0 tests"),
            "suíte transacional ausente: {stdout}"
        );
        native_outputs.push(stdout);
    }
    let (cases, mut report) = differential();
    report["native_outputs"] = serde_json::json!(native_outputs);
    context
        .publish(cases, began.elapsed(), report)
        .expect("recibo após verificações reais e cleanup");
}
