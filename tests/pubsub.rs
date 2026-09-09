//! TCP contract and Pub/Sub differential tests with a product-independent RESP oracle.
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

use gate_receipt::GateContext;
use redis_reference::RedisReference;
use serde_json::json;
use sider_process::SiderProcess;
use wire::Response;

const IO_TIMEOUT: Duration = Duration::from_secs(5);
const MESSAGES: u8 = 64;
const RECONNECTIONS: usize = 16;

fn connect(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, IO_TIMEOUT).unwrap();
    stream.set_nodelay(true).unwrap();
    stream.set_read_timeout(Some(IO_TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(IO_TIMEOUT)).unwrap();
    stream
}

fn send(client: &mut TcpStream, args: &[&[u8]]) {
    client
        .write_all(&wire::request(
            &args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>(),
        ))
        .unwrap();
}

fn receive(client: &mut TcpStream, expected: Response, transcript: &mut Vec<Vec<u8>>) {
    let actual = wire::read_response(client).unwrap();
    assert_eq!(actual.value, expected);
    transcript.push(actual.bytes);
}

fn bulk(value: &[u8]) -> Response {
    Response::Bulk(Some(value.to_vec()))
}
fn acknowledgement(kind: &[u8], channel: Option<&[u8]>, count: i64) -> Response {
    Response::Array(Some(vec![
        bulk(kind),
        Response::Bulk(channel.map(<[u8]>::to_vec)),
        Response::Integer(count),
    ]))
}
fn notification(channel: &[u8], payload: &[u8]) -> Response {
    Response::Array(Some(vec![bulk(b"message"), bulk(channel), bulk(payload)]))
}

fn scenario(address: SocketAddr) -> Vec<Vec<u8>> {
    let mut transcript = Vec::new();
    let mut publisher = connect(address);
    let mut first = connect(address);
    let mut second = connect(address);
    let channel = b"r08:\0\xff\r\n";

    send(&mut publisher, &[b"PUBLISH", channel, b""]);
    receive(&mut publisher, Response::Integer(0), &mut transcript);
    send(&mut first, &[b"UNSUBSCRIBE"]);
    receive(
        &mut first,
        acknowledgement(b"unsubscribe", None, 0),
        &mut transcript,
    );
    send(&mut first, &[b"SUBSCRIBE", channel, b"", channel]);
    for (name, count) in [
        (channel.as_slice(), 1),
        (b"".as_slice(), 2),
        (channel.as_slice(), 2),
    ] {
        receive(
            &mut first,
            acknowledgement(b"subscribe", Some(name), count),
            &mut transcript,
        );
    }
    send(&mut second, &[b"sUbScRiBe", channel]);
    receive(
        &mut second,
        acknowledgement(b"subscribe", Some(channel), 1),
        &mut transcript,
    );
    for payload in [None, Some(b"\0\xff\r\n".as_slice())] {
        match payload {
            Some(payload) => send(&mut first, &[b"PING", payload]),
            None => send(&mut first, &[b"PING"]),
        }
        receive(
            &mut first,
            Response::Array(Some(vec![bulk(b"pong"), bulk(payload.unwrap_or(b""))])),
            &mut transcript,
        );
    }
    for (name, args) in [
        (b"get".as_slice(), vec![b"GET".as_slice(), channel]),
        (b"publish", vec![b"PUBLISH".as_slice(), channel, b"blocked"]),
    ] {
        send(&mut first, &args);
        let error = format!(
            "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
            String::from_utf8_lossy(name)
        );
        receive(
            &mut first,
            Response::Error(error.into_bytes()),
            &mut transcript,
        );
    }
    // Two real queues receive identical bytes, without duplicating repeated subscriptions.
    for index in 0..MESSAGES {
        let payload: Vec<u8> = if index == 0 {
            vec![]
        } else {
            vec![index, 0xff, 0, b'\r', b'\n']
        };
        send(&mut publisher, &[b"PUBLISH", channel, &payload]);
        receive(&mut publisher, Response::Integer(2), &mut transcript);
        receive(&mut first, notification(channel, &payload), &mut transcript);
        receive(
            &mut second,
            notification(channel, &payload),
            &mut transcript,
        );
    }
    send(&mut publisher, &[b"PUBLISH", b"", b"empty-channel"]);
    receive(&mut publisher, Response::Integer(1), &mut transcript);
    receive(
        &mut first,
        notification(b"", b"empty-channel"),
        &mut transcript,
    );
    send(&mut first, &[b"UNSUBSCRIBE", b"missing", channel, channel]);
    for (name, count) in [
        (b"missing".as_slice(), 2),
        (channel.as_slice(), 1),
        (channel.as_slice(), 1),
    ] {
        receive(
            &mut first,
            acknowledgement(b"unsubscribe", Some(name), count),
            &mut transcript,
        );
    }
    send(&mut first, &[b"UNSUBSCRIBE"]);
    receive(
        &mut first,
        acknowledgement(b"unsubscribe", Some(b""), 0),
        &mut transcript,
    );
    send(&mut first, &[b"PING"]);
    receive(
        &mut first,
        Response::Simple(b"PONG".to_vec()),
        &mut transcript,
    );
    // Response EOF is the cleanup barrier; it does not depend on sleeps or PUBLISH polling.
    second.shutdown(Shutdown::Write).unwrap();
    let mut remaining = Vec::new();
    second.read_to_end(&mut remaining).unwrap();
    assert!(remaining.is_empty());
    send(&mut publisher, &[b"PUBLISH", channel, b"after-eof"]);
    receive(&mut publisher, Response::Integer(0), &mut transcript);
    for _ in 0..RECONNECTIONS {
        let mut client = connect(address);
        send(&mut client, &[b"SUBSCRIBE", channel]);
        receive(
            &mut client,
            acknowledgement(b"subscribe", Some(channel), 1),
            &mut transcript,
        );
        client.shutdown(Shutdown::Write).unwrap();
        client.read_to_end(&mut Vec::new()).unwrap();
        send(&mut publisher, &[b"PUBLISH", channel, b"after-reconnect"]);
        receive(&mut publisher, Response::Integer(0), &mut transcript);
    }
    // Pub/Sub does not create keys; the connection returns to the normal worker path.
    send(&mut first, &[b"GET", channel]);
    receive(&mut first, Response::Bulk(None), &mut transcript);
    send(&mut publisher, &[b"SET", b"r08-independent-key", b"value"]);
    receive(
        &mut publisher,
        Response::Simple(b"OK".to_vec()),
        &mut transcript,
    );
    send(&mut publisher, &[b"DEL", b"r08-independent-key"]);
    receive(&mut publisher, Response::Integer(1), &mut transcript);
    transcript
}

#[test]
fn pubsub_tcp_contract() {
    let mut sider = SiderProcess::start(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        env!("CARGO_PKG_VERSION"),
    );
    assert!(!scenario(sider.address()).is_empty());
    sider.assert_alive();
    sider.finish();
}

fn differential() -> (u64, serde_json::Value) {
    let reference = match std::env::var("SIDER_TEST_RUNNER_CONTAINER") {
        Ok(id) => RedisReference::start_shared(&id),
        Err(_) => RedisReference::start(),
    };
    let mut sider = SiderProcess::start(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        env!("CARGO_PKG_VERSION"),
    );
    let actual = scenario(sider.address());
    let expected = scenario(reference.address());
    assert_eq!(
        actual, expected,
        "RESP2 Pub/Sub transcripts must be identical"
    );
    sider.assert_alive();
    sider.finish();
    reference.finish();
    let report = json!({ "suite": "resp2-pubsub-r08", "binary_comparisons": actual.len(), "messages": MESSAGES, "reconnections": RECONNECTIONS, "cleanup_confirmed": true });
    eprintln!("{report}");
    (actual.len() as u64, report)
}

#[test]
#[ignore = "requires Linux Docker and the pinned Redis image"]
fn pubsub_matches_redis() {
    differential();
}

#[test]
#[ignore = "external gate: clean checkout and Linux release context"]
fn release_pubsub_gate() {
    let context = GateContext::from_env("pubsub").expect("actual gate context");
    let started = Instant::now();
    // The gate also runs deterministic queue, timeout, and RAII scenarios.
    let output = process::run(
        std::process::Command::new("cargo")
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args(["test", "--locked", "--lib", "pubsub_", "--", "--nocapture"]),
        Duration::from_secs(180),
    )
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let native = String::from_utf8(output.stdout).unwrap();
    assert!(
        native.contains("test result: ok.") && !native.contains("running 0 tests"),
        "missing native Pub/Sub suite: {native}"
    );
    let (cases, mut report) = differential();
    report["native_pubsub_output"] = native.into();
    context
        .publish(cases, started.elapsed(), report)
        .expect("receipt only after execution and cleanup");
}
