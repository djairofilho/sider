//! Literal TCP and Redis differential evidence, independent of the product codec.
#![forbid(unsafe_code)]

#[path = "common/process.rs"]
mod process;
#[path = "common/redis_reference.rs"]
mod redis_reference;
#[path = "common/sider_process.rs"]
mod sider_process;

use sider_process::SiderProcess;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::time::Duration;

fn connect(address: SocketAddr) -> TcpStream {
    let socket = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    socket.set_nodelay(true).unwrap();
    socket
}

fn send(socket: &mut TcpStream, args: &[&[u8]], fragmented: bool) {
    let mut request = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        write!(request, "${}\r\n", arg.len()).unwrap();
        request.extend_from_slice(arg);
        request.extend_from_slice(b"\r\n");
    }
    if fragmented {
        for byte in request {
            socket.write_all(&[byte]).unwrap();
        }
    } else {
        socket.write_all(&request).unwrap();
    }
}

// This bounded oracle reads framing only. It never calls Sider's codec.
fn read(socket: &mut TcpStream, depth: usize) -> Vec<u8> {
    assert!(depth < 16);
    let mut prefix = [0];
    socket.read_exact(&mut prefix).unwrap();
    let mut result = vec![prefix[0]];
    loop {
        let mut byte = [0];
        socket.read_exact(&mut byte).unwrap();
        result.push(byte[0]);
        assert!(result.len() <= 1024);
        if result.ends_with(b"\r\n") {
            break;
        }
    }
    let payload = &result[1..result.len() - 2];
    match prefix[0] {
        b'$' | b'!' | b'=' => {
            let count: i64 = std::str::from_utf8(payload).unwrap().parse().unwrap();
            if count >= 0 {
                assert!(count < 65536);
                let mut bytes = vec![0; count as usize + 2];
                socket.read_exact(&mut bytes).unwrap();
                assert!(bytes.ends_with(b"\r\n"));
                result.extend(bytes);
            }
        }
        b'*' | b'%' | b'~' | b'>' | b'|' => {
            let mut count: i64 = std::str::from_utf8(payload).unwrap().parse().unwrap();
            if matches!(prefix[0], b'%' | b'|') {
                count *= 2;
            }
            assert!(count <= 128);
            for _ in 0..count {
                result.extend(read(socket, depth + 1));
            }
        }
        b'+' | b'-' | b':' | b'_' | b'#' | b',' | b'(' => {}
        _ => panic!("invalid response prefix"),
    }
    result
}

fn exchange(
    socket: &mut TcpStream,
    args: &[&[u8]],
    expected: &[u8],
    transcript: &mut Vec<Vec<u8>>,
) {
    send(socket, args, true);
    let actual = read(socket, 0);
    assert_eq!(actual, expected, "{args:?}");
    transcript.push(actual);
}

fn scenario(address: SocketAddr) -> Vec<Vec<u8>> {
    let mut socket = connect(address);
    let mut transcript = Vec::new();
    exchange(
        &mut socket,
        &[b"GET", b"missing"],
        b"$-1\r\n",
        &mut transcript,
    );
    send(&mut socket, &[b"HELLO", b"3"], true);
    let hello = read(&mut socket, 0);
    assert!(hello.starts_with(b"%7\r\n"));
    assert!(
        hello
            .windows(b"$5\r\nproto\r\n:3\r\n".len())
            .any(|s| s == b"$5\r\nproto\r\n:3\r\n")
    );
    for (args, expected) in [
        (vec![b"GET".as_slice(), b"missing"], b"_\r\n".as_slice()),
        (vec![b"ECHO", b"\0\xff\r\n"], b"$4\r\n\0\xff\r\n\r\n"),
        (vec![b"HGETALL", b"hash"], b"%0\r\n"),
        (vec![b"HSET", b"hash", b"a", b"\xff"], b":1\r\n"),
        (
            vec![b"HGETALL", b"hash"],
            b"%1\r\n$1\r\na\r\n$1\r\n\xff\r\n",
        ),
        (vec![b"SMEMBERS", b"set"], b"~0\r\n"),
        (vec![b"SADD", b"set", b"message"], b":1\r\n"),
        (vec![b"SMEMBERS", b"set"], b"~1\r\n$7\r\nmessage\r\n"),
        (vec![b"ZADD", b"z", b"1.5", b"member"], b":1\r\n"),
        (vec![b"ZSCORE", b"z", b"member"], b",1.5\r\n"),
        (
            vec![b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"],
            b"*1\r\n*2\r\n$6\r\nmember\r\n,1.5\r\n",
        ),
        (
            vec![b"GET", b"hash"],
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
        ),
        (vec![b"MGET", b"missing", b"hash"], b"*2\r\n_\r\n_\r\n"),
        (vec![b"MULTI"], b"+OK\r\n"),
        (vec![b"HGETALL", b"hash"], b"+QUEUED\r\n"),
        (vec![b"SMEMBERS", b"set"], b"+QUEUED\r\n"),
        (vec![b"ZSCORE", b"z", b"member"], b"+QUEUED\r\n"),
        (vec![b"GET", b"missing"], b"+QUEUED\r\n"),
        (
            vec![b"EXEC"],
            b"*4\r\n%1\r\n$1\r\na\r\n$1\r\n\xff\r\n~1\r\n$7\r\nmessage\r\n,1.5\r\n_\r\n",
        ),
        (
            vec![b"SUBSCRIBE", b"channel"],
            b">3\r\n$9\r\nsubscribe\r\n$7\r\nchannel\r\n:1\r\n",
        ),
        (vec![b"PING"], b"+PONG\r\n"),
        (vec![b"GET", b"missing"], b"_\r\n"),
        (vec![b"PUBLISH", b"channel", b"\xff"], b":1\r\n"),
    ] {
        exchange(&mut socket, &args, expected, &mut transcript);
    }
    let notification = read(&mut socket, 0);
    assert_eq!(
        notification,
        b">3\r\n$7\r\nmessage\r\n$7\r\nchannel\r\n$1\r\n\xff\r\n"
    );
    transcript.push(notification);
    exchange(
        &mut socket,
        &[b"UNSUBSCRIBE"],
        b">3\r\n$11\r\nunsubscribe\r\n$7\r\nchannel\r\n:0\r\n",
        &mut transcript,
    );
    exchange(
        &mut socket,
        &[b"WATCH", b"resp3-watch"],
        b"+OK\r\n",
        &mut transcript,
    );
    exchange(&mut socket, &[b"MULTI"], b"+OK\r\n", &mut transcript);
    exchange(
        &mut socket,
        &[b"GET", b"resp3-watch"],
        b"+QUEUED\r\n",
        &mut transcript,
    );
    let mut writer = connect(address);
    exchange(
        &mut writer,
        &[b"SET", b"resp3-watch", b"changed"],
        b"+OK\r\n",
        &mut transcript,
    );
    exchange(&mut socket, &[b"EXEC"], b"_\r\n", &mut transcript);
    send(&mut socket, &[b"HELLO", b"2"], true);
    assert!(read(&mut socket, 0).starts_with(b"*14\r\n"));
    exchange(
        &mut socket,
        &[b"GET", b"missing"],
        b"$-1\r\n",
        &mut transcript,
    );
    transcript
}

#[test]
fn resp3_tcp_literal_subset() {
    let sider = SiderProcess::start(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        env!("CARGO_PKG_VERSION"),
    );
    scenario(sider.address());
    negotiation_failures(sider.address());
    let expected = b"*4\r\n>3\r\n$9\r\nsubscribe\r\n$4\r\ntx-a\r\n:1\r\n>3\r\n$9\r\nsubscribe\r\n$4\r\ntx-b\r\n:2\r\n:1\r\n+PONG\r\n>3\r\n$11\r\nunsubscribe\r\n$4\r\ntx-a\r\n:1\r\n>3\r\n$11\r\nunsubscribe\r\n$4\r\ntx-b\r\n:0\r\n>3\r\n$7\r\nmessage\r\n$4\r\ntx-a\r\n$7\r\npayload\r\n$12\r\nresp3-finish\r\n";
    assert_eq!(pubsub_exec(sider.address()), expected);
    sider.finish();
}

fn negotiation_failures(address: SocketAddr) {
    let mut socket = connect(address);
    let mut transcript = Vec::new();
    exchange(
        &mut socket,
        &[b"HELLO", b"4"],
        b"-NOPROTO unsupported protocol version\r\n",
        &mut transcript,
    );
    exchange(
        &mut socket,
        &[b"GET", b"missing"],
        b"$-1\r\n",
        &mut transcript,
    );
    exchange(
        &mut socket,
        &[b"HELLO", b"no"],
        b"-ERR Protocol version is not an integer or out of range\r\n",
        &mut transcript,
    );
    exchange(
        &mut socket,
        &[b"HELLO", b"3", b"AUTH", b"default", b"secret"],
        b"-ERR unsupported HELLO options\r\n",
        &mut transcript,
    );
    exchange(
        &mut socket,
        &[b"GET", b"missing"],
        b"$-1\r\n",
        &mut transcript,
    );
    send(&mut socket, &[b"HELLO", b"3"], true);
    assert!(read(&mut socket, 0).starts_with(b"%7\r\n"));
    send(&mut socket, &[b"HELLO"], true);
    assert!(read(&mut socket, 0).starts_with(b"%7\r\n"));
    let mut separate = connect(address);
    exchange(
        &mut separate,
        &[b"GET", b"missing"],
        b"$-1\r\n",
        &mut transcript,
    );
    exchange(
        &mut socket,
        &[b"HELLO", b"2", b"SETNAME", b"name"],
        b"-ERR unsupported HELLO options\r\n",
        &mut transcript,
    );
    exchange(
        &mut socket,
        &[b"GET", b"missing"],
        b"_\r\n",
        &mut transcript,
    );
    exchange(&mut socket, &[b"MULTI"], b"+OK\r\n", &mut transcript);
    exchange(
        &mut socket,
        &[b"HELLO", b"2"],
        b"-ERR HELLO is not supported inside MULTI\r\n",
        &mut transcript,
    );
    exchange(
        &mut socket,
        &[b"EXEC"],
        b"-EXECABORT Transaction discarded because of previous errors.\r\n",
        &mut transcript,
    );
    exchange(
        &mut socket,
        &[b"GET", b"missing"],
        b"_\r\n",
        &mut transcript,
    );
    socket.write_all(b"*2\r\n$5\r\nHELLO\r\n:3\r\n").unwrap();
    assert_eq!(read(&mut socket, 0), b"-ERR invalid request format\r\n");
    assert_eq!(socket.read(&mut [0]).unwrap(), 0);
}

#[test]
#[ignore = "requires Docker and the pinned Redis image"]
fn resp3_differential_subset() {
    let reference = redis_reference::RedisReference::start();
    let sider = SiderProcess::start(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        env!("CARGO_PKG_VERSION"),
    );
    assert_eq!(scenario(sider.address()), scenario(reference.address()));
    assert_eq!(
        pubsub_exec(sider.address()),
        pubsub_exec(reference.address())
    );
    sider.finish();
    reference.finish();
}

fn pubsub_exec(address: SocketAddr) -> Vec<u8> {
    let mut socket = connect(address);
    send(&mut socket, &[b"HELLO", b"3"], false);
    read(&mut socket, 0);
    for args in [
        vec![b"MULTI".as_slice()],
        vec![b"SUBSCRIBE", b"tx-a", b"tx-b"],
        vec![b"PUBLISH", b"tx-a", b"payload"],
        vec![b"PING"],
        vec![b"UNSUBSCRIBE", b"tx-a", b"tx-b"],
    ] {
        send(&mut socket, &args, false);
        read(&mut socket, 0);
    }
    send(&mut socket, &[b"EXEC"], false);
    send(&mut socket, &[b"ECHO", b"resp3-finish"], false);
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"$12\r\nresp3-finish\r\n") {
        let mut byte = [0];
        socket.read_exact(&mut byte).unwrap();
        bytes.push(byte[0]);
        assert!(bytes.len() < 4096);
    }
    bytes
}
