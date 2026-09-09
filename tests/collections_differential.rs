//! Diferencial de coleções: pares e sets normalizados, listas comparadas byte a byte.

#![forbid(unsafe_code)]

#[path = "common/process.rs"]
mod process;
#[path = "common/redis_reference.rs"]
mod redis_reference;
#[path = "common/sider_process.rs"]
mod sider_process;
#[path = "common/wire.rs"]
mod wire;

use std::collections::BTreeMap;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::time::Duration;
use wire::Response;

struct Pair {
    sider: TcpStream,
    redis: TcpStream,
    exact: usize,
    normalized: usize,
}

impl Pair {
    fn new(sider: SocketAddr, redis: SocketAddr) -> Self {
        fn connect(address: SocketAddr) -> TcpStream {
            let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream.set_nodelay(true).unwrap();
            stream
        }
        Self {
            sider: connect(sider),
            redis: connect(redis),
            exact: 0,
            normalized: 0,
        }
    }

    fn run(&mut self, args: &[&[u8]]) -> Response {
        let bytes = wire::request(&args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>());
        self.sider.write_all(&bytes).unwrap();
        self.redis.write_all(&bytes).unwrap();
        let actual = wire::read_response(&mut self.sider).unwrap();
        let expected = wire::read_response(&mut self.redis).unwrap();
        let name = args[0].to_ascii_uppercase();
        if matches!(name.as_slice(), b"HGETALL" | b"SMEMBERS")
            && matches!(actual.value, Response::Array(Some(_)))
        {
            assert_eq!(
                normalize(&actual.value, name == b"HGETALL"),
                normalize(&expected.value, name == b"HGETALL"),
                "{args:?}"
            );
            self.normalized += 1;
        } else {
            assert_eq!(actual, expected, "{args:?}");
            self.exact += 1;
        }
        actual.value
    }
}

fn normalize(value: &Response, pairs: bool) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let Response::Array(Some(values)) = value else {
        panic!("array esperado: {value:?}")
    };
    let bulk = |value: &Response| match value {
        Response::Bulk(Some(value)) => value.clone(),
        _ => panic!("bulk não nulo esperado: {value:?}"),
    };
    let mut result = BTreeMap::new();
    if pairs {
        assert_eq!(values.len() % 2, 0, "pares completos");
        for pair in values.chunks_exact(2) {
            assert!(
                result.insert(bulk(&pair[0]), bulk(&pair[1])).is_none(),
                "campo repetido"
            );
        }
    } else {
        for value in values {
            assert!(
                result.insert(bulk(value), vec![]).is_none(),
                "membro repetido"
            );
        }
    }
    result
}

fn fixtures(pair: &mut Pair) {
    for args in [
        vec![b"HGET".as_slice(), b"{r05}h", b"missing"],
        vec![b"HGETALL", b"{r05}h"],
        vec![b"HLEN", b"{r05}h"],
        vec![
            b"HSET", b"{r05}h", b"", b"", b"\xff", b"old", b"\xff", b"\0new",
        ],
        vec![b"HGETALL", b"{r05}h"],
        vec![b"HEXISTS", b"{r05}h", b""],
        vec![b"HDEL", b"{r05}h", b"", b"", b"absent"],
        vec![b"HDEL", b"{r05}h", b"\xff"],
        vec![b"EXISTS", b"{r05}h"],
        vec![b"LPOP", b"{r05}l"],
        vec![b"LLEN", b"{r05}l"],
        vec![b"LRANGE", b"{r05}l", b"0", b"-1"],
        vec![b"LPUSH", b"{r05}l", b"a", b"", b"\0\xff"],
        vec![b"RPUSH", b"{r05}l", b"c", b"d"],
        vec![
            b"LRANGE",
            b"{r05}l",
            b"-9223372036854775808",
            b"9223372036854775807",
        ],
        vec![b"LRANGE", b"{r05}l", b"-3", b"-2"],
        vec![b"LRANGE", b"{r05}l", b"0", b"-6"],
        vec![b"LRANGE", b"{r05}l", b"5", b"6"],
        vec![b"LRANGE", b"{r05}l", b"3", b"1"],
        vec![b"LRANGE", b"{r05}l", b"+1", b"-1"],
        vec![b"LRANGE", b"{r05}l", b"0", b"01"],
        vec![b"LPOP", b"{r05}l"],
        vec![b"RPOP", b"{r05}l"],
        vec![b"SISMEMBER", b"{r05}s", b""],
        vec![b"SMEMBERS", b"{r05}s"],
        vec![b"SREM", b"{r05}s", b""],
        vec![b"SADD", b"{r05}s", b"\xff", b"", b"a", b"a", b"01", b"1"],
        vec![b"SMEMBERS", b"{r05}s"],
        vec![b"SCARD", b"{r05}s"],
        vec![b"SREM", b"{r05}s", b"a", b"a", b"none"],
    ] {
        pair.run(&args);
    }
    for name in [
        b"HSET".as_slice(),
        b"HGET",
        b"HDEL",
        b"HEXISTS",
        b"HLEN",
        b"HGETALL",
        b"LPUSH",
        b"RPUSH",
        b"LPOP",
        b"RPOP",
        b"LLEN",
        b"LRANGE",
        b"SADD",
        b"SREM",
        b"SISMEMBER",
        b"SCARD",
        b"SMEMBERS",
    ] {
        pair.run(&[name]);
    }
    let creators: &[&[&[u8]]] = &[
        &[b"SET", b"{r05}type", b"string"],
        &[b"HSET", b"{r05}type", b"field", b"value"],
        &[b"LPUSH", b"{r05}type", b"value"],
        &[b"SADD", b"{r05}type", b"value"],
    ];
    let readers: &[&[&[u8]]] = &[
        &[b"GET", b"{r05}type"],
        &[b"INCR", b"{r05}type"],
        &[b"HGET", b"{r05}type", b"field"],
        &[b"HSET", b"{r05}type", b"f", b"v"],
        &[b"HDEL", b"{r05}type", b"f"],
        &[b"HEXISTS", b"{r05}type", b"f"],
        &[b"HLEN", b"{r05}type"],
        &[b"HGETALL", b"{r05}type"],
        &[b"LPUSH", b"{r05}type", b"v"],
        &[b"RPUSH", b"{r05}type", b"v"],
        &[b"LPOP", b"{r05}type"],
        &[b"RPOP", b"{r05}type"],
        &[b"LLEN", b"{r05}type"],
        &[b"LRANGE", b"{r05}type", b"0", b"-1"],
        &[b"SADD", b"{r05}type", b"v"],
        &[b"SREM", b"{r05}type", b"v"],
        &[b"SISMEMBER", b"{r05}type", b"v"],
        &[b"SCARD", b"{r05}type"],
        &[b"SMEMBERS", b"{r05}type"],
        &[b"MGET", b"{r05}type", b"{r05}absent"],
        &[b"SET", b"{r05}type", b"v", b"GET", b"NX"],
    ];
    for creator in creators {
        for reader in readers {
            pair.run(&[b"DEL", b"{r05}type"]);
            pair.run(creator);
            pair.run(reader);
        }
        pair.run(&[b"PEXPIRE", b"{r05}type", b"60000"]);
        pair.run(creator);
        // SET limpa TTL; as três coleções o preservam.
        pair.run(&[b"PERSIST", b"{r05}type"]);
        pair.run(&[b"PEXPIRE", b"{r05}type", b"0"]);
        pair.run(&[b"EXISTS", b"{r05}type"]);
    }
}

fn generated(pair: &mut Pair) {
    for seed in [1u64, 42, 0x51de_0500, u64::MAX] {
        let mut state = seed;
        for _ in 0..256 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let member = [(state >> 32) as u8 % 8, 0, 255];
            let value = state.to_le_bytes();
            let args: Vec<&[u8]> = match state % 12 {
                0 => vec![b"HSET", b"{r05}h", &member, &value, &member, &value],
                1 => vec![b"HDEL", b"{r05}h", &member, &member],
                2 => vec![b"HGET", b"{r05}h", &member],
                3 => vec![b"HGETALL", b"{r05}h"],
                4 => vec![b"LPUSH", b"{r05}l", &member, &value],
                5 => vec![b"RPUSH", b"{r05}l", &member],
                6 => vec![b"LPOP", b"{r05}l"],
                7 => vec![b"RPOP", b"{r05}l"],
                8 => vec![b"SADD", b"{r05}s", &member, &member],
                9 => vec![b"SREM", b"{r05}s", &member, &member],
                10 => vec![b"SISMEMBER", b"{r05}s", &member],
                _ => vec![b"SMEMBERS", b"{r05}s"],
            };
            pair.run(&args);
            pair.run(&[b"LRANGE", b"{r05}l", b"-5", b"-1"]);
        }
        pair.run(&[b"HGETALL", b"{r05}h"]);
        pair.run(&[b"SMEMBERS", b"{r05}s"]);
        pair.run(&[b"LRANGE", b"{r05}l", b"0", b"-1"]);
        pair.run(&[b"DEL", b"{r05}h", b"{r05}l", b"{r05}s"]);
    }
}

#[test]
#[ignore = "exige Docker e Redis fixado; execute explicitamente"]
fn collections_match_redis() {
    let redis = redis_reference::RedisReference::start();
    let mut sider = sider_process::SiderProcess::start(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        env!("CARGO_PKG_VERSION"),
    );
    let mut pair = Pair::new(sider.address(), redis.address());
    fixtures(&mut pair);
    generated(&mut pair);
    eprintln!(
        "R05: {} respostas exatas, {} arrays normalizados; total {}",
        pair.exact,
        pair.normalized,
        pair.exact + pair.normalized
    );
    drop(pair);
    sider.assert_alive();
    sider.finish();
    redis.finish();
}
