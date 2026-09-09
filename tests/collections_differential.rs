//! Collection differential tests: normalized pairs and sets, lists compared byte for byte.

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
            if let (Response::Array(Some(actual)), Response::Array(Some(expected))) =
                (&actual.value, &expected.value)
            {
                assert_eq!(actual.len(), expected.len(), "length: {args:?}");
                for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
                    assert_eq!(actual, expected, "elemento {index}: {args:?}");
                }
            }
            assert_eq!(actual, expected, "{args:?}");
            self.exact += 1;
        }
        actual.value
    }
}

fn normalize(value: &Response, pairs: bool) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let Response::Array(Some(values)) = value else {
        panic!("expected array: {value:?}")
    };
    let bulk = |value: &Response| match value {
        Response::Bulk(Some(value)) => value.clone(),
        _ => panic!("expected non-null bulk: {value:?}"),
    };
    let mut result = BTreeMap::new();
    if pairs {
        assert_eq!(values.len() % 2, 0, "complete pairs");
        for pair in values.chunks_exact(2) {
            assert!(
                result.insert(bulk(&pair[0]), bulk(&pair[1])).is_none(),
                "repeated field"
            );
        }
    } else {
        for value in values {
            assert!(
                result.insert(bulk(value), vec![]).is_none(),
                "repeated member"
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
        // SET clears TTL; all three collections preserve it.
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
#[ignore = "requires Docker and pinned Redis; run explicitly"]
fn collections_match_redis() {
    assert!(collections_cases().0 > 0);
}

fn reference() -> redis_reference::RedisReference {
    match std::env::var("SIDER_TEST_RUNNER_CONTAINER") {
        Ok(id) => redis_reference::RedisReference::start_shared(&id),
        Err(_) => redis_reference::RedisReference::start(),
    }
}

fn collections_cases() -> (usize, usize) {
    let redis = reference();
    let mut sider = sider_process::SiderProcess::start(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        env!("CARGO_PKG_VERSION"),
    );
    let mut pair = Pair::new(sider.address(), redis.address());
    fixtures(&mut pair);
    generated(&mut pair);
    eprintln!(
        "R05: {} exact responses, {} normalized arrays; total {}",
        pair.exact,
        pair.normalized,
        pair.exact + pair.normalized
    );
    let counts = (pair.exact, pair.normalized);
    drop(pair);
    sider.assert_alive();
    sider.finish();
    redis.finish();
    counts
}

fn sorted_fixtures(pair: &mut Pair) {
    for args in [
        vec![b"ZCARD".as_slice(), b"{r06}z"],
        vec![b"ZSCORE", b"{r06}z", b"missing"],
        vec![b"ZRANGE", b"{r06}z", b"0", b"-1", b"WITHSCORES"],
        vec![b"ZREM", b"{r06}z", b"missing"],
        vec![
            b"ZADD", b"{r06}z", b"-inf", b"lo", b"inf", b"hi", b"1", b"\xff", b"1", b"", b"1",
            b"\0",
        ],
        vec![b"ZRANGE", b"{r06}z", b"0", b"-1", b"WITHSCORES"],
        vec![
            b"ZADD", b"{r06}z", b"2", b"\xff", b"1", b"\xff", b"3", b"new", b"4", b"new",
        ],
        vec![b"ZRANGE", b"{r06}z", b"-3", b"-2"],
        vec![
            b"ZRANGE",
            b"{r06}z",
            b"-9223372036854775808",
            b"9223372036854775807",
        ],
        vec![b"ZRANGE", b"{r06}z", b"0", b"-100"],
        vec![b"ZRANGE", b"{r06}z", b"100", b"200"],
        vec![b"ZRANGE", b"{r06}z", b"3", b"1"],
        vec![b"ZRANGE", b"{r06}z", b"+1", b"2"],
        vec![b"ZRANGE", b"{r06}z", b"0", b"9223372036854775808"],
        vec![b"ZRANGE", b"{r06}z", b"0", b"2", b"invalid"],
        vec![b"ZADD", b"{r06}z", b"0", b"lo", b"NaN", b"bad"],
        vec![b"ZRANGE", b"{r06}z", b"0", b"-1", b"WITHSCORES"],
        vec![
            b"ZREM", b"{r06}z", b"lo", b"lo", b"hi", b"new", b"\xff", b"", b"\0",
        ],
        vec![b"EXISTS", b"{r06}z"],
    ] {
        pair.run(&args);
    }
    for name in [b"ZADD".as_slice(), b"ZREM", b"ZCARD", b"ZSCORE", b"ZRANGE"] {
        pair.run(&[name]);
    }
    for score in [
        "-0",
        "-0.0",
        "+0",
        "0e-9999",
        "1e-9999",
        "inf",
        "+Infinity",
        "-INF",
        "NaN",
        "nan(1)",
        "1e309",
        "-1e309",
        "1e-7",
        "1e-6",
        "1e-5",
        "1e19",
        "1e20",
        "1e23",
        "4.9406564584124654e-324",
        "2.2250738585072014e-308",
        "1.7976931348623157e308",
        "9007199254740991",
        "9007199254740992",
        "9007199254740993",
        "4611686018427387904",
        "4611686018427387905",
        "0.00012345678901234567",
        "123456789012345.67",
        "01.5",
        ".5",
        "1.",
        "",
        " 1",
        "1 ",
        "1_000",
        "0x1p0",
        "-0x1.8p1",
        "0x.8",
        "0x1p-1074",
        "0x1p-1075",
        "0x1.fffffffffffffp1023",
        "0x1.00000000000008p0",
        "0x1.0000000000000800001p0",
        "0x1.fffffffffffff8p0",
        "0x0.fffffffffffff8p-1022",
        "0x1p99999999999999999999999999999999",
        "0x0p99999999999999999999999999999999",
        "0x1p",
        "0xp0",
        "0x1.2.3",
        "0x1p1p1",
    ] {
        pair.run(&[b"DEL", b"{r06}score"]);
        pair.run(&[b"ZADD", b"{r06}score", score.as_bytes(), b"m"]);
        pair.run(&[b"ZSCORE", b"{r06}score", b"m"]);
        pair.run(&[b"ZRANGE", b"{r06}score", b"0", b"-1", b"WITHSCORES"]);
    }
    for creator in [
        vec![b"SET".as_slice(), b"{r06}type", b"v"],
        vec![b"HSET", b"{r06}type", b"f", b"v"],
        vec![b"LPUSH", b"{r06}type", b"v"],
        vec![b"SADD", b"{r06}type", b"v"],
    ] {
        pair.run(&[b"DEL", b"{r06}type"]);
        pair.run(&creator);
        for args in [
            vec![b"ZADD".as_slice(), b"{r06}type", b"1", b"m"],
            vec![b"ZREM", b"{r06}type", b"m"],
            vec![b"ZCARD", b"{r06}type"],
            vec![b"ZSCORE", b"{r06}type", b"m"],
            vec![b"ZRANGE", b"{r06}type", b"0", b"-1"],
        ] {
            pair.run(&args);
        }
    }
    pair.run(&[b"DEL", b"{r06}type"]);
    pair.run(&[b"ZADD", b"{r06}type", b"1", b"m"]);
    for args in [
        vec![b"GET".as_slice(), b"{r06}type"],
        vec![b"HGET", b"{r06}type", b"m"],
        vec![b"LLEN", b"{r06}type"],
        vec![b"SCARD", b"{r06}type"],
        vec![b"MGET", b"{r06}type"],
        vec![b"SET", b"{r06}type", b"v", b"GET", b"NX"],
    ] {
        pair.run(&args);
    }
    pair.run(&[b"PEXPIRE", b"{r06}type", b"60000"]);
    pair.run(&[b"ZADD", b"{r06}type", b"2", b"m"]);
    pair.run(&[b"PERSIST", b"{r06}type"]);
    pair.run(&[b"PEXPIRE", b"{r06}type", b"0"]);
    pair.run(&[b"ZCARD", b"{r06}type"]);
}

fn sorted_generated(pair: &mut Pair) {
    for seed in [1u64, 42, 0x51de_0600, u64::MAX] {
        let mut state = seed;
        for step in 0..512 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let member = [(state >> 32) as u8 % 32, 0, 255];
            let number = f64::from_bits(state);
            let score = if step % 3 == 0 {
                ((state % 17) as i64 - 8).to_string()
            } else {
                number.to_string()
            };
            if step % 5 == 0 {
                pair.run(&[b"ZREM", b"{r06}z", &member, &member]);
            } else {
                pair.run(&[
                    b"ZADD",
                    b"{r06}z",
                    score.as_bytes(),
                    &member,
                    score.as_bytes(),
                    &member,
                ]);
            }
            pair.run(&[b"ZSCORE", b"{r06}z", &member]);
            pair.run(&[b"ZCARD", b"{r06}z"]);
            pair.run(&[b"ZRANGE", b"{r06}z", b"0", b"-1", b"WITHSCORES"]);
        }
        pair.run(&[b"DEL", b"{r06}z"]);
    }
}

#[test]
#[ignore = "requires Docker and pinned Redis; run explicitly"]
fn sorted_sets_match_redis() {
    assert!(sorted_cases() > 0);
}

fn sorted_cases() -> usize {
    let redis = reference();
    let mut sider = sider_process::SiderProcess::start(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        env!("CARGO_PKG_VERSION"),
    );
    let mut pair = Pair::new(sider.address(), redis.address());
    sorted_fixtures(&mut pair);
    sorted_generated(&mut pair);
    let mut seed = 0x51de_0602u64;
    for _ in 0..32 {
        pair.run(&[b"DEL", b"{r06}scores"]);
        let mut args = vec![b"ZADD".to_vec(), b"{r06}scores".to_vec()];
        for member in 0..400u32 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let score = f64::from_bits(seed);
            if !score.is_finite() {
                continue;
            }
            args.push(score.to_string().into_bytes());
            args.push(member.to_le_bytes().to_vec());
        }
        pair.run(&args.iter().map(Vec::as_slice).collect::<Vec<_>>());
        pair.run(&[b"ZRANGE", b"{r06}scores", b"0", b"-1", b"WITHSCORES"]);
    }
    assert_eq!(pair.normalized, 0, "sorted sets do not allow normalization");
    eprintln!(
        "R06: {} exact responses, including scores and ordering",
        pair.exact
    );
    let count = pair.exact;
    drop(pair);
    sider.assert_alive();
    sider.finish();
    redis.finish();
    count
}

#[test]
#[ignore = "release gate requires exact context and pinned Redis"]
fn release_types_gate() {
    let context = gate_receipt::GateContext::from_env("types").unwrap();
    let began = std::time::Instant::now();
    let (exact, normalized) = collections_cases();
    context
        .publish(
            (exact + normalized) as u64,
            began.elapsed(),
            serde_json::json!({
                "exact_responses":exact,"normalized_responses":normalized,
                "normalization":"HGETALL pairs and SMEMBERS ordering; all other responses are literal",
                "durability":"the persistence typed_ suite is part of the native/crash/recovery gates"
            }),
        )
        .unwrap();
}

#[test]
#[ignore = "release gate requires exact context and pinned Redis"]
fn release_sorted_sets_gate() {
    let context = gate_receipt::GateContext::from_env("sorted_sets").unwrap();
    let began = std::time::Instant::now();
    let exact = sorted_cases();
    context
        .publish(
            exact as u64,
            began.elapsed(),
            serde_json::json!({
                "exact_responses":exact,"normalized_responses":0,
                "generated_float_batches":32,"samples_per_batch":400,
                "durability":"the persistence typed_ suite is part of the native/crash/recovery gates"
            }),
        )
        .unwrap();
}
