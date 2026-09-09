//! Corpus R11 entre famílias; reutiliza sockets, oráculo e referência do gate existente.

use super::{Pair, RedisReference, Response, Sequence};
use serde_json::{Value, json};
use std::net::SocketAddr;

pub const SEEDS: [u64; 4] = [1, 42, 0x511d_e011, 0xfeed_face_dead_beef];
const ROUNDS: usize = 40;

fn run(pair: &mut Pair, args: &[&[u8]], context: &str) -> Response {
    pair.exchange(
        &args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>(),
        context,
    )
    .value
}

fn create(pair: &mut Pair, kind: usize, key: &[u8], payload: &[u8], context: &str) {
    match kind {
        0 => run(pair, &[b"SET", key, payload], context),
        1 => run(pair, &[b"HSET", key, b"f\0\xff", payload], context),
        2 => run(pair, &[b"RPUSH", key, payload], context),
        3 => run(pair, &[b"SADD", key, payload], context),
        4 => run(pair, &[b"ZADD", key, b"1.5", payload], context),
        _ => unreachable!(),
    };
}

fn observe(pair: &mut Pair, kind: usize, key: &[u8], payload: &[u8], context: &str) {
    match kind {
        0 => run(pair, &[b"GET", key], context),
        1 => run(pair, &[b"HGET", key, b"f\0\xff"], context),
        2 => run(pair, &[b"LRANGE", key, b"0", b"-1"], context),
        3 => run(pair, &[b"SISMEMBER", key, payload], context),
        4 => run(pair, &[b"ZRANGE", key, b"0", b"-1", b"WITHSCORES"], context),
        _ => unreachable!(),
    };
}

fn typed_ttl(pair: &mut Pair, seed: u64, key: &[u8]) {
    let mut rng = Sequence(seed);
    for round in 0..ROUNDS {
        let kind = round % 5;
        let payload = rng.bytes();
        let context = format!("R11 seed={seed} round={round} type={kind}");
        run(pair, &[b"DEL", key], &context);
        create(pair, kind, key, &payload, &context);
        run(pair, &[b"PEXPIRE", key, b"60000"], &context);
        observe(pair, kind, key, &payload, &context);
        run(pair, &[b"MGET", key, key], &context);
        run(pair, &[b"SET", key, b"ignored", b"GET", b"NX"], &context);
        observe(pair, kind, key, &payload, &context);
        assert_eq!(
            run(pair, &[b"PERSIST", key], &context),
            Response::Integer(1)
        );
        run(pair, &[b"PEXPIRE", key, b"60000"], &context);
        // Rejeição por tipo ou inteiro precisa preservar o prazo anterior.
        if kind == 0 {
            run(pair, &[b"HSET", key, b"f", b"wrong"], &context);
        } else {
            run(pair, &[b"INCR", key], &context);
        }
        assert_eq!(
            run(pair, &[b"PERSIST", key], &context),
            Response::Integer(1)
        );
        run(pair, &[b"PEXPIRE", key, b"60000"], &context);
        run(
            pair,
            &[b"SET", key, b"replacement", b"XX", b"KEEPTTL"],
            &context,
        );
        assert_eq!(
            run(pair, &[b"PERSIST", key], &context),
            Response::Integer(1)
        );
        run(pair, &[b"GET", key], &context);
        run(pair, &[b"PEXPIRE", key, b"0"], &context);
        run(pair, &[b"TTL", key], &context);
        create(pair, (kind + 1) % 5, key, &payload, &context);
        observe(pair, (kind + 1) % 5, key, &payload, &context);
        assert_eq!(run(pair, &[b"TTL", key], &context), Response::Integer(-1));
        run(pair, &[b"PING", &payload], &context);
    }
    run(pair, &[b"DEL", key], "R11 cleanup typed TTL");
}

fn transactions(pair: &mut Pair, other: &mut Pair, seed: u64, key: &[u8], auxiliary: &[u8]) {
    let context = format!("R11 transações seed={seed}");
    let mut rng = Sequence(seed);
    for round in 0..10 {
        let payload = rng.bytes();
        run(pair, &[b"DEL", key, auxiliary], &context);
        create(pair, round % 5, key, &payload, &context);
        run(pair, &[b"PEXPIRE", key, b"60000"], &context);
        run(pair, &[b"WATCH", key], &context);
        run(other, &[b"PEXPIRE", key, b"0"], &context);
        run(pair, &[b"MULTI"], &context);
        run(pair, &[b"SET", auxiliary, b"must-not-exist"], &context);
        assert_eq!(run(pair, &[b"EXEC"], &context), Response::Array(None));
        run(pair, &[b"GET", auxiliary], &context);
        // ABA no tipo: a chave observada retorna a ausente, mas WATCH deve abortar.
        run(pair, &[b"WATCH", key], &context);
        create(other, (round + 1) % 5, key, &payload, &context);
        run(other, &[b"DEL", key], &context);
        run(pair, &[b"MULTI"], &context);
        run(pair, &[b"HSET", key, b"f", b"discarded"], &context);
        assert_eq!(run(pair, &[b"EXEC"], &context), Response::Array(None));
        run(pair, &[b"MULTI"], &context);
        for args in [
            vec![b"SET".as_slice(), key, b"not-an-integer"],
            vec![b"INCR", key],
            vec![b"HSET", key, b"f", b"wrong-type"],
            vec![b"PEXPIRE", key, b"0"],
            vec![b"HSET", key, b"f", payload.as_slice()],
            vec![b"PEXPIRE", key, b"60000"],
            vec![b"HGET", key, b"f"],
        ] {
            run(pair, &args, &context);
        }
        run(pair, &[b"EXEC"], &context);
        assert_eq!(
            run(pair, &[b"PERSIST", key], &context),
            Response::Integer(1)
        );
        run(pair, &[b"HGET", key, b"f"], &context);
        // DISCARD e UNWATCH não podem alterar o valor ou conservar uma observação.
        run(pair, &[b"WATCH", key], &context);
        run(pair, &[b"UNWATCH"], &context);
        run(other, &[b"HSET", key, b"f", b"changed"], &context);
        run(pair, &[b"MULTI"], &context);
        run(pair, &[b"SET", key, b"discarded"], &context);
        run(pair, &[b"DISCARD"], &context);
        run(pair, &[b"HGET", key, b"f"], &context);
        run(pair, &[b"MULTI"], &context);
        run(pair, &[b"HGET", key, b"f"], &context);
        run(pair, &[b"EXEC"], &context);
    }
    run(pair, &[b"DEL", key, auxiliary], "R11 cleanup transactions");
}

fn pubsub(pair: &mut Pair, subscriber: &mut Pair, seed: u64, key: &[u8]) {
    let mut channel = format!("r11:{seed}:channel:").into_bytes();
    channel.extend_from_slice(b"\0\xff");
    let context = format!("R11 Pub/Sub seed={seed}");
    run(subscriber, &[b"SUBSCRIBE", &channel], &context);
    let mut rng = Sequence(seed);
    for _ in 0..4 {
        let payload = rng.bytes();
        run(pair, &[b"DEL", key], &context);
        run(pair, &[b"MULTI"], &context);
        for args in [
            vec![b"SET".as_slice(), key, payload.as_slice()],
            vec![b"HSET", key, b"wrong", b"type"],
            vec![b"PUBLISH", channel.as_slice(), payload.as_slice()],
            vec![b"PEXPIRE", key, b"0"],
            vec![b"HSET", key, b"f", payload.as_slice()],
        ] {
            run(pair, &args, &context);
        }
        run(pair, &[b"EXEC"], &context);
        subscriber.compare(None, &context);
        run(pair, &[b"HGET", key, b"f"], &context);
        run(pair, &[b"MULTI"], &context);
        run(pair, &[b"SUBSCRIBE", &channel], &context);
        run(pair, &[b"PUBLISH", &channel, &payload], &context);
        run(pair, &[b"PING", &payload], &context);
        run(pair, &[b"EXEC"], &context);
        pair.compare(None, &context);
        subscriber.compare(None, &context);
        run(pair, &[b"UNSUBSCRIBE", &channel], &context);
        run(pair, &[b"GET", key], &context);
    }
    run(subscriber, &[b"UNSUBSCRIBE", &channel], &context);
    run(subscriber, &[b"PING"], &context);
    run(pair, &[b"DEL", key], "R11 cleanup Pub/Sub");
}

fn cli(reference: &RedisReference, sider: SocketAddr) -> u64 {
    let cases: &[&[&str]] = &[
        &["DEL", "r11-cli"],
        &["HSET", "r11-cli", "f", "v"],
        &["HGET", "r11-cli", "f"],
        &["PEXPIRE", "r11-cli", "0"],
        &["RPUSH", "r11-cli", "a", "b"],
        &["LRANGE", "r11-cli", "0", "-1"],
        &["SET", "r11-cli", "string"],
        &["SADD", "r11-cli", "wrong-type"],
        &["DEL", "r11-cli"],
        &["ZADD", "r11-cli", "1.5", "member"],
        &["ZRANGE", "r11-cli", "0", "-1", "WITHSCORES"],
        &["PEXPIRE", "r11-cli", "0"],
        &["SADD", "r11-cli", "member"],
        &["SISMEMBER", "r11-cli", "member"],
        &["DEL", "r11-cli"],
        &["TTL", "r11-cli"],
    ];
    for args in cases {
        assert_eq!(
            reference.cli_at(sider, args),
            reference.cli(args),
            "R11 CLI {args:?}"
        );
    }
    cases.len() as u64
}

pub fn audit(sider: SocketAddr, reference: &RedisReference, require_cli: bool) -> (u64, Value) {
    let mut pair = Pair::new(sider, reference.address());
    let mut other = Pair::new(sider, reference.address());
    let mut subscriber = Pair::new(sider, reference.address());
    for seed in SEEDS {
        let mut key = format!("r11:{{{seed}}}:key:").into_bytes();
        key.extend_from_slice(b"\0\xff");
        let auxiliary = format!("r11:{{{seed}}}:auxiliary").into_bytes();
        typed_ttl(&mut pair, seed, &key);
        transactions(&mut pair, &mut other, seed, &key, &auxiliary);
        pubsub(&mut pair, &mut subscriber, seed, &key);
    }
    let binary = pair.finish() + other.finish() + subscriber.finish();
    let cli_cases = if require_cli {
        cli(reference, sider)
    } else {
        0
    };
    (
        binary + cli_cases,
        json!({
            "task":"R11-02","seeds":SEEDS,"typed_rounds_per_seed":ROUNDS,
            "transaction_rounds_per_seed":10,"pubsub_rounds_per_seed":4,
            "binary_comparisons":binary,"cli_cases":cli_cases,"checks":binary+cli_cases,
            "time_model":"PEXPIRE 0 e TTL persistente/ausente, sem tolerância temporal nova",
            "normalization":"nenhuma; leituras sem ordem pública são evitadas neste corpus",
            "cleanup_confirmed":true,
        }),
    )
}
