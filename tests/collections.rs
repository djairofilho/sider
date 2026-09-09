//! Coleções tipadas, quota, TTL e mutações resolvidas persistíveis.

#![forbid(unsafe_code)]

use bytes::Bytes;
use sider::command::{Command, ExecutionError, Reply, RequestError, parse};
use sider::persistence::format::{self, Limits, Next, Record};
use sider::resp::Frame;
use sider::storage::{Mutation, Store, StoreConfig, SystemClock, Value};
use std::sync::Arc;
use std::time::Duration;

fn command(args: &[&[u8]]) -> Result<Command, RequestError> {
    parse(Frame::Array(Some(
        args.iter()
            .map(|arg| Frame::Bulk(Some(Bytes::copy_from_slice(arg))))
            .collect(),
    )))
}
fn run(store: &mut Store, args: &[&[u8]]) -> Reply {
    store.execute(command(args).unwrap())
}
fn bulk(value: &[u8]) -> Reply {
    Reply::Bulk(Some(Bytes::copy_from_slice(value)))
}

#[test]
fn hash_counts_new_fields_and_preserves_binary_values() {
    let mut store = Store::new();
    assert_eq!(
        run(
            &mut store,
            &[
                b"hSeT", b"h\0", b"\xff", b"first", b"", b"", b"\xff", b"last"
            ]
        ),
        Reply::Integer(2)
    );
    assert_eq!(run(&mut store, &[b"HGET", b"h\0", b"\xff"]), bulk(b"last"));
    assert_eq!(
        run(&mut store, &[b"HGET", b"h\0", b"missing"]),
        Reply::Bulk(None)
    );
    assert_eq!(
        run(&mut store, &[b"HEXISTS", b"h\0", b""]),
        Reply::Integer(1)
    );
    assert_eq!(run(&mut store, &[b"HLEN", b"h\0"]), Reply::Integer(2));
    assert_eq!(
        run(&mut store, &[b"HGETALL", b"h\0"]),
        Reply::Array(vec![bulk(b""), bulk(b""), bulk(b"\xff"), bulk(b"last")])
    );
    assert_eq!(
        run(&mut store, &[b"HDEL", b"h\0", b"\xff", b"\xff", b"missing"]),
        Reply::Integer(1)
    );
    assert_eq!(run(&mut store, &[b"HDEL", b"h\0", b""]), Reply::Integer(1));
    assert_eq!(run(&mut store, &[b"EXISTS", b"h\0"]), Reply::Integer(0));
    assert_eq!(store.used_bytes(), 0);
}

#[test]
fn missing_hash_reads_do_not_create_entries() {
    let mut store = Store::new();
    for args in [
        &[b"HLEN".as_slice(), b"h"][..],
        &[b"HEXISTS".as_slice(), b"h", b"f"],
        &[b"HDEL".as_slice(), b"h", b"f"],
    ] {
        assert_eq!(run(&mut store, args), Reply::Integer(0));
    }
    assert_eq!(run(&mut store, &[b"HGET", b"h", b"f"]), Reply::Bulk(None));
    assert_eq!(run(&mut store, &[b"HGETALL", b"h"]), Reply::Array(vec![]));
    assert!(store.is_empty());
}

#[test]
fn wrongtype_get_mget_and_set_get_follow_string_contracts() {
    let mut store = Store::new();
    run(&mut store, &[b"HSET", b"h", b"f", b"v"]);
    for args in [
        &[b"GET".as_slice(), b"h"][..],
        &[b"INCR".as_slice(), b"h"],
        &[b"SET".as_slice(), b"h", b"new", b"GET", b"NX"],
        &[b"SET".as_slice(), b"h", b"new", b"GET", b"XX"],
    ] {
        assert_eq!(
            run(&mut store, args),
            Reply::Error(ExecutionError::WrongType)
        );
    }
    assert_eq!(
        run(&mut store, &[b"MGET", b"h", b"missing"]),
        Reply::Array(vec![Reply::Bulk(None), Reply::Bulk(None)])
    );
    assert_eq!(run(&mut store, &[b"HGET", b"h", b"f"]), bulk(b"v"));
    assert_eq!(run(&mut store, &[b"SET", b"h", b"string"]), Reply::Ok);
    for args in [
        &[b"HGET".as_slice(), b"h", b"f"][..],
        &[b"HSET".as_slice(), b"h", b"f", b"v"],
        &[b"HDEL".as_slice(), b"h", b"f"],
        &[b"HLEN".as_slice(), b"h"],
        &[b"HEXISTS".as_slice(), b"h", b"f"],
        &[b"HGETALL".as_slice(), b"h"],
    ] {
        assert_eq!(
            run(&mut store, args),
            Reply::Error(ExecutionError::WrongType)
        );
    }
    assert_eq!(run(&mut store, &[b"GET", b"h"]), bulk(b"string"));
}

#[tokio::test(start_paused = true)]
async fn hash_quota_rejects_complete_batch_and_mutations_keep_ttl() {
    let mut store = Store::with_config(
        StoreConfig {
            max_dataset_bytes: 195,
        },
        Arc::new(SystemClock),
    )
    .unwrap();
    assert_eq!(
        run(&mut store, &[b"HSET", b"h", b"f", b"v"]),
        Reply::Integer(1)
    );
    assert_eq!(store.used_bytes(), 195);
    run(&mut store, &[b"PEXPIRE", b"h", b"100"]);
    assert_eq!(
        run(&mut store, &[b"HSET", b"h", b"f", b"x", b"g", b"v"]),
        Reply::Error(ExecutionError::OutOfMemory)
    );
    assert_eq!(run(&mut store, &[b"HGET", b"h", b"f"]), bulk(b"v"));
    assert_eq!(
        run(
            &mut store,
            &[b"HSET", b"h", b"f", b"huge abandoned", b"f", b"x"]
        ),
        Reply::Integer(0)
    );
    assert_eq!(run(&mut store, &[b"PTTL", b"h"]), Reply::Integer(100));
    assert_eq!(store.used_bytes(), 195);
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_eq!(store.expire_due(64), 1);
    assert_eq!(store.used_bytes(), 0);
}

#[test]
fn prepared_hash_does_not_mutate_snapshot_and_aof_recovers_the_type() {
    let mut store = Store::new();
    run(&mut store, &[b"HSET", b"h", b"f", b"old"]);
    let original = store.snapshot();
    let prepared = store.prepare(command(&[b"HSET", b"h", b"f", b"new", b"g", b"\0\xff"]).unwrap());
    assert_eq!(store.snapshot(), original);
    let encoded = format::encode(
        &Record::Batch {
            sequence: 1,
            batch: prepared.batch.clone(),
        },
        Limits::default(),
    )
    .unwrap();
    let Next::Record(Record::Batch { batch, .. }) =
        format::read_record(encoded.as_slice(), Limits::default()).unwrap()
    else {
        panic!("lote AOF esperado");
    };
    let mut recovered = Store::new();
    recovered.replay(&batch.mutations).unwrap();
    store.apply(prepared);
    assert_eq!(store.snapshot(), recovered.snapshot());
    let Mutation::Put {
        value: Value::Hash(fields),
        ..
    } = &original[0]
    else {
        panic!("hash esperado");
    };
    assert_eq!(fields.get(b"f".as_slice()).unwrap(), b"old".as_slice());
    assert_eq!(run(&mut recovered, &[b"HGET", b"h", b"f"]), bulk(b"new"));
}

#[test]
fn hash_arity_is_validated_before_any_effect() {
    for (args, expected) in [
        (&[b"HSET".as_slice(), b"h", b"f"][..], "hset"),
        (&[b"HGET".as_slice(), b"h"][..], "hget"),
        (&[b"HDEL".as_slice(), b"h"][..], "hdel"),
        (&[b"HEXISTS".as_slice()][..], "hexists"),
        (&[b"HLEN".as_slice(), b"h", b"extra"][..], "hlen"),
        (&[b"HGETALL".as_slice()][..], "hgetall"),
    ] {
        assert_eq!(command(args), Err(RequestError::WrongArity(expected)));
    }
}

#[test]
fn lists_keep_insertion_order_and_normalize_inclusive_ranges() {
    let mut store = Store::new();
    assert_eq!(
        run(&mut store, &[b"LPUSH", b"l", b"a", b"b", b"\0\xff"]),
        Reply::Integer(3)
    );
    assert_eq!(
        run(&mut store, &[b"RPUSH", b"l", b"c", b"d"]),
        Reply::Integer(5)
    );
    for (start, stop, expected) in [
        (
            "0",
            "-1",
            vec![
                bulk(b"\0\xff"),
                bulk(b"b"),
                bulk(b"a"),
                bulk(b"c"),
                bulk(b"d"),
            ],
        ),
        ("-3", "-2", vec![bulk(b"a"), bulk(b"c")]),
        (
            "-9223372036854775808",
            "1",
            vec![bulk(b"\0\xff"), bulk(b"b")],
        ),
        ("4", "9223372036854775807", vec![bulk(b"d")]),
        ("0", "-6", vec![]),
        ("5", "8", vec![]),
        ("3", "1", vec![]),
    ] {
        assert_eq!(
            run(
                &mut store,
                &[b"LRANGE", b"l", start.as_bytes(), stop.as_bytes()]
            ),
            Reply::Array(expected)
        );
    }
    assert_eq!(run(&mut store, &[b"LPOP", b"l"]), bulk(b"\0\xff"));
    assert_eq!(run(&mut store, &[b"RPOP", b"l"]), bulk(b"d"));
    assert_eq!(run(&mut store, &[b"LLEN", b"l"]), Reply::Integer(3));
    run(&mut store, &[b"LPOP", b"l"]);
    run(&mut store, &[b"LPOP", b"l"]);
    run(&mut store, &[b"LPOP", b"l"]);
    assert!(store.is_empty());
    assert_eq!(store.used_bytes(), 0);
    assert_eq!(run(&mut store, &[b"RPOP", b"l"]), Reply::Bulk(None));
    assert_eq!(
        run(&mut store, &[b"LRANGE", b"l", b"0", b"-1"]),
        Reply::Array(vec![])
    );
    assert!(store.is_empty());
}

#[tokio::test(start_paused = true)]
async fn list_push_is_atomic_at_quota_and_pop_preserves_ttl() {
    let mut store = Store::with_config(
        StoreConfig {
            max_dataset_bytes: 195,
        },
        Arc::new(SystemClock),
    )
    .unwrap();
    assert_eq!(
        run(&mut store, &[b"RPUSH", b"l", b"a", b"b"]),
        Reply::Integer(2)
    );
    run(&mut store, &[b"PEXPIRE", b"l", b"100"]);
    assert_eq!(
        run(&mut store, &[b"LPUSH", b"l", b"c", b"d"]),
        Reply::Error(ExecutionError::OutOfMemory)
    );
    assert_eq!(
        run(&mut store, &[b"LRANGE", b"l", b"0", b"-1"]),
        Reply::Array(vec![bulk(b"a"), bulk(b"b")])
    );
    assert_eq!(run(&mut store, &[b"LPOP", b"l"]), bulk(b"a"));
    assert_eq!(store.used_bytes(), 162);
    assert_eq!(run(&mut store, &[b"PTTL", b"l"]), Reply::Integer(100));
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_eq!(run(&mut store, &[b"LLEN", b"l"]), Reply::Integer(0));
    assert_eq!(store.used_bytes(), 0);
}

#[test]
fn lists_validate_type_and_arguments_and_roundtrip_resolved_aof() {
    let mut store = Store::new();
    run(&mut store, &[b"SET", b"l", b"v"]);
    for args in [
        &[b"LPUSH".as_slice(), b"l", b"v"][..],
        &[b"RPUSH", b"l", b"v"],
        &[b"LPOP", b"l"],
        &[b"RPOP", b"l"],
        &[b"LLEN", b"l"],
        &[b"LRANGE", b"l", b"0", b"-1"],
    ] {
        assert_eq!(
            run(&mut store, args),
            Reply::Error(ExecutionError::WrongType)
        );
    }
    assert_eq!(
        command(&[b"LRANGE", b"l", b"+0", b"1"]),
        Err(RequestError::InvalidInteger)
    );
    assert_eq!(
        command(&[b"LPOP", b"l", b"1"]),
        Err(RequestError::WrongArity("lpop"))
    );
    run(&mut store, &[b"DEL", b"l"]);
    let prepared = store.prepare(command(&[b"LPUSH", b"l", b"a", b"\0\xff", b"b"]).unwrap());
    assert!(store.is_empty());
    let record = Record::Batch {
        sequence: 1,
        batch: prepared.batch.clone(),
    };
    let encoded = format::encode(&record, Limits::default()).unwrap();
    assert_eq!(
        format::read_record(encoded.as_slice(), Limits::default()).unwrap(),
        Next::Record(record)
    );
    let mut recovered = Store::new();
    recovered.replay(&prepared.batch.mutations).unwrap();
    store.apply(prepared);
    assert_eq!(store.snapshot(), recovered.snapshot());
    assert_eq!(
        run(&mut recovered, &[b"LRANGE", b"l", b"0", b"-1"]),
        Reply::Array(vec![bulk(b"b"), bulk(b"\0\xff"), bulk(b"a")])
    );
}
