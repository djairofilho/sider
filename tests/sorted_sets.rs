//! Scores, ordenação, quota, TTL e pós-imagens de sorted sets.
#![forbid(unsafe_code)]

use bytes::Bytes;
use sider::command::{Command, ExecutionError, Reply, RequestError, Score, parse};
use sider::persistence::format::{self, Limits, Next, Record};
use sider::resp::Frame;
use sider::storage::{SortedSet, Store, StoreConfig, SystemClock};
use std::collections::HashMap;
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
fn score_parser_and_printing_cover_decimal_hex_and_ieee_boundaries() {
    for (input, expected) in [
        ("-0", "0"),
        ("-0.0", "0"),
        ("+0", "0"),
        ("0e-9999", "0"),
        ("inf", "inf"),
        ("+Infinity", "inf"),
        ("-INF", "-inf"),
        ("1e-7", "1e-7"),
        ("1e-6", "0.000001"),
        ("1e20", "1e+20"),
        ("1e23", "99999999999999990000000"),
        ("4.9406564584124654e-324", "5e-324"),
        ("0.00012345678901234567", "1.2345678901234567e-4"),
        ("01.5", "1.5"),
        (".5", "0.5"),
        ("1.", "1"),
        ("0x1p0", "1"),
        ("0x1.0000000000000800001p0", "1.0000000000000002"),
        ("-0X1.8p1", "-3"),
        ("0x.8", "0.5"),
        ("0x1p-1074", "5e-324"),
        ("0x1.fffffffffffffp1023", "1.7976931348623157e+308"),
    ] {
        assert_eq!(
            Score::parse(input.as_bytes()).unwrap().to_bytes(),
            expected,
            "{input}"
        );
    }
    for input in [
        "",
        "NaN",
        "nan(1)",
        "1e309",
        "1e-9999",
        "0x1p-1075",
        "0x1p1024",
        "1 ",
        " 1",
        "--1",
        "0x",
        "0xp0",
        "0x1p",
        "0x1p1p1",
        "0x1.2.3",
        "0x1 p0",
        "1_000",
    ] {
        assert!(Score::parse(input.as_bytes()).is_none(), "{input}");
    }
    assert!(Score::new(f64::NAN).is_none());
    assert_eq!(Score::new(-0.0), Score::new(0.0));
}

#[test]
fn sorted_set_indices_agree_after_generated_updates_and_removals() {
    let mut sorted = SortedSet::default();
    let mut expected = HashMap::new();
    let mut seed = 0x51de_0601u64;
    for step in 0..4096 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let member = Bytes::from(vec![(seed >> 32) as u8 % 64, 0, 255]);
        if step % 3 == 0 {
            assert_eq!(sorted.remove(&member), expected.remove(&member).is_some());
        } else {
            let score = (seed % 17) as f64 - 8.0;
            let previous = expected.insert(member.clone(), score);
            assert_eq!(
                sorted.insert(member, Score::new(score).unwrap()),
                (previous.is_none(), previous != Some(score))
            );
        }
        let mut ordered: Vec<_> = expected
            .iter()
            .map(|(member, score)| (*score, member.clone()))
            .collect();
        ordered.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then_with(|| a.1.cmp(&b.1)));
        let actual: Vec<_> = sorted
            .iter()
            .map(|(score, member)| (score.get(), member.clone()))
            .collect();
        assert_eq!(actual, ordered, "step {step}");
        assert_eq!(sorted.len(), expected.len());
        for (member, score) in &expected {
            assert_eq!(sorted.get(member).unwrap().get(), *score);
        }
    }
}

#[test]
fn score_format_roundtrips_ieee_exponents_and_neighboring_mantissas() {
    for exponent in 0..2047u64 {
        for mantissa in [
            0,
            1,
            2,
            (1u64 << 51) - 1,
            1u64 << 51,
            (1u64 << 52) - 2,
            (1u64 << 52) - 1,
        ] {
            for sign in [0, 1u64 << 63] {
                let value = f64::from_bits(sign | (exponent << 52) | mantissa);
                let score = Score::new(value).unwrap();
                let rendered = score.to_bytes();
                assert_eq!(
                    Score::parse(&rendered),
                    Some(score),
                    "bits={:016x}, texto={rendered:?}",
                    value.to_bits()
                );
            }
        }
    }
}

#[test]
fn zadd_validates_all_pairs_and_preserves_binary_order_on_updates() {
    let mut store = Store::new();
    assert_eq!(
        run(
            &mut store,
            &[
                b"ZADD", b"z", b"1", b"\xff", b"1", b"", b"-inf", b"lo", b"inf", b"hi", b"2",
                b"\xff"
            ]
        ),
        Reply::Integer(4)
    );
    assert_eq!(
        run(&mut store, &[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"]),
        Reply::Array(vec![
            bulk(b"lo"),
            bulk(b"-inf"),
            bulk(b""),
            bulk(b"1"),
            bulk(b"\xff"),
            bulk(b"2"),
            bulk(b"hi"),
            bulk(b"inf")
        ])
    );
    assert_eq!(
        run(&mut store, &[b"ZADD", b"z", b"1", b"\xff"]),
        Reply::Integer(0)
    );
    assert_eq!(
        run(&mut store, &[b"ZRANGE", b"z", b"-3", b"-2"]),
        Reply::Array(vec![bulk(b""), bulk(b"\xff")])
    );
    let before = store.snapshot();
    assert_eq!(
        command(&[b"ZADD", b"z", b"0", b"lo", b"NaN", b"bad"]),
        Err(RequestError::InvalidFloat)
    );
    assert_eq!(store.snapshot(), before);
    assert_eq!(
        run(
            &mut store,
            &[b"ZREM", b"z", b"lo", b"lo", b"hi", b"", b"\xff"]
        ),
        Reply::Integer(4)
    );
    assert!(store.is_empty());
    assert_eq!(run(&mut store, &[b"ZCARD", b"z"]), Reply::Integer(0));
    assert_eq!(
        run(&mut store, &[b"ZSCORE", b"z", b"missing"]),
        Reply::Bulk(None)
    );
    assert_eq!(
        run(&mut store, &[b"ZRANGE", b"z", b"0", b"-1"]),
        Reply::Array(vec![])
    );
    assert!(store.is_empty());
}

#[tokio::test(start_paused = true)]
async fn sorted_set_quota_rejects_batch_and_preserves_ttl_and_original_scores() {
    let mut store = Store::with_config(
        StoreConfig {
            max_dataset_bytes: 226,
        },
        Arc::new(SystemClock),
    )
    .unwrap();
    run(&mut store, &[b"ZADD", b"z", b"1", b"a"]);
    assert_eq!(store.used_bytes(), 226);
    run(&mut store, &[b"PEXPIRE", b"z", b"100"]);
    assert_eq!(
        run(&mut store, &[b"ZADD", b"z", b"2", b"a", b"3", b"b"]),
        Reply::Error(ExecutionError::OutOfMemory)
    );
    assert_eq!(run(&mut store, &[b"ZSCORE", b"z", b"a"]), bulk(b"1"));
    assert_eq!(
        run(&mut store, &[b"ZADD", b"z", b"2", b"a", b"-0", b"a"]),
        Reply::Integer(0)
    );
    assert_eq!(run(&mut store, &[b"PTTL", b"z"]), Reply::Integer(100));
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_eq!(store.expire_due(1), 1);
    assert_eq!(store.used_bytes(), 0);
}

#[test]
fn sorted_sets_reject_wrongtype_and_unsupported_options() {
    let mut store = Store::new();
    run(&mut store, &[b"SET", b"z", b"v"]);
    for args in [
        &[b"ZADD".as_slice(), b"z", b"1", b"m"][..],
        &[b"ZREM", b"z", b"m"],
        &[b"ZCARD", b"z"],
        &[b"ZSCORE", b"z", b"m"],
        &[b"ZRANGE", b"z", b"0", b"-1"],
    ] {
        assert_eq!(
            run(&mut store, args),
            Reply::Error(ExecutionError::WrongType)
        );
    }
    for args in [
        &[b"ZADD".as_slice(), b"z", b"NX", b"1", b"m"][..],
        &[b"ZRANGE", b"z", b"0", b"1", b"REV"],
        &[b"ZRANGE", b"z", b"0", b"1", b"BYSCORE"],
        &[b"ZRANGE", b"z", b"0", b"1", b"WITHSCORES", b"extra"],
    ] {
        assert!(command(args).is_err());
    }
    assert_eq!(
        command(&[b"ZRANGE", b"z", b"9223372036854775808", b"0"]),
        Err(RequestError::InvalidInteger)
    );
}

#[test]
fn aof_preserves_score_bits_and_rejects_nan_even_with_valid_checksum() {
    let mut store = Store::new();
    run(
        &mut store,
        &[b"ZADD", b"z", b"1e23", b"a", b"5e-324", b"b", b"inf", b"c"],
    );
    let snapshot = store.snapshot();
    let record = Record::Snapshot(snapshot[0].clone());
    let mut encoded = format::encode(&record, Limits::default()).unwrap();
    assert_eq!(
        format::read_record(encoded.as_slice(), Limits::default()).unwrap(),
        Next::Record(record)
    );
    let mut recovered = Store::new();
    recovered.replay(&snapshot).unwrap();
    assert_eq!(
        run(&mut store, &[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"]),
        run(
            &mut recovered,
            &[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"]
        )
    );
    let score_position = encoded.len() - 17;
    encoded[score_position..score_position + 8].copy_from_slice(&f64::NAN.to_bits().to_le_bytes());
    let checksum = format::checksum(&encoded[12..]);
    encoded[8..12].copy_from_slice(&checksum.to_le_bytes());
    assert!(format::read_record(encoded.as_slice(), Limits::default()).is_err());
}
