//! Injected time for exact boundaries, conditional options, and bounded cleanup.

#![forbid(unsafe_code)]

use bytes::Bytes;
use sider::command::{ExecutionError, Reply, RequestError, parse};
use sider::resp::Frame;
use sider::storage::{Clock, Store};
use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::time::Instant;

struct TestClock {
    base: Instant,
    elapsed: AtomicU64,
    wall: AtomicI64,
}
impl TestClock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            base: Instant::now(),
            elapsed: AtomicU64::new(0),
            wall: AtomicI64::new(1_000_000),
        })
    }
    fn advance(&self, millis: u64) {
        self.elapsed.fetch_add(millis, Ordering::SeqCst);
    }
}
impl Clock for TestClock {
    fn now(&self) -> Instant {
        self.base + Duration::from_millis(self.elapsed.load(Ordering::SeqCst))
    }
    fn unix_millis(&self) -> i64 {
        self.wall.load(Ordering::SeqCst) + self.elapsed.load(Ordering::SeqCst) as i64
    }
}
fn frame(args: &[&[u8]]) -> Frame {
    Frame::Array(Some(
        args.iter()
            .map(|value| Frame::Bulk(Some(Bytes::copy_from_slice(value))))
            .collect(),
    ))
}
fn execute(store: &mut Store, args: &[&[u8]]) -> Reply {
    store.execute(parse(frame(args)).unwrap())
}
fn bulk(value: &'static [u8]) -> Reply {
    Reply::Bulk(Some(Bytes::from_static(value)))
}

#[test]
fn set_conditions_get_and_keepttl_are_independent() {
    let clock = TestClock::new();
    let mut store = Store::with_clock(clock.clone());
    assert_eq!(
        execute(
            &mut store,
            &[b"SET", b"k", b"old", b"PX", b"2000", b"NX", b"GET"]
        ),
        Reply::Bulk(None)
    );
    assert_eq!(
        execute(&mut store, &[b"SET", b"k", b"blocked", b"NX", b"GET"]),
        bulk(b"old")
    );
    assert_eq!(execute(&mut store, &[b"PTTL", b"k"]), Reply::Integer(2000));
    clock.advance(500);
    assert_eq!(
        execute(
            &mut store,
            &[b"SET", b"k", b"new", b"GET", b"XX", b"KEEPTTL"]
        ),
        bulk(b"old")
    );
    assert_eq!(execute(&mut store, &[b"PTTL", b"k"]), Reply::Integer(1500));
    assert_eq!(
        execute(&mut store, &[b"SET", b"absent", b"v", b"XX", b"GET"]),
        Reply::Bulk(None)
    );
    assert_eq!(
        execute(&mut store, &[b"EXISTS", b"absent"]),
        Reply::Integer(0)
    );
    execute(&mut store, &[b"SET", b"k", b"persistent"]);
    assert_eq!(execute(&mut store, &[b"PTTL", b"k"]), Reply::Integer(-1));
    clock.advance(2000);
    assert_eq!(store.expire_due(100), 0);
    assert_eq!(execute(&mut store, &[b"GET", b"k"]), bulk(b"persistent"));
}

#[test]
fn set_option_syntax_and_invalid_durations_preserve_data_and_deadline() {
    let clock = TestClock::new();
    let mut store = Store::with_clock(clock);
    execute(&mut store, &[b"SET", b"k", b"old", b"EX", b"2"]);
    let invalid: &[(&[&[u8]], RequestError)] = &[
        (&[b"NX", b"XX"], RequestError::Syntax),
        (&[b"EX"], RequestError::Syntax),
        (&[b"EX", b"2", b"PX", b"3"], RequestError::Syntax),
        (&[b"KEEPTTL", b"PX", b"3"], RequestError::Syntax),
        (&[b"PX", b"3", b"KEEPTTL"], RequestError::Syntax),
        (&[b"EX", b"0"], RequestError::InvalidSetExpiry),
        (&[b"PX", b"-1"], RequestError::InvalidSetExpiry),
        (&[b"PX", b"+1"], RequestError::InvalidInteger),
        (
            &[b"EX", b"9223372036854775807"],
            RequestError::InvalidSetExpiry,
        ),
        (&[b"EX", b"bad"], RequestError::InvalidInteger),
        (&[b"XX", b"INVALID"], RequestError::Syntax),
    ];
    for (options, error) in invalid {
        let mut args = vec![b"SET".as_slice(), b"k", b"new"];
        args.extend_from_slice(options);
        assert_eq!(parse(frame(&args)), Err(error.clone()));
        assert_eq!(execute(&mut store, &[b"GET", b"k"]), bulk(b"old"));
        assert_eq!(execute(&mut store, &[b"PTTL", b"k"]), Reply::Integer(2000));
    }
    assert_eq!(
        execute(
            &mut store,
            &[
                b"SET",
                b"absent",
                b"v",
                b"PX",
                b"9223372036854775807",
                b"XX"
            ]
        ),
        Reply::Error(ExecutionError::InvalidSetExpiry)
    );
    assert_eq!(
        execute(
            &mut store,
            &[
                b"SET", b"same", b"v", b"EX", b"invalid", b"EX", b"2", b"NX", b"NX", b"GET", b"GET"
            ]
        ),
        Reply::Bulk(None)
    );
    assert_eq!(
        execute(&mut store, &[b"PTTL", b"same"]),
        Reply::Integer(2000)
    );
}

#[test]
fn exact_expiration_boundary_rounding_and_wall_jump() {
    let clock = TestClock::new();
    let mut store = Store::with_clock(clock.clone());
    execute(&mut store, &[b"SET", b"k", b"v", b"PX", b"1500"]);
    assert_eq!(execute(&mut store, &[b"TTL", b"k"]), Reply::Integer(2));
    clock.advance(1);
    assert_eq!(execute(&mut store, &[b"TTL", b"k"]), Reply::Integer(1));
    clock.wall.store(-1_000_000, Ordering::SeqCst);
    clock.advance(1498);
    assert_eq!(execute(&mut store, &[b"PTTL", b"k"]), Reply::Integer(1));
    clock.advance(1);
    assert_eq!(execute(&mut store, &[b"GET", b"k"]), Reply::Bulk(None));
    assert_eq!(execute(&mut store, &[b"PTTL", b"k"]), Reply::Integer(-2));
    assert!(store.is_empty());
}

#[test]
fn passive_expiration_is_applied_to_all_string_accesses() {
    let clock = TestClock::new();
    let mut store = Store::with_clock(clock.clone());
    for key in [
        b"get".as_slice(),
        b"exists",
        b"mget",
        b"del",
        b"incr",
        b"decr",
        b"set",
        b"persist",
        b"expire",
    ] {
        execute(&mut store, &[b"SET", key, b"10", b"PX", b"10"]);
    }
    clock.advance(10);
    assert_eq!(execute(&mut store, &[b"GET", b"get"]), Reply::Bulk(None));
    assert_eq!(
        execute(&mut store, &[b"EXISTS", b"exists", b"exists"]),
        Reply::Integer(0)
    );
    assert_eq!(
        execute(&mut store, &[b"MGET", b"mget"]),
        Reply::Array(vec![Reply::Bulk(None)])
    );
    assert_eq!(execute(&mut store, &[b"DEL", b"del"]), Reply::Integer(0));
    assert_eq!(execute(&mut store, &[b"INCR", b"incr"]), Reply::Integer(1));
    assert_eq!(execute(&mut store, &[b"DECR", b"decr"]), Reply::Integer(-1));
    assert_eq!(
        execute(&mut store, &[b"SET", b"set", b"v", b"NX"]),
        Reply::Ok
    );
    assert_eq!(
        execute(&mut store, &[b"PERSIST", b"persist"]),
        Reply::Integer(0)
    );
    assert_eq!(
        execute(&mut store, &[b"EXPIRE", b"expire", b"10"]),
        Reply::Integer(0)
    );
}

#[test]
fn persist_updates_and_limited_active_cleanup_never_remove_new_values() {
    let clock = TestClock::new();
    let mut store = Store::with_clock(clock.clone());
    for key in [b"a".as_slice(), b"b", b"c"] {
        execute(&mut store, &[b"SET", key, b"1", b"PX", b"10"]);
    }
    assert_eq!(execute(&mut store, &[b"PERSIST", b"a"]), Reply::Integer(1));
    assert_eq!(execute(&mut store, &[b"PERSIST", b"a"]), Reply::Integer(0));
    assert_eq!(
        execute(&mut store, &[b"PEXPIRE", b"b", b"20"]),
        Reply::Integer(1)
    );
    assert_eq!(execute(&mut store, &[b"INCR", b"b"]), Reply::Integer(2));
    assert_eq!(execute(&mut store, &[b"PTTL", b"b"]), Reply::Integer(20));
    clock.advance(10);
    assert_eq!(store.expire_due(0), 0);
    assert_eq!(store.len(), 3);
    assert_eq!(store.expire_due(1), 1);
    assert_eq!(store.len(), 2);
    assert_eq!(execute(&mut store, &[b"GET", b"b"]), bulk(b"2"));
    execute(&mut store, &[b"MSET", b"b", b"new"]);
    clock.advance(20);
    assert_eq!(store.expire_due(10), 0);
    assert_eq!(execute(&mut store, &[b"GET", b"b"]), bulk(b"new"));
    assert_eq!(
        execute(&mut store, &[b"EXPIRE", b"a", b"0"]),
        Reply::Integer(1)
    );
    assert_eq!(
        execute(&mut store, &[b"PEXPIRE", b"b", b"-1"]),
        Reply::Integer(1)
    );
    assert!(store.is_empty());
}
