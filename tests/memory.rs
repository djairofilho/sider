//! Logical quota and atomic rejection, keeping accounting distinct from RSS.

#![forbid(unsafe_code)]

use bytes::Bytes;
use sider::command::{ExecutionError, Reply, parse};
use sider::resp::Frame;
use sider::storage::{Store, StoreConfig, SystemClock, worker};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

fn command(args: &[&[u8]]) -> sider::command::Command {
    parse(Frame::Array(Some(
        args.iter()
            .map(|arg| Frame::Bulk(Some(Bytes::copy_from_slice(arg))))
            .collect(),
    )))
    .unwrap()
}
fn execute(store: &mut Store, args: &[&[u8]]) -> Reply {
    store.execute(command(args))
}
fn store(limit: usize) -> Store {
    Store::with_config(
        StoreConfig {
            max_dataset_bytes: limit,
        },
        Arc::new(SystemClock),
    )
    .unwrap()
}

#[test]
fn exact_limit_rejects_growth_and_releases_smaller_values_or_deletions() {
    let mut store = store(132);
    assert_eq!(execute(&mut store, &[b"SET", b"k", b"123"]), Reply::Ok);
    assert_eq!(store.used_bytes(), 132);
    assert_eq!(
        execute(&mut store, &[b"SET", b"k", b"1234"]),
        Reply::Error(ExecutionError::OutOfMemory)
    );
    assert_eq!(store.used_bytes(), 132);
    assert_eq!(
        execute(&mut store, &[b"GET", b"k"]),
        Reply::Bulk(Some(Bytes::from_static(b"123")))
    );
    assert_eq!(execute(&mut store, &[b"SET", b"k", b""]), Reply::Ok);
    assert_eq!(store.used_bytes(), 129);
    assert_eq!(
        execute(&mut store, &[b"SET", b"other", b""]),
        Reply::Error(ExecutionError::OutOfMemory)
    );
    assert_eq!(
        execute(&mut store, &[b"DEL", b"k", b"k"]),
        Reply::Integer(1)
    );
    assert_eq!(store.used_bytes(), 0);
    assert_eq!(execute(&mut store, &[b"SET", b"", b"1234"]), Reply::Ok);
    assert_eq!(store.used_bytes(), 132);
}

#[test]
fn mset_validates_final_deduplicated_batch_before_any_effect() {
    let mut store = store(262);
    assert_eq!(
        execute(&mut store, &[b"MSET", b"a", b"1234", b"b", b""]),
        Reply::Ok
    );
    assert_eq!(store.used_bytes(), 262);
    assert_eq!(
        execute(&mut store, &[b"MSET", b"b", b"1234", b"a", b""]),
        Reply::Ok
    );
    assert_eq!(
        execute(
            &mut store,
            &[
                b"MSET",
                b"a",
                b"huge abandoned value",
                b"a",
                b"",
                b"b",
                b"1234"
            ]
        ),
        Reply::Ok
    );
    assert_eq!(store.used_bytes(), 262);
    assert_eq!(
        execute(&mut store, &[b"MSET", b"b", b"", b"c", b"x"]),
        Reply::Error(ExecutionError::OutOfMemory)
    );
    assert_eq!(store.used_bytes(), 262);
    assert_eq!(
        execute(&mut store, &[b"MGET", b"a", b"b", b"c"]),
        Reply::Array(vec![
            Reply::Bulk(Some(Bytes::new())),
            Reply::Bulk(Some(Bytes::from_static(b"1234"))),
            Reply::Bulk(None)
        ])
    );
}

#[tokio::test(start_paused = true)]
async fn rejected_increment_and_set_keep_value_and_ttl_until_expiration() {
    let mut store = store(130);
    assert_eq!(
        execute(&mut store, &[b"SET", b"k", b"9", b"PX", b"100"]),
        Reply::Ok
    );
    assert_eq!(
        execute(&mut store, &[b"INCR", b"k"]),
        Reply::Error(ExecutionError::OutOfMemory)
    );
    assert_eq!(
        execute(&mut store, &[b"SET", b"k", b"10", b"GET"]),
        Reply::Error(ExecutionError::OutOfMemory)
    );
    assert_eq!(
        execute(&mut store, &[b"GET", b"k"]),
        Reply::Bulk(Some(Bytes::from_static(b"9")))
    );
    assert_eq!(execute(&mut store, &[b"PTTL", b"k"]), Reply::Integer(100));
    assert_eq!(store.used_bytes(), 130);
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_eq!(store.expire_due(1), 1);
    assert_eq!(store.used_bytes(), 0);
    assert_eq!(
        execute(&mut store, &[b"DECR", b"k"]),
        Reply::Error(ExecutionError::OutOfMemory)
    );
    assert_eq!(execute(&mut store, &[b"INCR", b"k"]), Reply::Integer(1));
}

#[tokio::test(start_paused = true)]
async fn worker_actively_reclaims_untouched_expired_keys() {
    let (stop, shutdown) = watch::channel(false);
    let (handle, worker) =
        worker::channel_with_store(4, Duration::from_secs(5), shutdown, store(130)).unwrap();
    let task = tokio::spawn(worker.run());
    assert_eq!(
        handle
            .execute(command(&[b"SET", b"a", b"v", b"PX", b"50"]))
            .await
            .unwrap(),
        Reply::Ok
    );
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_eq!(
        handle
            .execute(command(&[b"SET", b"b", b"v"]))
            .await
            .unwrap(),
        Reply::Ok
    );
    stop.send_replace(true);
    task.await.unwrap();
}

#[test]
fn invalid_quota_is_rejected_before_startup() {
    for limit in [0, usize::MAX] {
        assert!(
            Store::with_config(
                StoreConfig {
                    max_dataset_bytes: limit
                },
                Arc::new(SystemClock)
            )
            .is_err()
        );
        assert!(
            sider::ServerConfig {
                max_dataset_bytes: limit,
                ..sider::ServerConfig::default()
            }
            .validate()
            .is_err()
        );
    }
}
