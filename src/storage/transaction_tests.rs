use std::{fs, io, path::PathBuf, sync::Arc};

use bytes::Bytes;

use super::*;
use crate::persistence::{self, AofConfig, FaultInjector};
use crate::pubsub::{Hub, Message};
use crate::storage::{StoreConfig, SystemClock};

fn set(key: &'static [u8], value: &'static [u8]) -> Command {
    Command::Set {
        key: Bytes::from_static(key),
        value: Bytes::from_static(value),
    }
}

#[tokio::test]
async fn transactions_batches_never_interleave_and_watch_is_checked_in_owner() {
    let (stop, shutdown) = watch::channel(false);
    let (db, owner) = channel(8, Duration::from_secs(5), shutdown).unwrap();
    let running = tokio::spawn(owner.run());
    let commands = vec![
        Command::Incr {
            key: Bytes::from_static(b"counter")
        };
        64
    ];
    let (a, b) = tokio::join!(
        db.execute_batch(commands.clone(), vec![]),
        db.execute_batch(commands, vec![])
    );
    let mut replies = [a.unwrap(), b.unwrap()];
    replies.sort_by_key(|reply| match reply {
        Reply::Array(values) => match values[0] {
            Reply::Integer(value) => value,
            _ => panic!(),
        },
        _ => panic!(),
    });
    assert_eq!(
        replies,
        [
            Reply::Array((1..=64).map(Reply::Integer).collect()),
            Reply::Array((65..=128).map(Reply::Integer).collect())
        ]
    );
    let (_, watched) = db
        .watch(vec![Bytes::from_static(b"counter")])
        .await
        .unwrap();
    db.execute(set(b"counter", b"128")).await.unwrap();
    assert_eq!(
        db.execute_batch(vec![set(b"counter", b"blocked")], watched)
            .await
            .unwrap(),
        Reply::NullArray
    );
    assert_eq!(
        db.execute(Command::Get {
            key: Bytes::from_static(b"counter")
        })
        .await
        .unwrap(),
        Reply::Bulk(Some(Bytes::from_static(b"128")))
    );
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn transactions_cross_shard_batch_is_rejected_without_enqueuing_any_part() {
    let (_stop, shutdown) = watch::channel(false);
    let (db, owners) = channel_with_stores(
        2,
        Duration::from_secs(5),
        shutdown,
        vec![Store::new(), Store::new()],
    )
    .unwrap();
    assert_ne!(db.router().shard_for(b"a"), db.router().shard_for(b"b"));
    assert_eq!(
        db.execute_batch(vec![set(b"a", b"1"), set(b"b", b"2")], vec![])
            .await
            .unwrap(),
        Reply::Error(ExecutionError::CrossShard)
    );
    assert!(
        owners
            .iter()
            .all(|owner| owner.requests.is_empty() && owner.store.is_empty())
    );
}

#[tokio::test(start_paused = true)]
async fn transactions_snapshot_waits_for_accepted_batch_even_after_client_cancellation() {
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };
    let (stop, shutdown) = watch::channel(false);
    let (db, owner) = channel(2, Duration::from_secs(5), shutdown).unwrap();
    let mut batch = Box::pin(db.execute_batch(vec![set(b"a", b"1"), set(b"b", b"1")], vec![]));
    poll_fn(|cx| {
        assert!(batch.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(batch); // O pedido aceito mantém a admissão e continua pertencendo ao worker.
    let mut snapshot = Box::pin(db.snapshot(None));
    poll_fn(|cx| {
        assert!(snapshot.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(owner.requests.len(), 1);
    let running = tokio::spawn(owner.run());
    let snapshot = snapshot.await.unwrap();
    assert_eq!(snapshot.mutations.len(), 2);
    assert_eq!(snapshot.mutations[0].key().as_ref(), b"a");
    assert_eq!(snapshot.mutations[1].key().as_ref(), b"b");
    stop.send_replace(true);
    running.await.unwrap();
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-tx-unit-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct ErrorAt(&'static str);
impl FaultInjector for ErrorAt {
    fn hit(&self, point: &'static str) -> io::Result<()> {
        if point == self.0 {
            Err(io::Error::other("falha AOF transacional injetada"))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn transactions_aof_failures_and_record_limit_have_no_data_or_pubsub_effects() {
    for point in [
        "before_append",
        "after_append",
        "before_sync",
        "after_sync",
        "before_reply",
        "record_limit",
    ] {
        let directory = Directory::new();
        let mut config = AofConfig::new(directory.0.clone());
        if point == "record_limit" {
            config.limits.max_record_bytes = 64;
        }
        let recovered = persistence::recover_with_faults(
            config,
            StoreConfig::default(),
            Arc::new(SystemClock),
            Arc::new(ErrorAt(point)),
        )
        .unwrap();
        let (store, aof, writer) = recovered.start();
        let (_stop, shutdown) = watch::channel(false);
        let (_db, owner) = channel_with_store(2, Duration::from_secs(5), shutdown, store).unwrap();
        let mut owner = owner.with_aof(aof, 0);
        let hub = Hub::default();
        let mut listener = hub.connect(1, 4).unwrap();
        listener.subscribe(vec![Bytes::from_static(b"a")]).unwrap();
        let subscription = hub.connect(1, 4).unwrap();
        let commands = vec![
            set(b"k", &[b'x'; 80]),
            Command::Publish {
                channel: Bytes::from_static(b"a"),
                message: Bytes::from_static(b"not-delivered"),
            },
            Command::Subscribe {
                channels: vec![Bytes::from_static(b"b")],
            },
        ];
        let (reply, response) = oneshot::channel();
        let healthy = owner
            .apply(Request::Transaction {
                commands,
                watched: vec![],
                subscription,
                limits: RespLimits::default(),
                reply,
                _admission: _db.barrier.clone().read_owned().await,
            })
            .await;
        assert!(owner.store.is_empty(), "{point}");
        assert!(listener.try_message().is_none(), "{point}");
        assert_eq!(
            hub.publish(Message {
                channel: Bytes::from_static(b"b"),
                payload: Bytes::new()
            }),
            0,
            "{point}"
        );
        let result = response.await.unwrap();
        if point == "record_limit" {
            assert!(healthy);
            let result = result.unwrap();
            assert!(!result.subscription.active());
            assert_eq!(
                result.output.unwrap(),
                encode_reply(
                    Reply::Error(ExecutionError::AofRecordLimit),
                    RespLimits::default()
                )
                .unwrap()
            );
            assert_eq!(
                owner
                    .commit(owner.store.prepare(Command::Ping(None)))
                    .await
                    .unwrap(),
                Reply::Pong
            );
        } else {
            assert!(!healthy);
            assert!(result.is_err());
        }
        drop(owner);
        assert_eq!(writer.await.unwrap().is_ok(), point == "record_limit");
    }
}
