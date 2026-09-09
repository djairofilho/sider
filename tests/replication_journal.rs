//! Histórico assíncrono com consumidores reais de frames e orçamento explícito.

use bytes::Bytes;
use sider::replication::Cursor;
use sider::replication::journal::{Error, Journal, Limits};
use sider::replication::protocol::{self, Message};
use sider::storage::{Mutation, MutationOrigin, ResolvedBatch, Store};

const START: Cursor = Cursor {
    epoch: [9; 16],
    sequence: 20,
};

fn journal(bytes: usize, batches: usize) -> Journal {
    Journal::new(
        START,
        Limits {
            max_bytes: bytes,
            max_batches: batches,
            max_frame_bytes: bytes,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn slow_subscriber_loses_history_without_blocking_healthy_replay() {
    let journal = journal(250, 2);
    let mut slow = journal.subscribe(START).unwrap();
    let mut healthy = journal.subscribe(START).unwrap();
    let mut target = Store::new();
    for sequence in 21..=120 {
        let message = Message::Batch {
            sequence,
            batch: ResolvedBatch {
                origin: MutationOrigin::Client,
                mutations: vec![Mutation::Put {
                    key: Bytes::from_static(b"counter"),
                    value: Bytes::from(sequence.to_string()).into(),
                    expires_at_unix_ms: None,
                }],
            },
        };
        let frame = protocol::encode(&message, protocol::Limits::default()).unwrap();
        journal.publish(sequence, frame).unwrap();
        let entry = healthy.next().await.unwrap();
        assert_eq!(entry.cursor.sequence, sequence);
        let Message::Batch {
            sequence: actual,
            batch,
        } = protocol::decode(entry.frame, protocol::Limits::default()).unwrap()
        else {
            panic!("lote ausente")
        };
        assert_eq!(actual, sequence);
        target.replay(&batch.mutations).unwrap();
        assert!(journal.status().unwrap().bytes <= 250);
    }
    assert_eq!(slow.next().await, Err(Error::Lagged));
    assert_eq!(target.len(), 1);
    assert!(
        matches!(&target.snapshot()[0], Mutation::Put { value, .. } if value.as_string().unwrap().as_ref() == b"120")
    );
}

#[tokio::test]
async fn subscription_wakes_for_publish_and_close_and_rejects_invalid_positions() {
    let journal = journal(20, 3);
    assert!(matches!(
        journal.subscribe(Cursor {
            epoch: [8; 16],
            ..START
        }),
        Err(Error::Epoch)
    ));
    assert!(matches!(
        journal.subscribe(Cursor {
            sequence: 21,
            ..START
        }),
        Err(Error::Sequence)
    ));
    let mut subscriber = journal.subscribe(START).unwrap();
    let waiting = tokio::spawn(async move { subscriber.next().await });
    journal.publish(21, Bytes::from_static(b"record")).unwrap();
    assert_eq!(waiting.await.unwrap().unwrap().cursor.sequence, 21);
    let mut subscriber = journal
        .subscribe(Cursor {
            sequence: 21,
            ..START
        })
        .unwrap();
    let waiting = tokio::spawn(async move { subscriber.next().await });
    journal.close();
    assert_eq!(waiting.await.unwrap(), Err(Error::Closed));
    assert_eq!(
        journal.publish(22, Bytes::from_static(b"x")),
        Err(Error::Closed)
    );
}

#[test]
fn budget_count_sequence_overflow_and_rejections_preserve_state() {
    let journal = journal(10, 2);
    journal.publish(21, Bytes::from_static(b"123456")).unwrap();
    let before = journal.status().unwrap();
    for (sequence, bytes, error) in [
        (21, b"x".as_slice(), Error::Sequence),
        (23, b"x", Error::Sequence),
        (22, b"12345678901", Error::Limit),
        (22, b"", Error::Limit),
    ] {
        assert_eq!(
            journal.publish(sequence, Bytes::copy_from_slice(bytes)),
            Err(error)
        );
        assert_eq!(journal.status().unwrap(), before);
    }
    journal.publish(22, Bytes::from_static(b"12345")).unwrap();
    assert_eq!(journal.status().unwrap().oldest_sequence, Some(22));
    journal.publish(23, Bytes::from_static(b"1")).unwrap();
    journal.publish(24, Bytes::from_static(b"2")).unwrap();
    assert_eq!(journal.status().unwrap().batches, 2);
    assert_eq!(journal.status().unwrap().bytes, 2);
    assert!(matches!(journal.subscribe(START), Err(Error::Lagged)));
    let end = Journal::new(
        Cursor {
            sequence: u64::MAX,
            ..START
        },
        Limits {
            max_bytes: 1,
            max_batches: 1,
            max_frame_bytes: 1,
        },
    )
    .unwrap();
    assert_eq!(
        end.publish(0, Bytes::from_static(b"x")),
        Err(Error::Sequence)
    );
    assert_eq!(end.status().unwrap().head.sequence, u64::MAX);
}
