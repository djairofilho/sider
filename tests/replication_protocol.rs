//! Evidência parcial R09: transporte real, replay tipado e limites do protocolo.

use std::time::Duration;

use bytes::Bytes;
use sider::command::parse;
use sider::persistence::format;
use sider::replication::Cursor;
use sider::replication::protocol::{self, Error, Hello, Limits, Message, Reject};
use sider::resp::Frame;
use sider::storage::{Mutation, MutationOrigin, ResolvedBatch, Store};
use tokio::io::{AsyncWriteExt, duplex};
use tokio::net::{TcpListener, TcpStream};

const DEADLINE: Duration = Duration::from_secs(5);
const CURSOR: Cursor = Cursor {
    epoch: [7; 16],
    sequence: 11,
};

fn hello() -> Hello {
    Hello {
        sider_version: Bytes::from_static(b"0.1.0"),
        record_version: 2,
        shard_count: 4,
        routing_version: 1,
        max_record_bytes: 4096,
        max_mutations: 100_000,
        max_snapshot_bytes: 65536,
        cursor: Some(CURSOR),
    }
}

fn batch() -> ResolvedBatch {
    ResolvedBatch {
        origin: MutationOrigin::Expiration,
        mutations: vec![Mutation::Delete {
            key: Bytes::from_static(b"\0\xff"),
        }],
    }
}

#[test]
fn protocol_rejects_every_truncation_and_single_byte_corruption() {
    let messages = [
        Message::Hello(hello()),
        Message::Export {
            sider_version: hello().sider_version,
            max_record_bytes: 4096,
            max_snapshot_bytes: 65536,
        },
        Message::Continue(CURSOR),
        Message::FullStart {
            cursor: CURSOR,
            entries: 2,
        },
        Message::SnapshotEntry(Mutation::Put {
            key: Bytes::new(),
            value: Bytes::from_static(b"\0\xff").into(),
            expires_at_unix_ms: Some(i64::MAX),
        }),
        Message::FullEnd {
            cursor: CURSOR,
            entries: 2,
            digest: 42,
        },
        Message::Batch {
            sequence: 12,
            batch: batch(),
        },
        Message::Ack(CURSOR),
        Message::Heartbeat(CURSOR),
        Message::Reject(Reject::FullRequired),
        Message::StatusRequest,
        Message::Promote,
        Message::Promoted(CURSOR),
        Message::Status {
            readonly: true,
            cursor: CURSOR,
            upstream_sequence: Some(12),
            connected: true,
            backlog_bytes: 0,
            full_syncs: 1,
            partial_syncs: 2,
        },
    ];
    for message in messages {
        let frame = protocol::encode(&message, Limits::default()).unwrap();
        assert_eq!(
            protocol::decode(frame.clone(), Limits::default()).unwrap(),
            message
        );
        for size in 0..frame.len() {
            assert!(protocol::decode(frame.slice(..size), Limits::default()).is_err());
        }
        for index in 0..frame.len() {
            let mut changed = frame.to_vec();
            changed[index] ^= 0x80;
            assert!(protocol::decode(Bytes::from(changed), Limits::default()).is_err());
        }
        let mut extra = frame.to_vec();
        extra.push(0);
        assert!(protocol::decode(Bytes::from(extra), Limits::default()).is_err());
    }
}

#[test]
fn handshake_checks_versions_layout_and_receiver_capacity() {
    let source = hello();
    assert_eq!(hello().accepts(&source), Ok(()));
    let mut receiver = hello();
    receiver.sider_version = Bytes::from_static(b"0.2.0");
    assert_eq!(receiver.accepts(&source), Err(Reject::IncompatibleVersion));
    receiver = hello();
    receiver.record_version += 1;
    assert_eq!(receiver.accepts(&source), Err(Reject::IncompatibleVersion));
    receiver = hello();
    receiver.shard_count *= 2;
    assert_eq!(receiver.accepts(&source), Err(Reject::IncompatibleLayout));
    receiver = hello();
    receiver.routing_version += 1;
    assert_eq!(receiver.accepts(&source), Err(Reject::IncompatibleLayout));
    receiver = hello();
    receiver.max_record_bytes -= 1;
    assert_eq!(receiver.accepts(&source), Err(Reject::ResourceLimit));
    receiver = hello();
    receiver.max_snapshot_bytes -= 1;
    assert_eq!(receiver.accepts(&source), Err(Reject::ResourceLimit));
    receiver = hello();
    receiver.max_mutations -= 1;
    assert_eq!(receiver.accepts(&source), Err(Reject::ResourceLimit));
    assert!(
        protocol::encode(
            &Message::SnapshotEntry(Mutation::Delete { key: Bytes::new() }),
            Limits::default()
        )
        .is_err()
    );
}

#[test]
fn nested_lengths_and_encoder_budget_do_not_escape_the_outer_frame_limit() {
    let message = Message::Batch {
        sequence: 12,
        batch: batch(),
    };
    let mut frame = protocol::encode(&message, Limits::default())
        .unwrap()
        .to_vec();
    let offset = protocol::HEADER_BYTES;
    frame[offset..offset + 4].copy_from_slice(&1_000_000u32.to_le_bytes());
    frame[offset + 4..offset + 8].copy_from_slice(&(!1_000_000u32).to_le_bytes());
    let digest = format::snapshot_digest(format::checksum(&frame[..20]), &frame[offset..]);
    frame[20..24].copy_from_slice(&digest.to_le_bytes());
    assert!(matches!(
        protocol::decode(Bytes::from(frame), Limits::default()),
        Err(Error::Invalid("comprimento do registro"))
    ));
    let message = Message::SnapshotEntry(Mutation::Put {
        key: Bytes::new(),
        value: Bytes::from(vec![0; 1000]).into(),
        expires_at_unix_ms: None,
    });
    assert!(matches!(
        protocol::encode(
            &message,
            Limits {
                max_frame_bytes: 100,
                ..Limits::default()
            }
        ),
        Err(Error::Record(format::FormatError::Limit))
    ));
}

#[tokio::test(start_paused = true)]
async fn read_limits_header_before_body_and_times_out_partial_frame() {
    let frame = protocol::encode(&Message::Ack(CURSOR), Limits::default()).unwrap();
    let (mut sender, mut receiver) = duplex(128);
    sender.write_all(&frame[..10]).await.unwrap();
    assert!(matches!(
        protocol::read(&mut receiver, Limits::default(), DEADLINE).await,
        Err(Error::Timeout)
    ));
    drop(sender);
    assert!(matches!(
        protocol::read(&mut receiver, Limits::default(), DEADLINE).await,
        Err(Error::Io(_))
    ));

    let mut oversized = frame[..protocol::HEADER_BYTES].to_vec();
    oversized[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
    oversized[16..20].copy_from_slice(&0u32.to_le_bytes());
    let (mut sender, mut receiver) = duplex(128);
    sender.write_all(&oversized).await.unwrap();
    assert!(matches!(
        protocol::read(&mut receiver, Limits::default(), DEADLINE).await,
        Err(Error::Limit)
    ));
    let invalid_limits = Limits {
        max_frame_bytes: 0,
        ..Limits::default()
    };
    assert!(matches!(
        protocol::read(&mut receiver, invalid_limits, DEADLINE).await,
        Err(Error::Limit)
    ));
}

fn execute(store: &mut Store, args: &[&[u8]]) {
    let command = parse(Frame::Array(Some(
        args.iter()
            .map(|arg| Frame::Bulk(Some(Bytes::copy_from_slice(arg))))
            .collect(),
    )))
    .unwrap();
    store.execute(command);
}

#[tokio::test]
async fn tcp_transports_all_types_snapshot_and_resolved_batch_to_real_replay() {
    let mut source = Store::new();
    for args in [
        vec![b"SET".as_slice(), b"s", b"\0\xff"],
        vec![b"HSET", b"h", b"\xff", b""],
        vec![b"RPUSH", b"l", b"\xff", b""],
        vec![b"SADD", b"t", b"\0", b"\xff"],
        vec![b"ZADD", b"z", b"-inf", b"\xff", b"1e-7", b"\0"],
    ] {
        execute(&mut source, &args);
    }
    let initial = source.snapshot();
    let mut changed = initial.clone();
    if let Mutation::Put {
        expires_at_unix_ms, ..
    } = &mut changed[0]
    {
        *expires_at_unix_ms = Some(i64::MAX);
    }
    changed.push(Mutation::Delete {
        key: Bytes::from_static(b"missing"),
    });
    let batch = ResolvedBatch {
        origin: MutationOrigin::Client,
        mutations: changed,
    };
    source.replay(&batch.mutations).unwrap();
    let expected = source.snapshot();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let primary = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let Message::Hello(receiver) = protocol::read(&mut socket, Limits::default(), DEADLINE)
            .await
            .unwrap()
        else {
            panic!("handshake ausente")
        };
        receiver.accepts(&hello()).unwrap();
        protocol::write(
            &mut socket,
            &Message::FullStart {
                cursor: CURSOR,
                entries: initial.len() as u64,
            },
            Limits::default(),
            DEADLINE,
        )
        .await
        .unwrap();
        let mut digest = 0;
        for mutation in initial {
            let message = Message::SnapshotEntry(mutation);
            let frame = protocol::encode(&message, Limits::default()).unwrap();
            digest = format::snapshot_digest(digest, &frame);
            // Fragmentação real do stream: nenhuma escrita equivale a um frame completo.
            for fragment in frame.chunks(7) {
                socket.write_all(fragment).await.unwrap();
            }
        }
        protocol::write(
            &mut socket,
            &Message::FullEnd {
                cursor: CURSOR,
                entries: 5,
                digest,
            },
            Limits::default(),
            DEADLINE,
        )
        .await
        .unwrap();
        protocol::write(
            &mut socket,
            &Message::Batch {
                sequence: 12,
                batch,
            },
            Limits::default(),
            DEADLINE,
        )
        .await
        .unwrap();
        assert_eq!(
            protocol::read(&mut socket, Limits::default(), DEADLINE)
                .await
                .unwrap(),
            Message::Ack(Cursor {
                sequence: 12,
                ..CURSOR
            })
        );
    });
    let mut socket = TcpStream::connect(address).await.unwrap();
    protocol::write(
        &mut socket,
        &Message::Hello(hello()),
        Limits::default(),
        DEADLINE,
    )
    .await
    .unwrap();
    assert_eq!(
        protocol::read(&mut socket, Limits::default(), DEADLINE)
            .await
            .unwrap(),
        Message::FullStart {
            cursor: CURSOR,
            entries: 5
        }
    );
    let mut staged = Store::new();
    let mut mutations = Vec::new();
    let mut digest = 0;
    for _ in 0..5 {
        let (frame, message) = protocol::read_with_frame(&mut socket, Limits::default(), DEADLINE)
            .await
            .unwrap();
        digest = format::snapshot_digest(digest, &frame);
        let Message::SnapshotEntry(mutation) = message else {
            panic!("entrada ausente")
        };
        mutations.push(mutation);
    }
    assert_eq!(
        protocol::read(&mut socket, Limits::default(), DEADLINE)
            .await
            .unwrap(),
        Message::FullEnd {
            cursor: CURSOR,
            entries: 5,
            digest
        }
    );
    staged.replay(&mutations).unwrap();
    let Message::Batch {
        sequence: 12,
        batch,
    } = protocol::read(&mut socket, Limits::default(), DEADLINE)
        .await
        .unwrap()
    else {
        panic!("lote ausente")
    };
    staged.replay(&batch.mutations).unwrap();
    assert_eq!(staged.snapshot(), expected);
    protocol::write(
        &mut socket,
        &Message::Ack(Cursor {
            sequence: 12,
            ..CURSOR
        }),
        Limits::default(),
        DEADLINE,
    )
    .await
    .unwrap();
    primary.await.unwrap();
}
