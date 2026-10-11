//! Transport 3 is an isolated record-1/header-7 codec until a session selects it.

use std::time::Duration;

use bytes::Bytes;
use sider::persistence::{composed_format, format};
use sider::replication::Cursor;
use sider::replication::protocol::{self, ComposedHello, Hello, Limits, Message, Reject};
use sider::storage::{Mutation, MutationOrigin, ResolvedBatch};
use tokio::io::duplex;

const CURSOR: Cursor = Cursor {
    epoch: [7; 16],
    sequence: 11,
};

fn hello() -> ComposedHello {
    ComposedHello {
        base: Hello {
            sider_version: Bytes::from_static(b"1.0.1"),
            record_version: 1,
            shard_count: 4,
            routing_version: 1,
            max_record_bytes: 4096,
            max_mutations: 100_000,
            max_snapshot_bytes: 65536,
            cursor: Some(CURSOR),
        },
        aof_header_version: composed_format::HEADER_VERSION,
        required_capabilities: composed_format::MIXED_OWNER_BATCH,
    }
}

fn reseal(frame: &mut [u8]) {
    let digest = format::snapshot_digest(format::checksum(&frame[..20]), &frame[24..]);
    frame[20..24].copy_from_slice(&digest.to_le_bytes());
}

#[test]
fn hello_is_exact_and_older_transports_remain_separate() {
    let message = Message::ComposedHello(hello());
    let frame = protocol::encode_composed(&message, Limits::default()).unwrap();
    assert_eq!(&frame[..8], b"SIDERREP");
    assert_eq!(u16::from_le_bytes(frame[8..10].try_into().unwrap()), 3);
    assert_eq!(frame[10], 1);
    assert_eq!(u32::from_le_bytes(frame[12..16].try_into().unwrap()), 67);
    assert_eq!(&frame[frame.len() - 8..], &[7, 0, 0, 0, 1, 0, 0, 0]);
    assert_eq!(
        protocol::decode_composed(frame.clone(), Limits::default()).unwrap(),
        message
    );
    assert!(protocol::decode(frame.clone(), Limits::default()).is_err());
    assert!(protocol::decode_memory(frame, Limits::default()).is_err());
    assert!(protocol::encode(&message, Limits::default()).is_err());
    assert!(protocol::encode_memory(&message, Limits::default()).is_err());
    assert!(protocol::encode_composed(&Message::Hello(hello().base), Limits::default()).is_err());
}

#[test]
fn incompatible_profiles_are_rejected_even_with_valid_checksums() {
    let limits = Limits::default();
    let message = Message::ComposedHello(hello());
    let frame = protocol::encode_composed(&message, limits).unwrap();
    for (offset, value) in [
        (frame.len() - 8, 6),
        (frame.len() - 4, 0),
        (frame.len() - 4, 2),
    ] {
        let mut changed = frame.to_vec();
        changed[offset] = value;
        reseal(&mut changed);
        assert!(protocol::decode_composed(Bytes::from(changed), limits).is_err());
    }
    let mut receiver = hello();
    receiver.required_capabilities = 0;
    assert_eq!(receiver.accepts(&hello()), Err(Reject::IncompatibleVersion));
    receiver = hello();
    receiver.base.max_record_bytes -= 1;
    assert_eq!(receiver.accepts(&hello()), Err(Reject::ResourceLimit));
    receiver = hello();
    receiver.base.shard_count += 1;
    assert_eq!(receiver.accepts(&hello()), Err(Reject::IncompatibleLayout));
    let mut changed = hello();
    changed.base.record_version = 2;
    assert!(protocol::encode_composed(&Message::ComposedHello(changed), limits).is_err());
}

#[test]
fn legacy_records_roundtrip_and_invalid_frames_fail() {
    let limits = Limits::default();
    let messages = [
        Message::ComposedHello(hello()),
        Message::SnapshotEntry(Mutation::Put {
            key: Bytes::from_static(b"snapshot"),
            value: Bytes::from_static(b"value").into(),
            expires_at_unix_ms: None,
        }),
        Message::Batch {
            sequence: 12,
            batch: ResolvedBatch {
                origin: MutationOrigin::Client,
                mutations: vec![
                    Mutation::Put {
                        key: Bytes::from_static(b"a"),
                        value: Bytes::from_static(b"one").into(),
                        expires_at_unix_ms: None,
                    },
                    Mutation::Delete {
                        key: Bytes::from_static(b"b"),
                    },
                ],
            },
        },
    ];
    for message in messages {
        let frame = protocol::encode_composed(&message, limits).unwrap();
        assert_eq!(
            protocol::decode_composed(frame.clone(), limits).unwrap(),
            message
        );
        for length in 0..frame.len() {
            assert!(protocol::decode_composed(frame.slice(..length), limits).is_err());
        }
        let mut extra = frame.to_vec();
        extra.push(0);
        assert!(protocol::decode_composed(Bytes::from(extra), limits).is_err());
    }
    assert!(
        protocol::encode_composed(
            &Message::Export {
                sider_version: Bytes::from_static(b"1.0.1"),
                max_record_bytes: 4096,
                max_snapshot_bytes: 65536,
            },
            limits,
        )
        .is_err()
    );
}

#[tokio::test]
async fn admitted_read_rejects_wrong_version_before_payload_allocation() {
    let limits = Limits::default();
    let (mut output, mut input) = duplex(256);
    protocol::write_composed(
        &mut output,
        &Message::ComposedHello(hello()),
        limits,
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let (_, message, admitted) = protocol::read_composed_admitted(
        &mut input,
        limits,
        Duration::from_secs(1),
        |kind| async move { Ok::<_, protocol::Error>(kind == 1) },
    )
    .await
    .unwrap();
    assert!(admitted);
    assert_eq!(message, Message::ComposedHello(hello()));

    let (mut output, mut input) = duplex(256);
    protocol::write(
        &mut output,
        &Message::Hello(hello().base),
        limits,
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(
        protocol::read_composed_authorized(&mut input, limits, Duration::from_secs(1), |_| true,)
            .await
            .is_err()
    );
}
