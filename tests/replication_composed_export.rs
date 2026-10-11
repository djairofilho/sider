//! An explicit captured snapshot can be exported only after profile negotiation.

use std::time::Duration;

use bytes::Bytes;
use sider::persistence::{composed_format, format};
use sider::replication::{
    Cursor, composed,
    protocol::{self, ComposedHello, Hello, Limits, Message, Reject},
};
use sider::storage::Mutation;
use tokio::io::duplex;

const DEADLINE: Duration = Duration::from_secs(3);
const CURSOR: Cursor = Cursor {
    epoch: [7; 16],
    sequence: 12,
};

fn source() -> ComposedHello {
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

fn request() -> Message {
    Message::ComposedExport {
        sider_version: source().base.sider_version,
        record_version: 1,
        aof_header_version: composed_format::HEADER_VERSION,
        required_capabilities: composed_format::MIXED_OWNER_BATCH,
        max_record_bytes: 4096,
        max_snapshot_bytes: 65536,
    }
}

fn put(key: &'static [u8], value: &'static [u8]) -> Mutation {
    Mutation::Put {
        key: Bytes::from_static(key),
        value: Bytes::from_static(value).into(),
        expires_at_unix_ms: None,
    }
}

async fn next(input: &mut tokio::io::DuplexStream) -> (Bytes, Message) {
    protocol::read_composed_authorized(input, Limits::default(), DEADLINE, |_| true)
        .await
        .unwrap()
}

#[tokio::test]
async fn exported_snapshot_matches_one_consistent_profile_and_digest() {
    let images = vec![put(b"a", b"one"), put(b"z", b"two")];
    let (mut producer, mut consumer) = duplex(64);
    let exported = images.clone();
    let task = tokio::spawn(async move {
        composed::export_snapshot(
            &mut producer,
            source(),
            CURSOR,
            &exported,
            Limits::default(),
            DEADLINE,
        )
        .await
        .unwrap();
    });
    protocol::write_composed(&mut consumer, &request(), Limits::default(), DEADLINE)
        .await
        .unwrap();
    assert_eq!(
        next(&mut consumer).await.1,
        Message::ComposedHello(source())
    );
    assert_eq!(
        next(&mut consumer).await.1,
        Message::FullStart {
            cursor: CURSOR,
            entries: 2
        }
    );
    let mut digest = 0;
    for image in images {
        let (frame, message) = next(&mut consumer).await;
        assert_eq!(message, Message::SnapshotEntry(image));
        digest = format::snapshot_digest(digest, &frame);
    }
    assert_eq!(
        next(&mut consumer).await.1,
        Message::FullEnd {
            cursor: CURSOR,
            entries: 2,
            digest
        }
    );
    task.await.unwrap();
}

#[tokio::test]
async fn version_and_capacity_mismatch_reject_before_snapshot_payload() {
    let mut wrong_version = request();
    if let Message::ComposedExport { sider_version, .. } = &mut wrong_version {
        *sider_version = Bytes::from_static(b"0.0.0");
    }
    let mut insufficient_capacity = request();
    if let Message::ComposedExport {
        max_record_bytes, ..
    } = &mut insufficient_capacity
    {
        *max_record_bytes = 4095;
    }
    for (request, expected) in [
        (wrong_version, Reject::IncompatibleVersion),
        (insufficient_capacity, Reject::ResourceLimit),
    ] {
        let (mut producer, mut consumer) = duplex(64);
        let task = tokio::spawn(async move {
            composed::export_snapshot(
                &mut producer,
                source(),
                CURSOR,
                &[put(b"a", b"value")],
                Limits::default(),
                DEADLINE,
            )
            .await
            .unwrap();
        });
        protocol::write_composed(&mut consumer, &request, Limits::default(), DEADLINE)
            .await
            .unwrap();
        assert_eq!(next(&mut consumer).await.1, Message::Reject(expected));
        task.await.unwrap();
    }
}

#[tokio::test]
async fn malformed_snapshot_fails_before_advertising_a_cut() {
    for images in [
        vec![put(b"z", b"one"), put(b"a", b"two")],
        vec![put(b"a", b"one"), put(b"a", b"two")],
        vec![Mutation::Delete {
            key: Bytes::from_static(b"a"),
        }],
    ] {
        let (mut producer, mut consumer) = duplex(64);
        let task = tokio::spawn(async move {
            composed::export_snapshot(
                &mut producer,
                source(),
                CURSOR,
                &images,
                Limits::default(),
                DEADLINE,
            )
            .await
        });
        protocol::write_composed(&mut consumer, &request(), Limits::default(), DEADLINE)
            .await
            .unwrap();
        assert!(next_result(&mut consumer).await.is_err());
        assert!(task.await.unwrap().is_err());
    }
}

#[tokio::test]
async fn oversized_transfer_fails_before_advertising_a_cut() {
    let (mut producer, mut consumer) = duplex(64);
    let task = tokio::spawn(async move {
        let mut small_source = source();
        small_source.base.max_snapshot_bytes = 64;
        composed::export_snapshot(
            &mut producer,
            small_source,
            CURSOR,
            &[put(b"a", &[b'x'; 100])],
            Limits::default(),
            DEADLINE,
        )
        .await
    });
    protocol::write_composed(&mut consumer, &request(), Limits::default(), DEADLINE)
        .await
        .unwrap();
    assert!(next_result(&mut consumer).await.is_err());
    assert!(task.await.unwrap().is_err());
}

#[tokio::test]
async fn record_larger_than_advertised_limit_fails_before_hello() {
    let (mut producer, mut consumer) = duplex(64);
    let task = tokio::spawn(async move {
        let mut small_source = source();
        small_source.base.max_record_bytes = 64;
        composed::export_snapshot(
            &mut producer,
            small_source,
            CURSOR,
            &[put(b"a", &[b'x'; 100])],
            Limits::default(),
            DEADLINE,
        )
        .await
    });
    protocol::write_composed(&mut consumer, &request(), Limits::default(), DEADLINE)
        .await
        .unwrap();
    assert!(next_result(&mut consumer).await.is_err());
    assert!(task.await.unwrap().is_err());
}

async fn next_result(
    input: &mut tokio::io::DuplexStream,
) -> Result<(Bytes, Message), protocol::Error> {
    protocol::read_composed_authorized(input, Limits::default(), DEADLINE, |_| true).await
}
