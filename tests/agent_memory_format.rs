//! Frozen memory-format fixtures. The production AOF reader remains codec 1.

use bytes::Bytes;
use sider::persistence::memory_format::{self, Header, Mutation, Next, Record};
use sider::persistence::{DurableLayout, ReplicationMetadata, Role, format};
use sider::storage::memory::{Collection, Descriptor, Edit, Member, Scalar};
use std::collections::BTreeMap;

struct TestDirectory(std::path::PathBuf);
impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-memory-format-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TestDirectory {
    fn drop(&mut self) {
        assert_eq!(self.0.parent(), Some(std::env::temp_dir().as_path()));
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn descriptor() -> Descriptor {
    Descriptor {
        dimension: 2,
        provider: Bytes::from_static(b"p"),
        model: Bytes::from_static(b"m"),
        revision: Bytes::from_static(b"r"),
        transform: Bytes::from_static(b"t"),
    }
}

fn hex(input: &str) -> Vec<u8> {
    input
        .split_whitespace()
        .map(|byte| u8::from_str_radix(byte, 16).unwrap())
        .collect()
}

#[test]
fn memory_format_frozen_header_and_empty_snapshot() {
    let header = Header {
        sequence: 0,
        layout: DurableLayout::default(),
        replication: None,
    };
    let expected_header = hex(
        "53 49 44 45 52 41 4f 46 04 00 00 00 00 00 00 00 00 00 00 00 01 00 00 00 01 00 00 00 02 00 00 00 b2 e1 d1 d1",
    );
    assert_eq!(
        memory_format::encode_header(header).unwrap(),
        expected_header
    );
    assert_eq!(
        memory_format::read_header(expected_header.as_slice()).unwrap(),
        header
    );
    let record = Record::Snapshot(Mutation::Image {
        key: Bytes::from_static(b"c"),
        collection: Collection::new(descriptor()).unwrap(),
        key_deadline: None,
    });
    let expected_record = hex(
        "32 00 00 00 cd ff ff ff 62 41 9a 44 01 07 01 00 00 00 63 01 00 00 00 02 00 00 00 01 01 01 00 00 00 70 01 00 00 00 6d 01 00 00 00 72 01 00 00 00 74 00 00 00 00 00 00 00 00 00 00 00 00 00",
    );
    assert_eq!(
        memory_format::encode(&record, format::Limits::default()).unwrap(),
        expected_record
    );
    assert_eq!(
        memory_format::read_record(expected_record.as_slice(), format::Limits::default()).unwrap(),
        Next::Record(Box::new(record))
    );
}

#[test]
fn memory_format_headers_reject_every_truncation_corruption_and_unsupported_profile() {
    for replication in [
        None,
        Some(ReplicationMetadata {
            role: Role::Primary,
            epoch: [7; 16],
        }),
        Some(ReplicationMetadata {
            role: Role::Replica,
            epoch: [0; 16],
        }),
    ] {
        let header = Header {
            sequence: 9,
            layout: DurableLayout {
                shard_count: 4,
                ..DurableLayout::default()
            },
            replication,
        };
        let encoded = memory_format::encode_header(header).unwrap();
        for length in 0..encoded.len() {
            assert!(
                memory_format::read_header(&encoded[..length]).is_err(),
                "prefix {length}"
            );
        }
        for position in 0..encoded.len() {
            let mut corrupt = encoded.clone();
            corrupt[position] ^= 0x80;
            assert!(
                memory_format::read_header(corrupt.as_slice()).is_err(),
                "byte {position}"
            );
        }
        assert!(matches!(
            format::read_header_with_layout(encoded.as_slice()),
            Err(format::FormatError::Version(4 | 5))
        ));
        for (offset, value) in [(8, 7u32), (24, 2), (28, 1)] {
            let mut unsupported = encoded.clone();
            unsupported[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            let end = unsupported.len() - 4;
            let crc = format::checksum(&unsupported[..end]);
            unsupported[end..].copy_from_slice(&crc.to_le_bytes());
            assert!(memory_format::read_header(unsupported.as_slice()).is_err());
        }
        if replication.is_some() {
            for (offset, value) in [(32, 0), (32, 3), (33, 1), (34, 1), (35, 1)] {
                let mut invalid = encoded.clone();
                invalid[offset] = value;
                let crc = format::checksum(&invalid[..52]);
                invalid[52..].copy_from_slice(&crc.to_le_bytes());
                assert!(memory_format::read_header(invalid.as_slice()).is_err());
            }
        }
    }
}

fn member() -> Member {
    Member {
        id: Bytes::from_static(b"\xff\0"),
        namespace: Bytes::new(),
        content: Bytes::from_static(b"\0\r\n\xfe"),
        vector: Bytes::from([(-0.0f32).to_le_bytes(), f32::from_bits(1).to_le_bytes()].concat()),
        deadline: Some(1001),
        metadata: BTreeMap::from([
            (Bytes::from_static(b"a"), Scalar::Bytes(Bytes::new())),
            (Bytes::from_static(b"b"), Scalar::Integer(i64::MIN)),
            (Bytes::from_static(b"c"), Scalar::Bool(true)),
        ]),
        provenance: BTreeMap::from([(Bytes::from_static(b"source"), Bytes::from_static(b"\xff"))]),
    }
}

#[test]
fn memory_format_binary_delta_roundtrip_and_every_prefix() {
    let record = Record::Batch {
        sequence: 17,
        origin: sider::storage::MutationOrigin::Client,
        mutations: vec![Mutation::Delta {
            key: Bytes::from_static(b"c"),
            descriptor: descriptor(),
            edits: vec![Edit::Upsert(member())],
            key_deadline: Some(-1),
        }],
    };
    let encoded = memory_format::encode(&record, format::Limits::default()).unwrap();
    assert_eq!(
        memory_format::read_record(&encoded[..0], format::Limits::default()).unwrap(),
        Next::End
    );
    for length in 1..encoded.len() {
        assert_eq!(
            memory_format::read_record(&encoded[..length], format::Limits::default()).unwrap(),
            Next::IncompleteTail,
            "prefix {length}"
        );
    }
    assert_eq!(
        memory_format::read_record(encoded.as_slice(), format::Limits::default()).unwrap(),
        Next::Record(Box::new(record))
    );
    for position in 0..encoded.len() {
        let mut corrupt = encoded.clone();
        corrupt[position] ^= 0x80;
        assert!(
            memory_format::read_record(corrupt.as_slice(), format::Limits::default()).is_err(),
            "byte {position}"
        );
    }
}

#[test]
fn memory_format_guarded_replay_retains_elapsed_then_persisted_members_and_is_atomic() {
    let collection = Collection::new(descriptor()).unwrap();
    let retained = collection
        .apply_edits(&descriptor(), &[Edit::Upsert(member())], b"c")
        .unwrap();
    assert!(retained.live_member(b"\xff\0", 2000).is_none());
    let mut persistent = member();
    persistent.deadline = None;
    let extended = retained
        .apply_edits(&descriptor(), &[Edit::Upsert(persistent.clone())], b"c")
        .unwrap();
    assert_eq!(extended.live_member(b"\xff\0", 2000), Some(&persistent));
    assert!(
        retained
            .apply_edits(
                &descriptor(),
                &[Edit::Remove(Bytes::from_static(b"absent"))],
                b"c"
            )
            .is_err()
    );
    let mut wrong = descriptor();
    wrong.model = Bytes::from_static(b"different");
    assert!(
        retained
            .apply_edits(&wrong, &[Edit::Remove(member().id)], b"c")
            .is_err()
    );
    assert_eq!(retained.retained_count(), 1);
    assert!(
        retained
            .apply_edits(
                &descriptor(),
                &[Edit::Remove(member().id.clone()), Edit::Remove(member().id)],
                b"c"
            )
            .is_err()
    );
}

#[test]
fn memory_format_preflights_snapshot_and_rejects_delta_snapshots_nonfinite_and_quota() {
    let collection = Collection::new(descriptor()).unwrap();
    let image = Mutation::Image {
        key: Bytes::from_static(b"c"),
        collection,
        key_deadline: None,
    };
    let record = Record::Snapshot(image);
    let encoded = memory_format::encode(&record, format::Limits::default()).unwrap();
    assert!(
        memory_format::encode(
            &record,
            format::Limits {
                max_record_bytes: encoded.len() - 13,
                ..format::Limits::default()
            }
        )
        .is_err()
    );
    let delta = Mutation::Delta {
        key: Bytes::from_static(b"c"),
        descriptor: descriptor(),
        edits: vec![],
        key_deadline: None,
    };
    assert!(memory_format::encode(&Record::Snapshot(delta), format::Limits::default()).is_err());
    for bits in [
        0x7f800000u32,
        0xff800000,
        0x7fc00000,
        0x7f800001,
        0xffc00001,
    ] {
        let mut invalid = member();
        invalid.vector = Bytes::from([bits.to_le_bytes(), 0u32.to_le_bytes()].concat());
        assert!(invalid.validate(&descriptor()).is_err());
    }
    let mut high_dimension = descriptor();
    high_dimension.dimension = 4096;
    let mut collection = Collection::new(high_dimension.clone()).unwrap();
    for number in 0..256 {
        let mut valid = member();
        valid.id = Bytes::from(format!("{number:04}"));
        valid.vector = Bytes::from(vec![0; 4096 * 4]);
        collection = collection
            .apply_edits(&high_dimension, &[Edit::Upsert(valid)], b"c")
            .unwrap();
    }
    let before = collection.clone();
    let mut excess = member();
    excess.id = Bytes::from_static(b"last");
    excess.vector = Bytes::from(vec![0; 4096 * 4]);
    assert!(
        collection
            .apply_edits(&high_dimension, &[Edit::Upsert(excess)], b"c")
            .is_err()
    );
    assert_eq!(collection, before);
}

#[test]
fn memory_format_semantic_corruption_rejected_after_checksum_recomputation() {
    let record = Record::Snapshot(Mutation::Image {
        key: Bytes::from_static(b"c"),
        collection: Collection::new(descriptor()).unwrap(),
        key_deadline: None,
    });
    let encoded = memory_format::encode(&record, format::Limits::default()).unwrap();
    for (offset, value) in [(14, u32::MAX), (19, 2), (23, 0), (23, 4097), (49, 1025)] {
        let mut corrupt = encoded.clone();
        corrupt[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        let crc = format::checksum(&corrupt[12..]);
        corrupt[8..12].copy_from_slice(&crc.to_le_bytes());
        assert!(
            memory_format::read_record(corrupt.as_slice(), format::Limits::default()).is_err(),
            "offset {offset}, value {value}"
        );
    }
    for (offset, value) in [(27, 2), (28, 2), (53, 2), (54, 1)] {
        let mut corrupt = encoded.clone();
        corrupt[offset] = value;
        let crc = format::checksum(&corrupt[12..]);
        corrupt[8..12].copy_from_slice(&crc.to_le_bytes());
        assert!(
            memory_format::read_record(corrupt.as_slice(), format::Limits::default()).is_err(),
            "offset {offset}, value {value}"
        );
    }
}

#[test]
fn memory_format_production_refusal_preserves_every_torn_memory_prefix() {
    use sha2::{Digest, Sha256};
    use sider::persistence::{self, AofConfig, AofError};
    use sider::storage::{StoreConfig, SystemClock};
    use std::sync::Arc;
    let record = Record::Snapshot(Mutation::Image {
        key: Bytes::from_static(b"c"),
        collection: Collection::new(descriptor()).unwrap(),
        key_deadline: None,
    });
    let encoded = memory_format::encode(&record, format::Limits::default()).unwrap();
    for replication in [
        None,
        Some(ReplicationMetadata {
            role: Role::Replica,
            epoch: [0; 16],
        }),
    ] {
        let header = memory_format::encode_header(Header {
            sequence: 0,
            layout: DurableLayout::default(),
            replication,
        })
        .unwrap();
        let directory = TestDirectory::new();
        let file = directory.path().join("generation-00000000000000000000.aof");
        for prefix in 0..=encoded.len() {
            let bytes = [header.as_slice(), &encoded[..prefix]].concat();
            std::fs::write(&file, &bytes).unwrap();
            let before = Sha256::digest(&bytes);
            let recovered = persistence::recover(
                AofConfig::new(directory.path().to_owned()),
                StoreConfig::default(),
                Arc::new(SystemClock),
            );
            assert!(
                matches!(
                    recovered,
                    Err(AofError::Format(format::FormatError::Version(4 | 5)))
                ),
                "prefix {prefix}"
            );
            assert_eq!(
                Sha256::digest(std::fs::read(&file).unwrap()),
                before,
                "prefix {prefix} modified"
            );
            assert!(
                !std::fs::read_dir(directory.path())
                    .unwrap()
                    .any(|entry| entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with("tail-")),
                "refusal attempted tail repair"
            );
        }
    }
}

#[test]
fn composed_header_production_refusal_preserves_torn_records() {
    use sha2::{Digest, Sha256};
    use sider::persistence::{self, AofConfig, AofError};
    use sider::storage::{StoreConfig, SystemClock};
    use std::sync::Arc;

    // Literal header-7 fixtures from docs/durable-format-registry.md.
    let profiles = [
        (
            hex("53 49 44 45 52 41 4f 46 07 00 00 00 40 00 00 00 \
                 09 00 00 00 00 00 00 00 04 00 00 00 01 00 00 00 \
                 01 00 00 00 01 00 00 00 00 00 00 00 00 00 00 00 \
                 00 00 00 00 00 00 00 00 00 00 00 00 fa fc 1a 97"),
            format::encode(
                &format::Record::Seal {
                    sequence: 9,
                    entries: 0,
                    digest: 0,
                },
                format::Limits::default(),
            )
            .unwrap(),
        ),
        (
            hex("53 49 44 45 52 41 4f 46 07 00 00 00 40 00 00 00 \
                 09 00 00 00 00 00 00 00 04 00 00 00 01 00 00 00 \
                 02 00 00 00 01 00 00 00 02 00 00 00 01 02 03 04 \
                 05 06 07 08 09 0a 0b 0c 0d 0e 0f 10 f5 46 fd ba"),
            memory_format::encode(
                &Record::Snapshot(Mutation::Image {
                    key: Bytes::from_static(b"c"),
                    collection: Collection::new(descriptor()).unwrap(),
                    key_deadline: None,
                }),
                format::Limits::default(),
            )
            .unwrap(),
        ),
    ];

    for (header, record) in profiles {
        assert_eq!(header.len(), 64);
        assert_eq!(
            format::checksum(&header[..60]),
            u32::from_le_bytes(header[60..].try_into().unwrap())
        );
        let directory = TestDirectory::new();
        let file = directory.path().join("generation-00000000000000000000.aof");
        for prefix in 0..=record.len() {
            let bytes = [header.as_slice(), &record[..prefix]].concat();
            std::fs::write(&file, &bytes).unwrap();
            let before = Sha256::digest(&bytes);
            assert!(
                matches!(
                    persistence::recover(
                        AofConfig::new(directory.path().to_owned()),
                        StoreConfig::default(),
                        Arc::new(SystemClock),
                    ),
                    Err(AofError::Format(format::FormatError::Version(7)))
                ),
                "record version {}, prefix {prefix}",
                header[32]
            );
            assert_eq!(Sha256::digest(std::fs::read(&file).unwrap()), before);
            assert!(
                !std::fs::read_dir(directory.path())
                    .unwrap()
                    .any(|entry| entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with("tail-")),
                "refusal attempted tail repair"
            );
        }
    }
}
