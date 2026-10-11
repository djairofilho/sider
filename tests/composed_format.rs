//! Proposed header-7 grammar, independent of production AOF admission.

use sider::persistence::{
    DurableLayout, ReplicationMetadata, Role,
    composed_format::{self, Header, RecordVersion},
    format,
};

fn hex(input: &str) -> Vec<u8> {
    input
        .split_whitespace()
        .map(|byte| u8::from_str_radix(byte, 16).unwrap())
        .collect()
}

fn fixtures() -> [(Header, Vec<u8>); 2] {
    let layout = DurableLayout {
        shard_count: 4,
        ..DurableLayout::default()
    };
    [
        (
            Header {
                sequence: 9,
                layout,
                record_version: RecordVersion::Legacy,
                mixed_owner_batch: true,
                replication: None,
            },
            hex("53 49 44 45 52 41 4f 46 07 00 00 00 40 00 00 00 \
                 09 00 00 00 00 00 00 00 04 00 00 00 01 00 00 00 \
                 01 00 00 00 01 00 00 00 00 00 00 00 00 00 00 00 \
                 00 00 00 00 00 00 00 00 00 00 00 00 fa fc 1a 97"),
        ),
        (
            Header {
                sequence: 9,
                layout,
                record_version: RecordVersion::Memory,
                mixed_owner_batch: true,
                replication: Some(ReplicationMetadata {
                    role: Role::Replica,
                    epoch: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                }),
            },
            hex("53 49 44 45 52 41 4f 46 07 00 00 00 40 00 00 00 \
                 09 00 00 00 00 00 00 00 04 00 00 00 01 00 00 00 \
                 02 00 00 00 01 00 00 00 02 00 00 00 01 02 03 04 \
                 05 06 07 08 09 0a 0b 0c 0d 0e 0f 10 f5 46 fd ba"),
        ),
    ]
}

fn change_field(bytes: &[u8], offset: usize, value: &[u8]) -> Vec<u8> {
    let mut changed = bytes.to_vec();
    changed[offset..offset + value.len()].copy_from_slice(value);
    let crc = format::checksum(&changed[..60]);
    changed[60..].copy_from_slice(&crc.to_le_bytes());
    changed
}

#[test]
fn composed_header_matches_independent_literal_fixtures() {
    for (header, expected) in fixtures() {
        assert_eq!(expected.len(), 64);
        assert_eq!(
            composed_format::encode_header(header).unwrap().as_slice(),
            expected
        );
        assert_eq!(
            composed_format::read_header(expected.as_slice()).unwrap(),
            header
        );
    }
}

#[test]
fn composed_header_all_profiles_have_explicit_fixed_bytes() {
    for record_version in [RecordVersion::Legacy, RecordVersion::Memory] {
        for mixed_owner_batch in [false, true] {
            for replication in [
                None,
                Some(ReplicationMetadata {
                    role: Role::Primary,
                    epoch: [1; 16],
                }),
                Some(ReplicationMetadata {
                    role: Role::Replica,
                    epoch: [2; 16],
                }),
            ] {
                let header = Header {
                    sequence: u64::MAX,
                    layout: DurableLayout::default(),
                    record_version,
                    mixed_owner_batch,
                    replication,
                };
                let bytes = composed_format::encode_header(header).unwrap();
                assert_eq!(bytes.len(), 64);
                assert_eq!(
                    composed_format::read_header(bytes.as_slice()).unwrap(),
                    header
                );
                assert_eq!(bytes[41..44], [0; 3]);
                assert_eq!(bytes[36..40], u32::from(mixed_owner_batch).to_le_bytes());
                assert_eq!(bytes[40] == 0, replication.is_none());
            }
        }
    }
}

#[test]
fn composed_header_rejects_every_truncation_and_corrupt_byte() {
    for (_, fixture) in fixtures() {
        for length in 0..fixture.len() {
            assert!(
                composed_format::read_header(&fixture[..length]).is_err(),
                "truncated at {length}"
            );
        }
        for offset in 0..fixture.len() {
            let mut corrupt = fixture.clone();
            corrupt[offset] ^= 0x80;
            assert!(
                composed_format::read_header(corrupt.as_slice()).is_err(),
                "corrupt at {offset}"
            );
        }
    }
}

#[test]
fn composed_header_rejects_invalid_fields_even_with_matching_checksum() {
    let (header, fixture) = &fixtures()[0];
    for (offset, value) in [
        (12, 63u32.to_le_bytes().to_vec()),
        (12, 65u32.to_le_bytes().to_vec()),
        (24, 0u32.to_le_bytes().to_vec()),
        (24, 257u32.to_le_bytes().to_vec()),
        (28, 2u32.to_le_bytes().to_vec()),
        (32, 0u32.to_le_bytes().to_vec()),
        (32, 3u32.to_le_bytes().to_vec()),
        (36, 2u32.to_le_bytes().to_vec()),
        (36, 3u32.to_le_bytes().to_vec()),
        (40, vec![3]),
        (41, vec![1]),
        (42, vec![1]),
        (43, vec![1]),
        (44, vec![1]),
    ] {
        let changed = change_field(fixture, offset, &value);
        assert!(
            composed_format::read_header(changed.as_slice()).is_err(),
            "offset {offset}"
        );
    }
    let mut zero_epoch = *header;
    zero_epoch.replication = Some(ReplicationMetadata {
        role: Role::Primary,
        epoch: [0; 16],
    });
    assert!(composed_format::encode_header(zero_epoch).is_err());
    for role in [1u8, 2] {
        let changed = change_field(fixture, 40, &[role]);
        assert!(composed_format::read_header(changed.as_slice()).is_err());
    }
    let mut invalid_layout = *header;
    invalid_layout.layout.shard_count = 0;
    assert!(composed_format::encode_header(invalid_layout).is_err());
}

#[test]
fn composed_header_reader_stops_at_one_header() {
    let (_, fixture) = &fixtures()[0];
    let mut input = std::io::Cursor::new([fixture.as_slice(), b"first-record"].concat());
    composed_format::read_header(&mut input).unwrap();
    assert_eq!(input.position(), 64);
}
