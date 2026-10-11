//! Isolated codec for the proposed 64-byte composed AOF header (version 7).
//!
//! Parsing a header does not select a supported profile. Production recovery,
//! append, backup, replication, and migration still reject version 7. This module
//! has no directory-opening, record-scanning, or tail-repair API.

use std::io::{self, Read};

use super::{DurableLayout, ReplicationMetadata, Role, format};

pub const HEADER_VERSION: u32 = 7;
pub const HEADER_BYTES: usize = 64;
pub const MIXED_OWNER_BATCH: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum RecordVersion {
    Legacy = 1,
    Memory = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub sequence: u64,
    pub layout: DurableLayout,
    pub record_version: RecordVersion,
    pub mixed_owner_batch: bool,
    pub replication: Option<ReplicationMetadata>,
}

impl Header {
    fn validate(self) -> Result<(), format::FormatError> {
        self.layout
            .validate()
            .map_err(|_| format::FormatError::Corrupt("shard configuration"))?;
        if self
            .replication
            .is_some_and(|metadata| metadata.epoch == [0; 16])
        {
            return Err(format::FormatError::Corrupt("replication epoch"));
        }
        Ok(())
    }
}

pub fn encode_header(header: Header) -> Result<[u8; HEADER_BYTES], format::FormatError> {
    header.validate()?;
    let mut output = [0; HEADER_BYTES];
    output[..8].copy_from_slice(format::MAGIC);
    output[8..12].copy_from_slice(&HEADER_VERSION.to_le_bytes());
    output[12..16].copy_from_slice(&(HEADER_BYTES as u32).to_le_bytes());
    output[16..24].copy_from_slice(&header.sequence.to_le_bytes());
    output[24..28].copy_from_slice(&header.layout.shard_count.to_le_bytes());
    output[28..32].copy_from_slice(&header.layout.routing_version.to_le_bytes());
    output[32..36].copy_from_slice(&(header.record_version as u32).to_le_bytes());
    output[36..40].copy_from_slice(&u32::from(header.mixed_owner_batch).to_le_bytes());
    if let Some(metadata) = header.replication {
        output[40] = match metadata.role {
            Role::Primary => 1,
            Role::Replica => 2,
        };
        output[44..60].copy_from_slice(&metadata.epoch);
    }
    let crc = format::checksum(&output[..60]);
    output[60..64].copy_from_slice(&crc.to_le_bytes());
    Ok(output)
}

/// Reads exactly one fixed-size header. Unsupported profiles must be rejected by
/// a caller before opening or scanning records; this function only validates the
/// registered grammar.
pub fn read_header(mut input: impl Read) -> Result<Header, format::FormatError> {
    let mut bytes = [0; HEADER_BYTES];
    match input.read_exact(&mut bytes[..12]) {
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            return Err(format::FormatError::Header);
        }
        result => result?,
    }
    if &bytes[..8] != format::MAGIC {
        return Err(format::FormatError::Header);
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    if version != HEADER_VERSION {
        return Err(format::FormatError::Version(version));
    }
    match input.read_exact(&mut bytes[12..]) {
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            return Err(format::FormatError::Header);
        }
        result => result?,
    }
    if u32::from_le_bytes(bytes[12..16].try_into().unwrap()) != HEADER_BYTES as u32 {
        return Err(format::FormatError::Corrupt("header length"));
    }
    if format::checksum(&bytes[..60]) != u32::from_le_bytes(bytes[60..64].try_into().unwrap()) {
        return Err(format::FormatError::Corrupt("header checksum"));
    }
    let record_version = match u32::from_le_bytes(bytes[32..36].try_into().unwrap()) {
        1 => RecordVersion::Legacy,
        2 => RecordVersion::Memory,
        _ => return Err(format::FormatError::Corrupt("record version")),
    };
    let capabilities = u32::from_le_bytes(bytes[36..40].try_into().unwrap());
    if capabilities & !MIXED_OWNER_BATCH != 0 {
        return Err(format::FormatError::Corrupt("required capabilities"));
    }
    if bytes[41..44] != [0; 3] {
        return Err(format::FormatError::Corrupt("reserved header bytes"));
    }
    let epoch: [u8; 16] = bytes[44..60].try_into().unwrap();
    let replication = match bytes[40] {
        0 if epoch == [0; 16] => None,
        0 => return Err(format::FormatError::Corrupt("independent epoch")),
        1 | 2 if epoch == [0; 16] => {
            return Err(format::FormatError::Corrupt("replication epoch"));
        }
        role @ (1 | 2) => Some(ReplicationMetadata {
            role: if role == 1 {
                Role::Primary
            } else {
                Role::Replica
            },
            epoch,
        }),
        _ => return Err(format::FormatError::Corrupt("role")),
    };
    let header = Header {
        sequence: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        layout: DurableLayout {
            shard_count: u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
            routing_version: u32::from_le_bytes(bytes[28..32].try_into().unwrap()),
        },
        record_version,
        mixed_owner_batch: capabilities & MIXED_OWNER_BATCH != 0,
        replication,
    };
    header.validate()?;
    Ok(header)
}
