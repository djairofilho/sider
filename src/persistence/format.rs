//! Formato AOF v1: cabeçalho, registros limitados e CRC-32/ISO-HDLC.

use std::io::{self, Read, Write};

use bytes::Bytes;
use thiserror::Error;

use crate::storage::{Mutation, MutationOrigin, ResolvedBatch};

pub const MAGIC: &[u8; 8] = b"SIDERAOF";
pub const VERSION: u32 = 1;
pub const HEADER_BYTES: usize = 24;
pub const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_record_bytes: usize,
    pub max_mutations: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_record_bytes: 8 * 1024 * 1024,
            max_mutations: 100_000,
        }
    }
}

#[derive(Debug, Error)]
pub enum FormatError {
    #[error("falha de I/O: {0}")]
    Io(#[from] io::Error),
    #[error("cabeçalho AOF inválido ou incompleto")]
    Header,
    #[error("versão AOF não suportada: {0}")]
    Version(u32),
    #[error("registro AOF excede os limites")]
    Limit,
    #[error("registro AOF corrompido: {0}")]
    Corrupt(&'static str),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Snapshot(Mutation),
    Seal { sequence: u64 },
    Batch { sequence: u64, batch: ResolvedBatch },
}

#[derive(Debug, PartialEq, Eq)]
pub enum Next {
    End,
    IncompleteTail,
    Record(Record),
}

pub fn write_header(mut output: impl Write, sequence: u64) -> Result<(), FormatError> {
    let mut header = Vec::with_capacity(HEADER_BYTES);
    header.extend_from_slice(MAGIC);
    header.extend_from_slice(&VERSION.to_le_bytes());
    header.extend_from_slice(&sequence.to_le_bytes());
    header.extend_from_slice(&checksum(&header).to_le_bytes());
    output.write_all(&header)?;
    Ok(())
}

pub fn read_header(mut input: impl Read) -> Result<u64, FormatError> {
    let mut header = [0; HEADER_BYTES];
    match input.read_exact(&mut header) {
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            return Err(FormatError::Header);
        }
        result => result?,
    }
    if &header[..8] != MAGIC
        || checksum(&header[..20]) != u32::from_le_bytes(header[20..].try_into().unwrap())
    {
        return Err(FormatError::Header);
    }
    let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
    if version != VERSION {
        return Err(FormatError::Version(version));
    }
    Ok(u64::from_le_bytes(header[12..20].try_into().unwrap()))
}

fn mutation_size(mutation: &Mutation) -> Result<usize, FormatError> {
    let extra = match mutation {
        Mutation::Put { value, .. } => value.len().checked_add(13),
        Mutation::Delete { .. } => Some(0),
    };
    if mutation.key().len() > u32::MAX as usize {
        return Err(FormatError::Limit);
    }
    if matches!(mutation, Mutation::Put { value, .. } if value.len() > u32::MAX as usize) {
        return Err(FormatError::Limit);
    }
    mutation
        .key()
        .len()
        .checked_add(5)
        .and_then(|size| size.checked_add(extra?))
        .ok_or(FormatError::Limit)
}

pub fn encode(record: &Record, limits: Limits) -> Result<Vec<u8>, FormatError> {
    let size = match record {
        Record::Snapshot(mutation) => 1usize.checked_add(mutation_size(mutation)?),
        Record::Seal { .. } => Some(9),
        Record::Batch { batch, .. } => {
            if batch.mutations.is_empty()
                || batch.mutations.len() > limits.max_mutations
                || batch.mutations.len() > u32::MAX as usize
            {
                return Err(FormatError::Limit);
            }
            batch.mutations.iter().try_fold(14usize, |size, mutation| {
                size.checked_add(mutation_size(mutation).ok()?)
            })
        }
    }
    .ok_or(FormatError::Limit)?;
    if size > limits.max_record_bytes || size > MAX_RECORD_BYTES {
        return Err(FormatError::Limit);
    }
    let mut output = Vec::with_capacity(size + 12);
    output.extend_from_slice(&(size as u32).to_le_bytes());
    output.extend_from_slice(&(!(size as u32)).to_le_bytes());
    output.extend_from_slice(&0u32.to_le_bytes());
    match record {
        Record::Snapshot(mutation) => {
            output.push(1);
            encode_mutation(&mut output, mutation);
        }
        Record::Seal { sequence } => {
            output.push(2);
            output.extend_from_slice(&sequence.to_le_bytes());
        }
        Record::Batch { sequence, batch } => {
            output.push(3);
            output.extend_from_slice(&sequence.to_le_bytes());
            output.push(match batch.origin {
                MutationOrigin::Client => 1,
                MutationOrigin::Expiration => 2,
            });
            output.extend_from_slice(&(batch.mutations.len() as u32).to_le_bytes());
            for mutation in &batch.mutations {
                encode_mutation(&mut output, mutation);
            }
        }
    }
    let crc = checksum(&output[12..]);
    output[8..12].copy_from_slice(&crc.to_le_bytes());
    Ok(output)
}

fn encode_mutation(output: &mut Vec<u8>, mutation: &Mutation) {
    output.push(if matches!(mutation, Mutation::Put { .. }) {
        1
    } else {
        2
    });
    output.extend_from_slice(&(mutation.key().len() as u32).to_le_bytes());
    output.extend_from_slice(mutation.key());
    if let Mutation::Put {
        value,
        expires_at_unix_ms,
        ..
    } = mutation
    {
        output.extend_from_slice(&(value.len() as u32).to_le_bytes());
        output.extend_from_slice(value);
        output.push(u8::from(expires_at_unix_ms.is_some()));
        output.extend_from_slice(&expires_at_unix_ms.unwrap_or(0).to_le_bytes());
    }
}

/// Comprimento e seu complemento são validados antes da alocação; checksum antes do decode.
pub fn read_record(mut input: impl Read, limits: Limits) -> Result<Next, FormatError> {
    let mut prefix = [0; 12];
    loop {
        match input.read(&mut prefix[..1]) {
            Ok(0) => return Ok(Next::End),
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    if let Err(error) = input.read_exact(&mut prefix[1..]) {
        return if error.kind() == io::ErrorKind::UnexpectedEof {
            Ok(Next::IncompleteTail)
        } else {
            Err(error.into())
        };
    }
    let size = u32::from_le_bytes(prefix[..4].try_into().unwrap());
    if !size != u32::from_le_bytes(prefix[4..8].try_into().unwrap()) {
        return Err(FormatError::Corrupt("comprimento"));
    }
    let size = size as usize;
    if size == 0 || size > limits.max_record_bytes || size > MAX_RECORD_BYTES {
        return Err(FormatError::Limit);
    }
    let mut body = vec![0; size];
    if let Err(error) = input.read_exact(&mut body) {
        return if error.kind() == io::ErrorKind::UnexpectedEof {
            Ok(Next::IncompleteTail)
        } else {
            Err(error.into())
        };
    }
    if checksum(&body) != u32::from_le_bytes(prefix[8..].try_into().unwrap()) {
        return Err(FormatError::Corrupt("checksum"));
    }
    decode(Bytes::from(body), limits).map(Next::Record)
}

struct Cursor {
    bytes: Bytes,
    offset: usize,
}
impl Cursor {
    fn take(&mut self, count: usize) -> Result<Bytes, FormatError> {
        let end = self
            .offset
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(FormatError::Corrupt("comprimento interno"))?;
        let bytes = self.bytes.slice(self.offset..end);
        self.offset = end;
        Ok(bytes)
    }
    fn byte(&mut self) -> Result<u8, FormatError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, FormatError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.as_ref().try_into().unwrap(),
        ))
    }
    fn u64(&mut self) -> Result<u64, FormatError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.as_ref().try_into().unwrap(),
        ))
    }
    fn blob(&mut self) -> Result<Bytes, FormatError> {
        let size = self.u32()? as usize;
        self.take(size)
    }
    fn mutation(&mut self) -> Result<Mutation, FormatError> {
        let tag = self.byte()?;
        let key = self.blob()?;
        match tag {
            1 => {
                let value = self.blob()?;
                let expires = self.byte()?;
                let deadline = i64::from_le_bytes(self.take(8)?.as_ref().try_into().unwrap());
                let expires_at_unix_ms = match expires {
                    0 if deadline == 0 => None,
                    1 => Some(deadline),
                    _ => return Err(FormatError::Corrupt("deadline")),
                };
                Ok(Mutation::Put {
                    key,
                    value,
                    expires_at_unix_ms,
                })
            }
            2 => Ok(Mutation::Delete { key }),
            _ => Err(FormatError::Corrupt("tipo de mutação")),
        }
    }
}

fn decode(bytes: Bytes, limits: Limits) -> Result<Record, FormatError> {
    let mut cursor = Cursor { bytes, offset: 0 };
    let record = match cursor.byte()? {
        1 => Record::Snapshot(cursor.mutation()?),
        2 => Record::Seal {
            sequence: cursor.u64()?,
        },
        3 => {
            let sequence = cursor.u64()?;
            let origin = match cursor.byte()? {
                1 => MutationOrigin::Client,
                2 => MutationOrigin::Expiration,
                _ => return Err(FormatError::Corrupt("origem")),
            };
            let count = cursor.u32()? as usize;
            if count == 0
                || count > limits.max_mutations
                || count > (cursor.bytes.len() - cursor.offset) / 5
            {
                return Err(FormatError::Limit);
            }
            let mut mutations = Vec::with_capacity(count);
            for _ in 0..count {
                mutations.push(cursor.mutation()?);
            }
            Record::Batch {
                sequence,
                batch: ResolvedBatch { origin, mutations },
            }
        }
        _ => return Err(FormatError::Corrupt("tipo de registro")),
    };
    if cursor.offset != cursor.bytes.len() {
        return Err(FormatError::Corrupt("bytes extras"));
    }
    Ok(record)
}

/// CRC-32/ISO-HDLC, polinômio refletido 0xedb88320; detecta corrupção, não autentica conteúdo.
pub fn checksum(bytes: &[u8]) -> u32 {
    const TABLE: [u32; 256] = {
        let mut table = [0; 256];
        let mut index = 0;
        while index < 256 {
            let mut crc = index as u32;
            let mut bit = 0;
            while bit < 8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ 0xedb88320
                } else {
                    crc >> 1
                };
                bit += 1;
            }
            table[index] = crc;
            index += 1;
        }
        table
    };
    let mut crc = u32::MAX;
    for byte in bytes {
        crc = TABLE[((crc ^ u32::from(*byte)) & 255) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record() -> Record {
        Record::Batch {
            sequence: 42,
            batch: ResolvedBatch {
                origin: MutationOrigin::Client,
                mutations: vec![
                    Mutation::Put {
                        key: Bytes::from_static(b"\xff\0"),
                        value: Bytes::from_static(b"v\r\n"),
                        expires_at_unix_ms: Some(-1),
                    },
                    Mutation::Delete { key: Bytes::new() },
                ],
            },
        }
    }
    #[test]
    fn checksum_matches_standard_vector() {
        assert_eq!(checksum(b"123456789"), 0xcbf43926);
    }
    #[test]
    fn every_record_prefix_is_incomplete_and_complete_record_roundtrips() {
        let record = record();
        let encoded = encode(&record, Limits::default()).unwrap();
        assert_eq!(
            read_record(&encoded[..0], Limits::default()).unwrap(),
            Next::End
        );
        for length in 1..encoded.len() {
            assert_eq!(
                read_record(&encoded[..length], Limits::default()).unwrap(),
                Next::IncompleteTail,
                "prefix {length}"
            );
        }
        assert_eq!(
            read_record(encoded.as_slice(), Limits::default()).unwrap(),
            Next::Record(record)
        );
    }
    #[test]
    fn every_single_byte_corruption_is_rejected() {
        let encoded = encode(&record(), Limits::default()).unwrap();
        for position in 0..encoded.len() {
            let mut corrupt = encoded.clone();
            corrupt[position] ^= 0x80;
            assert!(
                read_record(corrupt.as_slice(), Limits::default()).is_err(),
                "byte {position}"
            );
        }
    }
    #[test]
    fn invalid_header_version_and_limits_fail_before_payload_allocation() {
        let mut header = Vec::new();
        write_header(&mut header, 7).unwrap();
        for length in 0..HEADER_BYTES {
            assert!(read_header(&header[..length]).is_err());
        }
        assert_eq!(read_header(header.as_slice()).unwrap(), 7);
        header[8..12].copy_from_slice(&2u32.to_le_bytes());
        let crc = checksum(&header[..20]);
        header[20..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            read_header(header.as_slice()),
            Err(FormatError::Version(2))
        ));
        let huge = [
            u32::MAX.to_le_bytes(),
            0u32.to_le_bytes(),
            0u32.to_le_bytes(),
        ]
        .concat();
        assert!(matches!(
            read_record(huge.as_slice(), Limits::default()),
            Err(FormatError::Limit)
        ));
        assert!(
            encode(
                &record(),
                Limits {
                    max_record_bytes: 1,
                    ..Limits::default()
                }
            )
            .is_err()
        );
    }
}
