//! Isolated record-2 codec for the accepted memory contract.
//!
//! Default recovery, append, backup, and transport still use record 1. They
//! continue refusing headers 4/5; an explicit development AOF profile exercises
//! record-2 recovery, append, and compaction. Separate explicit library backup APIs
//! export/verify/restore schema 2; default backup and all live transport remain legacy.
//! This module intentionally has no directory-opening or tail-repair API.

use super::{DurableLayout, ReplicationMetadata, Role, format};
use crate::storage::{
    MutationOrigin,
    memory::{
        Collection, Descriptor, Edit, MAX_EDITS, MAX_MEMBERS, Member, Scalar, validate_edits,
    },
};
use bytes::Bytes;
use format::{FormatError, Limits};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read};

pub const RECORD_VERSION: u32 = 2;
pub const HEADER_VERSION: u32 = 4;
pub const REPLICATION_HEADER_VERSION: u32 = 5;
pub const HEADER_BYTES: usize = 36;
pub const REPLICATION_HEADER_BYTES: usize = 56;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub sequence: u64,
    pub layout: DurableLayout,
    pub replication: Option<ReplicationMetadata>,
}

pub fn encode_header(header: Header) -> Result<Vec<u8>, FormatError> {
    header
        .layout
        .validate()
        .map_err(|_| FormatError::Corrupt("layout"))?;
    if header
        .replication
        .is_some_and(|metadata| metadata.role == Role::Primary && metadata.epoch == [0; 16])
    {
        return Err(FormatError::Corrupt("primary epoch"));
    }
    let mut output = Vec::with_capacity(if header.replication.is_some() {
        REPLICATION_HEADER_BYTES
    } else {
        HEADER_BYTES
    });
    output.extend_from_slice(format::MAGIC);
    output.extend_from_slice(
        &if header.replication.is_some() {
            REPLICATION_HEADER_VERSION
        } else {
            HEADER_VERSION
        }
        .to_le_bytes(),
    );
    output.extend_from_slice(&header.sequence.to_le_bytes());
    output.extend_from_slice(&header.layout.shard_count.to_le_bytes());
    output.extend_from_slice(&header.layout.routing_version.to_le_bytes());
    output.extend_from_slice(&RECORD_VERSION.to_le_bytes());
    if let Some(metadata) = header.replication {
        output.push(match metadata.role {
            Role::Primary => 1,
            Role::Replica => 2,
        });
        output.extend_from_slice(&[0; 3]);
        output.extend_from_slice(&metadata.epoch);
    }
    let crc = format::checksum(&output);
    output.extend_from_slice(&crc.to_le_bytes());
    Ok(output)
}

/// Reads only fixed supported header lengths; rejects versions before records.
pub fn read_header(mut input: impl Read) -> Result<Header, FormatError> {
    let mut prefix = [0; 12];
    input.read_exact(&mut prefix)?;
    if &prefix[..8] != format::MAGIC {
        return Err(FormatError::Corrupt("magic"));
    }
    let version = u32::from_le_bytes(prefix[8..12].try_into().expect("fixed version"));
    let length = match version {
        HEADER_VERSION => HEADER_BYTES,
        REPLICATION_HEADER_VERSION => REPLICATION_HEADER_BYTES,
        _ => return Err(FormatError::Version(version)),
    };
    let mut bytes = [0; REPLICATION_HEADER_BYTES];
    bytes[..12].copy_from_slice(&prefix);
    input.read_exact(&mut bytes[12..length])?;
    let crc = u32::from_le_bytes(bytes[length - 4..length].try_into().expect("fixed crc"));
    if format::checksum(&bytes[..length - 4]) != crc {
        return Err(FormatError::Corrupt("header checksum"));
    }
    let layout = DurableLayout {
        shard_count: u32::from_le_bytes(bytes[20..24].try_into().expect("fixed shard count")),
        routing_version: u32::from_le_bytes(
            bytes[24..28].try_into().expect("fixed routing version"),
        ),
    };
    layout
        .validate()
        .map_err(|_| FormatError::Corrupt("layout"))?;
    if u32::from_le_bytes(bytes[28..32].try_into().expect("fixed record version")) != RECORD_VERSION
    {
        return Err(FormatError::Corrupt("record version"));
    }
    let replication = if version == REPLICATION_HEADER_VERSION {
        if bytes[33..36] != [0; 3] {
            return Err(FormatError::Corrupt("reserved header bytes"));
        }
        let role = match bytes[32] {
            1 => Role::Primary,
            2 => Role::Replica,
            _ => return Err(FormatError::Corrupt("role")),
        };
        let epoch = bytes[36..52].try_into().expect("fixed epoch");
        if role == Role::Primary && epoch == [0; 16] {
            return Err(FormatError::Corrupt("primary epoch"));
        }
        Some(ReplicationMetadata { role, epoch })
    } else {
        None
    };
    Ok(Header {
        sequence: u64::from_le_bytes(bytes[12..20].try_into().expect("fixed sequence")),
        layout,
        replication,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mutation {
    Legacy(crate::storage::Mutation),
    Image {
        key: Bytes,
        collection: Collection,
        key_deadline: Option<i64>,
    },
    Delta {
        key: Bytes,
        descriptor: Descriptor,
        edits: Vec<Edit>,
        key_deadline: Option<i64>,
    },
}

impl Mutation {
    pub fn from_storage(mutation: crate::storage::Mutation) -> Self {
        match mutation {
            crate::storage::Mutation::Put {
                key,
                value: crate::storage::Value::Memory(collection),
                expires_at_unix_ms,
            } => Self::Image {
                key,
                collection: (*collection).clone(),
                key_deadline: expires_at_unix_ms,
            },
            mutation => Self::Legacy(mutation),
        }
    }
    pub fn key(&self) -> &Bytes {
        match self {
            Self::Legacy(mutation) => mutation.key(),
            Self::Image { key, .. } | Self::Delta { key, .. } => key,
        }
    }

    pub fn encoded_bytes(&self) -> Result<usize, FormatError> {
        match self {
            Self::Legacy(mutation) => format::mutation_size(mutation),
            Self::Image {
                key, collection, ..
            } => {
                collection
                    .validate_key(key)
                    .map_err(|_| FormatError::Corrupt("memory image"))?;
                Ok(5 + key.len() + collection.encoded_bytes() + 9)
            }
            Self::Delta {
                key,
                descriptor,
                edits,
                ..
            } => {
                if key.is_empty() || key.len() > 256 || edits.len() > MAX_EDITS {
                    return Err(FormatError::Corrupt("memory delta bounds"));
                }
                validate_edits(descriptor, edits)
                    .map_err(|_| FormatError::Corrupt("memory delta"))?;
                edits
                    .iter()
                    .try_fold(18 + key.len() + descriptor.encoded_bytes(), |size, edit| {
                        size.checked_add(edit.encoded_bytes())
                            .ok_or(FormatError::Limit)
                    })
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Snapshot(Mutation),
    Seal {
        sequence: u64,
        entries: u64,
        digest: u32,
    },
    Batch {
        sequence: u64,
        origin: MutationOrigin,
        mutations: Vec<Mutation>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Next {
    End,
    IncompleteTail,
    Record(Box<Record>),
}

/// Preflights every field and the complete payload before reserving the frame.
pub fn encode(record: &Record, limits: Limits) -> Result<Vec<u8>, FormatError> {
    let size = match record {
        Record::Snapshot(Mutation::Delta { .. }) => {
            return Err(FormatError::Corrupt("delta snapshot"));
        }
        Record::Snapshot(mutation) => mutation
            .encoded_bytes()?
            .checked_add(1)
            .ok_or(FormatError::Limit)?,
        Record::Seal { .. } => 21,
        Record::Batch { mutations, .. } => {
            if mutations.is_empty()
                || mutations.len() > limits.max_mutations
                || mutations.len() > u32::MAX as usize
            {
                return Err(FormatError::Limit);
            }
            let mut keys = BTreeSet::new();
            mutations.iter().try_fold(14usize, |size, mutation| {
                if !keys.insert(mutation.key()) {
                    return Err(FormatError::Corrupt("duplicate batch key"));
                }
                size.checked_add(mutation.encoded_bytes()?)
                    .ok_or(FormatError::Limit)
            })?
        }
    };
    if size > limits.max_record_bytes || size > format::MAX_RECORD_BYTES {
        return Err(FormatError::Limit);
    }
    let mut output = Vec::with_capacity(size + 12);
    output.extend_from_slice(&(size as u32).to_le_bytes());
    output.extend_from_slice(&(!(size as u32)).to_le_bytes());
    output.extend_from_slice(&[0; 4]);
    match record {
        Record::Snapshot(mutation) => {
            output.push(1);
            encode_mutation(&mut output, mutation)?;
        }
        Record::Seal {
            sequence,
            entries,
            digest,
        } => {
            output.push(2);
            output.extend_from_slice(&sequence.to_le_bytes());
            output.extend_from_slice(&entries.to_le_bytes());
            output.extend_from_slice(&digest.to_le_bytes());
        }
        Record::Batch {
            sequence,
            origin,
            mutations,
        } => {
            output.push(3);
            output.extend_from_slice(&sequence.to_le_bytes());
            output.push(match origin {
                MutationOrigin::Client => 1,
                MutationOrigin::Expiration => 2,
            });
            output.extend_from_slice(&(mutations.len() as u32).to_le_bytes());
            for mutation in mutations {
                encode_mutation(&mut output, mutation)?;
            }
        }
    }
    debug_assert_eq!(output.len(), size + 12);
    let crc = format::checksum(&output[12..]);
    output[8..12].copy_from_slice(&crc.to_le_bytes());
    Ok(output)
}

fn blob(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    output.extend_from_slice(bytes);
}
fn deadline(output: &mut Vec<u8>, deadline: Option<i64>) {
    output.push(u8::from(deadline.is_some()));
    output.extend_from_slice(&deadline.unwrap_or(0).to_le_bytes());
}
fn descriptor(output: &mut Vec<u8>, descriptor: &Descriptor) {
    output.extend_from_slice(&1u32.to_le_bytes());
    output.extend_from_slice(&descriptor.dimension.to_le_bytes());
    output.extend_from_slice(&[1, 1]);
    for value in [
        &descriptor.provider,
        &descriptor.model,
        &descriptor.revision,
        &descriptor.transform,
    ] {
        blob(output, value);
    }
}
fn member(output: &mut Vec<u8>, member: &Member, dimension: u32) {
    blob(output, &member.id);
    blob(output, &member.namespace);
    blob(output, &member.content);
    output.extend_from_slice(&dimension.to_le_bytes());
    output.extend_from_slice(&member.vector);
    deadline(output, member.deadline);
    output.extend_from_slice(&(member.metadata.len() as u32).to_le_bytes());
    for (field, scalar) in &member.metadata {
        blob(output, field);
        match scalar {
            Scalar::Bytes(value) => {
                output.push(1);
                blob(output, value);
            }
            Scalar::Integer(value) => {
                output.push(2);
                output.extend_from_slice(&value.to_le_bytes());
            }
            Scalar::Bool(value) => {
                output.push(3);
                output.push(u8::from(*value));
            }
        }
    }
    output.extend_from_slice(&(member.provenance.len() as u32).to_le_bytes());
    for (field, value) in &member.provenance {
        blob(output, field);
        blob(output, value);
    }
}
fn encode_mutation(output: &mut Vec<u8>, mutation: &Mutation) -> Result<(), FormatError> {
    match mutation {
        Mutation::Legacy(mutation) => format::encode_mutation(output, mutation)?,
        Mutation::Image {
            key,
            collection,
            key_deadline,
        } => {
            output.push(7);
            blob(output, key);
            descriptor(output, collection.descriptor());
            output.extend_from_slice(&(collection.retained_count() as u32).to_le_bytes());
            for record in collection.members().values() {
                member(output, record, collection.descriptor().dimension);
            }
            deadline(output, *key_deadline);
        }
        Mutation::Delta {
            key,
            descriptor: schema,
            edits,
            key_deadline,
        } => {
            output.push(8);
            blob(output, key);
            descriptor(output, schema);
            output.extend_from_slice(&(edits.len() as u32).to_le_bytes());
            for edit in edits {
                match edit {
                    Edit::Upsert(record) => {
                        output.push(1);
                        member(output, record, schema.dimension);
                    }
                    Edit::Remove(id) => {
                        output.push(2);
                        blob(output, id);
                    }
                }
            }
            deadline(output, *key_deadline);
        }
    }
    Ok(())
}

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
        return incomplete(error);
    }
    let size = u32::from_le_bytes(prefix[..4].try_into().expect("fixed length"));
    if !size != u32::from_le_bytes(prefix[4..8].try_into().expect("fixed complement")) {
        return Err(FormatError::Corrupt("length"));
    }
    let size = size as usize;
    if size == 0 || size > limits.max_record_bytes || size > format::MAX_RECORD_BYTES {
        return Err(FormatError::Limit);
    }
    let mut body = vec![0; size];
    if let Err(error) = input.read_exact(&mut body) {
        return incomplete(error);
    }
    if format::checksum(&body) != u32::from_le_bytes(prefix[8..12].try_into().expect("fixed crc")) {
        return Err(FormatError::Corrupt("checksum"));
    }
    decode(Bytes::from(body), limits).map(|record| Next::Record(Box::new(record)))
}
fn incomplete(error: io::Error) -> Result<Next, FormatError> {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        Ok(Next::IncompleteTail)
    } else {
        Err(error.into())
    }
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
            .ok_or(FormatError::Corrupt("internal length"))?;
        let bytes = self.bytes.slice(self.offset..end);
        self.offset = end;
        Ok(bytes)
    }
    fn byte(&mut self) -> Result<u8, FormatError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, FormatError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.as_ref().try_into().expect("four bytes"),
        ))
    }
    fn u64(&mut self) -> Result<u64, FormatError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.as_ref().try_into().expect("eight bytes"),
        ))
    }
    fn i64(&mut self) -> Result<i64, FormatError> {
        Ok(i64::from_le_bytes(
            self.take(8)?.as_ref().try_into().expect("eight bytes"),
        ))
    }
    fn blob(&mut self, minimum: usize, maximum: usize) -> Result<Bytes, FormatError> {
        let size = self.u32()? as usize;
        if size < minimum || size > maximum {
            return Err(FormatError::Corrupt("memory blob length"));
        }
        self.take(size)
    }
    fn deadline(&mut self, member: bool) -> Result<Option<i64>, FormatError> {
        let flag = self.byte()?;
        let value = self.i64()?;
        match flag {
            0 if value == 0 => Ok(None),
            1 if !member || value > 0 => Ok(Some(value)),
            _ => Err(FormatError::Corrupt("deadline")),
        }
    }
    fn descriptor(&mut self) -> Result<Descriptor, FormatError> {
        if self.u32()? != 1 {
            return Err(FormatError::Corrupt("memory schema version"));
        }
        let dimension = self.u32()?;
        if !(1..=4096).contains(&dimension) {
            return Err(FormatError::Corrupt("memory dimension"));
        }
        if self.byte()? != 1 || self.byte()? != 1 {
            return Err(FormatError::Corrupt("memory encoding or metric"));
        }
        Ok(Descriptor {
            dimension,
            provider: self.blob(1, 256)?,
            model: self.blob(1, 256)?,
            revision: self.blob(1, 256)?,
            transform: self.blob(1, 256)?,
        })
    }
    fn count(&mut self, maximum: usize, minimum_encoded_size: usize) -> Result<usize, FormatError> {
        let count = self.u32()? as usize;
        if count > maximum || count > (self.bytes.len() - self.offset) / minimum_encoded_size {
            return Err(FormatError::Corrupt("memory count"));
        }
        Ok(count)
    }
    fn member(&mut self, descriptor: &Descriptor) -> Result<Member, FormatError> {
        let id = self.blob(1, 256)?;
        let namespace = self.blob(0, 256)?;
        let content = self.blob(0, 65_536)?;
        if self.u32()? != descriptor.dimension {
            return Err(FormatError::Corrupt("memory vector dimension"));
        }
        let vector = self.take(descriptor.dimension as usize * 4)?;
        let deadline = self.deadline(true)?;
        let count = self.count(32, 6)?;
        let mut metadata = BTreeMap::new();
        let mut previous: Option<Bytes> = None;
        for _ in 0..count {
            let field = self.blob(1, 64)?;
            ordered(&mut previous, &field)?;
            let scalar = match self.byte()? {
                1 => Scalar::Bytes(self.blob(0, 4096)?),
                2 => Scalar::Integer(self.i64()?),
                3 => Scalar::Bool(match self.byte()? {
                    0 => false,
                    1 => true,
                    _ => return Err(FormatError::Corrupt("memory bool")),
                }),
                _ => return Err(FormatError::Corrupt("memory scalar")),
            };
            metadata.insert(field, scalar);
        }
        let count = self.count(16, 9)?;
        let mut provenance = BTreeMap::new();
        previous = None;
        for _ in 0..count {
            let field = self.blob(1, 64)?;
            ordered(&mut previous, &field)?;
            let value = self.blob(0, 4096)?;
            provenance.insert(field, value);
        }
        let member = Member {
            id,
            namespace,
            content,
            vector,
            deadline,
            metadata,
            provenance,
        };
        member
            .validate(descriptor)
            .map_err(|_| FormatError::Corrupt("memory member"))?;
        Ok(member)
    }
    fn mutation(&mut self) -> Result<Mutation, FormatError> {
        let start = self.offset;
        let tag = self.byte()?;
        if matches!(tag, 1..=6) {
            let mut legacy = format::Cursor {
                bytes: self.bytes.clone(),
                offset: start,
            };
            let mutation = legacy.mutation()?;
            self.offset = legacy.offset;
            return Ok(Mutation::Legacy(mutation));
        }
        if !matches!(tag, 7 | 8) {
            return Err(FormatError::Corrupt("mutation type"));
        }
        let key = self.blob(1, 256)?;
        let descriptor = self.descriptor()?;
        if tag == 7 {
            let count = self.count(MAX_MEMBERS, 38)?;
            if count
                .checked_mul(descriptor.dimension as usize)
                .is_none_or(|work| work > crate::storage::memory::MAX_COMPONENTS)
            {
                return Err(FormatError::Corrupt("memory component quota"));
            }
            let mut members = Vec::with_capacity(count);
            let mut previous = None;
            for _ in 0..count {
                let member = self.member(&descriptor)?;
                ordered(&mut previous, &member.id)?;
                members.push(member);
            }
            let key_deadline = self.deadline(false)?;
            let collection = Collection::from_members(descriptor, members, &key)
                .map_err(|_| FormatError::Corrupt("memory image quota"))?;
            Ok(Mutation::Image {
                key,
                collection,
                key_deadline,
            })
        } else {
            let count = self.count(MAX_EDITS, 6)?;
            let mut edits = Vec::with_capacity(count);
            let mut previous = None;
            for _ in 0..count {
                let edit = match self.byte()? {
                    1 => Edit::Upsert(self.member(&descriptor)?),
                    2 => Edit::Remove(self.blob(1, 256)?),
                    _ => return Err(FormatError::Corrupt("memory edit")),
                };
                ordered(&mut previous, edit.id())?;
                edits.push(edit);
            }
            let key_deadline = self.deadline(false)?;
            Ok(Mutation::Delta {
                key,
                descriptor,
                edits,
                key_deadline,
            })
        }
    }
}
fn ordered(previous: &mut Option<Bytes>, value: &Bytes) -> Result<(), FormatError> {
    if previous.as_ref().is_some_and(|previous| previous >= value) {
        return Err(FormatError::Corrupt("memory order or duplicate"));
    }
    *previous = Some(value.clone());
    Ok(())
}
fn decode(bytes: Bytes, limits: Limits) -> Result<Record, FormatError> {
    let mut cursor = Cursor { bytes, offset: 0 };
    let record = match cursor.byte()? {
        1 => {
            let mutation = cursor.mutation()?;
            if matches!(mutation, Mutation::Delta { .. }) {
                return Err(FormatError::Corrupt("delta snapshot"));
            }
            Record::Snapshot(mutation)
        }
        2 => Record::Seal {
            sequence: cursor.u64()?,
            entries: cursor.u64()?,
            digest: cursor.u32()?,
        },
        3 => {
            let sequence = cursor.u64()?;
            let origin = match cursor.byte()? {
                1 => MutationOrigin::Client,
                2 => MutationOrigin::Expiration,
                _ => return Err(FormatError::Corrupt("source")),
            };
            let count = cursor.count(limits.max_mutations, 5)?;
            if count == 0 {
                return Err(FormatError::Limit);
            }
            let mut mutations = Vec::with_capacity(count);
            let mut keys = BTreeSet::new();
            for _ in 0..count {
                let mutation = cursor.mutation()?;
                if !keys.insert(mutation.key().clone()) {
                    return Err(FormatError::Corrupt("duplicate batch key"));
                }
                mutations.push(mutation);
            }
            Record::Batch {
                sequence,
                origin,
                mutations,
            }
        }
        _ => return Err(FormatError::Corrupt("record type")),
    };
    if cursor.offset != cursor.bytes.len() {
        return Err(FormatError::Corrupt("bytes extras"));
    }
    Ok(record)
}
