//! Explicit development profile selection; never inferred from current values.
use super::{DurableLayout, ReplicationMetadata, composed_format, format, memory_format};
use crate::storage::{Mutation, ResolvedBatch, Value};
use std::io::{Read, Write};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Profile {
    Legacy,
    Memory,
    /// Internal recovery proof only; public configuration never selects this profile.
    ComposedLegacyMixed,
}

impl Profile {
    pub fn selected(memory: bool) -> Self {
        if memory { Self::Memory } else { Self::Legacy }
    }

    pub fn write_header(
        self,
        mut output: impl Write,
        sequence: u64,
        layout: DurableLayout,
        replication: Option<ReplicationMetadata>,
    ) -> Result<(), format::FormatError> {
        if self == Self::ComposedLegacyMixed {
            if replication.is_some() {
                return Err(format::FormatError::Corrupt("unsupported composed role"));
            }
            output.write_all(&composed_format::encode_header(composed_format::Header {
                sequence,
                layout,
                record_version: composed_format::RecordVersion::Legacy,
                mixed_owner_batch: true,
                replication: None,
            })?)?;
            Ok(())
        } else if self == Self::Memory {
            output.write_all(&memory_format::encode_header(memory_format::Header {
                sequence,
                layout,
                replication,
            })?)?;
            Ok(())
        } else {
            format::write_header_with_replication(output, sequence, layout, replication)
        }
    }

    pub fn read_header(self, input: impl Read) -> Result<format::Header, format::FormatError> {
        if self == Self::ComposedLegacyMixed {
            let header = composed_format::read_header(input)?;
            if header.record_version != composed_format::RecordVersion::Legacy
                || !header.mixed_owner_batch
                || header.replication.is_some()
            {
                return Err(format::FormatError::Corrupt("unsupported composed profile"));
            }
            return Ok(format::Header {
                sequence: header.sequence,
                layout: header.layout,
                replication: None,
                format_version: composed_format::HEADER_VERSION,
                bytes: composed_format::HEADER_BYTES,
            });
        }
        if self != Self::Memory {
            return format::read_header_with_layout(input);
        }
        let header = memory_format::read_header(input)?;
        Ok(format::Header {
            sequence: header.sequence,
            layout: header.layout,
            replication: header.replication,
            format_version: if header.replication.is_some() { 5 } else { 4 },
            bytes: if header.replication.is_some() { 56 } else { 36 },
        })
    }

    pub fn read_record(
        self,
        input: impl Read,
        limits: format::Limits,
    ) -> Result<memory_format::Next, format::FormatError> {
        if self == Self::Memory {
            return memory_format::read_record(input, limits);
        }
        Ok(match format::read_record(input, limits)? {
            format::Next::End => memory_format::Next::End,
            format::Next::IncompleteTail => memory_format::Next::IncompleteTail,
            format::Next::Record(record) => memory_format::Next::Record(Box::new(convert(record))),
        })
    }

    pub fn encode_legacy(
        self,
        record: format::Record,
        limits: format::Limits,
    ) -> Result<Vec<u8>, format::FormatError> {
        self.encode(&convert(record), limits)
    }

    pub fn encode(
        self,
        record: &memory_format::Record,
        limits: format::Limits,
    ) -> Result<Vec<u8>, format::FormatError> {
        if self == Self::Memory {
            return memory_format::encode(record, limits);
        }
        let legacy = match record {
            memory_format::Record::Snapshot(mutation) => {
                format::Record::Snapshot(legacy(mutation)?)
            }
            memory_format::Record::Seal {
                sequence,
                entries,
                digest,
            } => format::Record::Seal {
                sequence: *sequence,
                entries: *entries,
                digest: *digest,
            },
            memory_format::Record::Batch {
                sequence,
                origin,
                mutations,
            } => format::Record::Batch {
                sequence: *sequence,
                batch: ResolvedBatch {
                    origin: *origin,
                    mutations: mutations.iter().map(legacy).collect::<Result<_, _>>()?,
                },
            },
        };
        format::encode(&legacy, limits)
    }
}

fn legacy(mutation: &memory_format::Mutation) -> Result<Mutation, format::FormatError> {
    match mutation {
        memory_format::Mutation::Legacy(mutation) => Ok(mutation.clone()),
        memory_format::Mutation::Image {
            key,
            collection,
            key_deadline,
        } => Ok(Mutation::Put {
            key: key.clone(),
            value: Value::Memory(std::sync::Arc::new(collection.clone())),
            expires_at_unix_ms: *key_deadline,
        }),
        memory_format::Mutation::Delta { .. } => Err(format::FormatError::Version(2)),
    }
}

fn convert(record: format::Record) -> memory_format::Record {
    match record {
        format::Record::Snapshot(mutation) => {
            memory_format::Record::Snapshot(memory_format::Mutation::from_storage(mutation))
        }
        format::Record::Seal {
            sequence,
            entries,
            digest,
        } => memory_format::Record::Seal {
            sequence,
            entries,
            digest,
        },
        format::Record::Batch { sequence, batch } => memory_format::Record::Batch {
            sequence,
            origin: batch.origin,
            mutations: batch
                .mutations
                .into_iter()
                .map(memory_format::Mutation::from_storage)
                .collect(),
        },
    }
}
