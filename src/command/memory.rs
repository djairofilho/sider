//! Frozen `sider-memory/1` lifecycle grammar. Binary payloads are moved unchanged.

use super::{Command, RequestError, SetCondition, parse_decimal};
use crate::storage::memory::{Descriptor, Member, Operation, Scalar, Ttl};
use bytes::Bytes;
use std::collections::BTreeMap;

pub const MAX_REQUEST_BYTES: usize = 262_144;
pub const MAX_REPLY_BYTES: usize = 131_072;
const ARGUMENT: &str = "ERR invalid memory argument";
const LIMIT: &str = "ERR memory limit exceeded";
const VECTOR: &str = "ERR memory vector length mismatch";
const TTL: &str = "ERR invalid memory ttl";

pub(crate) fn name(raw: &[u8]) -> Option<&'static str> {
    [
        "mem.create",
        "mem.info",
        "mem.put",
        "mem.get",
        "mem.del",
        "mem.pttl",
        "mem.expire",
        "mem.persist",
    ]
    .into_iter()
    .find(|kind| raw.eq_ignore_ascii_case(kind.as_bytes()))
}
fn error(message: &'static str) -> RequestError {
    RequestError::Memory(message)
}
fn bounded(value: &[u8], minimum: usize, maximum: usize) -> Result<(), RequestError> {
    if (minimum..=maximum).contains(&value.len()) {
        Ok(())
    } else {
        Err(error(LIMIT))
    }
}
fn unsigned(raw: &[u8]) -> Result<(), RequestError> {
    if raw == b"0"
        || (matches!(raw.first(), Some(b'1'..=b'9')) && raw.iter().all(u8::is_ascii_digit))
    {
        Ok(())
    } else {
        Err(error(ARGUMENT))
    }
}
fn count(raw: &[u8], maximum: usize) -> Result<usize, RequestError> {
    unsigned(raw)?;
    raw.iter().try_fold(0usize, |value, digit| {
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(usize::from(digit - b'0')))
            .filter(|value| *value <= maximum)
            .ok_or_else(|| error(LIMIT))
    })
}
fn bulk_size(length: usize) -> usize {
    length.saturating_add(length.checked_ilog10().unwrap_or(0) as usize + 6)
}

pub(crate) fn parse(raw: &[u8], args: Vec<Bytes>) -> Result<Command, RequestError> {
    let kind = name(raw).expect("recognized memory command");
    let length = args.len() + 1;
    let request_size = args.iter().fold(
        bulk_size(raw.len()).saturating_add(length.checked_ilog10().unwrap_or(0) as usize + 4),
        |size, arg| size.saturating_add(bulk_size(arg.len())),
    );
    if request_size > MAX_REQUEST_BYTES {
        return Err(error(LIMIT));
    }
    let arity = match kind {
        "mem.create" => 8,
        "mem.info" => 1,
        "mem.expire" => 3,
        "mem.put" => 13,
        _ => 2,
    };
    if (kind == "mem.put" && args.len() < arity) || (kind != "mem.put" && args.len() != arity) {
        return Err(RequestError::WrongArity(kind));
    }
    let mut args = args.into_iter();
    let key = args.next().expect("validated arity");
    bounded(&key, 1, 256)?;
    let operation = match kind {
        "mem.create" => {
            let version = args.next().unwrap();
            unsigned(&version)?;
            if version != b"1"[..] {
                return Err(error("ERR unsupported memory schema version"));
            }
            let dimension = count(&args.next().unwrap(), 4096)?;
            if dimension == 0 {
                return Err(error(LIMIT));
            }
            if !args.next().unwrap().eq_ignore_ascii_case(b"L2SQ") {
                return Err(error("ERR unsupported memory metric"));
            }
            Operation::Create(identity(&mut args, dimension as u32)?)
        }
        "mem.info" => Operation::Info,
        "mem.put" => {
            let id = args.next().unwrap();
            bounded(&id, 1, 256)?;
            let namespace = args.next().unwrap();
            bounded(&namespace, 0, 256)?;
            let content = args.next().unwrap();
            bounded(&content, 0, 65_536)?;
            let mut descriptor = identity(&mut args, 1)?;
            let vector = args.next().unwrap();
            if vector.is_empty() || !vector.len().is_multiple_of(4) {
                return Err(error(VECTOR));
            }
            bounded(&vector, 4, 4096 * 4)?;
            if vector.chunks_exact(4).any(|coordinate| {
                !f32::from_le_bytes(coordinate.try_into().expect("four bytes")).is_finite()
            }) {
                return Err(error("ERR memory vector contains non-finite value"));
            }
            descriptor.dimension = (vector.len() / 4) as u32;
            let raw_ttl = args.next().unwrap();
            let ttl = if raw_ttl.eq_ignore_ascii_case(b"KEEP") {
                Ttl::Keep
            } else {
                match parse_decimal(&raw_ttl) {
                    Some(-1) => Ttl::Persistent,
                    Some(value) if value > 0 => Ttl::After(value),
                    _ => return Err(error(TTL)),
                }
            };
            let condition = match args.next().unwrap().as_ref() {
                value if value.eq_ignore_ascii_case(b"UPSERT") => SetCondition::Always,
                value if value.eq_ignore_ascii_case(b"NX") => SetCondition::Missing,
                value if value.eq_ignore_ascii_case(b"XX") => SetCondition::Present,
                _ => return Err(error(ARGUMENT)),
            };
            let metadata_count = count(&args.next().unwrap(), 32)?;
            if args.len() < metadata_count * 3 + 1 {
                return Err(RequestError::WrongArity(kind));
            }
            let mut metadata = BTreeMap::new();
            for _ in 0..metadata_count {
                let field = args.next().unwrap();
                bounded(&field, 1, 64)?;
                let scalar_type = args.next().unwrap();
                let value = args.next().unwrap();
                let value = if scalar_type.eq_ignore_ascii_case(b"BYTES") {
                    bounded(&value, 0, 4096)?;
                    Scalar::Bytes(value)
                } else if scalar_type.eq_ignore_ascii_case(b"I64") {
                    Scalar::Integer(parse_decimal(&value).ok_or_else(|| error(ARGUMENT))?)
                } else if scalar_type.eq_ignore_ascii_case(b"BOOL") {
                    Scalar::Bool(match value.as_ref() {
                        b"0" => false,
                        b"1" => true,
                        _ => return Err(error(ARGUMENT)),
                    })
                } else {
                    return Err(error(ARGUMENT));
                };
                if metadata.insert(field, value).is_some() {
                    return Err(error(ARGUMENT));
                }
            }
            let provenance_count = count(&args.next().unwrap(), 16)?;
            if args.len() != provenance_count * 2 {
                return Err(RequestError::WrongArity(kind));
            }
            let mut provenance = BTreeMap::new();
            for _ in 0..provenance_count {
                let field = args.next().unwrap();
                bounded(&field, 1, 64)?;
                let value = args.next().unwrap();
                bounded(&value, 0, 4096)?;
                if provenance.insert(field, value).is_some() {
                    return Err(error(ARGUMENT));
                }
            }
            let member = Member {
                id,
                namespace,
                content,
                vector,
                deadline: None,
                metadata,
                provenance,
            };
            member.validate(&descriptor).map_err(|_| error(LIMIT))?;
            Operation::Put {
                descriptor,
                member: Box::new(member),
                ttl,
                condition,
            }
        }
        _ => {
            let id = args.next().unwrap();
            bounded(&id, 1, 256)?;
            match kind {
                "mem.get" => Operation::Get { id },
                "mem.del" => Operation::Delete { id },
                "mem.pttl" => Operation::Pttl { id },
                "mem.persist" => Operation::Persist { id },
                "mem.expire" => Operation::Expire {
                    id,
                    milliseconds: parse_decimal(&args.next().unwrap()).ok_or_else(|| error(TTL))?,
                },
                _ => unreachable!("recognized lifecycle command"),
            }
        }
    };
    Ok(Command::Memory {
        key,
        operation: Box::new(operation),
    })
}
fn identity(
    args: &mut std::vec::IntoIter<Bytes>,
    dimension: u32,
) -> Result<Descriptor, RequestError> {
    let provider = args.next().unwrap();
    bounded(&provider, 1, 256)?;
    let model = args.next().unwrap();
    bounded(&model, 1, 256)?;
    let revision = args.next().unwrap();
    bounded(&revision, 1, 256)?;
    let transform = args.next().unwrap();
    bounded(&transform, 1, 256)?;
    Ok(Descriptor {
        dimension,
        provider,
        model,
        revision,
        transform,
    })
}
