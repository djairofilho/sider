//! Operações de coleções binárias, validadas antes de consultar o banco.

use super::{Command, RequestError};
use bytes::Bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HashCommand {
    Set { entries: Vec<(Bytes, Bytes)> },
    Get { field: Bytes },
    Delete { fields: Vec<Bytes> },
    Exists { field: Bytes },
    Len,
    GetAll,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListCommand {
    Push { left: bool, values: Vec<Bytes> },
    Pop { left: bool },
    Len,
    Range { start: i64, stop: i64 },
}

pub(super) fn parse(
    name: Bytes,
    mut args: std::vec::IntoIter<Bytes>,
) -> Result<Command, RequestError> {
    let name = name.to_ascii_uppercase();
    let (canonical, valid) = match name.as_slice() {
        b"HSET" => ("hset", args.len() >= 3 && args.len() % 2 == 1),
        b"HGET" => ("hget", args.len() == 2),
        b"HDEL" => ("hdel", args.len() >= 2),
        b"HEXISTS" => ("hexists", args.len() == 2),
        b"HLEN" => ("hlen", args.len() == 1),
        b"HGETALL" => ("hgetall", args.len() == 1),
        b"LPUSH" => ("lpush", args.len() >= 2),
        b"RPUSH" => ("rpush", args.len() >= 2),
        b"LPOP" => ("lpop", args.len() == 1),
        b"RPOP" => ("rpop", args.len() == 1),
        b"LLEN" => ("llen", args.len() == 1),
        b"LRANGE" => ("lrange", args.len() == 3),
        _ => return Err(RequestError::UnknownCommand),
    };
    if !valid {
        return Err(RequestError::WrongArity(canonical));
    }
    let key = args.next().ok_or(RequestError::WrongArity(canonical))?;
    if name[0] == b'L' || name[0] == b'R' {
        let operation = match name.as_slice() {
            b"LPUSH" | b"RPUSH" => ListCommand::Push {
                left: name[0] == b'L',
                values: args.collect(),
            },
            b"LPOP" | b"RPOP" => ListCommand::Pop {
                left: name[0] == b'L',
            },
            b"LLEN" => ListCommand::Len,
            b"LRANGE" => {
                let start =
                    super::parse_decimal(&args.next().ok_or(RequestError::WrongArity(canonical))?)
                        .ok_or(RequestError::InvalidInteger)?;
                let stop =
                    super::parse_decimal(&args.next().ok_or(RequestError::WrongArity(canonical))?)
                        .ok_or(RequestError::InvalidInteger)?;
                ListCommand::Range { start, stop }
            }
            _ => unreachable!("nome validado"),
        };
        return Ok(Command::List { key, operation });
    }
    let operation = match name.as_slice() {
        b"HSET" => {
            let mut entries = Vec::with_capacity(args.len() / 2);
            while let Some(field) = args.next() {
                entries.push((
                    field,
                    args.next().ok_or(RequestError::WrongArity(canonical))?,
                ));
            }
            HashCommand::Set { entries }
        }
        b"HGET" => HashCommand::Get {
            field: args.next().ok_or(RequestError::WrongArity(canonical))?,
        },
        b"HDEL" => HashCommand::Delete {
            fields: args.collect(),
        },
        b"HEXISTS" => HashCommand::Exists {
            field: args.next().ok_or(RequestError::WrongArity(canonical))?,
        },
        b"HLEN" => HashCommand::Len,
        b"HGETALL" => HashCommand::GetAll,
        _ => unreachable!("nome validado"),
    };
    Ok(Command::Hash { key, operation })
}
