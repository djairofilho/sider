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
        _ => return Err(RequestError::UnknownCommand),
    };
    if !valid {
        return Err(RequestError::WrongArity(canonical));
    }
    let key = args.next().ok_or(RequestError::WrongArity(canonical))?;
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
