//! Basic sorted-set subset, with scores and options validated before execution.

use super::{Command, RequestError, Score, parse_decimal};
use bytes::Bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SortedSetCommand {
    Add {
        entries: Vec<(Score, Bytes)>,
    },
    Remove {
        members: Vec<Bytes>,
    },
    Card,
    Score {
        member: Bytes,
    },
    Range {
        start: i64,
        stop: i64,
        with_scores: bool,
    },
}

pub(super) fn parse(
    name: Bytes,
    mut args: std::vec::IntoIter<Bytes>,
) -> Result<Command, RequestError> {
    let name = name.to_ascii_uppercase();
    let (canonical, valid) = match name.as_slice() {
        b"ZADD" => ("zadd", args.len() >= 3),
        b"ZREM" => ("zrem", args.len() >= 2),
        b"ZCARD" => ("zcard", args.len() == 1),
        b"ZSCORE" => ("zscore", args.len() == 2),
        b"ZRANGE" => ("zrange", args.len() >= 3),
        _ => return Err(RequestError::UnknownCommand),
    };
    if !valid {
        return Err(RequestError::WrongArity(canonical));
    }
    let key = args.next().ok_or(RequestError::WrongArity(canonical))?;
    let operation = match name.as_slice() {
        b"ZADD" => {
            if !args.len().is_multiple_of(2) {
                return Err(RequestError::Syntax);
            }
            let mut entries = Vec::with_capacity(args.len() / 2);
            while let Some(score) = args.next() {
                let score = Score::parse(&score).ok_or(RequestError::InvalidFloat)?;
                let member = args.next().ok_or(RequestError::Syntax)?;
                entries.push((score, member));
            }
            SortedSetCommand::Add { entries }
        }
        b"ZREM" => SortedSetCommand::Remove {
            members: args.collect(),
        },
        b"ZCARD" => SortedSetCommand::Card,
        b"ZSCORE" => SortedSetCommand::Score {
            member: args.next().ok_or(RequestError::WrongArity(canonical))?,
        },
        b"ZRANGE" => {
            let start = parse_decimal(&args.next().ok_or(RequestError::WrongArity(canonical))?)
                .ok_or(RequestError::InvalidInteger)?;
            let stop = parse_decimal(&args.next().ok_or(RequestError::WrongArity(canonical))?)
                .ok_or(RequestError::InvalidInteger)?;
            let with_scores = match args.next() {
                None => false,
                Some(option) if option.eq_ignore_ascii_case(b"WITHSCORES") && args.len() == 0 => {
                    true
                }
                _ => return Err(RequestError::Syntax),
            };
            SortedSetCommand::Range {
                start,
                stop,
                with_scores,
            }
        }
        _ => unreachable!("validated name"),
    };
    Ok(Command::SortedSet { key, operation })
}
