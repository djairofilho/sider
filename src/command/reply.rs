//! Reply without socket or channel dependencies.

use bytes::Bytes;
use thiserror::Error;

use super::Command;
use crate::resp::Frame;

/// Recoverable execution errors that do not close the connection or worker.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ExecutionError {
    #[error("READONLY You can't write against a read only replica.")]
    ReadOnly,
    #[error("CROSSSLOT Keys in request don't hash to the same slot")]
    CrossShard,
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    #[error("ERR value is not an integer or out of range")]
    InvalidInteger,
    #[error("ERR increment or decrement would overflow")]
    IntegerOverflow,
    #[error("ERR invalid expire time in 'set' command")]
    InvalidSetExpiry,
    #[error("ERR invalid expire time in '{0}' command")]
    InvalidExpiry(&'static str),
    #[error("OOM dataset memory quota exceeded")]
    OutOfMemory,
    #[error("ERR AOF record limit exceeded")]
    AofRecordLimit,
    /// Native staged memory lifecycle diagnostics; errors never contain user data.
    #[error("{0}")]
    Memory(&'static str),
    /// Pub/Sub depends on connection context and does not execute in the map.
    #[error("ERR command requires connection context")]
    ConnectionOnly,
}

/// Result of synchronous execution in storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// PING without an argument.
    Pong,
    /// Accepted SET mutation.
    Ok,
    /// Binary message/value or missing key.
    Bulk(Option<Bytes>),
    /// Number of removed keys.
    Integer(i64),
    /// Ordered replies from multi-key commands.
    Array(Vec<Reply>),
    /// EXEC invalidated by a watched-key change or expiry.
    NullArray,
    /// Rejection without storage effects.
    Error(ExecutionError),
}

impl From<Reply> for Frame {
    fn from(reply: Reply) -> Self {
        match reply {
            Reply::Pong => Self::Simple(Bytes::from_static(b"PONG")),
            Reply::Ok => Self::Simple(Bytes::from_static(b"OK")),
            Reply::Bulk(value) => Self::Bulk(value),
            Reply::Integer(value) => Self::Integer(value),
            Reply::Array(values) => Self::Array(Some(values.into_iter().map(Self::from).collect())),
            Reply::NullArray => Self::Array(None),
            Reply::Error(error) => Self::Error(Bytes::from(error.to_string())),
        }
    }
}

pub(crate) fn resp3(frame: Frame) -> Frame {
    match frame {
        Frame::Bulk(None) | Frame::Array(None) => Frame::Null,
        Frame::Array(Some(values)) => Frame::Array(Some(values.into_iter().map(resp3).collect())),
        frame => frame,
    }
}

fn typed_resp3(frame: Frame, name: &[u8]) -> Frame {
    match (name, frame) {
        (b"hgetall", Frame::Array(Some(values))) => {
            Frame::Map(values.into_iter().map(resp3).collect())
        }
        (b"smembers", Frame::Array(Some(values))) => {
            Frame::Set(values.into_iter().map(resp3).collect())
        }
        (b"zscore", Frame::Bulk(Some(value))) => Frame::Double(value),
        (_, frame) => resp3(frame),
    }
}

pub(crate) fn push(frame: Frame, protocol: u8) -> Frame {
    match frame {
        Frame::Array(Some(values)) if protocol == 3 => {
            Frame::Push(values.into_iter().map(resp3).collect())
        }
        frame => frame,
    }
}

pub(crate) fn command_resp3(frame: Frame, command: &Command) -> Frame {
    use crate::command::{HashCommand, SetCommand, SortedSetCommand};
    match command {
        Command::Hash {
            operation: HashCommand::GetAll,
            ..
        } => typed_resp3(frame, b"hgetall"),
        Command::SetCollection {
            operation: SetCommand::Members,
            ..
        } => typed_resp3(frame, b"smembers"),
        Command::SortedSet {
            operation: SortedSetCommand::Score { .. },
            ..
        } => typed_resp3(frame, b"zscore"),
        Command::SortedSet {
            operation: SortedSetCommand::Range {
                with_scores: true, ..
            },
            ..
        } => match frame {
            Frame::Array(Some(values)) => Frame::Array(Some(
                values
                    .chunks_exact(2)
                    .map(|pair| {
                        Frame::Array(Some(vec![
                            pair[0].clone(),
                            match &pair[1] {
                                Frame::Bulk(Some(score)) => Frame::Double(score.clone()),
                                other => other.clone(),
                            },
                        ]))
                    })
                    .collect(),
            )),
            frame => resp3(frame),
        },
        _ => resp3(frame),
    }
}
