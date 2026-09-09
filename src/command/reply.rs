//! Reply without socket or channel dependencies.

use bytes::Bytes;
use thiserror::Error;

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
