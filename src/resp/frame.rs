//! The five RESP2 types. All payloads retain bytes without requiring UTF-8.

use bytes::Bytes;

/// A protocol frame, not yet validated as an executable command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// Simple string; cannot contain CR or LF.
    Simple(Bytes),
    /// Protocol/application error; cannot contain CR or LF.
    Error(Bytes),
    /// Signed 64-bit decimal integer.
    Integer(i64),
    /// Binary bulk; `None` is null, unlike an empty payload.
    Bulk(Option<Bytes>),
    /// Heterogeneous array; `None` is null, unlike an empty array.
    Array(Option<Vec<Frame>>),
}
