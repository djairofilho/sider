//! RESP2 and fixed-length RESP3 types. All payloads retain bytes without requiring UTF-8.

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
    /// RESP3 null, independent of the former RESP2 null type.
    Null,
    /// RESP3 boolean.
    Boolean(bool),
    /// RESP3 double, retained as validated ASCII to preserve equality and spelling.
    Double(Bytes),
    /// RESP3 arbitrary precision integer, retained as decimal bytes.
    BigNumber(Bytes),
    /// Binary error payload.
    BlobError(Bytes),
    /// Three-byte format followed by a colon and binary content.
    Verbatim(Bytes),
    /// Alternating keys and values; the element count must be even.
    Map(Vec<Frame>),
    /// Unordered collection.
    Set(Vec<Frame>),
    /// Out-of-band notification.
    Push(Vec<Frame>),
    /// Alternating metadata keys and values. The following data frame is separate.
    Attribute(Vec<Frame>),
}
