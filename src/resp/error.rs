//! Framing errors do not carry client keys or values.

use thiserror::Error;

use crate::ConfigError;

/// Terminal decoder failure; the caller must discard the connection/decoder.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    /// Invalid RESP2 syntax.
    #[error("invalid RESP: {0}")]
    Malformed(&'static str),
    /// Frame exceeds a configured budget.
    #[error("RESP limit exceeded: {0}")]
    LimitExceeded(&'static str),
    /// The buffer was shortened while a frame was incomplete.
    #[error("RESP buffer changed during incomplete decoding")]
    BufferChanged,
    /// The decoder has already failed and cannot recover synchronization.
    #[error("RESP decoder already terminated by an error")]
    Poisoned,
}

/// The encoder validates everything before changing the output.
#[derive(Debug, Error)]
pub enum EncodeError {
    /// Content invalid for its RESP2 type.
    #[error("invalid RESP frame: {0}")]
    InvalidFrame(&'static str),
    /// Frame exceeds a configured budget.
    #[error("RESP limit exceeded: {0}")]
    LimitExceeded(&'static str),
    /// Configuration must be valid even for a small frame.
    #[error(transparent)]
    InvalidLimits(#[from] ConfigError),
}
