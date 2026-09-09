//! Configuration errors that may be reported during initialization.

use std::net::AddrParseError;

use thiserror::Error;

/// Failure to interpret a Sider configuration variable.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Inconsistent network limits, channels, or timeouts are rejected at startup.
    #[error("invalid server limits: {reason}")]
    InvalidServerLimits {
        /// Violated constraint, without request data.
        reason: &'static str,
    },

    /// A textual option contains data that cannot be interpreted as UTF-8.
    #[error("{name} must contain valid UTF-8 text")]
    NonUnicodeValue {
        /// Configuration variable name.
        name: &'static str,
    },

    /// A limit or timeout is not a representable decimal integer.
    #[error("{name} is invalid: {value:?}; use only decimal digits, without a sign or spaces")]
    InvalidInteger {
        /// Configuration variable name.
        name: &'static str,
        /// Received value, preserved for diagnostics.
        value: String,
    },

    /// Invalid limits are rejected before creating the codec.
    #[error("invalid RESP limits: {reason}")]
    InvalidRespLimits {
        /// Violated configuration constraint, without client content.
        reason: &'static str,
    },

    /// The address contains text that cannot be represented as UTF-8.
    #[error("SIDER_ADDR must contain valid UTF-8 text")]
    NonUnicodeAddress,

    /// The address does not contain a literal IP address and valid port.
    #[error("SIDER_ADDR is invalid: {value:?}; use an IP address and port, such as 127.0.0.1:6379")]
    InvalidAddress {
        /// Received value, preserved for diagnostics.
        value: String,
        /// Original error from the standard library address parser.
        #[source]
        source: AddrParseError,
    },
}
