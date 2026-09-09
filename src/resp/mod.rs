//! Incremental RESP2 codec without network, command, or storage dependencies.
//!
//! Incomplete frames retain the buffer. After a protocol error, discard the
//! decoder and close the connection: no resynchronization is attempted.
//!
//! ```
//! use bytes::BytesMut;
//! use sider::resp::{Decoder, RespLimits, encode};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let limits = RespLimits::default();
//! let mut decoder = Decoder::new(limits)?;
//! let mut input = BytesMut::from(&b"$5\r\nhello"[..]);
//! assert!(decoder.decode(&mut input)?.is_none());
//! input.extend_from_slice(b"\r\n+OK\r\n");
//! let frame = decoder.decode(&mut input)?.unwrap();
//! assert_eq!(&input[..], b"+OK\r\n");
//! let mut output = BytesMut::new();
//! encode(&frame, &mut output, limits)?;
//! assert_eq!(&output[..], b"$5\r\nhello\r\n");
//! # Ok(())
//! # }
//! ```

mod decoder;
mod encoder;
mod error;
mod frame;
mod limits;

pub use decoder::Decoder;
pub use encoder::encode;
pub use error::{EncodeError, ProtocolError};
pub use frame::Frame;
pub use limits::RespLimits;
