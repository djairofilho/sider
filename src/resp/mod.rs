//! Codec RESP2 incremental sem dependência de rede, comandos ou armazenamento.
//!
//! Frames incompletos preservam o buffer. Após um erro de protocolo, descarte o
//! decoder e encerre a conexão: não existe tentativa de ressincronização.
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
