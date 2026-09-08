//! Núcleo síncrono do Sider, independente da camada de rede.
//!
//! Worker e TCP serão adicionados conforme o `PLANO.md`.

#![forbid(unsafe_code)]

pub mod command;
pub mod config;
pub mod error;
pub mod resp;
pub mod storage;

pub use config::ServerConfig;
pub use error::ConfigError;
