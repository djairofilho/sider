//! Servidor Sider com núcleo síncrono e worker proprietário acessível por RESP2/TCP.

#![forbid(unsafe_code)]

pub mod command;
pub mod config;
mod connection;
pub mod error;
pub mod readiness;
pub mod resp;
pub mod server;
pub mod storage;

pub use config::ServerConfig;
pub use error::ConfigError;
