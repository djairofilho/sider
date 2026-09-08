//! Configuração e codec RESP2 do Sider, independentes da camada de rede.
//!
//! Comandos e armazenamento serão adicionados conforme o `PLANO.md`.

#![forbid(unsafe_code)]

pub mod config;
pub mod error;
pub mod resp;

pub use config::ServerConfig;
pub use error::ConfigError;
