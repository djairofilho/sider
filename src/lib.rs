//! Base do Sider: configuração e erros independentes da camada de rede.
//!
//! RESP2, comandos e armazenamento serão adicionados conforme o `PLANO.md`.

#![forbid(unsafe_code)]

pub mod config;
pub mod error;

pub use config::ServerConfig;
pub use error::ConfigError;
