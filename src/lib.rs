//! Sider server with a synchronous core and an owner worker reachable over RESP2/TCP.

#![forbid(unsafe_code)]

pub mod command;
pub mod config;
mod connection;
pub mod error;
mod metrics;
pub mod persistence;
mod pubsub;
pub mod readiness;
pub mod replication;
pub mod resp;
pub mod server;
pub mod storage;

pub use config::ServerConfig;
pub use error::ConfigError;
