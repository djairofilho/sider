//! Backup por exportação consistente e restauração sem sobrescrever dados existentes.

mod archive;
pub mod cli;
mod client;
mod manifest;

use std::path::PathBuf;
use std::time::Duration;

pub use archive::{VerifiedBackup, restore, verify};
pub use client::{ExportOptions, export, export_stream};
pub use manifest::Manifest;

use super::{DurableLayout, format};

pub const MAX_BACKUP_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const SNAPSHOT: &str = "snapshot.aof";
pub const MANIFEST: &str = "backup-manifest.json";
pub const CHECKSUMS: &str = "SHA256SUMS";

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_record_bytes: usize,
    pub max_mutations: usize,
    pub max_snapshot_bytes: u64,
    pub max_dataset_bytes: usize,
    /// Prazo total de transferência, incluindo conexão, cabeçalhos e EOF.
    pub timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_record_bytes: format::MAX_RECORD_BYTES,
            max_mutations: 100_000,
            max_snapshot_bytes: 256 * 1024 * 1024,
            max_dataset_bytes: 64 * 1024 * 1024,
            timeout: Duration::from_secs(120),
        }
    }
}

impl Limits {
    pub fn validate(self) -> Result<(), Error> {
        if !(64..=format::MAX_RECORD_BYTES).contains(&self.max_record_bytes)
            || self.max_mutations == 0
            || self.max_mutations > 100_000
            || !(128..=MAX_BACKUP_BYTES).contains(&self.max_snapshot_bytes)
            || self.max_dataset_bytes == 0
            || self.max_dataset_bytes > isize::MAX as usize
            || self.timeout.is_zero()
            || std::time::Instant::now()
                .checked_add(self.timeout)
                .is_none()
        {
            return Err(Error::Invalid("limites de backup"));
        }
        Ok(())
    }

    fn record(self) -> format::Limits {
        format::Limits {
            max_record_bytes: self.max_record_bytes,
            max_mutations: self.max_mutations,
        }
    }

    fn transport(self) -> crate::replication::protocol::Limits {
        crate::replication::protocol::Limits {
            max_frame_bytes: self.max_record_bytes + 128,
            record: self.record(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RestoreOptions {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub layout: DurableLayout,
    pub limits: Limits,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("backup inválido: {0}")]
    Invalid(&'static str),
    #[error("I/O de backup: {0}")]
    Io(#[from] std::io::Error),
    #[error("formato de backup: {0}")]
    Format(#[from] format::FormatError),
    #[error("transporte de backup: {0}")]
    Transport(#[from] crate::replication::protocol::Error),
    #[error("manifesto de backup inválido")]
    Manifest(#[from] serde_json::Error),
    #[error("persistência de backup: {0}")]
    Persistence(#[from] super::AofError),
    #[error("configuração de backup: {0}")]
    Configuration(#[from] crate::ConfigError),
    #[error("prazo total do backup excedido")]
    Timeout,
}
