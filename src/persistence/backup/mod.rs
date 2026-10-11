//! Backup through consistent export and restoration without overwriting existing data.

mod archive;
pub mod cli;
mod client;
mod manifest;
mod memory;

use std::path::PathBuf;
use std::time::Duration;

pub use archive::{
    VerifiedBackup, restore, restore_composed, restore_memory, restore_memory_with_faults, verify,
    verify_composed, verify_memory,
};
pub use client::{
    ExportOptions, export, export_composed, export_composed_secure, export_composed_stream,
    export_memory, export_memory_secure, export_memory_stream, export_secure, export_stream,
};
pub use manifest::Manifest;
pub use memory::{
    OfflineMemoryExportOptions, export_memory_offline, export_memory_offline_with_faults,
};

use super::{DurableLayout, format};

pub const MAX_BACKUP_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const SNAPSHOT: &str = "snapshot.aof";
pub const MANIFEST: &str = "backup-manifest.json";
pub const CHECKSUMS: &str = "SHA256SUMS";

/// Explicit archive allowlist; legacy APIs never infer a memory profile from bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Legacy,
    Memory,
    Composed,
}

impl Profile {
    pub(super) fn versions(self) -> (u32, u32, u32) {
        match self {
            Self::Legacy => (1, format::LAYOUT_VERSION, format::VERSION),
            Self::Memory => (
                2,
                super::memory_format::HEADER_VERSION,
                super::memory_format::RECORD_VERSION,
            ),
            Self::Composed => (
                3,
                super::composed_format::HEADER_VERSION,
                super::composed_format::RecordVersion::Legacy as u32,
            ),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_record_bytes: usize,
    pub max_mutations: usize,
    pub max_snapshot_bytes: u64,
    pub max_dataset_bytes: usize,
    /// Total transfer timeout, including connection, headers, and EOF.
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
            return Err(Error::Invalid("backup limits"));
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
    #[error("invalid backup: {0}")]
    Invalid(&'static str),
    #[error("backup I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("backup format: {0}")]
    Format(#[from] format::FormatError),
    #[error("backup transport: {0}")]
    Transport(#[from] crate::replication::protocol::Error),
    #[error("invalid backup manifest")]
    Manifest(#[from] serde_json::Error),
    #[error("backup persistence: {0}")]
    Persistence(#[from] super::AofError),
    #[error("backup configuration: {0}")]
    Configuration(#[from] crate::ConfigError),
    #[error("total backup timeout exceeded")]
    Timeout,
}
