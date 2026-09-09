//! Internal transport and bounded history of resolved Sider-to-Sider batches.
//!
//! Integration with roles, snapshots, and AOF belongs to the replication coordinator.

pub mod config;
pub mod journal;
pub mod protocol;
pub mod session;
pub mod state;

/// A position identifies state only within the same primary epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub epoch: [u8; 16],
    pub sequence: u64,
}

/// Independent epoch for every primary initialization or explicit promotion.
pub fn new_epoch() -> Result<[u8; 16], std::io::Error> {
    let mut epoch = [0; 16];
    getrandom::fill(&mut epoch).map_err(|error| std::io::Error::other(error.to_string()))?;
    if epoch == [0; 16] {
        return Err(std::io::Error::other("invalid random epoch"));
    }
    Ok(epoch)
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("replication I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("replication protocol: {0}")]
    Protocol(#[from] protocol::Error),
    #[error("replication persistence: {0}")]
    Persistence(#[from] crate::persistence::AofError),
    #[error("replication worker: {0}")]
    Database(#[from] crate::storage::worker::DbError),
    #[error("replication snapshot: {0}")]
    Snapshot(#[from] crate::storage::snapshot::SnapshotError),
    #[error("replication replay: {0}")]
    Replay(#[from] crate::storage::ReplayError),
    #[error("replication configuration: {0}")]
    Config(#[from] crate::ConfigError),
    #[error("replication history: {0}")]
    Journal(#[from] journal::Error),
    #[error("replication task: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("stale replication session")]
    Stale,
    #[error("invalid replication sequence")]
    Sequence,
    #[error("replication limit exceeded")]
    Limit,
}
