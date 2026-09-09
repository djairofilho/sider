//! Transporte interno e histórico limitado de lotes resolvidos Sider → Sider.
//!
//! A integração com papéis, snapshot e AOF pertence ao coordenador de replicação.

pub mod config;
pub mod journal;
pub mod protocol;
pub mod session;
pub mod state;

/// Uma posição só identifica estado dentro da mesma época do primário.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub epoch: [u8; 16],
    pub sequence: u64,
}

/// Época independente a cada inicialização de primário ou promoção explícita.
pub fn new_epoch() -> Result<[u8; 16], std::io::Error> {
    let mut epoch = [0; 16];
    getrandom::fill(&mut epoch).map_err(std::io::Error::other)?;
    if epoch == [0; 16] {
        return Err(std::io::Error::other("época aleatória inválida"));
    }
    Ok(epoch)
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O de replicação: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocolo de replicação: {0}")]
    Protocol(#[from] protocol::Error),
    #[error("persistência da replicação: {0}")]
    Persistence(#[from] crate::persistence::AofError),
    #[error("worker da replicação: {0}")]
    Database(#[from] crate::storage::worker::DbError),
    #[error("snapshot da replicação: {0}")]
    Snapshot(#[from] crate::storage::snapshot::SnapshotError),
    #[error("replay da replicação: {0}")]
    Replay(#[from] crate::storage::ReplayError),
    #[error("configuração da replicação: {0}")]
    Config(#[from] crate::ConfigError),
    #[error("histórico da replicação: {0}")]
    Journal(#[from] journal::Error),
    #[error("tarefa de replicação: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("sessão de replicação obsoleta")]
    Stale,
    #[error("sequência de replicação inválida")]
    Sequence,
    #[error("limite de replicação excedido")]
    Limit,
}
