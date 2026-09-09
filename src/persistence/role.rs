//! Papel e época publicados junto da geração AOF, sem sidecar independente.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Primary,
    Replica,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationMetadata {
    pub role: Role,
    /// Na réplica, identifica o upstream cujo prefixo está neste AOF.
    /// Zero é reservado para réplica que ainda não instalou um snapshot.
    pub epoch: [u8; 16],
}
