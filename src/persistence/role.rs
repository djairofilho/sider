//! Role and epoch published with the AOF generation, without an independent sidecar.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Primary,
    Replica,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationMetadata {
    pub role: Role,
    /// On a replica, identifies the upstream whose prefix is in this AOF.
    /// Zero is reserved for a replica that has not yet installed a snapshot.
    pub epoch: [u8; 16],
}
