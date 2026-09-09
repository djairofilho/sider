//! AOF binário com lotes resolvidos e um único proprietário da escrita.

pub mod format;
mod layout;
pub mod migration;
mod writer;
pub use layout::{DurableLayout, ROUTING_VERSION};
pub use writer::{
    AofConfig, AofError, AofHandle, FaultInjector, NoFaults, Recovered, RecoveryMetadata,
    SyncPolicy, recover, recover_with_faults,
};
