//! Binary AOF with resolved batches and a single write owner.

pub mod backup;
mod diagnostics;
pub mod format;
mod layout;
pub mod migration;
mod role;
mod writer;
pub use diagnostics::AofDiagnostics;
pub use layout::{DurableLayout, ROUTING_VERSION};
pub use role::{ReplicationMetadata, Role};
pub use writer::{
    AofConfig, AofDiagnosticsHandle, AofError, AofHandle, FaultInjector, NoFaults, Recovered,
    RecoveryMetadata, SyncPolicy, recover, recover_with_faults,
};
