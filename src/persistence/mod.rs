//! Binary AOF with resolved batches and a single write owner.

pub mod backup;
pub mod composed_format;
mod diagnostics;
pub mod format;
mod layout;
pub mod memory_format;
pub mod migration;
mod profile;
mod role;
mod writer;
pub use diagnostics::AofDiagnostics;
pub use layout::{DurableLayout, ROUTING_VERSION};
pub use role::{ReplicationMetadata, Role};
#[cfg(test)]
pub(crate) use writer::recover_composed_legacy_mixed;
pub use writer::{
    AofConfig, AofDiagnosticsHandle, AofError, AofHandle, FaultInjector, NoFaults, Recovered,
    RecoveryMetadata, SyncPolicy, recover, recover_with_faults,
};
