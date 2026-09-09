//! AOF binário com lotes resolvidos e um único proprietário da escrita.

pub mod format;
mod writer;
pub use writer::{
    AofConfig, AofError, AofHandle, FaultInjector, NoFaults, Recovered, SyncPolicy, recover,
    recover_with_faults,
};
