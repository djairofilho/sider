//! Fixed-cardinality operational state, without paths or persisted content.

use std::sync::{Arc, Mutex};

use super::AofError;

/// Writer snapshot; sequences come from durable state, without parallel counting.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AofDiagnostics {
    pub running: bool,
    pub failed: bool,
    pub written_sequence: u64,
    pub synced_sequence: u64,
    pub generation: u64,
    pub bytes_since_compaction: u64,
    pub dirty: bool,
    pub compacting: bool,
    pub queue_depth: usize,
    pub queue_capacity: usize,
    pub records_written_total: u64,
    pub syncs_total: u64,
    pub fatal_failures_total: u64,
    pub record_rejections_total: u64,
    pub compactions_total: u64,
    pub compaction_failures_total: u64,
    pub last_error: Option<&'static str>,
}

#[derive(Clone, Default)]
pub(super) struct Shared(pub(super) Arc<Mutex<AofDiagnostics>>);

impl Shared {
    pub(super) fn update(&self, update: impl FnOnce(&mut AofDiagnostics)) {
        update(&mut self.0.lock().expect("AOF diagnostics lock poisoned"));
    }

    pub(super) fn snapshot(&self) -> AofDiagnostics {
        self.0
            .lock()
            .expect("AOF diagnostics lock poisoned")
            .clone()
    }

    pub(super) fn error(&self, error: &AofError) {
        self.update(|state| state.last_error = Some(category(error)));
    }

    pub(super) fn stopped(&self, error: Option<&AofError>) {
        self.update(|state| {
            state.running = false;
            if let Some(error) = error {
                state.failed = true;
                state.fatal_failures_total = state.fatal_failures_total.saturating_add(1);
                if !matches!(error, AofError::Unavailable) || state.last_error.is_none() {
                    state.last_error = Some(category(error));
                }
            }
        });
    }
}

fn category(error: &AofError) -> &'static str {
    match error {
        AofError::Io(_) => "io",
        AofError::Format(super::format::FormatError::Limit) => "record_limit",
        AofError::Format(_) => "format",
        AofError::Config(_) => "configuration",
        AofError::Replay(_) => "replay",
        AofError::Locked => "directory_locked",
        AofError::Unavailable => "unavailable",
        AofError::Sequence => "sequence",
        AofError::Compacting => "compaction_busy",
        AofError::DeltaLimit => "compaction_delta_limit",
        AofError::LayoutMismatch { .. } => "layout_mismatch",
        AofError::CrossShard => "cross_shard",
        AofError::ShardQuota { .. } => "shard_quota",
        AofError::Migration(_) => "migration",
    }
}
