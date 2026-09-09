//! Observable role and position; no status read waits on network or disk.

use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use super::{Cursor, journal::Journal};
use crate::persistence::Role;

#[derive(Clone, Debug)]
pub struct Status {
    pub role: Role,
    pub applied: Cursor,
    pub upstream_sequence: Option<u64>,
    pub connected: bool,
    pub full_syncs: u64,
    pub partial_syncs: u64,
    pub reconnects: u64,
    pub generation: u64,
}

struct State {
    status: Status,
    journal: Option<Journal>,
}

#[derive(Clone)]
pub struct Runtime {
    inner: Arc<Mutex<State>>,
    changed: watch::Sender<u64>,
}

impl Runtime {
    pub fn new(role: Role, applied: Cursor) -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            inner: Arc::new(Mutex::new(State {
                status: Status {
                    role,
                    applied,
                    upstream_sequence: None,
                    connected: false,
                    full_syncs: 0,
                    partial_syncs: 0,
                    reconnects: 0,
                    generation: 0,
                },
                journal: None,
            })),
            changed,
        }
    }

    pub fn status(&self) -> Status {
        let state = self.inner.lock().expect("replication state lock poisoned");
        let mut status = state.status.clone();
        if let Some(journal) = &state.journal
            && let Ok(head) = journal.status()
        {
            status.applied = head.head;
        }
        status
    }

    pub fn readonly(&self) -> bool {
        self.inner
            .lock()
            .expect("replication state lock poisoned")
            .status
            .role
            == Role::Replica
    }

    pub fn journal(&self) -> Option<Journal> {
        self.inner
            .lock()
            .expect("replication state lock poisoned")
            .journal
            .clone()
    }

    pub fn begin_session(&self) -> u64 {
        let mut state = self.inner.lock().expect("replication state lock poisoned");
        state.status.generation = state
            .status
            .generation
            .checked_add(1)
            .expect("session epochs exhausted");
        state.status.connected = false;
        state.status.reconnects = state.status.reconnects.saturating_add(1);
        let generation = state.status.generation;
        self.changed.send_replace(generation);
        generation
    }

    pub fn accepts(&self, generation: u64) -> bool {
        let state = self.inner.lock().expect("replication state lock poisoned");
        state.status.role == Role::Replica && state.status.generation == generation
    }

    pub fn applied(&self, cursor: Cursor) {
        self.inner
            .lock()
            .expect("replication state lock poisoned")
            .status
            .applied = cursor;
    }

    pub fn connected(&self, generation: u64, upstream_sequence: u64, full: bool) {
        let mut state = self.inner.lock().expect("replication state lock poisoned");
        if state.status.generation != generation || state.status.role != Role::Replica {
            return;
        }
        state.status.connected = true;
        state.status.upstream_sequence = Some(upstream_sequence);
        if full {
            state.status.full_syncs = state.status.full_syncs.saturating_add(1);
        } else {
            state.status.partial_syncs = state.status.partial_syncs.saturating_add(1);
        }
    }

    pub fn upstream_head(&self, generation: u64, sequence: u64) {
        let mut state = self.inner.lock().expect("replication state lock poisoned");
        if state.status.generation == generation && state.status.role == Role::Replica {
            state.status.upstream_sequence = Some(sequence);
        }
    }

    pub fn disconnected(&self, generation: u64) {
        let mut state = self.inner.lock().expect("replication state lock poisoned");
        if state.status.generation == generation {
            state.status.connected = false;
        }
    }

    /// Called under the barrier after persisting role/epoch and binding the journal.
    pub fn primary(&self, cursor: Cursor, journal: Journal) {
        let mut state = self.inner.lock().expect("replication state lock poisoned");
        state.status.role = Role::Primary;
        state.status.applied = cursor;
        state.status.connected = false;
        state.status.generation = state
            .status
            .generation
            .checked_add(1)
            .expect("session epochs exhausted");
        state.journal = Some(journal);
        self.changed.send_replace(state.status.generation);
    }

    pub async fn cancelled(&self, generation: u64) {
        let mut changed = self.changed.subscribe();
        while self.accepts(generation) {
            if changed.changed().await.is_err() {
                break;
            }
        }
    }
}
