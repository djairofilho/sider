//! Ephemeral observations: only keys with live tokens occupy the registry.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use bytes::Bytes;
use tokio::time::Instant;

#[derive(Clone, Default)]
pub(super) struct Registry(Arc<Mutex<BTreeMap<Bytes, Weak<AtomicBool>>>>);

/// Observation owned by a connection or accepted request; Drop performs UNWATCH.
/// It is not cloneable: each record represents an explicitly acquired observation.
pub struct WatchToken {
    key: Bytes,
    changed: Arc<AtomicBool>,
    deadline: Option<Instant>,
    registry: Registry,
}

impl WatchToken {
    pub fn key(&self) -> &Bytes {
        &self.key
    }

    pub(crate) fn changed(&self, now: Instant) -> bool {
        self.invalidated() || self.deadline.is_some_and(|deadline| deadline <= now)
    }

    pub(super) fn invalidated(&self) -> bool {
        self.changed.load(Ordering::Acquire)
    }
}

impl Drop for WatchToken {
    fn drop(&mut self) {
        let mut entries = self
            .registry
            .0
            .lock()
            .expect("WATCH registry lock poisoned");
        if Arc::strong_count(&self.changed) == 1
            && entries
                .get(&self.key)
                .is_some_and(|value| value.ptr_eq(&Arc::downgrade(&self.changed)))
        {
            entries.remove(&self.key);
        }
    }
}

impl Registry {
    fn token(&self, key: Bytes, deadline: Option<Instant>) -> WatchToken {
        let mut entries = self.0.lock().expect("WATCH registry lock poisoned");
        let changed = entries
            .get(&key)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let changed = Arc::new(AtomicBool::new(false));
                entries.insert(key.clone(), Arc::downgrade(&changed));
                changed
            });
        WatchToken {
            key,
            changed,
            deadline,
            registry: self.clone(),
        }
    }

    pub(super) fn invalidate(&self, key: &Bytes) {
        if let Some(changed) = self
            .0
            .lock()
            .expect("WATCH registry lock poisoned")
            .remove(key)
            .and_then(|value| value.upgrade())
        {
            changed.store(true, Ordering::Release);
        }
    }
}

impl super::Store {
    /// The worker resolves key expiries before obtaining these observations.
    pub fn watch(&self, keys: Vec<Bytes>) -> Vec<WatchToken> {
        keys.into_iter()
            .map(|key| {
                let deadline = self.values.get(&key).and_then(|entry| entry.expires_at);
                self.watches.token(key, deadline)
            })
            .collect()
    }

    /// Checked by the same owner that will prepare and apply the batch.
    pub fn watches_valid(&self, tokens: &[WatchToken]) -> bool {
        let now = self.clock.now();
        tokens
            .iter()
            .all(|token| Arc::ptr_eq(&self.watches.0, &token.registry.0) && !token.changed(now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transactions_watch_tokens_release_and_do_not_reuse_invalid_generations() {
        let registry = Registry::default();
        let first = registry.token(Bytes::from_static(b"k"), None);
        let second = registry.token(Bytes::from_static(b"k"), None);
        assert_eq!(registry.0.lock().unwrap().len(), 1);
        drop(first);
        assert_eq!(registry.0.lock().unwrap().len(), 1);
        registry.invalidate(&Bytes::from_static(b"k"));
        assert!(second.changed(Instant::now()));
        let next = registry.token(Bytes::from_static(b"k"), None);
        drop(second);
        assert!(!next.changed(Instant::now()));
        assert_eq!(registry.0.lock().unwrap().len(), 1);
        drop(next);
        assert!(registry.0.lock().unwrap().is_empty());
        for index in 0..1024 {
            drop(registry.token(Bytes::from(index.to_string()), None));
        }
        assert!(registry.0.lock().unwrap().is_empty());
    }
}
