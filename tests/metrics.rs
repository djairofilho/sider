//! Indicadores lidos por consumidores reais, sem payloads ou labels dinâmicos.
#![forbid(unsafe_code)]

use std::{
    fs, io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use sider::command::Command;
use sider::persistence::{self, AofConfig, FaultInjector};
use sider::storage::{Store, StoreConfig, SystemClock};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-metrics-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn config(&self) -> AofConfig {
        AofConfig::new(self.0.clone())
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn mutation(key: &'static [u8], value: Bytes) -> sider::storage::ResolvedBatch {
    Store::new()
        .prepare(Command::Set {
            key: Bytes::from_static(key),
            value,
        })
        .batch
}

#[tokio::test]
async fn metrics_aof_reports_confirmed_sequences_and_compaction_without_io_reads() {
    let directory = Directory::new();
    let recovered = persistence::recover(
        directory.config(),
        StoreConfig::default(),
        Arc::new(SystemClock),
    )
    .unwrap();
    let (mut store, aof, task) = recovered.start();
    let initial = aof.diagnostics();
    assert!(initial.running && !initial.failed);
    assert_eq!(initial.written_sequence, 0);
    let a = mutation(b"secret-a", Bytes::from_static(b"secret-value"));
    let b = mutation(b"secret-b", Bytes::from_static(b"another-secret"));
    let other = aof.clone();
    let (first, second) = tokio::join!(aof.append(a.clone()), other.append(b.clone()));
    let mut sequences = [first.unwrap(), second.unwrap()];
    sequences.sort();
    assert_eq!(sequences, [1, 2]);
    let state = aof.diagnostics();
    assert_eq!(state.records_written_total, 2);
    assert_eq!(state.syncs_total, 2);
    assert_eq!(state.written_sequence, 2);
    assert_eq!(state.synced_sequence, 2);
    assert!(!state.dirty);
    assert!(!format!("{state:?}").contains("secret"));
    store.replay(&a.mutations).unwrap();
    store.replay(&b.mutations).unwrap();
    aof.compact(store.snapshot()).await.unwrap();
    let state = aof.diagnostics();
    assert_eq!(state.generation, 1);
    assert_eq!(state.compactions_total, 1);
    assert_eq!(state.records_written_total, 2);
    assert!(!state.compacting);
    let observer = aof.diagnostics_handle();
    drop((aof, other));
    task.await.unwrap().unwrap();
    assert!(
        !observer.snapshot().running,
        "observador não impede encerramento"
    );
}

struct DiskFailure;
impl FaultInjector for DiskFailure {
    fn hit(&self, point: &'static str) -> io::Result<()> {
        if point == "before_sync" {
            Err(io::Error::other("secret path must not enter diagnostics"))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn metrics_aof_distinguishes_fatal_io_from_recoverable_record_limit() {
    let directory = Directory::new();
    let recovered = persistence::recover_with_faults(
        directory.config(),
        StoreConfig::default(),
        Arc::new(SystemClock),
        Arc::new(DiskFailure),
    )
    .unwrap();
    let (_, aof, task) = recovered.start();
    assert!(
        aof.append(mutation(b"key", Bytes::from_static(b"value")))
            .await
            .is_err()
    );
    assert!(task.await.unwrap().is_err());
    let state = aof.diagnostics();
    assert!(!state.running && state.failed && state.dirty);
    assert_eq!(state.fatal_failures_total, 1);
    assert_eq!(state.last_error, Some("io"));
    assert_eq!(state.written_sequence, 1);
    assert_eq!(state.synced_sequence, 0);
    assert_eq!(state.queue_depth, 0);
    assert!(!format!("{state:?}").contains("secret"));
    drop(aof);

    let second = Directory::new();
    let mut config = second.config();
    config.limits.max_record_bytes = 64;
    let recovered =
        persistence::recover(config, StoreConfig::default(), Arc::new(SystemClock)).unwrap();
    let (_, aof, task) = recovered.start();
    assert!(
        aof.append(mutation(b"key", Bytes::from(vec![0; 128])))
            .await
            .is_err()
    );
    let state = aof.diagnostics();
    assert!(state.running && !state.failed);
    assert_eq!(state.record_rejections_total, 1);
    assert_eq!(state.fatal_failures_total, 0);
    assert_eq!(state.last_error, Some("record_limit"));
    assert_eq!(state.written_sequence, 0);
    aof.append(mutation(b"key", Bytes::from_static(b"v")))
        .await
        .unwrap();
    assert_eq!(aof.diagnostics().synced_sequence, 1);
    drop(aof);
    task.await.unwrap().unwrap();
}
