//! Actual writer and worker proof for one composed mixed-owner decision.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::watch;

use super::*;
use crate::persistence::{self, AofConfig, FaultInjector, NoFaults};
use crate::storage::{Store, StoreConfig, SystemClock, worker};

fn set(key: &'static [u8], value: &'static [u8]) -> Command {
    Command::Set {
        key: Bytes::from_static(key),
        value: Bytes::from_static(value),
    }
}

fn get(key: &'static [u8]) -> Command {
    Command::Get {
        key: Bytes::from_static(key),
    }
}

#[tokio::test]
async fn one_mixed_exec_appends_one_record_and_restarts_both_owners() {
    let directory = std::env::temp_dir().join(format!(
        "sider-mixed-coordinator-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir(&directory).unwrap();
    let mut config = AofConfig::new(directory.clone());
    config.layout.shard_count = 2;
    let global_quota = StoreConfig {
        max_dataset_bytes: 4_096,
    };
    let recovered = persistence::recover_composed_legacy_mixed(
        config.clone(),
        global_quota,
        Arc::new(SystemClock),
        Arc::new(NoFaults),
    )
    .unwrap();
    assert_eq!(recovered.metadata.format_version, 7);
    let (_empty, aof, writer) = recovered.start();

    let (stop, shutdown) = watch::channel(false);
    let stores = (0..2)
        .map(|_| {
            Store::with_config(
                StoreConfig {
                    max_dataset_bytes: 2_048,
                },
                Arc::new(SystemClock),
            )
            .unwrap()
        })
        .collect();
    let (handle, owners) =
        worker::channel_with_stores(2, Duration::from_secs(5), shutdown, stores).unwrap();
    let handle = handle.with_composed_aof(aof.clone()).unwrap();
    let tasks: Vec<_> = owners
        .into_iter()
        .map(|owner| tokio::spawn(owner.with_aof(aof.clone(), 0).run()))
        .collect();
    assert_eq!(
        handle
            .execute_batch(
                vec![
                    set(b"a", b"first"),
                    set(b"b", b"second"),
                    Command::MGet {
                        keys: vec![Bytes::from_static(b"b"), Bytes::from_static(b"a")]
                    },
                ],
                Vec::new()
            )
            .await
            .unwrap(),
        Reply::Array(vec![
            Reply::Ok,
            Reply::Ok,
            Reply::Array(vec![
                Reply::Bulk(Some(Bytes::from_static(b"second"))),
                Reply::Bulk(Some(Bytes::from_static(b"first"))),
            ]),
        ])
    );
    assert_eq!(aof.status().await.unwrap().0, 1);
    assert_eq!(aof.diagnostics().records_written_total, 1);
    assert_eq!(
        handle.execute(get(b"a")).await.unwrap(),
        Reply::Bulk(Some(Bytes::from_static(b"first")))
    );
    assert_eq!(
        handle.execute(get(b"b")).await.unwrap(),
        Reply::Bulk(Some(Bytes::from_static(b"second")))
    );
    drop(handle);
    stop.send_replace(true);
    for task in tasks {
        task.await.unwrap();
    }
    drop(aof);
    writer.await.unwrap().unwrap();

    assert!(persistence::recover(config.clone(), global_quota, Arc::new(SystemClock)).is_err());
    let restarted = persistence::recover_composed_legacy_mixed(
        config,
        global_quota,
        Arc::new(SystemClock),
        Arc::new(NoFaults),
    )
    .unwrap();
    assert_eq!(restarted.metadata.sequence, 1);
    assert_eq!(restarted.metadata.format_version, 7);
    let mut store = restarted.store;
    assert_eq!(
        store.execute(get(b"a")),
        Reply::Bulk(Some(Bytes::from_static(b"first")))
    );
    assert_eq!(
        store.execute(get(b"b")),
        Reply::Bulk(Some(Bytes::from_static(b"second")))
    );
    drop(store);

    let parent = std::env::temp_dir().canonicalize().unwrap();
    let target = directory.canonicalize().unwrap();
    assert_eq!(target.parent(), Some(parent.as_path()));
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("sider-mixed-coordinator-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[tokio::test]
async fn oversized_combined_record_rejects_all_owners_without_an_append() {
    let directory = std::env::temp_dir().join(format!(
        "sider-mixed-coordinator-limit-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir(&directory).unwrap();
    let mut config = AofConfig::new(directory.clone());
    config.layout.shard_count = 2;
    config.limits.max_record_bytes = 64;
    let quota = StoreConfig {
        max_dataset_bytes: 4_096,
    };
    let recovered = persistence::recover_composed_legacy_mixed(
        config.clone(),
        quota,
        Arc::new(SystemClock),
        Arc::new(NoFaults),
    )
    .unwrap();
    let (_empty, aof, writer) = recovered.start();
    let (stop, shutdown) = watch::channel(false);
    let stores = (0..2)
        .map(|_| {
            Store::with_config(
                StoreConfig {
                    max_dataset_bytes: 2_048,
                },
                Arc::new(SystemClock),
            )
            .unwrap()
        })
        .collect();
    let (handle, owners) =
        worker::channel_with_stores(2, Duration::from_secs(5), shutdown, stores).unwrap();
    let handle = handle.with_composed_aof(aof.clone()).unwrap();
    let tasks: Vec<_> = owners
        .into_iter()
        .map(|owner| tokio::spawn(owner.with_aof(aof.clone(), 0).run()))
        .collect();
    assert_eq!(
        handle
            .execute_batch(
                vec![
                    set(b"a", b"01234567890123456789"),
                    set(b"b", b"01234567890123456789"),
                ],
                Vec::new()
            )
            .await
            .unwrap(),
        Reply::Error(ExecutionError::AofRecordLimit)
    );
    assert_eq!(aof.status().await.unwrap().0, 0);
    assert_eq!(handle.execute(get(b"a")).await.unwrap(), Reply::Bulk(None));
    assert_eq!(handle.execute(get(b"b")).await.unwrap(), Reply::Bulk(None));
    drop(handle);
    stop.send_replace(true);
    for task in tasks {
        task.await.unwrap();
    }
    drop(aof);
    writer.await.unwrap().unwrap();
    let restarted = persistence::recover_composed_legacy_mixed(
        config,
        quota,
        Arc::new(SystemClock),
        Arc::new(NoFaults),
    )
    .unwrap();
    assert_eq!(restarted.metadata.sequence, 0);
    assert!(restarted.store.is_empty());
    drop(restarted);
    let parent = std::env::temp_dir().canonicalize().unwrap();
    let target = directory.canonicalize().unwrap();
    assert_eq!(target.parent(), Some(parent.as_path()));
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("sider-mixed-coordinator-limit-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

struct FailAfterAppend;
impl FaultInjector for FailAfterAppend {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if point == "after_append" {
            Err(std::io::Error::other("injected after-append failure"))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn uncertain_writer_result_closes_admission_and_restart_recovers_whole_batch() {
    let directory = std::env::temp_dir().join(format!(
        "sider-mixed-coordinator-fault-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir(&directory).unwrap();
    let mut config = AofConfig::new(directory.clone());
    config.layout.shard_count = 2;
    let quota = StoreConfig {
        max_dataset_bytes: 4_096,
    };
    let recovered = persistence::recover_composed_legacy_mixed(
        config.clone(),
        quota,
        Arc::new(SystemClock),
        Arc::new(FailAfterAppend),
    )
    .unwrap();
    let (_empty, aof, writer) = recovered.start();
    let (stop, shutdown) = watch::channel(false);
    let stores = (0..2)
        .map(|_| {
            Store::with_config(
                StoreConfig {
                    max_dataset_bytes: 2_048,
                },
                Arc::new(SystemClock),
            )
            .unwrap()
        })
        .collect();
    let (handle, owners) =
        worker::channel_with_stores(2, Duration::from_secs(5), shutdown, stores).unwrap();
    let handle = handle.with_composed_aof(aof.clone()).unwrap();
    let tasks: Vec<_> = owners
        .into_iter()
        .map(|owner| tokio::spawn(owner.with_aof(aof.clone(), 0).run()))
        .collect();
    assert_eq!(
        handle
            .execute_batch(vec![set(b"a", b"left"), set(b"b", b"right")], Vec::new())
            .await,
        Err(DbError::Unavailable)
    );
    assert_eq!(handle.check_admission(), Err(DbError::Unavailable));
    assert_eq!(handle.execute(get(b"a")).await, Err(DbError::Unavailable));
    drop(handle);
    stop.send_replace(true);
    for task in tasks {
        task.await.unwrap();
    }
    drop(aof);
    assert!(writer.await.unwrap().is_err());
    let restarted = persistence::recover_composed_legacy_mixed(
        config,
        quota,
        Arc::new(SystemClock),
        Arc::new(NoFaults),
    )
    .unwrap();
    assert_eq!(restarted.metadata.sequence, 1);
    let mut store = restarted.store;
    assert_eq!(
        store.execute(get(b"a")),
        Reply::Bulk(Some(Bytes::from_static(b"left")))
    );
    assert_eq!(
        store.execute(get(b"b")),
        Reply::Bulk(Some(Bytes::from_static(b"right")))
    );
    drop(store);
    let parent = std::env::temp_dir().canonicalize().unwrap();
    let target = directory.canonicalize().unwrap();
    assert_eq!(target.parent(), Some(parent.as_path()));
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("sider-mixed-coordinator-fault-")
    );
    std::fs::remove_dir_all(target).unwrap();
}

#[tokio::test]
async fn lost_second_owner_after_durable_decision_requires_whole_batch_recovery() {
    let directory = std::env::temp_dir().join(format!(
        "sider-mixed-coordinator-owner-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir(&directory).unwrap();
    let mut config = AofConfig::new(directory.clone());
    config.layout.shard_count = 2;
    let quota = StoreConfig {
        max_dataset_bytes: 4_096,
    };
    let recovered = persistence::recover_composed_legacy_mixed(
        config.clone(),
        quota,
        Arc::new(SystemClock),
        Arc::new(NoFaults),
    )
    .unwrap();
    let (_empty, aof, writer) = recovered.start();
    let (stop, shutdown) = watch::channel(false);
    let stores = (0..2).map(|_| Store::new()).collect();
    let (handle, mut owners) =
        worker::channel_with_stores(2, Duration::from_secs(5), shutdown, stores).unwrap();
    let handle = handle.with_composed_aof(aof.clone()).unwrap();
    let first = tokio::spawn(owners.remove(0).with_aof(aof.clone(), 0).run());
    let second_aof = aof.clone();
    let second = tokio::spawn(async move {
        let mut worker = owners.remove(0).with_aof(second_aof, 0);
        loop {
            let control = worker.coordination.recv().await.unwrap();
            if matches!(control, Control::Apply { .. }) {
                // The second owner disappears after the writer accepted the record.
                drop(control);
                return;
            }
            worker.coordination_control(control);
        }
    });
    assert_eq!(
        handle
            .execute_batch(vec![set(b"a", b"left"), set(b"b", b"right")], Vec::new())
            .await,
        Err(DbError::Unavailable)
    );
    second.await.unwrap();
    assert_eq!(handle.check_admission(), Err(DbError::Unavailable));
    assert_eq!(handle.execute(get(b"a")).await, Err(DbError::Unavailable));
    assert_eq!(handle.execute(get(b"b")).await, Err(DbError::Unavailable));
    drop(handle);
    stop.send_replace(true);
    first.await.unwrap();
    drop(aof);
    writer.await.unwrap().unwrap();
    let restarted = persistence::recover_composed_legacy_mixed(
        config,
        quota,
        Arc::new(SystemClock),
        Arc::new(NoFaults),
    )
    .unwrap();
    assert_eq!(restarted.metadata.sequence, 1);
    let mut store = restarted.store;
    assert_eq!(
        store.execute(get(b"a")),
        Reply::Bulk(Some(Bytes::from_static(b"left")))
    );
    assert_eq!(
        store.execute(get(b"b")),
        Reply::Bulk(Some(Bytes::from_static(b"right")))
    );
    drop(store);
    let parent = std::env::temp_dir().canonicalize().unwrap();
    let target = directory.canonicalize().unwrap();
    assert_eq!(target.parent(), Some(parent.as_path()));
    assert!(
        target
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("sider-mixed-coordinator-owner-")
    );
    std::fs::remove_dir_all(target).unwrap();
}
