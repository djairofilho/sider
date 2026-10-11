//! Process-crash boundaries for the internal mixed-owner durable decision.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::watch;

use super::*;
use crate::persistence::{self, AofConfig, FaultInjector, NoFaults};
use crate::storage::{Mutation, Store, StoreConfig, SystemClock, Value, worker};

const TIMEOUT: Duration = Duration::from_secs(10);
const QUOTA: StoreConfig = StoreConfig {
    max_dataset_bytes: 4_096,
};

fn config(directory: &Path) -> AofConfig {
    let mut config = AofConfig::new(directory.to_path_buf());
    config.layout.shard_count = 2;
    config
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn set(key: &'static [u8], value: &'static [u8]) -> Command {
    Command::Set {
        key: Bytes::from_static(key),
        value: Bytes::from_static(value),
    }
}

fn get(store: &mut Store, key: &'static [u8]) -> Reply {
    store.execute(Command::Get {
        key: Bytes::from_static(key),
    })
}

fn recover(directory: &Path, faults: Arc<dyn FaultInjector>) -> persistence::Recovered {
    persistence::recover_composed_legacy_mixed(
        config(directory),
        QUOTA,
        Arc::new(SystemClock),
        faults,
    )
    .unwrap()
}

fn seed(directory: &Path) {
    runtime().block_on(async {
        let (_store, aof, writer) = recover(directory, Arc::new(NoFaults)).start();
        let batch = ResolvedBatch {
            origin: MutationOrigin::Client,
            mutations: [b"a".as_slice(), b"b".as_slice()]
                .into_iter()
                .map(|key| Mutation::Put {
                    key: Bytes::copy_from_slice(key),
                    value: Value::String(Bytes::from_static(b"old")),
                    expires_at_unix_ms: None,
                })
                .collect(),
        };
        assert_eq!(aof.append(batch).await.unwrap(), 1);
        drop(aof);
        writer.await.unwrap().unwrap();
    });
}

struct Pause {
    point: String,
    directory: PathBuf,
}

impl FaultInjector for Pause {
    fn hit(&self, point: &'static str) -> io::Result<()> {
        if point == self.point {
            let mut signal = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.directory.join("paused"))?;
            signal.write_all(point.as_bytes())?;
            signal.sync_all()?;
            loop {
                std::thread::park_timeout(Duration::from_secs(1));
            }
        }
        Ok(())
    }
}

#[test]
#[ignore = "crash helper executed only by the parent process"]
fn mixed_owner_crash_child() {
    let directory = PathBuf::from(std::env::var_os("SIDER_TEST_MIXED_AOF_DIR").unwrap());
    let point = std::env::var("SIDER_TEST_MIXED_POINT").unwrap();
    let recovered = recover(
        &directory,
        Arc::new(Pause {
            point,
            directory: directory.clone(),
        }),
    );
    runtime().block_on(async {
        let (global, aof, writer) = recovered.start();
        let mut stores = (0..2)
            .map(|_| {
                Store::with_config(
                    StoreConfig {
                        max_dataset_bytes: 2_048,
                    },
                    Arc::new(SystemClock),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        for mutation in global.snapshot() {
            let owner = config(&directory).layout.shard_for(mutation.key()).unwrap();
            stores[owner]
                .replay(std::slice::from_ref(&mutation))
                .unwrap();
        }
        drop(global);
        let (stop, shutdown) = watch::channel(false);
        let (handle, owners) = worker::channel_with_stores(2, TIMEOUT, shutdown, stores).unwrap();
        let handle = handle.with_composed_aof(aof.clone());
        let tasks = owners
            .into_iter()
            .map(|owner| tokio::spawn(owner.with_aof(aof.clone(), 0).run()))
            .collect::<Vec<_>>();
        assert_eq!(
            handle
                .execute_batch(vec![set(b"a", b"new"), set(b"b", b"new")], Vec::new())
                .await
                .unwrap(),
            Reply::Array(vec![Reply::Ok, Reply::Ok])
        );
        fs::write(directory.join("acknowledged"), b"new").unwrap();
        drop(handle);
        stop.send_replace(true);
        for task in tasks {
            task.await.unwrap();
        }
        drop(aof);
        writer.await.unwrap().unwrap();
    });
}

#[test]
fn process_crashes_replay_old_or_complete_new_mixed_batch() {
    let points = [
        "mixed_before_append",
        "before_append",
        "after_append",
        "before_sync",
        "after_sync",
        "before_reply",
        "mixed_before_first_apply",
        "mixed_after_first_apply",
        "mixed_after_all_apply",
    ];
    for point in points {
        let directory = std::env::temp_dir().join(format!(
            "sider-mixed-crash-{}-{}-{point}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        fs::create_dir(&directory).unwrap();
        seed(&directory);
        let mut child = ProcessCommand::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "storage::coordinator::crash_tests::mixed_owner_crash_child",
                "--nocapture",
            ])
            .env("SIDER_TEST_MIXED_AOF_DIR", &directory)
            .env("SIDER_TEST_MIXED_POINT", point)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + TIMEOUT;
        while !directory.join("paused").is_file() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before {point}"
            );
            assert!(Instant::now() < deadline, "point not reached: {point}");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!directory.join("acknowledged").exists());
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success());
        let recovered = recover(&directory, Arc::new(NoFaults));
        let sequence = recovered.metadata.sequence;
        let mut store = recovered.store;
        let a = get(&mut store, b"a");
        let b = get(&mut store, b"b");
        assert_eq!(a, b, "partial mixed batch after crash at {point}");
        let old = Reply::Bulk(Some(Bytes::from_static(b"old")));
        let new = Reply::Bulk(Some(Bytes::from_static(b"new")));
        assert!(a == old || a == new, "unexpected value at {point}: {a:?}");
        assert_eq!(
            sequence,
            if a == old { 1 } else { 2 },
            "wrong sequence at {point}"
        );
        if point == "mixed_before_append" || point == "before_append" {
            assert_eq!(a, old);
            assert_eq!(sequence, 1);
        }
        if matches!(
            point,
            "after_sync"
                | "before_reply"
                | "mixed_before_first_apply"
                | "mixed_after_first_apply"
                | "mixed_after_all_apply"
        ) {
            assert_eq!(a, new);
            assert_eq!(sequence, 2);
        }
        drop(store);
        let parent = std::env::temp_dir().canonicalize().unwrap();
        let target = directory.canonicalize().unwrap();
        assert_eq!(target.parent(), Some(parent.as_path()));
        assert!(
            target
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("sider-mixed-crash-")
        );
        fs::remove_dir_all(target).unwrap();
    }
}
