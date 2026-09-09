//! Persistência em disco e falhas de processos reais, executáveis em Windows e Linux.
#![forbid(unsafe_code)]

#[path = "common/baseline_manifest.rs"]
mod baseline_manifest;
#[path = "common/gate_receipt.rs"]
mod gate_receipt;
#[path = "common/process.rs"]
mod process;

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sider::command::{Command as DbCommand, Reply};
use sider::persistence::format::{self, Limits, Record};
use sider::persistence::{self, AofConfig, AofError, FaultInjector, SyncPolicy};
use sider::storage::{
    Clock, Mutation, MutationOrigin, ResolvedBatch, Store, StoreConfig, SystemClock,
};

static NEXT: AtomicU64 = AtomicU64::new(0);
const TIMEOUT: Duration = Duration::from_secs(10);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-aof-{}-{}-{}",
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

fn batch(value: &'static [u8]) -> ResolvedBatch {
    ResolvedBatch {
        origin: MutationOrigin::Client,
        mutations: [b"a", b"b"]
            .into_iter()
            .map(|key| Mutation::Put {
                key: Bytes::from_static(key),
                value: Bytes::from_static(value).into(),
                expires_at_unix_ms: None,
            })
            .collect(),
    }
}
fn get(store: &mut Store, key: &'static [u8]) -> Reply {
    store.execute(DbCommand::Get {
        key: Bytes::from_static(key),
    })
}
fn recover(directory: &Directory) -> persistence::Recovered {
    persistence::recover(
        directory.config(),
        StoreConfig::default(),
        Arc::new(SystemClock),
    )
    .unwrap()
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
fn seed(directory: &Directory) {
    runtime().block_on(async {
        let (_, handle, task) = recover(directory).start();
        assert_eq!(handle.append(batch(b"old")).await.unwrap(), 1);
        drop(handle);
        task.await.unwrap().unwrap();
    });
}

#[test]
fn writer_roundtrip_lock_and_shared_sequence() {
    let directory = Directory::new();
    let recovered = recover(&directory);
    assert!(matches!(
        persistence::recover(
            directory.config(),
            StoreConfig::default(),
            Arc::new(SystemClock)
        ),
        Err(AofError::Locked)
    ));
    runtime().block_on(async {
        let (_, handle, task) = recovered.start();
        let second = handle.clone();
        assert_eq!(handle.append(batch(b"first")).await.unwrap(), 1);
        assert_eq!(second.append(batch(b"second")).await.unwrap(), 2);
        assert_eq!(handle.flush().await.unwrap(), 2);
        drop((handle, second));
        task.await.unwrap().unwrap();
    });
    let mut recovered = recover(&directory);
    for key in [b"a", b"b"] {
        assert_eq!(
            get(&mut recovered.store, key),
            Reply::Bulk(Some(Bytes::from_static(b"second")))
        );
    }
}

struct ErrorAt(&'static str);
impl FaultInjector for ErrorAt {
    fn hit(&self, point: &'static str) -> io::Result<()> {
        if point == self.0 {
            Err(io::Error::other("falha de disco injetada"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn append_and_sync_errors_never_reply_success_and_stop_admission() {
    for point in [
        "before_append",
        "after_append",
        "before_sync",
        "after_sync",
        "before_reply",
    ] {
        let directory = Directory::new();
        runtime().block_on(async {
            let recovered = persistence::recover_with_faults(
                directory.config(),
                StoreConfig::default(),
                Arc::new(SystemClock),
                Arc::new(ErrorAt(point)),
            )
            .unwrap();
            let (store, aof, writer) = recovered.start();
            let (_stop, shutdown) = tokio::sync::watch::channel(false);
            let (db, worker) =
                sider::storage::worker::channel_with_store(2, TIMEOUT, shutdown, store).unwrap();
            let running = tokio::spawn(worker.with_aof(aof, 0).run());
            assert!(
                db.execute(DbCommand::Set {
                    key: Bytes::from_static(b"k"),
                    value: Bytes::from_static(b"v")
                })
                .await
                .is_err(),
                "{point}"
            );
            tokio::time::timeout(TIMEOUT, running)
                .await
                .unwrap()
                .unwrap();
            assert!(db.execute(DbCommand::Ping(None)).await.is_err());
            assert!(writer.await.unwrap().is_err());
        });
    }
}

struct Signals {
    sender: std::sync::mpsc::Sender<&'static str>,
}

struct ShortWrite(usize);
impl FaultInjector for ShortWrite {
    fn hit(&self, _: &'static str) -> io::Result<()> {
        Ok(())
    }
    fn write_append(&self, file: &mut fs::File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(&bytes[..self.0.min(bytes.len())])?;
        Err(io::Error::other("escrita curta seguida de falha"))
    }
}

#[test]
fn short_writes_leave_only_an_unapplied_tail() {
    for length in [1, 12, 22] {
        let directory = Directory::new();
        seed(&directory);
        runtime().block_on(async {
            let recovered = persistence::recover_with_faults(
                directory.config(),
                StoreConfig::default(),
                Arc::new(SystemClock),
                Arc::new(ShortWrite(length)),
            )
            .unwrap();
            let (_, handle, task) = recovered.start();
            assert!(handle.append(batch(b"new")).await.is_err());
            drop(handle);
            assert!(task.await.unwrap().is_err());
        });
        let mut recovered = recover(&directory);
        assert_eq!(
            get(&mut recovered.store, b"a"),
            Reply::Bulk(Some(Bytes::from_static(b"old")))
        );
    }
}

#[test]
fn rejected_commands_and_failed_conditions_do_not_append() {
    let directory = Directory::new();
    runtime().block_on(async {
        let recovered = persistence::recover(
            directory.config(),
            StoreConfig {
                max_dataset_bytes: 140,
            },
            Arc::new(SystemClock),
        )
        .unwrap();
        let (store, aof, writer) = recovered.start();
        let (stop, shutdown) = tokio::sync::watch::channel(false);
        let (db, worker) =
            sider::storage::worker::channel_with_store(2, TIMEOUT, shutdown, store).unwrap();
        let running = tokio::spawn(worker.with_aof(aof.clone(), 0).run());
        db.execute(DbCommand::Set {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"text"),
        })
        .await
        .unwrap();
        assert_eq!(aof.status().await.unwrap().0, 1);
        assert!(matches!(
            db.execute(DbCommand::Incr {
                key: Bytes::from_static(b"k")
            })
            .await
            .unwrap(),
            Reply::Error(_)
        ));
        assert!(matches!(
            db.execute(DbCommand::Set {
                key: Bytes::from_static(b"too-big"),
                value: Bytes::from_static(b"value")
            })
            .await
            .unwrap(),
            Reply::Error(_)
        ));
        assert_eq!(
            db.execute(DbCommand::SetWithOptions {
                key: Bytes::from_static(b"k"),
                value: Bytes::new(),
                options: sider::command::SetOptions {
                    condition: sider::command::SetCondition::Missing,
                    ..sider::command::SetOptions::default()
                }
            })
            .await
            .unwrap(),
            Reply::Bulk(None)
        );
        assert_eq!(aof.status().await.unwrap().0, 1);
        stop.send(true).unwrap();
        running.await.unwrap();
        drop(aof);
        writer.await.unwrap().unwrap();
    });
}
impl FaultInjector for Signals {
    fn hit(&self, point: &'static str) -> io::Result<()> {
        let _ = self.sender.send(point);
        Ok(())
    }
}

#[test]
fn periodic_sync_runs_without_a_second_write() {
    let directory = Directory::new();
    let mut config = directory.config();
    config.sync = SyncPolicy::Periodic(Duration::from_millis(30));
    let (sender, receiver) = std::sync::mpsc::channel();
    runtime().block_on(async {
        let recovered = persistence::recover_with_faults(
            config,
            StoreConfig::default(),
            Arc::new(SystemClock),
            Arc::new(Signals { sender }),
        )
        .unwrap();
        let (_, handle, task) = recovered.start();
        handle.append(batch(b"periodic")).await.unwrap();
        let mut events = Vec::new();
        loop {
            let point = receiver.recv_timeout(TIMEOUT).unwrap();
            events.push(point);
            if point == "after_sync" {
                break;
            }
        }
        assert!(
            events
                .iter()
                .position(|event| *event == "before_reply")
                .unwrap()
                < events
                    .iter()
                    .position(|event| *event == "before_sync")
                    .unwrap()
        );
        drop(handle);
        task.await.unwrap().unwrap();
    });
}

fn recovery_cases() -> u64 {
    let mut cases = 0;
    let directory = Directory::new();
    seed(&directory);
    let path = directory.0.join("generation-00000000000000000000.aof");
    let original = fs::read(&path).unwrap();
    let second = format::encode(
        &Record::Batch {
            sequence: 2,
            batch: batch(b"new"),
        },
        Limits::default(),
    )
    .unwrap();
    for prefix in 0..=second.len() {
        let candidate = Directory::new();
        let mut bytes = original.clone();
        bytes.extend_from_slice(&second[..prefix]);
        fs::write(
            candidate.0.join("generation-00000000000000000000.aof"),
            &bytes,
        )
        .unwrap();
        let mut recovered = recover(&candidate);
        let expected = if prefix == second.len() {
            b"new".as_slice()
        } else {
            b"old".as_slice()
        };
        for key in [b"a", b"b"] {
            assert_eq!(
                get(&mut recovered.store, key),
                Reply::Bulk(Some(Bytes::copy_from_slice(expected)))
            );
        }
        if prefix > 0 && prefix < second.len() {
            let backup = fs::read_dir(&candidate.0)
                .unwrap()
                .map(Result::unwrap)
                .find(|entry| entry.path().extension().is_some_and(|ext| ext == "bak"))
                .unwrap();
            assert_eq!(fs::read(backup.path()).unwrap(), bytes);
        }
        cases += 1;
    }
    let candidate = Directory::new();
    let corrupt_path = candidate.0.join("generation-00000000000000000000.aof");
    let mut corrupt = original.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(&corrupt_path, &corrupt).unwrap();
    assert!(
        persistence::recover(
            candidate.config(),
            StoreConfig::default(),
            Arc::new(SystemClock)
        )
        .is_err()
    );
    assert_eq!(fs::read(corrupt_path).unwrap(), corrupt);
    cases + 1
}

#[test]
fn recovery_at_every_batch_boundary_preserves_original_on_corruption() {
    assert!(recovery_cases() > 1);
}

struct FixedClock(i64);
impl Clock for FixedClock {
    fn now(&self) -> tokio::time::Instant {
        tokio::time::Instant::now()
    }
    fn unix_millis(&self) -> i64 {
        self.0
    }
}

#[test]
fn replay_restores_quota_and_absolute_deadlines_after_clock_movement() {
    let directory = Directory::new();
    runtime().block_on(async {
        let (_, handle, task) = recover(&directory).start();
        handle
            .append(ResolvedBatch {
                origin: MutationOrigin::Client,
                mutations: vec![Mutation::Put {
                    key: Bytes::from_static(b"k"),
                    value: Bytes::from_static(b"v").into(),
                    expires_at_unix_ms: Some(1000),
                }],
            })
            .await
            .unwrap();
        drop(handle);
        task.await.unwrap().unwrap();
    });
    for (now, expected) in [(900, 1), (1000, 0), (1100, 0)] {
        let recovered = persistence::recover(
            directory.config(),
            StoreConfig::default(),
            Arc::new(FixedClock(now)),
        )
        .unwrap();
        assert_eq!(recovered.store.len(), expected);
        assert_eq!(recovered.store.used_bytes(), expected * 130);
    }
    let source = fs::read(directory.0.join("generation-00000000000000000000.aof")).unwrap();
    assert!(
        persistence::recover(
            directory.config(),
            StoreConfig {
                max_dataset_bytes: 129
            },
            Arc::new(FixedClock(900))
        )
        .is_err()
    );
    assert_eq!(
        fs::read(directory.0.join("generation-00000000000000000000.aof")).unwrap(),
        source
    );
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
#[ignore = "helper executado somente pelo teste pai em processo isolado"]
fn aof_process_child() {
    let directory =
        PathBuf::from(std::env::var_os("SIDER_TEST_AOF_DIR").expect("diretório do processo filho"));
    let point = std::env::var("SIDER_TEST_AOF_POINT").unwrap();
    let compact = point.starts_with("compact_");
    let recovered = persistence::recover_with_faults(
        AofConfig::new(directory.clone()),
        StoreConfig::default(),
        Arc::new(SystemClock),
        Arc::new(Pause {
            point,
            directory: directory.clone(),
        }),
    )
    .unwrap();
    runtime().block_on(async {
        let (store, handle, task) = recovered.start();
        if compact {
            let finished = handle.begin_compaction(store.snapshot()).await.unwrap();
            handle.append(batch(b"new")).await.unwrap();
            fs::write(directory.join("acknowledged"), b"new").unwrap();
            finished.await.unwrap().unwrap();
        } else {
            handle.append(batch(b"new")).await.unwrap();
        }
        drop(handle);
        task.await.unwrap().unwrap();
    });
}

fn wait_file(path: &Path, child: &mut process::OwnedChild) {
    let deadline = Instant::now() + TIMEOUT;
    while !path.is_file() {
        child
            .assert_alive()
            .expect("processo encerrou antes do ponto de crash");
        assert!(
            Instant::now() < deadline,
            "ponto de crash não alcançado: {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn crash_cases() -> u64 {
    let mut cases = 0;
    for point in [
        "before_append",
        "after_append",
        "before_sync",
        "after_sync",
        "before_reply",
        "compact_before_snapshot",
        "compact_after_snapshot",
        "compact_before_publish",
        "compact_after_publish",
    ] {
        let directory = Directory::new();
        seed(&directory);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--ignored", "--exact", "aof_process_child", "--nocapture"])
            .env("SIDER_TEST_AOF_DIR", &directory.0)
            .env("SIDER_TEST_AOF_POINT", point);
        let mut child = process::OwnedChild::spawn(&mut command).unwrap();
        wait_file(&directory.0.join("paused"), &mut child);
        let acknowledged = directory.0.join("acknowledged").exists();
        child.terminate(TIMEOUT).unwrap();
        let mut recovered = recover(&directory);
        let a = get(&mut recovered.store, b"a");
        let b = get(&mut recovered.store, b"b");
        assert_eq!(a, b, "lote parcialmente recuperado em {point}");
        assert!(
            matches!(&a, Reply::Bulk(Some(value)) if value.as_ref() == b"old" || value.as_ref() == b"new")
        );
        if point == "before_append" {
            assert_eq!(a, Reply::Bulk(Some(Bytes::from_static(b"old"))));
        }
        if acknowledged || point == "after_sync" || point == "before_reply" {
            assert_eq!(
                a,
                Reply::Bulk(Some(Bytes::from_static(b"new"))),
                "confirmação perdida em {point}"
            );
        }
        cases += 1;
    }
    cases
}

#[test]
fn real_process_crashes_preserve_atomic_batches_and_compaction_generations() {
    assert_eq!(crash_cases(), 9);
}

#[test]
fn snapshot_compaction_replays_concurrent_delta_and_retains_old_generation() {
    let directory = Directory::new();
    seed(&directory);
    runtime().block_on(async {
        let recovered = recover(&directory);
        let (store, handle, task) = recovered.start();
        let completed = handle.begin_compaction(store.snapshot()).await.unwrap();
        for _ in 0..20 {
            handle.append(batch(b"new")).await.unwrap();
        }
        tokio::time::timeout(TIMEOUT, completed)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(handle);
        task.await.unwrap().unwrap();
    });
    assert!(
        directory
            .0
            .join("generation-00000000000000000000.aof")
            .exists()
    );
    assert!(
        directory
            .0
            .join("generation-00000000000000000001.aof")
            .exists()
    );
    let mut recovered = recover(&directory);
    assert_eq!(
        get(&mut recovered.store, b"a"),
        Reply::Bulk(Some(Bytes::from_static(b"new")))
    );
}

struct BlockSnapshot {
    reached: std::sync::mpsc::Sender<()>,
    released: (std::sync::Mutex<bool>, std::sync::Condvar),
}
impl BlockSnapshot {
    fn release(&self) {
        *self.released.0.lock().unwrap() = true;
        self.released.1.notify_all();
    }
}
impl FaultInjector for BlockSnapshot {
    fn hit(&self, point: &'static str) -> io::Result<()> {
        if point == "compact_after_snapshot" {
            let _ = self.reached.send(());
            let mut released = self.released.0.lock().unwrap();
            while !*released {
                released = self.released.1.wait(released).unwrap();
            }
        }
        Ok(())
    }
}

#[test]
fn bounded_delta_abort_preserves_writes_and_allows_retry() {
    let directory = Directory::new();
    seed(&directory);
    let (reached, signal) = std::sync::mpsc::channel();
    let hook = Arc::new(BlockSnapshot {
        reached,
        released: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
    });
    let mut config = directory.config();
    config.max_delta_bytes = 1;
    runtime().block_on(async {
        let recovered = persistence::recover_with_faults(
            config,
            StoreConfig::default(),
            Arc::new(SystemClock),
            hook.clone(),
        )
        .unwrap();
        let (mut store, handle, task) = recovered.start();
        let completed = handle.begin_compaction(store.snapshot()).await.unwrap();
        signal.recv_timeout(TIMEOUT).unwrap();
        handle.append(batch(b"new")).await.unwrap();
        hook.release();
        assert!(matches!(
            completed.await.unwrap(),
            Err(AofError::DeltaLimit)
        ));
        store.replay(&batch(b"new").mutations).unwrap();
        hook.release();
        handle.compact(store.snapshot()).await.unwrap();
        drop(handle);
        task.await.unwrap().unwrap();
    });
    let mut recovered = recover(&directory);
    assert_eq!(
        get(&mut recovered.store, b"a"),
        Reply::Bulk(Some(Bytes::from_static(b"new")))
    );
}

#[test]
fn expiration_tombstone_during_snapshot_prevents_resurrection() {
    let directory = Directory::new();
    seed(&directory);
    let (reached, signal) = std::sync::mpsc::channel();
    let hook = Arc::new(BlockSnapshot {
        reached,
        released: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
    });
    runtime().block_on(async {
        let recovered = persistence::recover_with_faults(
            directory.config(),
            StoreConfig::default(),
            Arc::new(SystemClock),
            hook.clone(),
        )
        .unwrap();
        let (store, handle, task) = recovered.start();
        let completed = handle.begin_compaction(store.snapshot()).await.unwrap();
        signal.recv_timeout(TIMEOUT).unwrap();
        handle
            .append(ResolvedBatch {
                origin: MutationOrigin::Expiration,
                mutations: vec![Mutation::Delete {
                    key: Bytes::from_static(b"a"),
                }],
            })
            .await
            .unwrap();
        hook.release();
        completed.await.unwrap().unwrap();
        drop(handle);
        task.await.unwrap().unwrap();
    });
    let mut recovered = recover(&directory);
    assert_eq!(get(&mut recovered.store, b"a"), Reply::Bulk(None));
    assert_eq!(
        get(&mut recovered.store, b"b"),
        Reply::Bulk(Some(Bytes::from_static(b"old")))
    );
}

#[test]
fn snapshot_disk_error_keeps_old_aof_writable() {
    for point in [
        "compact_before_snapshot",
        "compact_after_snapshot",
        "compact_before_publish",
    ] {
        let directory = Directory::new();
        seed(&directory);
        runtime().block_on(async {
            let recovered = persistence::recover_with_faults(
                directory.config(),
                StoreConfig::default(),
                Arc::new(SystemClock),
                Arc::new(ErrorAt(point)),
            )
            .unwrap();
            let (store, handle, task) = recovered.start();
            assert!(handle.compact(store.snapshot()).await.is_err());
            handle.append(batch(b"new")).await.unwrap();
            drop(handle);
            task.await.unwrap().unwrap();
        });
        let mut recovered = recover(&directory);
        assert_eq!(
            get(&mut recovered.store, b"a"),
            Reply::Bulk(Some(Bytes::from_static(b"new")))
        );
    }
}

#[test]
fn removed_snapshot_record_is_rejected_even_when_other_checksums_are_valid() {
    let directory = Directory::new();
    seed(&directory);
    runtime().block_on(async {
        let (store, handle, task) = recover(&directory).start();
        handle.compact(store.snapshot()).await.unwrap();
        drop(handle);
        task.await.unwrap().unwrap();
    });
    let path = directory.0.join("generation-00000000000000000001.aof");
    let mut bytes = fs::read(&path).unwrap();
    let header_bytes = format::read_header_with_layout(bytes.as_slice())
        .unwrap()
        .bytes;
    let record_size =
        u32::from_le_bytes(bytes[header_bytes..header_bytes + 4].try_into().unwrap()) as usize + 12;
    bytes.drain(header_bytes..header_bytes + record_size);
    fs::write(&path, &bytes).unwrap();
    assert!(
        persistence::recover(
            directory.config(),
            StoreConfig::default(),
            Arc::new(SystemClock)
        )
        .is_err()
    );
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn binary_refuses_corrupt_aof_before_bind_and_readiness() {
    let directory = Directory::new();
    fs::write(
        directory.0.join("generation-00000000000000000000.aof"),
        b"invalid header",
    )
    .unwrap();
    let ready = directory.0.join("ready.txt");
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_sider"));
    command
        .env_clear()
        .env("SIDER_AOF_DIR", &directory.0)
        .env("SIDER_READY_FILE", &ready)
        .env("SIDER_ADDR", held.local_addr().unwrap().to_string());
    // Uma porta já ocupada distingue a recuperação anterior ao bind: o erro deve ser do AOF.
    let mut child = process::OwnedChild::spawn(&mut command).unwrap();
    let output = child.wait(TIMEOUT).unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("AOF"));
    assert!(!ready.exists());
}

#[test]
fn aof_config_is_injected_and_rejects_invalid_policy_and_limits() {
    let config = sider::ServerConfig::from_lookup(|name| match name {
        "SIDER_AOF_DIR" => Some("data".into()),
        "SIDER_AOF_SYNC" => Some("everysec".into()),
        "SIDER_AOF_MAX_DELTA_BYTES" => Some("32".into()),
        _ => None,
    })
    .unwrap();
    assert_eq!(
        config.aof.as_ref().unwrap().sync,
        SyncPolicy::Periodic(Duration::from_secs(1))
    );
    for (name, value) in [
        ("SIDER_AOF_SYNC", "none"),
        ("SIDER_AOF_QUEUE_CAPACITY", "0"),
        ("SIDER_AOF_MAX_RECORD_BYTES", "67108865"),
        ("SIDER_AOF_MAX_DELTA_BYTES", "0"),
    ] {
        assert!(
            sider::ServerConfig::from_lookup(|key| if key == "SIDER_AOF_DIR" {
                Some("data".into())
            } else if key == name {
                Some(value.into())
            } else {
                None
            })
            .is_err()
        );
    }
}

#[test]
fn oversized_aof_record_is_rejected_without_losing_the_connection_or_state() {
    runtime().block_on(async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let directory = Directory::new();
        let mut aof = directory.config();
        aof.limits.max_record_bytes = 64;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, shutdown) = tokio::sync::oneshot::channel::<()>();
        let config = sider::ServerConfig {
            aof: Some(aof),
            ..sider::ServerConfig::default()
        };
        let running = tokio::spawn(sider::server::serve(listener, config, async {
            let _ = shutdown.await;
        }));
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        async fn exchange(client: &mut tokio::net::TcpStream, request: &[u8], expected: &[u8]) {
            client.write_all(request).await.unwrap();
            let mut response = vec![0; expected.len()];
            tokio::time::timeout(TIMEOUT, client.read_exact(&mut response))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response, expected);
        }
        exchange(
            &mut client,
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\nold\r\n",
            b"+OK\r\n",
        )
        .await;
        let mut large = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$100\r\n".to_vec();
        large.extend_from_slice(&[b'x'; 100]);
        large.extend_from_slice(b"\r\n");
        exchange(&mut client, &large, b"-ERR AOF record limit exceeded\r\n").await;
        exchange(&mut client, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;
        exchange(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
            b"$3\r\nold\r\n",
        )
        .await;
        exchange(
            &mut client,
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\nnew\r\n",
            b"+OK\r\n",
        )
        .await;
        drop(client);
        stop.send(()).unwrap();
        running.await.unwrap().unwrap();
        let mut recovered = recover(&directory);
        assert_eq!(
            get(&mut recovered.store, b"k"),
            Reply::Bulk(Some(Bytes::from_static(b"new")))
        );
    });
}

#[test]
#[ignore = "gate de release exige contexto exato; suíte interna roda diretamente"]
fn release_crash_gate() {
    let context = gate_receipt::GateContext::from_env("crash").unwrap();
    let began = Instant::now();
    let cases = crash_cases();
    short_writes_leave_only_an_unapplied_tail();
    append_and_sync_errors_never_reply_success_and_stop_admission();
    periodic_sync_runs_without_a_second_write();
    snapshot_disk_error_keeps_old_aof_writable();
    bounded_delta_abort_preserves_writes_and_allows_retry();
    expiration_tombstone_during_snapshot_prevents_resurrection();
    typed_process_crashes_preserve_complete_values_and_compaction_deltas();
    context
        .publish(
            cases + 14 + 36,
            began.elapsed(),
            serde_json::json!({ "process_crash_points": cases, "typed_process_crashes":36,"injected_io_cases": 14, "policies": ["always", "periodic"], "scope": "process_crash" }),
        )
        .unwrap();
}

#[test]
#[ignore = "gate de release exige contexto exato; suíte interna roda diretamente"]
fn release_recovery_gate() {
    let context = gate_receipt::GateContext::from_env("recovery").unwrap();
    let began = Instant::now();
    let cases = recovery_cases();
    replay_restores_quota_and_absolute_deadlines_after_clock_movement();
    removed_snapshot_record_is_rejected_even_when_other_checksums_are_valid();
    binary_refuses_corrupt_aof_before_bind_and_readiness();
    typed_roundtrip_compaction_restores_exact_data_ttl_and_quota();
    context
        .publish(
            cases + 6 + 4,
            began.elapsed(),
            serde_json::json!({ "prefixes_and_corruption": cases, "typed_families":4,"absolute_ttl_and_quota": true }),
        )
        .unwrap();
}

fn migration_cases() -> u64 {
    let directory = Directory::new();
    let hex = include_str!("fixtures/aof-v1.hex").trim().as_bytes();
    let fixture: Vec<u8> = hex
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    fs::write(
        directory.0.join("generation-00000000000000000000.aof"),
        fixture,
    )
    .unwrap();
    runtime().block_on(async {
        let recovered = recover(&directory);
        let (mut store, handle, task) = recovered.start();
        assert_eq!(
            get(&mut store, b"k"),
            Reply::Bulk(Some(Bytes::from_static(b"v1")))
        );
        handle.compact(store.snapshot()).await.unwrap();
        drop(handle);
        task.await.unwrap().unwrap();
    });
    let mut recovered = recover(&directory);
    assert_eq!(
        get(&mut recovered.store, b"k"),
        Reply::Bulk(Some(Bytes::from_static(b"v1")))
    );
    drop(recovered);
    let path = directory.0.join("generation-00000000000000000001.aof");
    let mut unknown = fs::read(&path).unwrap();
    let header_bytes = format::read_header_with_layout(unknown.as_slice())
        .unwrap()
        .bytes;
    unknown[8..12].copy_from_slice(&99u32.to_le_bytes());
    let crc = format::checksum(&unknown[..header_bytes - 4]);
    unknown[header_bytes - 4..header_bytes].copy_from_slice(&crc.to_le_bytes());
    fs::write(&path, &unknown).unwrap();
    assert!(
        persistence::recover(
            directory.config(),
            StoreConfig::default(),
            Arc::new(SystemClock)
        )
        .is_err()
    );
    assert_eq!(fs::read(path).unwrap(), unknown);
    3
}

#[test]
fn migration_from_fixed_v1_fixture_and_unknown_version_rejection() {
    assert_eq!(migration_cases(), 3);
}

#[test]
#[ignore = "gate de release exige contexto exato; suíte interna roda diretamente"]
fn release_migration_gate() {
    let context = gate_receipt::GateContext::from_env("migration").unwrap();
    let began = Instant::now();
    let cases = migration_cases();
    context
        .publish(
            cases,
            began.elapsed(),
            serde_json::json!({ "fixture": "aof-v1.hex", "unknown_version_preserved": true }),
        )
        .unwrap();
}

fn typed_command(args: &[&[u8]]) -> DbCommand {
    sider::command::parse(sider::resp::Frame::Array(Some(
        args.iter()
            .map(|arg| sider::resp::Frame::Bulk(Some(Bytes::copy_from_slice(arg))))
            .collect(),
    )))
    .unwrap()
}

fn typed_create(family: usize) -> DbCommand {
    typed_command(match family {
        0 => &[b"HSET", b"{typed}key", b"\0f", b"old", b"\xff", b""],
        1 => &[b"RPUSH", b"{typed}key", b"old", b"\0\xff"],
        2 => &[b"SADD", b"{typed}key", b"old", b"\0\xff"],
        3 => &[
            b"ZADD",
            b"{typed}key",
            b"1e23",
            b"old",
            b"5e-324",
            b"\0\xff",
        ],
        _ => unreachable!(),
    })
}

fn typed_update(family: usize, value: &[u8]) -> DbCommand {
    match family {
        0 => typed_command(&[b"HSET", b"{typed}key", b"\0f", value, b"new", value]),
        1 => typed_command(&[b"LPUSH", b"{typed}key", value, value]),
        2 => typed_command(&[b"SADD", b"{typed}key", value, value]),
        3 => typed_command(&[b"ZADD", b"{typed}key", b"-inf", b"old", b"2", value]),
        _ => unreachable!(),
    }
}

fn typed_read(family: usize) -> DbCommand {
    typed_command(match family {
        0 => &[b"HGETALL", b"{typed}key"],
        1 => &[b"LRANGE", b"{typed}key", b"0", b"-1"],
        2 => &[b"SMEMBERS", b"{typed}key"],
        3 => &[b"ZRANGE", b"{typed}key", b"0", b"-1", b"WITHSCORES"],
        _ => unreachable!(),
    })
}

async fn persist_command(
    store: &mut Store,
    aof: &persistence::AofHandle,
    command: DbCommand,
) -> Reply {
    let prepared = store.prepare(command);
    if !prepared.batch.mutations.is_empty() {
        aof.append(prepared.batch.clone()).await.unwrap();
    }
    store.apply(prepared)
}

struct TypedClock {
    now: tokio::time::Instant,
    unix: i64,
    elapsed: AtomicU64,
}
impl TypedClock {
    fn at(unix: i64) -> Arc<Self> {
        Arc::new(Self {
            now: tokio::time::Instant::now(),
            unix,
            elapsed: AtomicU64::new(0),
        })
    }
}
impl Clock for TypedClock {
    fn now(&self) -> tokio::time::Instant {
        self.now + Duration::from_millis(self.elapsed.load(Ordering::SeqCst))
    }
    fn unix_millis(&self) -> i64 {
        self.unix + self.elapsed.load(Ordering::SeqCst) as i64
    }
}

#[test]
fn typed_roundtrip_compaction_restores_exact_data_ttl_and_quota() {
    for family in 0..4 {
        let directory = Directory::new();
        let (expected, used) = runtime().block_on(async {
            let recovered = persistence::recover(
                directory.config(),
                StoreConfig::default(),
                TypedClock::at(1000),
            )
            .unwrap();
            let (mut store, aof, task) = recovered.start();
            persist_command(&mut store, &aof, typed_create(family)).await;
            persist_command(
                &mut store,
                &aof,
                typed_command(&[b"PEXPIRE", b"{typed}key", b"500"]),
            )
            .await;
            persist_command(&mut store, &aof, typed_update(family, b"new\0\xff")).await;
            let expected = store.snapshot();
            let used = store.used_bytes();
            aof.compact(store.snapshot()).await.unwrap();
            drop(aof);
            task.await.unwrap().unwrap();
            (expected, used)
        });
        let mut recovered = persistence::recover(
            directory.config(),
            StoreConfig::default(),
            TypedClock::at(1250),
        )
        .unwrap();
        assert_eq!(recovered.store.snapshot(), expected, "family {family}");
        assert_eq!(recovered.store.used_bytes(), used);
        assert_eq!(
            recovered
                .store
                .execute(typed_command(&[b"PTTL", b"{typed}key"])),
            Reply::Integer(250)
        );
        drop(recovered);
        let expired = persistence::recover(
            directory.config(),
            StoreConfig::default(),
            TypedClock::at(1500),
        )
        .unwrap();
        assert!(expired.store.is_empty());
        assert_eq!(expired.store.used_bytes(), 0);
    }
}

#[test]
fn typed_migration_preserves_legacy_string_fixture_and_new_values() {
    for family in 0..4 {
        let directory = Directory::new();
        let fixture: Vec<u8> = include_str!("fixtures/aof-v1.hex")
            .trim()
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        fs::write(
            directory.0.join("generation-00000000000000000000.aof"),
            fixture,
        )
        .unwrap();
        let expected = runtime().block_on(async {
            let (mut store, aof, writer) = recover(&directory).start();
            assert_eq!(
                get(&mut store, b"k"),
                Reply::Bulk(Some(Bytes::from_static(b"v1")))
            );
            persist_command(&mut store, &aof, typed_create(family)).await;
            persist_command(&mut store, &aof, typed_update(family, b"new")).await;
            let expected = store.snapshot();
            aof.compact(expected.clone()).await.unwrap();
            drop(aof);
            writer.await.unwrap().unwrap();
            expected
        });
        let mut recovered = recover(&directory);
        assert_eq!(recovered.store.snapshot(), expected);
        assert_eq!(
            get(&mut recovered.store, b"k"),
            Reply::Bulk(Some(Bytes::from_static(b"v1")))
        );
    }
}

#[test]
fn typed_migration_from_frozen_r04_binary_output_preserves_shards_and_elapsed_ttl() {
    // Bytes produzidos pelo migrador 4739d596 a partir da baseline real R03.
    // SHA256 b6be7a45ad5e57eb7957d10136afddec6488c60f7aa59bb1c5522388ba4a246f.
    for family in 0..4 {
        let directory = Directory::new();
        let bytes: Vec<_> = include_str!("fixtures/aof-r04-four-shards.hex")
            .trim()
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        fs::write(
            directory.0.join("generation-00000000000000000000.aof"),
            bytes,
        )
        .unwrap();
        let mut config = directory.config();
        config.layout.shard_count = 4;
        let clock = TypedClock::at(1789524588000);
        let expected = runtime().block_on(async {
            let recovered = persistence::recover(config.clone(), StoreConfig::default(), clock.clone()).unwrap();
            assert_eq!(recovered.metadata.sequence, 2);
            assert_eq!(recovered.metadata.layout.shard_count, 4);
            let (mut store, aof, writer) = recovered.start();
            assert_eq!(get(&mut store, b"r03:a"), Reply::Bulk(Some(Bytes::from_static(b"alpha"))));
            assert_eq!(get(&mut store, b"r03:b"), Reply::Bulk(Some(Bytes::from_static(b"42"))));
            assert_eq!(get(&mut store, b"r03:ttl"), Reply::Bulk(Some(Bytes::from_static(b"durable"))));
            assert!(matches!(store.execute(typed_command(&[b"PTTL", b"r03:ttl"])), Reply::Integer(ttl) if (536..=545).contains(&ttl)));
            persist_command(&mut store, &aof, typed_create(family)).await;
            persist_command(&mut store, &aof, typed_update(family, b"new")).await;
            let expected = store.execute(typed_read(family));
            aof.compact(store.snapshot()).await.unwrap();
            drop(aof);
            writer.await.unwrap().unwrap();
            expected
        });
        clock.elapsed.store(2000, Ordering::SeqCst);
        let mut recovered = persistence::recover(config, StoreConfig::default(), clock).unwrap();
        assert_eq!(recovered.store.execute(typed_read(family)), expected);
        assert_eq!(get(&mut recovered.store, b"r03:ttl"), Reply::Bulk(None));
        assert_eq!(
            get(&mut recovered.store, b"r03:a"),
            Reply::Bulk(Some(Bytes::from_static(b"alpha")))
        );
        assert_eq!(recovered.store.len(), 3);
    }
}

#[test]
fn sorted_set_migration_from_frozen_r05_binary_output_preserves_collections() {
    // Saída real a615f705; SHA256 87d12a8fc88698266882a6dce88506249fdd7dac3ffe594fc12e42cded47242b.
    let directory = Directory::new();
    let bytes: Vec<_> = include_str!("fixtures/aof-r05-collections.hex")
        .trim()
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    fs::write(
        directory.0.join("generation-00000000000000000000.aof"),
        bytes,
    )
    .unwrap();
    let mut config = directory.config();
    config.layout.shard_count = 4;
    let clock = TypedClock::at(1789527108000);
    let reads = [
        typed_command(&[b"GET", b"{r05}string"]),
        typed_command(&[b"LRANGE", b"{r05}list", b"0", b"-1"]),
        typed_command(&[b"SMEMBERS", b"{r05}set"]),
    ];
    let expected = runtime().block_on(async {
        let recovered = persistence::recover(config.clone(), StoreConfig::default(), clock.clone()).unwrap();
        assert_eq!(recovered.metadata.sequence, 5);
        assert_eq!(recovered.metadata.layout.shard_count, 4);
        let (mut store, aof, writer) = recovered.start();
        assert_eq!(store.len(), 4);
        assert_eq!(store.execute(typed_command(&[b"HGET", b"{r05}hash", b"a"])), Reply::Bulk(Some(Bytes::from_static(b"1"))));
        assert_eq!(store.execute(typed_command(&[b"HLEN", b"{r05}hash"])), Reply::Integer(2));
        assert!(matches!(store.execute(typed_command(&[b"PTTL", b"{r05}hash"])), Reply::Integer(ttl) if (404..=413).contains(&ttl)));
        let expected: Vec<_> = reads.iter().cloned().map(|command| store.execute(command)).collect();
        assert_eq!(expected[0], Reply::Bulk(Some(Bytes::from_static(b"alpha"))));
        assert_eq!(store.execute(typed_command(&[b"LLEN", b"{r05}list"])), Reply::Integer(2));
        assert_eq!(store.execute(typed_command(&[b"SCARD", b"{r05}set"])), Reply::Integer(2));
        persist_command(&mut store, &aof, typed_create(3)).await;
        persist_command(&mut store, &aof, typed_update(3, b"new")).await;
        let sorted = store.execute(typed_read(3));
        aof.compact(store.snapshot()).await.unwrap();
        drop(aof);
        writer.await.unwrap().unwrap();
        (expected, sorted)
    });
    clock.elapsed.store(2000, Ordering::SeqCst);
    let mut recovered = persistence::recover(config, StoreConfig::default(), clock).unwrap();
    for (command, expected) in reads.into_iter().zip(expected.0) {
        assert_eq!(recovered.store.execute(command), expected);
    }
    assert_eq!(recovered.store.execute(typed_read(3)), expected.1);
    assert_eq!(
        recovered
            .store
            .execute(typed_command(&[b"HLEN", b"{r05}hash"])),
        Reply::Integer(0)
    );
    assert_eq!(recovered.store.len(), 4);
}

#[test]
fn typed_quota_type_and_record_rejections_preserve_writer_and_dataset() {
    for family in 0..4 {
        let directory = Directory::new();
        let mut config = directory.config();
        config.limits.max_record_bytes = 128;
        runtime().block_on(async {
            let recovered = persistence::recover(
                config,
                StoreConfig {
                    max_dataset_bytes: 1024,
                },
                Arc::new(SystemClock),
            )
            .unwrap();
            let (store, aof, writer) = recovered.start();
            let (stop, shutdown) = tokio::sync::watch::channel(false);
            let (db, worker) =
                sider::storage::worker::channel_with_store(2, TIMEOUT, shutdown, store).unwrap();
            let running = tokio::spawn(worker.with_aof(aof.clone(), 0).run());
            assert!(matches!(
                db.execute(typed_create(family)).await.unwrap(),
                Reply::Integer(_)
            ));
            let before = db.execute(typed_read(family)).await.unwrap();
            assert_eq!(
                db.execute(typed_update(family, &[b'x'; 1000]))
                    .await
                    .unwrap(),
                Reply::Error(sider::command::ExecutionError::OutOfMemory)
            );
            assert_eq!(
                db.execute(typed_update(family, &[b'x'; 200]))
                    .await
                    .unwrap(),
                Reply::Error(sider::command::ExecutionError::AofRecordLimit)
            );
            assert_eq!(
                db.execute(typed_command(&[b"GET", b"{typed}key"]))
                    .await
                    .unwrap(),
                Reply::Error(sider::command::ExecutionError::WrongType)
            );
            assert_eq!(db.execute(typed_read(family)).await.unwrap(), before);
            assert_eq!(
                db.execute(DbCommand::Ping(None)).await.unwrap(),
                Reply::Pong
            );
            assert_eq!(
                aof.status().await.unwrap().0,
                1,
                "rejeições não geram sequência"
            );
            stop.send(true).unwrap();
            running.await.unwrap();
            drop((db, aof));
            writer.await.unwrap().unwrap();
            let mut recovered = recover(&directory);
            assert_eq!(recovered.store.execute(typed_read(family)), before);
        });
    }
}

#[test]
fn typed_compaction_captures_concurrent_postimages_and_passive_tombstones() {
    for family in 0..4 {
        let directory = Directory::new();
        let (reached, signal) = std::sync::mpsc::channel();
        let hook = Arc::new(BlockSnapshot {
            reached,
            released: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        });
        runtime().block_on(async {
            let recovered = persistence::recover_with_faults(
                directory.config(),
                StoreConfig::default(),
                TypedClock::at(1000),
                hook.clone(),
            )
            .unwrap();
            let (mut store, aof, writer) = recovered.start();
            persist_command(&mut store, &aof, typed_create(family)).await;
            let complete = aof.begin_compaction(store.snapshot()).await.unwrap();
            signal.recv_timeout(TIMEOUT).unwrap();
            persist_command(&mut store, &aof, typed_update(family, b"new")).await;
            let expected = store.snapshot();
            hook.release();
            complete.await.unwrap().unwrap();
            drop(aof);
            writer.await.unwrap().unwrap();
            let recovered = persistence::recover(
                directory.config(),
                StoreConfig::default(),
                TypedClock::at(1000),
            )
            .unwrap();
            assert_eq!(recovered.store.snapshot(), expected);
        });
        // Uma leitura persiste o tombstone passivo, inclusive antes de relógio voltar.
        runtime().block_on(async {
            let clock = TypedClock::at(1000);
            let recovered =
                persistence::recover(directory.config(), StoreConfig::default(), clock.clone())
                    .unwrap();
            let (mut store, aof, writer) = recovered.start();
            persist_command(
                &mut store,
                &aof,
                typed_command(&[b"PEXPIRE", b"{typed}key", b"1"]),
            )
            .await;
            aof.compact(store.snapshot()).await.unwrap();
            clock.elapsed.store(1, Ordering::SeqCst);
            let expired = store.prepare(typed_read(family));
            assert_eq!(expired.batch.origin, MutationOrigin::Expiration);
            assert_eq!(
                expired.batch.mutations,
                vec![Mutation::Delete {
                    key: Bytes::from_static(b"{typed}key")
                }]
            );
            aof.append(expired.batch.clone()).await.unwrap();
            store.apply(expired);
            assert_eq!(store.used_bytes(), 0);
            drop(aof);
            writer.await.unwrap().unwrap();
            let expired = persistence::recover(
                directory.config(),
                StoreConfig::default(),
                TypedClock::at(999),
            )
            .unwrap();
            assert!(expired.store.is_empty());
        });
    }
}

#[test]
#[ignore = "helper exclusivo de crashes tipados em processo filho"]
fn typed_aof_process_child() {
    let directory = PathBuf::from(std::env::var_os("SIDER_TEST_AOF_DIR").unwrap());
    let point = std::env::var("SIDER_TEST_AOF_POINT").unwrap();
    let family = std::env::var("SIDER_TEST_TYPED_FAMILY")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let compact = point.starts_with("compact_");
    let recovered = persistence::recover_with_faults(
        AofConfig::new(directory.clone()),
        StoreConfig::default(),
        Arc::new(SystemClock),
        Arc::new(Pause {
            point,
            directory: directory.clone(),
        }),
    )
    .unwrap();
    runtime().block_on(async {
        let (mut store, aof, writer) = recovered.start();
        if compact {
            let complete = aof.begin_compaction(store.snapshot()).await.unwrap();
            persist_command(&mut store, &aof, typed_update(family, b"new")).await;
            fs::write(directory.join("acknowledged"), b"new").unwrap();
            complete.await.unwrap().unwrap();
        } else {
            persist_command(&mut store, &aof, typed_update(family, b"new")).await;
        }
        drop(aof);
        writer.await.unwrap().unwrap();
    });
}

#[test]
fn typed_process_crashes_preserve_complete_values_and_compaction_deltas() {
    for family in 0..4 {
        let mut model = Store::new();
        model.execute(typed_create(family));
        let before = model.execute(typed_read(family));
        model.execute(typed_update(family, b"new"));
        let after = model.execute(typed_read(family));
        for point in [
            "before_append",
            "after_append",
            "before_sync",
            "after_sync",
            "before_reply",
            "compact_before_snapshot",
            "compact_after_snapshot",
            "compact_before_publish",
            "compact_after_publish",
        ] {
            let directory = Directory::new();
            runtime().block_on(async {
                let (mut store, aof, writer) = recover(&directory).start();
                persist_command(&mut store, &aof, typed_create(family)).await;
                drop(aof);
                writer.await.unwrap().unwrap();
            });
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--ignored",
                    "--exact",
                    "typed_aof_process_child",
                    "--nocapture",
                ])
                .env("SIDER_TEST_AOF_DIR", &directory.0)
                .env("SIDER_TEST_AOF_POINT", point)
                .env("SIDER_TEST_TYPED_FAMILY", family.to_string());
            let mut child = process::OwnedChild::spawn(&mut command).unwrap();
            wait_file(&directory.0.join("paused"), &mut child);
            let acknowledged = directory.0.join("acknowledged").exists();
            child.terminate(TIMEOUT).unwrap();
            let mut recovered = recover(&directory);
            let actual = recovered.store.execute(typed_read(family));
            assert!(
                actual == before || actual == after,
                "valor parcial: family={family}, point={point}"
            );
            if point == "before_append" {
                assert_eq!(actual, before);
            }
            if acknowledged || point == "after_sync" || point == "before_reply" {
                assert_eq!(
                    actual, after,
                    "confirmação perdida: family={family}, point={point}"
                );
            }
        }
    }
}
