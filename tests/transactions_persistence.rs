//! The batch produced by the real worker is the unit of append, replay, and crash recovery.
#![forbid(unsafe_code)]

#[path = "common/process.rs"]
mod process;

use bytes::Bytes;
use sider::command::{Command, ExecutionError, Reply};
use sider::persistence::{self, AofConfig, FaultInjector, format};
use sider::storage::{Store, StoreConfig, SystemClock, worker};
use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

const TIMEOUT: Duration = Duration::from_secs(10);
const GENERATION: &str = "generation-00000000000000000000.aof";
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-tx-aof-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn recover(&self) -> persistence::Recovered {
        persistence::recover(
            AofConfig::new(self.0.clone()),
            StoreConfig::default(),
            Arc::new(SystemClock),
        )
        .unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
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
fn commands(value: &'static [u8]) -> Vec<Command> {
    vec![set(b"a", value), set(b"b", value)]
}
fn get(store: &mut Store, key: &'static [u8]) -> Reply {
    store.execute(Command::Get {
        key: Bytes::from_static(key),
    })
}

fn parsed(args: &[&[u8]]) -> Command {
    sider::command::parse(sider::resp::Frame::Array(Some(
        args.iter()
            .map(|arg| sider::resp::Frame::Bulk(Some(Bytes::copy_from_slice(arg))))
            .collect(),
    )))
    .unwrap()
}

#[test]
fn transactions_typed_batch_replay_preserves_each_postimage_and_wrongtype_error() {
    let directory = Directory::new();
    runtime().block_on(async {
        let (store, aof, writer) = directory.recover().start();
        let (stop, shutdown) = tokio::sync::watch::channel(false);
        let (db, owner) = worker::channel_with_store(2, TIMEOUT, shutdown, store).unwrap();
        let running = tokio::spawn(owner.with_aof(aof.clone(), 0).run());
        let batch = vec![
            parsed(&[b"HSET", b"h", b"f", b"\0\xff"]),
            parsed(&[b"GET", b"h"]),
            parsed(&[b"RPUSH", b"l", b"a", b"b"]),
            parsed(&[b"SADD", b"s", b"a"]),
            parsed(&[b"ZADD", b"z", b"1.5", b"a"]),
        ];
        assert_eq!(
            db.execute_batch(batch, vec![]).await.unwrap(),
            Reply::Array(vec![
                Reply::Integer(1),
                Reply::Error(ExecutionError::WrongType),
                Reply::Integer(2),
                Reply::Integer(1),
                Reply::Integer(1)
            ])
        );
        assert_eq!(aof.status().await.unwrap().0, 1);
        db.begin_compaction(&aof)
            .await
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        stop.send_replace(true);
        running.await.unwrap();
        drop((db, aof));
        writer.await.unwrap().unwrap();
    });
    let mut recovered = directory.recover();
    assert_eq!(
        recovered.store.execute(parsed(&[b"HGET", b"h", b"f"])),
        Reply::Bulk(Some(Bytes::from_static(b"\0\xff")))
    );
    assert_eq!(
        recovered
            .store
            .execute(parsed(&[b"LRANGE", b"l", b"0", b"-1"])),
        Reply::Array(vec![
            Reply::Bulk(Some(Bytes::from_static(b"a"))),
            Reply::Bulk(Some(Bytes::from_static(b"b")))
        ])
    );
    assert_eq!(
        recovered.store.execute(parsed(&[b"SISMEMBER", b"s", b"a"])),
        Reply::Integer(1)
    );
    assert_eq!(
        recovered.store.execute(parsed(&[b"ZSCORE", b"z", b"a"])),
        Reply::Bulk(Some(Bytes::from_static(b"1.5")))
    );
}
fn seed(directory: &Directory) {
    runtime().block_on(async {
        let (store, aof, writer) = directory.recover().start();
        let (stop, shutdown) = tokio::sync::watch::channel(false);
        let (db, owner) = worker::channel_with_store(4, TIMEOUT, shutdown, store).unwrap();
        let running = tokio::spawn(owner.with_aof(aof.clone(), 0).run());
        assert_eq!(
            db.execute_batch(commands(b"old"), vec![]).await.unwrap(),
            Reply::Array(vec![Reply::Ok, Reply::Ok])
        );
        assert_eq!(aof.status().await.unwrap().0, 1);
        stop.send_replace(true);
        running.await.unwrap();
        drop((db, aof));
        writer.await.unwrap().unwrap();
    });
}

#[test]
fn transactions_one_append_runtime_error_and_watch_abort_survive_compaction() {
    let directory = Directory::new();
    runtime().block_on(async {
        let (store, aof, writer) = directory.recover().start();
        let watched = store.watch(vec![Bytes::from_static(b"a")]);
        let (stop, shutdown) = tokio::sync::watch::channel(false);
        let (db, owner) = worker::channel_with_store(4, TIMEOUT, shutdown, store).unwrap();
        let running = tokio::spawn(owner.with_aof(aof.clone(), 0).run());
        let replies = db
            .execute_batch(
                vec![
                    set(b"a", b"bad"),
                    Command::Incr {
                        key: Bytes::from_static(b"a"),
                    },
                    set(b"b", b"\0\xff"),
                ],
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(
            replies,
            Reply::Array(vec![
                Reply::Ok,
                Reply::Error(ExecutionError::InvalidInteger),
                Reply::Ok
            ])
        );
        assert_eq!(
            aof.status().await.unwrap().0,
            1,
            "EXEC produces one append, even with an individual error"
        );
        assert_eq!(
            db.execute_batch(commands(b"blocked"), watched)
                .await
                .unwrap(),
            Reply::NullArray
        );
        assert_eq!(
            aof.status().await.unwrap().0,
            1,
            "An aborted WATCH produces no record"
        );
        db.begin_compaction(&aof)
            .await
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        stop.send_replace(true);
        running.await.unwrap();
        drop((db, aof));
        writer.await.unwrap().unwrap();
    });
    let mut recovered = directory.recover();
    assert_eq!(
        get(&mut recovered.store, b"a"),
        Reply::Bulk(Some(Bytes::from_static(b"bad")))
    );
    assert_eq!(
        get(&mut recovered.store, b"b"),
        Reply::Bulk(Some(Bytes::from_static(b"\0\xff")))
    );
}

#[test]
fn transactions_every_truncated_prefix_recovers_all_or_none_of_real_batch() {
    let source = Directory::new();
    seed(&source);
    let bytes = fs::read(source.0.join(GENERATION)).unwrap();
    let mut input = io::Cursor::new(&bytes);
    format::read_header(&mut input).unwrap();
    assert!(matches!(
        format::read_record(&mut input, format::Limits::default()).unwrap(),
        format::Next::Record(format::Record::Seal {
            sequence: 0,
            entries: 0,
            ..
        })
    ));
    let batch_start = input.position() as usize;
    for end in batch_start..=bytes.len() {
        let directory = Directory::new();
        fs::write(directory.0.join(GENERATION), &bytes[..end]).unwrap();
        let mut recovered = directory.recover();
        let a = get(&mut recovered.store, b"a");
        let b = get(&mut recovered.store, b"b");
        assert_eq!(a, b, "prefix {end} of {}", bytes.len());
        assert_eq!(
            a,
            if end == bytes.len() {
                Reply::Bulk(Some(Bytes::from_static(b"old")))
            } else {
                Reply::Bulk(None)
            }
        );
    }
    eprintln!(
        "transactions: {} prefixes of a real batch",
        bytes.len() - batch_start + 1
    );
}

struct Pause {
    point: String,
    directory: PathBuf,
}
impl FaultInjector for Pause {
    fn hit(&self, point: &'static str) -> io::Result<()> {
        if point == self.point {
            let mut signal = fs::OpenOptions::new()
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
fn transactions_crash_child() {
    let directory = PathBuf::from(std::env::var_os("SIDER_TEST_TX_AOF_DIR").unwrap());
    let point = std::env::var("SIDER_TEST_TX_AOF_POINT").unwrap();
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
        let (store, aof, writer) = recovered.start();
        let (_stop, shutdown) = tokio::sync::watch::channel(false);
        let (db, owner) = worker::channel_with_store(4, TIMEOUT, shutdown, store).unwrap();
        let running = tokio::spawn(owner.with_aof(aof.clone(), 0).run());
        assert_eq!(
            db.execute_batch(commands(b"new"), vec![]).await.unwrap(),
            Reply::Array(vec![Reply::Ok, Reply::Ok])
        );
        fs::write(directory.join("acknowledged"), b"new").unwrap();
        if compact {
            db.begin_compaction(&aof)
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap();
        }
        drop((db, aof));
        running.await.unwrap();
        writer.await.unwrap().unwrap();
    });
}

#[test]
fn transactions_process_crashes_never_replay_half_an_exec() {
    let points = [
        "before_append",
        "after_append",
        "before_sync",
        "after_sync",
        "before_reply",
        "compact_before_snapshot",
        "compact_after_snapshot",
        "compact_before_publish",
        "compact_after_publish",
    ];
    for point in points {
        let directory = Directory::new();
        seed(&directory);
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "transactions_crash_child",
                "--nocapture",
            ])
            .env("SIDER_TEST_TX_AOF_DIR", &directory.0)
            .env("SIDER_TEST_TX_AOF_POINT", point);
        let mut child = process::OwnedChild::spawn(&mut command).unwrap();
        let deadline = Instant::now() + TIMEOUT;
        while !directory.0.join("paused").is_file() {
            child.assert_alive().unwrap();
            assert!(Instant::now() < deadline, "point not reached: {point}");
            std::thread::sleep(Duration::from_millis(5));
        }
        let acknowledged = directory.0.join("acknowledged").is_file();
        child.terminate(TIMEOUT).unwrap();
        let mut recovered = directory.recover();
        let a = get(&mut recovered.store, b"a");
        assert_eq!(
            a,
            get(&mut recovered.store, b"b"),
            "partial EXEC recovered at {point}"
        );
        assert!(
            matches!(&a, Reply::Bulk(Some(value)) if value.as_ref() == b"old" || value.as_ref() == b"new")
        );
        if point == "before_append" {
            assert_eq!(a, Reply::Bulk(Some(Bytes::from_static(b"old"))));
        }
        if acknowledged || point == "after_sync" || point == "before_reply" {
            assert_eq!(a, Reply::Bulk(Some(Bytes::from_static(b"new"))));
        }
    }
    eprintln!(
        "transactions: {} process crashes; no partial batch",
        points.len()
    );
}
