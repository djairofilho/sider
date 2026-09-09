#![forbid(unsafe_code)]

#[path = "common/gate_receipt.rs"]
mod gate_receipt;
#[path = "common/process.rs"]
mod process;
#[path = "common/wire.rs"]
mod wire;

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use bytes::Bytes;
use sider::command::{Command, ExecutionError, Reply};
use sider::persistence::{self, AofConfig, AofHandle, FaultInjector, NoFaults};
use sider::storage::worker::{self, DbHandle};
use sider::storage::{Store, StoreConfig, SystemClock};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(10);
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-shards-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn config(&self) -> AofConfig {
        let mut config = AofConfig::new(self.0.clone());
        config.layout.shard_count = 4;
        config
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Rig {
    database: DbHandle,
    stop: watch::Sender<bool>,
    workers: JoinSet<()>,
    aof: AofHandle,
    writer: JoinHandle<Result<(), persistence::AofError>>,
}
impl Rig {
    fn start(directory: &Directory, count: usize, faults: Arc<dyn FaultInjector>) -> Self {
        Self::start_with_local_threshold(directory, count, faults, 0)
    }
    fn start_with_local_threshold(
        directory: &Directory,
        count: usize,
        faults: Arc<dyn FaultInjector>,
        threshold: u64,
    ) -> Self {
        assert_eq!(directory.config().layout.shard_count as usize, count);
        let recovered = persistence::recover_with_faults(
            directory.config(),
            StoreConfig::default(),
            Arc::new(SystemClock),
            faults,
        )
        .unwrap();
        assert!(recovered.store.is_empty());
        let (_, aof, writer) = recovered.start();
        let (stop, shutdown) = watch::channel(false);
        let (database, workers) = worker::channel_with_stores(
            8,
            TIMEOUT,
            shutdown,
            (0..count).map(|_| Store::new()).collect(),
        )
        .unwrap();
        let mut running = JoinSet::new();
        for worker in workers {
            running.spawn(worker.with_aof(aof.clone(), threshold).run());
        }
        Self {
            database,
            stop,
            workers: running,
            aof,
            writer,
        }
    }
    async fn finish(mut self) {
        self.stop.send_replace(true);
        drop(self.database);
        while let Some(result) = self.workers.join_next().await {
            result.unwrap();
        }
        drop(self.aof);
        timeout(TIMEOUT, self.writer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

fn mset(tag: usize, value: usize) -> Command {
    let value = Bytes::from(value.to_string());
    Command::MSet {
        entries: vec![
            (Bytes::from(format!("{{{tag}}}:a")), value.clone()),
            (Bytes::from(format!("{{{tag}}}:b")), value),
        ],
    }
}

#[tokio::test]
async fn local_compaction_is_disabled_for_multi_shard_public_api() {
    let directory = Directory::new();
    let rig = Rig::start_with_local_threshold(&directory, 4, Arc::new(NoFaults), 1);
    for tag in 0..4 {
        assert_eq!(rig.database.execute(mset(tag, 7)).await.unwrap(), Reply::Ok);
    }
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(200)).await;
    tokio::time::resume();
    // Cada GET passa pelo worker depois do tick prioritário, sem espera arbitrária.
    for tag in 0..4 {
        assert_eq!(
            rig.database
                .execute(Command::Get {
                    key: Bytes::from(format!("{{{tag}}}:a"))
                })
                .await
                .unwrap(),
            Reply::Bulk(Some(Bytes::from_static(b"7")))
        );
    }
    rig.finish().await;
    assert!(
        !directory
            .0
            .join("generation-00000000000000000001.aof")
            .exists()
    );
    let recovered = persistence::recover(
        directory.config(),
        StoreConfig::default(),
        Arc::new(SystemClock),
    )
    .unwrap();
    assert_eq!(recovered.store.len(), 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_compaction_preserves_concurrent_shard_writes_and_sequence() {
    let directory = Directory::new();
    let rig = Rig::start(&directory, 4, Arc::new(NoFaults));
    let mut writes = JoinSet::new();
    for tag in 0..4 {
        let database = rig.database.clone();
        writes.spawn(async move {
            for value in 0..40 {
                assert_eq!(database.execute(mset(tag, value)).await.unwrap(), Reply::Ok);
            }
        });
    }
    for _ in 0..3 {
        let completion = rig.database.begin_compaction(&rig.aof).await.unwrap();
        timeout(TIMEOUT, completion)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    while let Some(result) = writes.join_next().await {
        result.unwrap();
    }
    let expected = rig.database.snapshot(Some(&rig.aof)).await.unwrap();
    assert_eq!(expected.sequence, Some(160));
    assert_eq!(expected.mutations.len(), 8);
    rig.database
        .begin_compaction(&rig.aof)
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    rig.finish().await;
    let recovered = persistence::recover(
        directory.config(),
        StoreConfig::default(),
        Arc::new(SystemClock),
    )
    .unwrap();
    assert_eq!(recovered.store.snapshot(), expected.mutations);
    let (_, handle, writer) = recovered.start();
    assert_eq!(handle.flush().await.unwrap(), 160);
    drop(handle);
    writer.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_shard_rejection_does_not_append_or_change_any_store() {
    let directory = Directory::new();
    let rig = Rig::start(&directory, 4, Arc::new(NoFaults));
    let router = sider::storage::routing::ShardRouter::new(4).unwrap();
    let left = Bytes::from_static(b"left");
    let right = (0..100)
        .map(|n| Bytes::from(format!("right-{n}")))
        .find(|key| router.shard_for(key) != router.shard_for(&left))
        .unwrap();
    assert_eq!(
        rig.database
            .execute(Command::MSet {
                entries: vec![
                    (left.clone(), Bytes::from_static(b"l")),
                    (right.clone(), Bytes::from_static(b"r")),
                ]
            })
            .await
            .unwrap(),
        Reply::Error(ExecutionError::CrossShard)
    );
    assert_eq!(
        rig.database
            .execute(Command::Del {
                keys: vec![left, right]
            })
            .await
            .unwrap(),
        Reply::Error(ExecutionError::CrossShard)
    );
    let snapshot = rig.database.snapshot(Some(&rig.aof)).await.unwrap();
    assert_eq!(snapshot.sequence, Some(0));
    assert!(snapshot.mutations.is_empty());
    rig.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_restarts_all_shards_and_rejects_incompatible_layout_before_bind() {
    use std::io::Write;
    let directory = Directory::new();
    let config = sider::ServerConfig {
        shards: 4,
        aof: Some(directory.config()),
        ..sider::ServerConfig::default()
    };
    for restart in [false, true] {
        let prepared = sider::server::prepare(&config).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let running = tokio::spawn(sider::server::serve_prepared(
            listener,
            config.clone(),
            async {
                let _ = shutdown.await;
            },
            prepared,
        ));
        let mut stream = std::net::TcpStream::connect_timeout(&address, TIMEOUT).unwrap();
        stream.set_read_timeout(Some(TIMEOUT)).unwrap();
        stream.set_write_timeout(Some(TIMEOUT)).unwrap();
        for tag in 0..4 {
            let left = format!("{{{tag}}}:a").into_bytes();
            let right = format!("{{{tag}}}:b").into_bytes();
            if !restart {
                stream
                    .write_all(&wire::request(&[
                        b"MSET".to_vec(),
                        left.clone(),
                        b"v".to_vec(),
                        right.clone(),
                        b"v".to_vec(),
                    ]))
                    .unwrap();
                assert_eq!(
                    wire::read_response(&mut stream).unwrap().value,
                    wire::Response::Simple(b"OK".to_vec())
                );
            }
            stream
                .write_all(&wire::request(&[b"MGET".to_vec(), left, right]))
                .unwrap();
            assert_eq!(
                wire::read_response(&mut stream).unwrap().value,
                wire::Response::Array(Some(vec![
                    wire::Response::Bulk(Some(b"v".to_vec())),
                    wire::Response::Bulk(Some(b"v".to_vec()))
                ]))
            );
        }
        drop(stream);
        stop.send(()).unwrap();
        timeout(TIMEOUT, running).await.unwrap().unwrap().unwrap();
    }
    let path = directory.0.join("generation-00000000000000000000.aof");
    let before = fs::read(&path).unwrap();
    let mut incompatible = config.clone();
    incompatible.shards = 1;
    incompatible.aof.as_mut().unwrap().layout.shard_count = 1;
    assert!(matches!(
        sider::server::prepare(&incompatible).await,
        Err(sider::server::ServerError::Persistence(
            persistence::AofError::LayoutMismatch { .. }
        ))
    ));
    assert_eq!(fs::read(path).unwrap(), before);

    let prepared = sider::server::prepare(&config).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut changed = config;
    changed.max_connections += 1;
    assert!(matches!(
        sider::server::serve_prepared(listener, changed, std::future::pending(), prepared).await,
        Err(sider::server::ServerError::Config(_))
    ));
}

struct BeforeReply {
    once: AtomicBool,
    reached: std::sync::mpsc::Sender<()>,
    released: (Mutex<bool>, Condvar),
}
impl BeforeReply {
    fn release(&self) {
        *self.released.0.lock().unwrap() = true;
        self.released.1.notify_all();
    }
}
impl FaultInjector for BeforeReply {
    fn hit(&self, point: &'static str) -> io::Result<()> {
        if point == "before_reply" && !self.once.swap(true, Ordering::SeqCst) {
            self.reached.send(()).unwrap();
            let (released, _) = self
                .released
                .1
                .wait_timeout_while(self.released.0.lock().unwrap(), TIMEOUT, |released| {
                    !*released
                })
                .unwrap();
            if !*released {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "barreira de teste não liberada",
                ));
            }
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_waits_for_durable_ack_and_apply_even_after_caller_cancellation() {
    let directory = Directory::new();
    let (reached, signal) = std::sync::mpsc::channel();
    let hook = Arc::new(BeforeReply {
        once: AtomicBool::new(false),
        reached,
        released: (Mutex::new(false), Condvar::new()),
    });
    let rig = Rig::start(&directory, 4, hook.clone());
    let database = rig.database.clone();
    let command = tokio::spawn(async move { database.execute(mset(0, 7)).await });
    signal.recv_timeout(TIMEOUT).unwrap();
    command.abort();
    assert!(command.await.unwrap_err().is_cancelled());
    {
        let snapshot = rig.database.snapshot(Some(&rig.aof));
        tokio::pin!(snapshot);
        std::future::poll_fn(|cx| {
            assert!(snapshot.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        hook.release();
        let snapshot = snapshot.await.unwrap();
        assert_eq!(snapshot.sequence, Some(1));
        assert_eq!(snapshot.mutations.len(), 2);
    }
    rig.finish().await;
}

#[test]
#[ignore = "gate de release exige contexto exato; cenários internos executam diretamente"]
fn release_sharding_gate() {
    let context = gate_receipt::GateContext::from_env("sharding").unwrap();
    let began = std::time::Instant::now();
    global_compaction_preserves_concurrent_shard_writes_and_sequence();
    cross_shard_rejection_does_not_append_or_change_any_store();
    snapshot_waits_for_durable_ack_and_apply_even_after_caller_cancellation();
    server_restarts_all_shards_and_rejects_incompatible_layout_before_bind();
    context.publish(4, began.elapsed(), serde_json::json!({
        "scenarios": ["concurrent_shard_compaction_sequence", "cross_shard_no_append", "snapshot_waits_ack_apply_after_cancel"],
        "shards":4,"durable_batches":160,"metrics":"benchmark exploratório separado; não concorrer com builds"
    })).unwrap();
}
