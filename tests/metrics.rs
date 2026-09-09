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

async fn tcp_command(client: &mut tokio::net::TcpStream, args: &[&[u8]]) -> sider::resp::Frame {
    use sider::resp::{Decoder, Frame, RespLimits, encode};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut output = bytes::BytesMut::new();
    encode(
        &Frame::Array(Some(
            args.iter()
                .map(|arg| Frame::Bulk(Some(Bytes::copy_from_slice(arg))))
                .collect(),
        )),
        &mut output,
        RespLimits::default(),
    )
    .unwrap();
    client.write_all(&output).await.unwrap();
    let mut input = bytes::BytesMut::new();
    let mut decoder = Decoder::new(RespLimits::default()).unwrap();
    loop {
        if let Some(frame) = decoder.decode(&mut input).unwrap() {
            return frame;
        }
        assert_ne!(
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                client.read_buf(&mut input)
            )
            .await
            .unwrap()
            .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn metrics_tcp_reports_admission_effective_configuration_and_private_dataset() {
    use sider::resp::Frame;
    use tokio::{
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
        sync::oneshot,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let config = sider::ServerConfig {
        bind_addr: address,
        max_connections: 1,
        shards: 4,
        ..sider::ServerConfig::default()
    };
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(sider::server::serve(listener, config, async {
        let _ = stopped.await;
    }));
    let mut client = TcpStream::connect(address).await.unwrap();
    assert_eq!(
        tcp_command(&mut client, &[b"PING"]).await,
        Frame::Simple(Bytes::from_static(b"PONG"))
    );
    let mut excess = TcpStream::connect(address).await.unwrap();
    let closed = tokio::time::timeout(std::time::Duration::from_secs(5), excess.read(&mut [0; 1]))
        .await
        .unwrap();
    assert!(
        matches!(closed, Ok(0))
            || matches!(closed, Err(ref error) if error.kind() == io::ErrorKind::ConnectionReset)
    );
    assert_eq!(
        tcp_command(&mut client, &[b"SET", b"secret-key", b"secret-value"]).await,
        Frame::Simple(Bytes::from_static(b"OK"))
    );
    let Frame::Bulk(Some(info)) = tcp_command(&mut client, &[b"INFO"]).await else {
        panic!()
    };
    let text = std::str::from_utf8(&info).unwrap();
    let fields: std::collections::BTreeMap<_, _> = text
        .lines()
        .filter_map(|line| line.split_once(':'))
        .collect();
    assert_eq!(fields["tcp_port"], address.port().to_string());
    assert_eq!(fields["connected_clients"], "1");
    assert_eq!(fields["total_connections_received"], "1");
    assert_eq!(fields["rejected_connections"], "1");
    assert_eq!(fields["commands_received_total"], "3");
    assert_eq!(fields["worker_requests_accepted_total"], "2");
    assert_eq!(fields["worker_queue_capacity"], "128");
    assert_eq!(fields["worker_queue_capacity_per_shard"], "32");
    assert_eq!(
        fields.len(),
        text.lines().filter(|line| line.contains(':')).count(),
        "cada indicador precisa ter nome único mesmo em INFO all"
    );
    assert_eq!(fields["shards"], "4");
    assert_eq!(fields["dataset_keys"], "1");
    assert_eq!(fields["aof_enabled"], "0");
    assert!(!text.contains("secret"));
    let Frame::Bulk(Some(filtered)) =
        tcp_command(&mut client, &[b"INFO", b"memory", b"unknown-secret"]).await
    else {
        panic!()
    };
    assert!(filtered.starts_with(b"# Memory\r\n"));
    assert!(!filtered.windows(8).any(|bytes| bytes == b"# Server"));
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
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

#[tokio::test]
async fn metrics_tcp_info_in_exec_reads_real_aof_without_appending_a_record() {
    use sider::resp::Frame;
    let directory = Directory::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut aof = directory.config();
    aof.layout.shard_count = 4;
    let config = sider::ServerConfig {
        bind_addr: address,
        shards: 4,
        aof: Some(aof),
        ..sider::ServerConfig::default()
    };
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(sider::server::serve(listener, config, async {
        let _ = stopped.await;
    }));
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    assert_eq!(
        tcp_command(&mut client, &[b"MULTI"]).await,
        Frame::Simple(Bytes::from_static(b"OK"))
    );
    assert_eq!(
        tcp_command(&mut client, &[b"INFO", b"persistence"]).await,
        Frame::Simple(Bytes::from_static(b"QUEUED"))
    );
    let Frame::Array(Some(replies)) = tcp_command(&mut client, &[b"EXEC"]).await else {
        panic!()
    };
    let Frame::Bulk(Some(info)) = &replies[0] else {
        panic!()
    };
    let text = std::str::from_utf8(info).unwrap();
    assert!(text.contains("aof_enabled:1\r\n"));
    assert!(text.contains("aof_running:1\r\n"));
    assert!(text.contains("aof_records_written_total:0\r\n"));
    tcp_command(&mut client, &[b"SET", b"key", b"value"]).await;
    let Frame::Bulk(Some(info)) = tcp_command(&mut client, &[b"INFO", b"persistence"]).await else {
        panic!()
    };
    let text = std::str::from_utf8(&info).unwrap();
    assert!(text.contains("aof_written_sequence:1\r\n"));
    assert!(text.contains("aof_synced_sequence:1\r\n"));
    assert!(text.contains("aof_records_written_total:1\r\n"));
    assert!(
        text.contains("aof_queue_capacity:32\r\n"),
        "o escritor é global, sem multiplicar a capacidade por shard"
    );
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

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
