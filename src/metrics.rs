//! Indicadores por instância: nomes fixos, observações curtas e nenhum dado do cliente.

use std::{
    fmt::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use bytes::Bytes;

use crate::{
    ServerConfig,
    command::InfoSections,
    persistence::{AofDiagnosticsHandle, SyncPolicy},
    resp::Frame,
};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy)]
#[repr(usize)]
pub(crate) enum Counter {
    Connections,
    RejectedConnections,
    Requests,
    ErrorReplies,
    ProtocolErrors,
    ConnectionFailures,
    WriteTimeouts,
    ResponseLimitFailures,
    WorkerAccepted,
    WorkerTimeouts,
    WorkerFailures,
    PubSubEvictions,
    PubSubDeliveries,
    ExpirationBatches,
    ExpirationKeys,
}
const COUNTERS: usize = 15;

#[derive(Clone, Copy, Default)]
pub(crate) struct DatasetStats {
    pub keys: usize,
    pub expiring: usize,
    pub used_bytes: usize,
    pub quota: usize,
}

#[derive(Clone)]
pub(crate) struct Metrics(Arc<Inner>);
struct Inner {
    began: Instant,
    counters: [AtomicU64; COUNTERS],
    connections: AtomicU64,
    pubsub_channels: AtomicU64,
    pubsub_subscribers: AtomicU64,
    pubsub_subscriptions: AtomicU64,
    datasets: Vec<Mutex<DatasetStats>>,
    queues: Vec<QueueStats>,
    config: Mutex<Option<ServerConfig>>,
    bound_port: AtomicU64,
    aof: Mutex<Option<AofDiagnosticsHandle>>,
}
struct QueueStats {
    used: AtomicUsize,
    capacity: usize,
}
impl Default for Metrics {
    fn default() -> Self {
        Self::new(0, 0)
    }
}
impl Metrics {
    pub(crate) fn new(shards: usize, capacity: usize) -> Self {
        Self(Arc::new(Inner {
            began: Instant::now(),
            counters: std::array::from_fn(|_| AtomicU64::new(0)),
            connections: AtomicU64::new(0),
            pubsub_channels: AtomicU64::new(0),
            pubsub_subscribers: AtomicU64::new(0),
            pubsub_subscriptions: AtomicU64::new(0),
            datasets: (0..shards)
                .map(|_| Mutex::new(DatasetStats::default()))
                .collect(),
            queues: (0..shards)
                .map(|_| QueueStats {
                    used: AtomicUsize::new(0),
                    capacity,
                })
                .collect(),
            config: Mutex::new(None),
            bound_port: AtomicU64::new(0),
            aof: Mutex::new(None),
        }))
    }
    pub(crate) fn add(&self, counter: Counter, count: u64) {
        let _ = self.0.counters[counter as usize].fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |value| Some(value.saturating_add(count)),
        );
    }
    fn count(&self, counter: Counter) -> u64 {
        self.0.counters[counter as usize].load(Ordering::Relaxed)
    }
    pub(crate) fn connection(&self) -> ConnectionGuard {
        self.add(Counter::Connections, 1);
        self.0.connections.fetch_add(1, Ordering::Relaxed);
        ConnectionGuard(self.clone())
    }
    pub(crate) fn configure(&self, config: &ServerConfig, port: u16) {
        *self
            .0
            .config
            .lock()
            .expect("configuração de métricas envenenada") = Some(config.clone());
        self.0.bound_port.store(u64::from(port), Ordering::Relaxed);
    }
    pub(crate) fn dataset(&self, shard: usize, value: DatasetStats) {
        *self.0.datasets[shard]
            .lock()
            .expect("métricas de dataset envenenadas") = value;
    }
    pub(crate) fn queue(&self, shard: usize, used: usize, capacity: usize) {
        debug_assert_eq!(self.0.queues[shard].capacity, capacity);
        self.0.queues[shard].used.store(used, Ordering::Relaxed);
    }
    pub(crate) fn aof(&self, source: AofDiagnosticsHandle) {
        *self.0.aof.lock().expect("observador AOF envenenado") = Some(source);
    }
    pub(crate) fn pubsub(&self, channels: usize, subscribers: usize, subscriptions: usize) {
        self.0
            .pubsub_channels
            .store(channels as u64, Ordering::Relaxed);
        self.0
            .pubsub_subscribers
            .store(subscribers as u64, Ordering::Relaxed);
        self.0
            .pubsub_subscriptions
            .store(subscriptions as u64, Ordering::Relaxed);
    }
    pub(crate) fn response(&self, frame: &Frame) {
        self.add(Counter::ErrorReplies, errors(frame));
    }

    pub(crate) fn render(&self, sections: InfoSections) -> Bytes {
        let mut text = String::new();
        macro_rules! number {
            ($name:literal, $value:expr) => {
                writeln!(text, concat!($name, ":{}\r"), $value).unwrap();
            };
        }
        if sections.contains(InfoSections::SERVER) {
            text.push_str("# Server\r\n");
            number!("sider_version", env!("CARGO_PKG_VERSION"));
            number!("metrics_schema_version", 1);
            number!("uptime_seconds", self.0.began.elapsed().as_secs());
            number!("tcp_port", self.0.bound_port.load(Ordering::Relaxed));
        }
        if sections.contains(InfoSections::CLIENTS) {
            text.push_str("# Clients\r\n");
            number!(
                "connected_clients",
                self.0.connections.load(Ordering::Relaxed)
            );
            number!(
                "total_connections_received",
                self.count(Counter::Connections)
            );
            number!(
                "rejected_connections",
                self.count(Counter::RejectedConnections)
            );
            number!(
                "pubsub_channels",
                self.0.pubsub_channels.load(Ordering::Relaxed)
            );
            number!(
                "pubsub_subscribers",
                self.0.pubsub_subscribers.load(Ordering::Relaxed)
            );
            number!(
                "pubsub_subscriptions",
                self.0.pubsub_subscriptions.load(Ordering::Relaxed)
            );
        }
        if sections.contains(InfoSections::STATS) {
            text.push_str("# Stats\r\n");
            number!("commands_received_total", self.count(Counter::Requests));
            number!(
                "command_error_replies_total",
                self.count(Counter::ErrorReplies)
            );
            number!("protocol_errors_total", self.count(Counter::ProtocolErrors));
            number!(
                "connection_failures_total",
                self.count(Counter::ConnectionFailures)
            );
            number!(
                "worker_requests_accepted_total",
                self.count(Counter::WorkerAccepted)
            );
            number!(
                "client_write_timeouts_total",
                self.count(Counter::WriteTimeouts)
            );
            number!(
                "response_encoding_failures_total",
                self.count(Counter::ResponseLimitFailures)
            );
            number!("worker_timeouts_total", self.count(Counter::WorkerTimeouts));
            number!("worker_failures_total", self.count(Counter::WorkerFailures));
            number!(
                "pubsub_evictions_total",
                self.count(Counter::PubSubEvictions)
            );
            number!(
                "pubsub_deliveries_total",
                self.count(Counter::PubSubDeliveries)
            );
            let queues = &self.0.queues;
            number!(
                "worker_queue_used",
                queues
                    .iter()
                    .map(|state| state.used.load(Ordering::Relaxed) as u64)
                    .fold(0u64, u64::saturating_add)
            );
            number!(
                "worker_queue_capacity",
                queues
                    .iter()
                    .map(|state| state.capacity as u64)
                    .fold(0u64, u64::saturating_add)
            );
        }
        if sections.contains(InfoSections::MEMORY) {
            text.push_str("# Memory\r\n");
            let datasets: Vec<_> = self
                .0
                .datasets
                .iter()
                .map(|state| *state.lock().expect("métricas de dataset envenenadas"))
                .collect();
            number!(
                "dataset_keys",
                datasets
                    .iter()
                    .map(|state| state.keys as u64)
                    .fold(0u64, u64::saturating_add)
            );
            number!(
                "dataset_expiring_keys",
                datasets
                    .iter()
                    .map(|state| state.expiring as u64)
                    .fold(0u64, u64::saturating_add)
            );
            number!(
                "dataset_logical_bytes",
                datasets
                    .iter()
                    .map(|state| state.used_bytes as u64)
                    .fold(0u64, u64::saturating_add)
            );
            number!(
                "dataset_quota_bytes",
                datasets
                    .iter()
                    .map(|state| state.quota as u64)
                    .fold(0u64, u64::saturating_add)
            );
            number!(
                "expiration_batches_total",
                self.count(Counter::ExpirationBatches)
            );
            number!(
                "expiration_batch_keys_removed_total",
                self.count(Counter::ExpirationKeys)
            );
        }
        if sections.contains(InfoSections::PERSISTENCE) {
            text.push_str("# Persistence\r\n");
            let source = self.0.aof.lock().expect("observador AOF envenenado");
            number!("aof_enabled", u8::from(source.is_some()));
            if let Some(source) = source.as_ref() {
                let state = source.snapshot();
                number!("aof_running", u8::from(state.running));
                number!("aof_failed", u8::from(state.failed));
                number!("aof_written_sequence", state.written_sequence);
                number!("aof_synced_sequence", state.synced_sequence);
                number!("aof_generation", state.generation);
                number!("aof_bytes_since_compaction", state.bytes_since_compaction);
                number!("aof_dirty", u8::from(state.dirty));
                number!("aof_compacting", u8::from(state.compacting));
                number!("aof_queue_used", state.queue_depth);
                number!("aof_queue_capacity", state.queue_capacity);
                number!("aof_records_written_total", state.records_written_total);
                number!("aof_active_file_syncs_total", state.syncs_total);
                number!("aof_fatal_failures_total", state.fatal_failures_total);
                number!("aof_record_rejections_total", state.record_rejections_total);
                number!("aof_compactions_total", state.compactions_total);
                number!(
                    "aof_compaction_failures_total",
                    state.compaction_failures_total
                );
                number!("aof_last_error", state.last_error.unwrap_or("none"));
            }
        }
        if sections.contains(InfoSections::CONFIG)
            && let Some(config) = self
                .0
                .config
                .lock()
                .expect("configuração de métricas envenenada")
                .as_ref()
        {
            text.push_str(&configuration(config));
        }
        Bytes::from(text)
    }
}

pub(crate) struct ConnectionGuard(Metrics);
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.0.connections.fetch_sub(1, Ordering::Relaxed);
    }
}

fn errors(frame: &Frame) -> u64 {
    match frame {
        Frame::Error(_) => 1,
        Frame::Array(Some(values)) => values.iter().map(errors).fold(0u64, u64::saturating_add),
        _ => 0,
    }
}

pub(crate) fn configuration(config: &ServerConfig) -> String {
    let mut text = String::from("# Config\r\n");
    macro_rules! field {
        ($name:ident) => {
            writeln!(text, concat!(stringify!($name), ":{}\r"), config.$name).unwrap();
        };
    }
    macro_rules! resp {
        ($name:ident) => {
            writeln!(
                text,
                concat!(stringify!($name), ":{}\r"),
                config.resp_limits.$name
            )
            .unwrap();
        };
    }
    resp!(max_frame_bytes);
    resp!(max_bulk_bytes);
    resp!(max_line_bytes);
    resp!(max_nodes);
    resp!(max_depth);
    field!(shards);
    field!(max_connections);
    writeln!(
        text,
        "worker_queue_capacity_per_shard:{}\r",
        config.worker_queue_capacity
    )
    .unwrap();
    field!(pubsub_max_channels);
    field!(pubsub_queue_capacity);
    field!(transaction_max_commands);
    field!(transaction_max_bytes);
    field!(watch_max_keys);
    field!(max_input_buffer_bytes);
    field!(max_response_bytes);
    field!(max_dataset_bytes);
    for (name, value) in [
        ("frame_timeout_ms", config.frame_timeout),
        ("request_timeout_ms", config.request_timeout),
        ("write_timeout_ms", config.write_timeout),
        ("shutdown_timeout_ms", config.shutdown_timeout),
    ] {
        writeln!(text, "{name}:{}\r", value.as_millis()).unwrap();
    }
    writeln!(
        text,
        "ready_file_enabled:{}\r",
        u8::from(config.ready_file.is_some())
    )
    .unwrap();
    writeln!(text, "aof_configured:{}\r", u8::from(config.aof.is_some())).unwrap();
    if let Some(aof) = &config.aof {
        writeln!(
            text,
            "aof_queue_capacity_configured:{}\r\naof_max_mutations:{}\r",
            aof.queue_capacity, aof.limits.max_mutations
        )
        .unwrap();
        match aof.sync {
            SyncPolicy::Always => text.push_str("aof_sync_policy:always\r\n"),
            SyncPolicy::Periodic(period) => {
                text.push_str("aof_sync_policy:periodic\r\n");
                writeln!(text, "aof_sync_period_ms:{}\r", period.as_millis()).unwrap();
            }
        }
        writeln!(
            text,
            "aof_max_record_bytes:{}\r\naof_max_delta_bytes:{}\r\naof_compact_after_bytes:{}\r",
            aof.limits.max_record_bytes, aof.max_delta_bytes, aof.compact_after_bytes
        )
        .unwrap();
    }
    text
}
