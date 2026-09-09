use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc as sync_mpsc};
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};

use super::DurableLayout;
use super::format::{self, FormatError, Limits, Next, Record};
use crate::ConfigError;
use crate::storage::{Clock, Mutation, ReplayError, ResolvedBatch, Store, StoreConfig};

static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(0);
fn temporary_path(directory: &Path, prefix: &str) -> PathBuf {
    directory.join(format!(
        "{prefix}-{}-{}.tmp",
        std::process::id(),
        NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed)
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncPolicy {
    /// A resposta aguarda sync_all do lote.
    Always,
    /// A resposta confirma write_all; a janela inclui o período e atrasos de I/O/agendamento.
    Periodic(Duration),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AofConfig {
    pub directory: PathBuf,
    pub layout: DurableLayout,
    pub sync: SyncPolicy,
    pub queue_capacity: usize,
    pub limits: Limits,
    /// Zero desabilita compactação automática; a API explícita continua disponível.
    pub compact_after_bytes: u64,
    pub max_delta_bytes: usize,
}
impl AofConfig {
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            layout: DurableLayout::default(),
            sync: SyncPolicy::Always,
            queue_capacity: 32,
            limits: Limits::default(),
            compact_after_bytes: 64 * 1024 * 1024,
            max_delta_bytes: 16 * 1024 * 1024,
        }
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.layout.validate()?;
        let bad = |reason| ConfigError::InvalidServerLimits { reason };
        if self.directory.as_os_str().is_empty() {
            return Err(bad("SIDER_AOF_DIR não pode ser vazio"));
        }
        if self.queue_capacity == 0 || self.queue_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(bad("fila AOF precisa ter capacidade válida"));
        }
        if self.limits.max_record_bytes < 64
            || self.limits.max_record_bytes > format::MAX_RECORD_BYTES
            || self.limits.max_mutations == 0
            || self.limits.max_mutations > 1_000_000
            || self.max_delta_bytes == 0
            || self.max_delta_bytes > isize::MAX as usize
        {
            return Err(bad("limites AOF inválidos"));
        }
        if let SyncPolicy::Periodic(period) = self.sync
            && (period.is_zero() || period > Duration::from_secs(60))
        {
            return Err(bad("período de sync AOF precisa estar entre 1 ms e 60 s"));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum AofError {
    #[error("configuração AOF: {0}")]
    Config(#[from] ConfigError),
    #[error("I/O AOF: {0}")]
    Io(#[from] io::Error),
    #[error("formato AOF: {0}")]
    Format(#[from] FormatError),
    #[error("replay AOF: {0}")]
    Replay(#[from] ReplayError),
    #[error("AOF já está aberto por outro processo")]
    Locked,
    #[error("escritor AOF indisponível")]
    Unavailable,
    #[error("sequência ou snapshot AOF inválido")]
    Sequence,
    #[error("compactação já está em andamento")]
    Compacting,
    #[error("delta da compactação excedeu o limite; AOF anterior preservado")]
    DeltaLimit,
    #[error(
        "configuração AOF divergente: esperado {expected:?}, encontrado {actual:?}; migração offline necessária"
    )]
    LayoutMismatch {
        expected: DurableLayout,
        actual: DurableLayout,
    },
    #[error("lote AOF cruza shards")]
    CrossShard,
    #[error("shard {shard} usa {used} bytes, excedendo a quota {quota}")]
    ShardQuota {
        shard: usize,
        used: usize,
        quota: usize,
    },
    #[error("migração offline recusada: {0}")]
    Migration(&'static str),
}

/// Pontos de falha injetáveis para ensaios reproduzíveis. Produção usa NoFaults.
pub trait FaultInjector: Send + Sync + 'static {
    fn hit(&self, point: &'static str) -> io::Result<()>;
    fn write_append(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(bytes)
    }
}
pub struct NoFaults;
impl FaultInjector for NoFaults {
    fn hit(&self, _: &'static str) -> io::Result<()> {
        Ok(())
    }
}

type Response<T> = oneshot::Sender<Result<T, AofError>>;
enum Request {
    Append(ResolvedBatch, Response<u64>),
    Flush(Response<u64>),
    Compact(Vec<Mutation>, Response<()>),
    Status(Response<(u64, u64, bool)>),
}

/// Canal limitado para um escritor global; clones podem ser compartilhados entre workers.
#[derive(Clone)]
pub struct AofHandle {
    requests: mpsc::Sender<Request>,
    failed: watch::Receiver<bool>,
}
impl AofHandle {
    pub async fn append(&self, batch: ResolvedBatch) -> Result<u64, AofError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Append(batch, reply))
            .await
            .map_err(|_| AofError::Unavailable)?;
        result.await.map_err(|_| AofError::Unavailable)?
    }
    pub async fn flush(&self) -> Result<u64, AofError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Flush(reply))
            .await
            .map_err(|_| AofError::Unavailable)?;
        result.await.map_err(|_| AofError::Unavailable)?
    }
    /// O chamador garante que o snapshot contém todo lote confirmado até a barreira.
    /// A chamada termina após o cutover ou aborto; appends por outros clones seguem livres.
    pub async fn compact(&self, snapshot: Vec<Mutation>) -> Result<(), AofError> {
        self.begin_compaction(snapshot)
            .await?
            .await
            .map_err(|_| AofError::Unavailable)?
    }
    pub async fn begin_compaction(
        &self,
        snapshot: Vec<Mutation>,
    ) -> Result<oneshot::Receiver<Result<(), AofError>>, AofError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Compact(snapshot, reply))
            .await
            .map_err(|_| AofError::Unavailable)?;
        Ok(result)
    }
    pub async fn status(&self) -> Result<(u64, u64, bool), AofError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Request::Status(reply))
            .await
            .map_err(|_| AofError::Unavailable)?;
        result.await.map_err(|_| AofError::Unavailable)?
    }
    pub async fn failed(&self) {
        let mut receiver = self.failed.clone();
        while !*receiver.borrow_and_update() {
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

pub struct Recovered {
    pub store: Store,
    pub metadata: RecoveryMetadata,
    writer: Writer,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryMetadata {
    pub layout: DurableLayout,
    pub sequence: u64,
    pub format_version: u32,
    pub shard_usage: Vec<usize>,
    pub valid_bytes: u64,
    pub incomplete_tail_bytes: u64,
}
impl Recovered {
    /// Inicia somente após replay completo; o JoinHandle supervisiona sync, append e fechamento.
    pub fn start(
        self,
    ) -> (
        Store,
        AofHandle,
        tokio::task::JoinHandle<Result<(), AofError>>,
    ) {
        let (requests, receiver) = mpsc::channel(self.writer.config.queue_capacity);
        let (failure, failed) = watch::channel(false);
        let runtime = tokio::runtime::Handle::current();
        let task = tokio::task::spawn_blocking(move || {
            let result = self.writer.run(receiver, runtime);
            if let Err(error) = &result {
                tracing::error!(%error, "escritor AOF encerrado com falha");
                failure.send_replace(true);
            }
            result
        });
        (self.store, AofHandle { requests, failed }, task)
    }
}

struct Compaction {
    ready: sync_mpsc::Receiver<Result<File, AofError>>,
    temporary: PathBuf,
    destination: PathBuf,
    delta: Vec<Vec<u8>>,
    delta_bytes: usize,
    aborted: bool,
    reply: Response<()>,
}

struct Writer {
    config: AofConfig,
    file: File,
    _lock: File,
    generation: u64,
    sequence: u64,
    bytes_since_compact: u64,
    dirty: bool,
    synced: Instant,
    compaction: Option<Compaction>,
    faults: Arc<dyn FaultInjector>,
}

pub fn recover(
    config: AofConfig,
    store_config: StoreConfig,
    clock: Arc<dyn Clock>,
) -> Result<Recovered, AofError> {
    recover_with_faults(config, store_config, clock, Arc::new(NoFaults))
}

/// Recuperação síncrona: não abre listener e preserva o arquivo em qualquer erro de validação.
pub fn recover_with_faults(
    config: AofConfig,
    store_config: StoreConfig,
    clock: Arc<dyn Clock>,
    faults: Arc<dyn FaultInjector>,
) -> Result<Recovered, AofError> {
    load(config, store_config, clock, faults, false)
}

pub(super) fn recover_read_only(
    config: AofConfig,
    store_config: StoreConfig,
    clock: Arc<dyn Clock>,
) -> Result<Recovered, AofError> {
    load(config, store_config, clock, Arc::new(NoFaults), true)
}

fn load(
    config: AofConfig,
    store_config: StoreConfig,
    clock: Arc<dyn Clock>,
    faults: Arc<dyn FaultInjector>,
    read_only: bool,
) -> Result<Recovered, AofError> {
    config.validate()?;
    store_config.validate()?;
    config.layout.quota(store_config.max_dataset_bytes, 0)?;
    if !read_only {
        fs::create_dir_all(&config.directory)?;
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(!read_only)
        .truncate(false)
        .open(config.directory.join("writer.lock"))?;
    lock.try_lock().map_err(|error| match error {
        fs::TryLockError::WouldBlock => AofError::Locked,
        fs::TryLockError::Error(error) => AofError::Io(error),
    })?;
    let mut generations = Vec::new();
    for entry in fs::read_dir(&config.directory)? {
        let entry = entry?;
        if let Some(name) = entry.file_name().to_str()
            && let Some(value) = name
                .strip_prefix("generation-")
                .and_then(|name| name.strip_suffix(".aof"))
        {
            if value.len() != 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(AofError::Sequence);
            }
            generations.push(value.parse::<u64>().map_err(|_| AofError::Sequence)?);
        }
    }
    generations.sort_unstable();
    let generation = generations.last().copied().unwrap_or(0);
    let path = generation_path(&config.directory, generation);
    if generations.is_empty() {
        if read_only {
            return Err(AofError::Sequence);
        }
        // Publica somente o arquivo inicial completo; resíduos .tmp nunca são candidatos.
        let temporary = temporary_path(&config.directory, "initial");
        let mut initial = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        format::write_header_with_layout(&mut initial, 0, config.layout)?;
        initial.write_all(&format::encode(
            &Record::Seal {
                sequence: 0,
                entries: 0,
                digest: 0,
            },
            config.limits,
        )?)?;
        initial.sync_all()?;
        drop(initial);
        fs::rename(&temporary, &path)?;
        sync_directory(&config.directory)?;
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(!read_only)
        .open(&path)?;
    let mut store = Store::with_config(store_config, clock)?;
    let header = format::read_header_with_layout(&mut file)?;
    if header.layout != config.layout {
        return Err(AofError::LayoutMismatch {
            expected: config.layout,
            actual: header.layout,
        });
    }
    let base = header.sequence;
    let mut shard_usage = vec![0; config.layout.shard_count as usize];
    let mut sequence = base;
    let mut sealed = false;
    let mut previous_key = None;
    let mut snapshot_entries = 0u64;
    let mut snapshot_digest = 0u32;
    let mut valid_bytes = header.bytes as u64;
    let incomplete = loop {
        match format::read_record(&mut file, config.limits)? {
            Next::End => break false,
            Next::IncompleteTail => break true,
            Next::Record(Record::Snapshot(mutation @ Mutation::Put { .. })) if !sealed => {
                if previous_key
                    .as_ref()
                    .is_some_and(|key| key >= mutation.key())
                {
                    return Err(AofError::Sequence);
                }
                previous_key = Some(mutation.key().clone());
                snapshot_entries += 1;
                snapshot_digest = format::snapshot_digest(
                    snapshot_digest,
                    &format::encode(&Record::Snapshot(mutation.clone()), config.limits)?,
                );
                replay_routed(
                    &mut store,
                    &[mutation],
                    config.layout,
                    store_config.max_dataset_bytes,
                    &mut shard_usage,
                )?;
            }
            Next::Record(Record::Seal {
                sequence: seal,
                entries,
                digest,
            }) if !sealed
                && seal == base
                && entries == snapshot_entries
                && digest == snapshot_digest =>
            {
                sealed = true
            }
            Next::Record(Record::Batch {
                sequence: next,
                batch,
            }) if sealed && sequence.checked_add(1) == Some(next) => {
                replay_routed(
                    &mut store,
                    &batch.mutations,
                    config.layout,
                    store_config.max_dataset_bytes,
                    &mut shard_usage,
                )?;
                sequence = next;
            }
            _ => return Err(AofError::Sequence),
        }
        valid_bytes = file.stream_position()?;
    };
    if !sealed {
        return Err(AofError::Sequence);
    }
    let incomplete_tail_bytes = if incomplete {
        file.metadata()?.len() - valid_bytes
    } else {
        0
    };
    if incomplete && !read_only {
        faults.hit("recovery_before_truncate")?;
        // Preserva o original para diagnóstico antes de descartar somente a cauda incompleta.
        let backup = config.directory.join(format!(
            "tail-{generation:020}-{}-{}.bak",
            std::process::id(),
            NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed)
        ));
        let mut original = File::open(&path)?;
        let mut saved = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(backup)?;
        io::copy(&mut original, &mut saved)?;
        saved.sync_all()?;
        sync_directory(&config.directory)?;
        file.set_len(valid_bytes)?;
        file.sync_all()?;
    }
    file.seek(SeekFrom::End(0))?;
    Ok(Recovered {
        store,
        metadata: RecoveryMetadata {
            layout: header.layout,
            sequence,
            format_version: header.format_version,
            shard_usage,
            valid_bytes,
            incomplete_tail_bytes,
        },
        writer: Writer {
            config,
            file,
            _lock: lock,
            generation,
            sequence,
            bytes_since_compact: valid_bytes,
            dirty: false,
            synced: Instant::now(),
            compaction: None,
            faults,
        },
    })
}

fn batch_shard(mutations: &[Mutation], layout: DurableLayout) -> Result<usize, AofError> {
    let mut selected = None;
    for mutation in mutations {
        let shard = layout.shard_for(mutation.key())?;
        if selected.is_some_and(|previous| previous != shard) {
            return Err(AofError::CrossShard);
        }
        selected = Some(shard);
    }
    Ok(selected.unwrap_or(0))
}

fn replay_routed(
    store: &mut Store,
    mutations: &[Mutation],
    layout: DurableLayout,
    total: usize,
    usage: &mut [usize],
) -> Result<(), AofError> {
    let shard = batch_shard(mutations, layout)?;
    let before = store.used_bytes();
    store.replay(mutations)?;
    let after = store.used_bytes();
    if after >= before {
        usage[shard] += after - before;
    } else {
        usage[shard] -= before - after;
    }
    let quota = layout.quota(total, shard)?;
    if usage[shard] > quota {
        return Err(AofError::ShardQuota {
            shard,
            used: usage[shard],
            quota,
        });
    }
    Ok(())
}

fn generation_path(directory: &Path, generation: u64) -> PathBuf {
    directory.join(format!("generation-{generation:020}.aof"))
}

// Rust não expõe sync de diretório portátil no Windows. Preservamos gerações antigas;
// garantia Windows cobre término do processo, sem prometer atomicidade contra queda de energia.
#[cfg(unix)]
pub(super) fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}
#[cfg(not(unix))]
pub(super) fn sync_directory(_: &Path) -> io::Result<()> {
    Ok(())
}

impl Writer {
    fn run(
        mut self,
        mut requests: mpsc::Receiver<Request>,
        runtime: tokio::runtime::Handle,
    ) -> Result<(), AofError> {
        loop {
            let request = runtime.block_on(async {
                tokio::time::timeout(Duration::from_millis(10), requests.recv()).await
            });
            match request {
                Ok(Some(Request::Append(batch, reply))) => {
                    let result = self.append(batch);
                    if result.is_err()
                        && !matches!(result, Err(AofError::Format(FormatError::Limit)))
                    {
                        let _ = reply.send(result);
                        return Err(AofError::Unavailable);
                    }
                    let _ = reply.send(result);
                }
                Ok(Some(Request::Flush(reply))) => {
                    if let Err(error) = self.sync() {
                        let _ = reply.send(Err(error));
                        return Err(AofError::Unavailable);
                    }
                    let _ = reply.send(Ok(self.sequence));
                }
                Ok(Some(Request::Compact(snapshot, reply))) => {
                    self.begin_compaction(snapshot, reply)
                }
                Ok(Some(Request::Status(reply))) => {
                    let _ = reply.send(Ok((
                        self.sequence,
                        self.bytes_since_compact,
                        self.compaction.is_some(),
                    )));
                }
                Ok(None) => {
                    self.sync()?;
                    return Ok(());
                }
                Err(_) => {}
            }
            if let SyncPolicy::Periodic(period) = self.config.sync
                && self.dirty
                && self.synced.elapsed() >= period
            {
                self.sync()?;
            }
            self.finish_compaction()?;
        }
    }

    fn append(&mut self, batch: ResolvedBatch) -> Result<u64, AofError> {
        batch_shard(&batch.mutations, self.config.layout)?;
        let sequence = self.sequence.checked_add(1).ok_or(AofError::Sequence)?;
        let encoded = format::encode(&Record::Batch { sequence, batch }, self.config.limits)?;
        self.faults.hit("before_append")?;
        self.faults.write_append(&mut self.file, &encoded)?;
        self.faults.hit("after_append")?;
        self.sequence = sequence;
        self.bytes_since_compact = self
            .bytes_since_compact
            .saturating_add(encoded.len() as u64);
        self.dirty = true;
        if let Some(compaction) = &mut self.compaction {
            if compaction.aborted {
                // O produtor anterior ainda termina; não abre outro snapshot em paralelo.
            } else if compaction
                .delta_bytes
                .checked_add(encoded.len())
                .is_none_or(|bytes| bytes > self.config.max_delta_bytes)
            {
                compaction.aborted = true;
                compaction.delta.clear();
                compaction.delta_bytes = 0;
            } else {
                compaction.delta_bytes += encoded.len();
                compaction.delta.push(encoded);
            }
        }
        if self.config.sync == SyncPolicy::Always {
            self.sync()?;
        }
        self.faults.hit("before_reply")?;
        Ok(sequence)
    }

    fn sync(&mut self) -> Result<(), AofError> {
        self.faults.hit("before_sync")?;
        self.file.sync_all()?;
        self.faults.hit("after_sync")?;
        self.synced = Instant::now();
        self.dirty = false;
        Ok(())
    }

    fn begin_compaction(&mut self, snapshot: Vec<Mutation>, reply: Response<()>) {
        if self.compaction.is_some() {
            let _ = reply.send(Err(AofError::Compacting));
            return;
        }
        let Some(generation) = self.generation.checked_add(1) else {
            let _ = reply.send(Err(AofError::Sequence));
            return;
        };
        let temporary =
            temporary_path(&self.config.directory, &format!("compact-{generation:020}"));
        let destination = generation_path(&self.config.directory, generation);
        let (sender, ready) = sync_mpsc::channel();
        let path = temporary.clone();
        let faults = self.faults.clone();
        let sequence = self.sequence;
        let limits = self.config.limits;
        let layout = self.config.layout;
        std::thread::spawn(move || {
            let result = (|| {
                faults.hit("compact_before_snapshot")?;
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&path)?;
                format::write_header_with_layout(&mut file, sequence, layout)?;
                let mut previous = None;
                let mut entries = 0u64;
                let mut digest = 0u32;
                for mutation in snapshot {
                    if !matches!(mutation, Mutation::Put { .. })
                        || previous.as_ref().is_some_and(|key| key >= mutation.key())
                    {
                        return Err(AofError::Sequence);
                    }
                    previous = Some(mutation.key().clone());
                    let encoded = format::encode(&Record::Snapshot(mutation), limits)?;
                    entries += 1;
                    digest = format::snapshot_digest(digest, &encoded);
                    file.write_all(&encoded)?;
                }
                file.write_all(&format::encode(
                    &Record::Seal {
                        sequence,
                        entries,
                        digest,
                    },
                    limits,
                )?)?;
                file.sync_all()?;
                faults.hit("compact_after_snapshot")?;
                Ok(file)
            })();
            if sender.send(result).is_err() {
                let _ = fs::remove_file(path);
            }
        });
        self.compaction = Some(Compaction {
            ready,
            temporary,
            destination,
            delta: Vec::new(),
            delta_bytes: 0,
            aborted: false,
            reply,
        });
    }

    fn finish_compaction(&mut self) -> Result<(), AofError> {
        let Some(compaction) = &self.compaction else {
            return Ok(());
        };
        let ready = match compaction.ready.try_recv() {
            Ok(result) => result,
            Err(sync_mpsc::TryRecvError::Empty) => return Ok(()),
            Err(sync_mpsc::TryRecvError::Disconnected) => Err(AofError::Unavailable),
        };
        let compaction = self.compaction.take().unwrap();
        if compaction.aborted {
            drop(ready);
            let _ = fs::remove_file(&compaction.temporary);
            let _ = compaction.reply.send(Err(AofError::DeltaLimit));
            return Ok(());
        }
        let result = (|| {
            let mut file = ready?;
            for delta in &compaction.delta {
                file.write_all(delta)?;
            }
            file.sync_all()?;
            self.faults.hit("compact_before_publish")?;
            // Destino único: não substitui arquivo aberto. A geração antiga continua recuperável.
            fs::rename(&compaction.temporary, &compaction.destination)?;
            sync_directory(&self.config.directory)?;
            self.faults.hit("compact_after_publish")?;
            self.file = file;
            self.generation += 1;
            self.bytes_since_compact = 0;
            self.dirty = false;
            self.synced = Instant::now();
            // Mantém uma geração anterior completa, retirando somente a que ficou obsoleta.
            if let Some(obsolete) = self.generation.checked_sub(2) {
                let path = generation_path(&self.config.directory, obsolete);
                if path.exists()
                    && let Err(error) = fs::remove_file(path)
                {
                    tracing::warn!(%error, "não foi possível retirar geração AOF antiga");
                }
            }
            Ok(())
        })();
        // Após publicar, falha é fatal: continuar no arquivo antigo perderia appends no próximo replay.
        let published = compaction.destination.exists();
        if result.is_err() && !published {
            let _ = fs::remove_file(&compaction.temporary);
        }
        let failed = result.is_err();
        let _ = compaction.reply.send(result);
        if failed && published {
            return Err(AofError::Unavailable);
        }
        Ok(())
    }
}
