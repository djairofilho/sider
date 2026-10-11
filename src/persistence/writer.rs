use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc as sync_mpsc};
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};

use super::format::{self, FormatError, Limits, Record};
use super::{DurableLayout, ReplicationMetadata, Role, memory_format, profile::Profile};
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
    /// The reply waits for the batch's sync_all.
    Always,
    /// The reply confirms write_all; the window includes the period and I/O/scheduling delays.
    Periodic(Duration),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AofConfig {
    /// Explicit record-2 profile for the server, backup, and replica paths.
    memory_profile: bool,
    pub directory: PathBuf,
    pub layout: DurableLayout,
    pub sync: SyncPolicy,
    pub queue_capacity: usize,
    pub limits: Limits,
    /// Zero disables automatic compaction; the explicit API remains available.
    pub compact_after_bytes: u64,
    pub max_delta_bytes: usize,
}
impl AofConfig {
    pub fn new(directory: PathBuf) -> Self {
        Self {
            memory_profile: false,
            directory,
            layout: DurableLayout::default(),
            sync: SyncPolicy::Always,
            queue_capacity: 32,
            limits: Limits::default(),
            compact_after_bytes: 64 * 1024 * 1024,
            max_delta_bytes: 16 * 1024 * 1024,
        }
    }
    /// Selects the record-2/header-4-or-5 profile explicitly.
    /// Offline memory backup and migration APIs also handle this profile.
    pub fn with_memory_profile(mut self) -> Self {
        self.memory_profile = true;
        self
    }
    pub fn is_memory_profile(&self) -> bool {
        self.memory_profile
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.layout.validate()?;
        let bad = |reason| ConfigError::InvalidServerLimits { reason };
        if self.directory.as_os_str().is_empty() {
            return Err(bad("SIDER_AOF_DIR cannot be empty"));
        }
        if self.queue_capacity == 0 || self.queue_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(bad("AOF queue must have valid capacity"));
        }
        if self.limits.max_record_bytes < 64
            || self.limits.max_record_bytes > format::MAX_RECORD_BYTES
            || self.limits.max_mutations == 0
            || self.limits.max_mutations > 1_000_000
            || self.max_delta_bytes == 0
            || self.max_delta_bytes > isize::MAX as usize
        {
            return Err(bad("invalid AOF limits"));
        }
        if let SyncPolicy::Periodic(period) = self.sync
            && (period.is_zero() || period > Duration::from_secs(60))
        {
            return Err(bad("AOF sync period must be between 1 ms and 60 s"));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum AofError {
    #[error("AOF configuration: {0}")]
    Config(#[from] ConfigError),
    #[error("AOF I/O: {0}")]
    Io(#[from] io::Error),
    #[error("AOF format: {0}")]
    Format(#[from] FormatError),
    #[error("replay AOF: {0}")]
    Replay(#[from] ReplayError),
    #[error("AOF is already open by another process")]
    Locked,
    #[error("AOF writer unavailable")]
    Unavailable,
    #[error("invalid AOF sequence or snapshot")]
    Sequence,
    #[error("compaction is already in progress")]
    Compacting,
    #[error("compaction delta exceeded the limit; prior AOF preserved")]
    DeltaLimit,
    #[error(
        "divergent AOF configuration: expected {expected:?}, found {actual:?}; offline migration required"
    )]
    LayoutMismatch {
        expected: DurableLayout,
        actual: DurableLayout,
    },
    #[error("AOF batch crosses shards")]
    CrossShard,
    #[error("shard {shard} uses {used} bytes, exceeding quota {quota}")]
    ShardQuota {
        shard: usize,
        used: usize,
        quota: usize,
    },
    #[error("offline migration rejected: {0}")]
    Migration(&'static str),
}

/// Injectable fault points for reproducible tests. Production uses NoFaults.
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
    AppendMemory(
        crate::storage::MutationOrigin,
        Vec<memory_format::Mutation>,
        Response<u64>,
    ),
    ReplicateMemory(
        u64,
        crate::storage::MutationOrigin,
        Vec<memory_format::Mutation>,
        Response<u64>,
    ),
    Append(ResolvedBatch, Response<u64>),
    Replicate(u64, ResolvedBatch, Response<u64>),
    Flush(Response<u64>),
    Compact(Vec<Mutation>, Response<()>),
    Status(Response<(u64, u64, bool)>),
    Install {
        snapshot: Vec<Mutation>,
        sequence: u64,
        metadata: ReplicationMetadata,
        reply: Response<()>,
    },
    InstallMemory {
        snapshot: Vec<memory_format::Mutation>,
        sequence: u64,
        metadata: ReplicationMetadata,
        reply: Response<()>,
    },
    Journal(crate::replication::journal::Journal, Response<()>),
}

/// Bounded channel for a global writer; clones can be shared among workers.
#[derive(Clone)]
pub struct AofHandle {
    memory_profile: bool,
    composed_profile: bool,
    layout: DurableLayout,
    limits: Limits,
    faults: Arc<dyn FaultInjector>,
    requests: mpsc::Sender<Request>,
    failed: watch::Receiver<bool>,
    diagnostics: super::diagnostics::Shared,
}

/// Observer that does not keep the writer or its queue open.
#[derive(Clone)]
pub struct AofDiagnosticsHandle {
    requests: mpsc::WeakSender<Request>,
    diagnostics: super::diagnostics::Shared,
}
impl AofDiagnosticsHandle {
    pub fn snapshot(&self) -> super::AofDiagnostics {
        let mut state = self.diagnostics.snapshot();
        state.queue_depth = self
            .requests
            .upgrade()
            .filter(|requests| !requests.is_closed())
            .map_or(0, |requests| requests.max_capacity() - requests.capacity());
        state
    }
}

impl AofHandle {
    pub(crate) fn layout(&self) -> DurableLayout {
        self.layout
    }

    pub(crate) fn is_composed_legacy_mixed(&self) -> bool {
        self.composed_profile
    }

    pub(crate) fn same_writer(&self, other: &Self) -> bool {
        self.requests.same_channel(&other.requests)
    }

    pub fn is_memory_profile(&self) -> bool {
        self.memory_profile
    }
    pub fn record_limits(&self) -> Limits {
        self.limits
    }
    pub(crate) fn lifecycle_fault(&self, point: &'static str) -> Result<(), AofError> {
        self.faults.hit(point).map_err(Into::into)
    }

    pub async fn append_memory(
        &self,
        origin: crate::storage::MutationOrigin,
        mutations: Vec<memory_format::Mutation>,
    ) -> Result<u64, AofError> {
        if !self.memory_profile {
            return Err(FormatError::Version(2).into());
        }
        let (reply, response) = oneshot::channel();
        self.requests
            .send(Request::AppendMemory(origin, mutations, reply))
            .await
            .map_err(|_| AofError::Unavailable)?;
        response.await.map_err(|_| AofError::Unavailable)?
    }
    /// Appends a record-2 replica batch at the exact upstream sequence.
    pub async fn append_expected_memory(
        &self,
        sequence: u64,
        origin: crate::storage::MutationOrigin,
        mutations: Vec<memory_format::Mutation>,
    ) -> Result<u64, AofError> {
        if !self.memory_profile {
            return Err(FormatError::Version(2).into());
        }
        let (reply, response) = oneshot::channel();
        self.requests
            .send(Request::ReplicateMemory(sequence, origin, mutations, reply))
            .await
            .map_err(|_| AofError::Unavailable)?;
        response.await.map_err(|_| AofError::Unavailable)?
    }
    /// The sequence comes from upstream and must exactly follow the local AOF.
    pub async fn append_expected(
        &self,
        sequence: u64,
        batch: ResolvedBatch,
    ) -> Result<u64, AofError> {
        if self.memory_profile {
            return Err(AofError::Migration(
                "selected replication transport is not activated",
            ));
        }
        let (reply, response) = oneshot::channel();
        self.requests
            .send(Request::Replicate(sequence, batch, reply))
            .await
            .map_err(|_| AofError::Unavailable)?;
        response.await.map_err(|_| AofError::Unavailable)?
    }
    /// The coordinator retains global exclusion until it swaps all stores.
    pub async fn install_snapshot(
        &self,
        snapshot: Vec<Mutation>,
        sequence: u64,
        metadata: ReplicationMetadata,
    ) -> Result<(), AofError> {
        if self.memory_profile {
            return Err(AofError::Migration(
                "selected replication installation is not activated",
            ));
        }
        let (reply, response) = oneshot::channel();
        self.requests
            .send(Request::Install {
                snapshot,
                sequence,
                metadata,
                reply,
            })
            .await
            .map_err(|_| AofError::Unavailable)?;
        response.await.map_err(|_| AofError::Unavailable)?
    }

    /// Publishes a complete record-2 replica image as one durable generation.
    pub async fn install_memory_snapshot(
        &self,
        snapshot: Vec<memory_format::Mutation>,
        sequence: u64,
        metadata: ReplicationMetadata,
    ) -> Result<(), AofError> {
        if !self.memory_profile {
            return Err(FormatError::Version(2).into());
        }
        let (reply, response) = oneshot::channel();
        self.requests
            .send(Request::InstallMemory {
                snapshot,
                sequence,
                metadata,
                reply,
            })
            .await
            .map_err(|_| AofError::Unavailable)?;
        response.await.map_err(|_| AofError::Unavailable)?
    }

    /// Binds the journal to the writer's same epoch/position under the global barrier.
    pub async fn attach_journal(
        &self,
        journal: crate::replication::journal::Journal,
    ) -> Result<(), AofError> {
        if self.composed_profile {
            return Err(AofError::Migration(
                "composed replication transport is not activated",
            ));
        }
        let (reply, response) = oneshot::channel();
        self.requests
            .send(Request::Journal(journal, reply))
            .await
            .map_err(|_| AofError::Unavailable)?;
        response.await.map_err(|_| AofError::Unavailable)?
    }
    /// Reads numeric fields and fixed categories without waiting for the writer or doing I/O.
    pub fn diagnostics(&self) -> super::AofDiagnostics {
        self.diagnostics_handle().snapshot()
    }

    pub fn diagnostics_handle(&self) -> AofDiagnosticsHandle {
        AofDiagnosticsHandle {
            requests: self.requests.downgrade(),
            diagnostics: self.diagnostics.clone(),
        }
    }

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
    /// The caller ensures the snapshot contains every batch confirmed through the barrier.
    /// The call ends after cutover or abort; appends by other clones remain free.
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
    pub replication: Option<ReplicationMetadata>,
}
impl Recovered {
    /// Identifies the generation actually selected and held open by recovery.
    pub(super) fn generation_path(&self) -> PathBuf {
        generation_path(&self.writer.config.directory, self.writer.generation)
    }

    /// Starts only after complete replay; the JoinHandle supervises sync, append, and close.
    pub fn start(
        self,
    ) -> (
        Store,
        AofHandle,
        tokio::task::JoinHandle<Result<(), AofError>>,
    ) {
        let (requests, receiver) = mpsc::channel(self.writer.config.queue_capacity);
        let (failure, failed) = watch::channel(false);
        let diagnostics = self.writer.diagnostics.clone();
        diagnostics.update(|state| {
            state.running = true;
            state.queue_capacity = requests.max_capacity();
        });
        let completed = diagnostics.clone();
        let memory_profile = self.writer.config.memory_profile;
        let composed_profile = self.writer.profile == Profile::ComposedLegacyMixed;
        let layout = self.writer.config.layout;
        let limits = self.writer.config.limits;
        let faults = self.writer.faults.clone();
        let runtime = tokio::runtime::Handle::current();
        let task = tokio::task::spawn_blocking(move || {
            let result = self.writer.run(receiver, runtime);
            completed.stopped(result.as_ref().err());
            if let Err(error) = &result {
                tracing::error!(%error, "AOF writer terminated with failure");
                failure.send_replace(true);
            }
            result
        });
        (
            self.store,
            AofHandle {
                memory_profile,
                composed_profile,
                layout,
                limits,
                faults,
                requests,
                failed,
                diagnostics,
            },
            task,
        )
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
    profile: Profile,
    file: File,
    _lock: DirectoryLock,
    generation: u64,
    sequence: u64,
    bytes_since_compact: u64,
    dirty: bool,
    synced: Instant,
    compaction: Option<Compaction>,
    faults: Arc<dyn FaultInjector>,
    diagnostics: super::diagnostics::Shared,
    replication: Option<ReplicationMetadata>,
    journal: Option<crate::replication::journal::Journal>,
    store_config: StoreConfig,
    clock: Arc<dyn Clock>,
}

pub(super) struct DirectoryLock(File);
impl DirectoryLock {
    pub(super) fn acquire(directory: &Path, create: bool) -> Result<Self, AofError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(directory.join("writer.lock"))?;
        file.try_lock().map_err(|error| match error {
            fs::TryLockError::WouldBlock => AofError::Locked,
            fs::TryLockError::Error(error) => AofError::Io(error),
        })?;
        Ok(Self(file))
    }
}
impl Drop for DirectoryLock {
    fn drop(&mut self) {
        // flock belongs to the open description: fork/dup can keep another descriptor alive.
        // Explicitly releases ownership before closing our handle.
        if let Err(error) = self.0.unlock() {
            tracing::warn!(%error, "failed to release AOF lock");
        }
    }
}

pub fn recover(
    config: AofConfig,
    store_config: StoreConfig,
    clock: Arc<dyn Clock>,
) -> Result<Recovered, AofError> {
    recover_with_faults(config, store_config, clock, Arc::new(NoFaults))
}

/// Synchronous recovery: does not open a listener and preserves the file on any validation error.
pub fn recover_with_faults(
    config: AofConfig,
    store_config: StoreConfig,
    clock: Arc<dyn Clock>,
    faults: Arc<dyn FaultInjector>,
) -> Result<Recovered, AofError> {
    load(config, store_config, clock, faults, false)
}

/// Opens the explicit header-7/record-1 mixed-owner profile in a library caller.
/// The server configuration does not select this profile. All owners using the
/// recovered store must share the returned writer handle for atomic batches.
pub fn recover_composed(
    config: AofConfig,
    store_config: StoreConfig,
    clock: Arc<dyn Clock>,
) -> Result<Recovered, AofError> {
    load_profile(
        config,
        store_config,
        clock,
        Arc::new(NoFaults),
        false,
        Profile::ComposedLegacyMixed,
    )
}

/// Development-only local profile; no server, backup, or replica configuration
/// can select it. The caller must bind every owner to the same writer handle.
#[cfg(test)]
pub(crate) fn recover_composed_legacy_mixed(
    config: AofConfig,
    store_config: StoreConfig,
    clock: Arc<dyn Clock>,
    faults: Arc<dyn FaultInjector>,
) -> Result<Recovered, AofError> {
    load_profile(
        config,
        store_config,
        clock,
        faults,
        false,
        Profile::ComposedLegacyMixed,
    )
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
    let profile = Profile::selected(config.memory_profile);
    load_profile(config, store_config, clock, faults, read_only, profile)
}

fn load_profile(
    config: AofConfig,
    store_config: StoreConfig,
    clock: Arc<dyn Clock>,
    faults: Arc<dyn FaultInjector>,
    read_only: bool,
    profile: Profile,
) -> Result<Recovered, AofError> {
    if (profile == Profile::Memory) != config.memory_profile {
        return Err(AofError::Migration("incompatible development AOF profile"));
    }
    config.validate()?;
    store_config.validate()?;
    config.layout.quota(store_config.max_dataset_bytes, 0)?;
    if !read_only {
        fs::create_dir_all(&config.directory)?;
    }
    let lock = DirectoryLock::acquire(&config.directory, !read_only)?;
    let mut generations = Vec::new();
    let mut residues = Vec::new();
    for entry in fs::read_dir(&config.directory)? {
        let entry = entry?;
        if !read_only
            && entry.file_type()?.is_file()
            && entry.file_name().to_str().is_some_and(|name| {
                name.starts_with("replication-install-") && name.ends_with(".tmp")
            })
        {
            residues.push(entry.path());
            continue;
        }
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
        // Publishes only the complete initial file; .tmp residues are never candidates.
        let temporary = temporary_path(&config.directory, "initial");
        let mut initial = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        profile.write_header(&mut initial, 0, config.layout, None)?;
        initial.write_all(&profile.encode_legacy(
            Record::Seal {
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
    let mut store = Store::with_config(store_config, clock.clone())?;
    let header = match profile.read_header(&mut file) {
        Err(FormatError::Version(1..=3)) if config.memory_profile => {
            return Err(AofError::Migration(
                "record-2 profile requires explicit offline migration from legacy AOF",
            ));
        }
        result => result?,
    };
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
        let record = match profile.read_record(&mut file, config.limits)? {
            memory_format::Next::End => break false,
            memory_format::Next::IncompleteTail => break true,
            memory_format::Next::Record(record) => *record,
        };
        match record {
            memory_format::Record::Snapshot(mutation)
                if !sealed
                    && matches!(
                        mutation,
                        memory_format::Mutation::Image { .. }
                            | memory_format::Mutation::Legacy(Mutation::Put { .. })
                    ) =>
            {
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
                    &profile.encode(
                        &memory_format::Record::Snapshot(mutation.clone()),
                        config.limits,
                    )?,
                );
                replay_routed_memory(
                    &mut store,
                    &[mutation],
                    config.layout,
                    store_config.max_dataset_bytes,
                    &mut shard_usage,
                    profile == Profile::ComposedLegacyMixed,
                )?;
            }
            memory_format::Record::Seal {
                sequence: seal,
                entries,
                digest,
            } if !sealed
                && seal == base
                && entries == snapshot_entries
                && digest == snapshot_digest =>
            {
                sealed = true
            }
            memory_format::Record::Batch {
                sequence: next,
                mutations,
                ..
            } if sealed && sequence.checked_add(1) == Some(next) => {
                replay_routed_memory(
                    &mut store,
                    &mutations,
                    config.layout,
                    store_config.max_dataset_bytes,
                    &mut shard_usage,
                    profile == Profile::ComposedLegacyMixed,
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
    // Validate the selected header and complete history before deleting residues.
    // Incompatible profiles and corrupt histories never enter cleanup or repair.
    for residue in residues {
        fs::remove_file(residue)?;
    }
    let incomplete_tail_bytes = if incomplete {
        file.metadata()?.len() - valid_bytes
    } else {
        0
    };
    if incomplete && !read_only {
        faults.hit("recovery_before_truncate")?;
        // Preserves the original for diagnosis before discarding only the incomplete tail.
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
            replication: header.replication,
        },
        writer: Writer {
            config,
            profile,
            file,
            _lock: lock,
            generation,
            sequence,
            bytes_since_compact: valid_bytes,
            dirty: false,
            synced: Instant::now(),
            compaction: None,
            faults,
            replication: header.replication,
            journal: None,
            store_config,
            clock,
            diagnostics: super::diagnostics::Shared(Arc::new(std::sync::Mutex::new(
                super::AofDiagnostics {
                    written_sequence: sequence,
                    synced_sequence: sequence,
                    generation,
                    bytes_since_compaction: valid_bytes,
                    ..super::AofDiagnostics::default()
                },
            ))),
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

fn replay_routed_memory(
    store: &mut Store,
    mutations: &[memory_format::Mutation],
    layout: DurableLayout,
    total: usize,
    usage: &mut [usize],
    allow_mixed: bool,
) -> Result<(), AofError> {
    let mut selected = None;
    for mutation in mutations {
        let shard = layout.shard_for(mutation.key())?;
        if !allow_mixed && selected.is_some_and(|previous| previous != shard) {
            return Err(AofError::CrossShard);
        }
        selected = Some(shard);
    }
    let prepared =
        store.prepare_memory_replay(mutations, crate::storage::MutationOrigin::Client)?;
    let mut projected = BTreeMap::new();
    for (key, before, after) in prepared.usage_deltas(store)? {
        let shard = layout.shard_for(key)?;
        let used = projected.entry(shard).or_insert(usage[shard]);
        *used = used
            .checked_sub(before)
            .and_then(|value| value.checked_add(after))
            .ok_or(ReplayError::Quota)?;
    }
    for (&shard, &used) in &projected {
        let quota = layout.quota(total, shard)?;
        if used > quota {
            return Err(AofError::ShardQuota { shard, used, quota });
        }
    }
    store.apply(prepared);
    for (shard, used) in projected {
        usage[shard] = used;
    }
    Ok(())
}

fn generation_path(directory: &Path, generation: u64) -> PathBuf {
    directory.join(format!("generation-{generation:020}.aof"))
}

// Rust does not expose portable directory sync on Windows. We retain old generations;
// the Windows guarantee covers process termination, without promising power-failure atomicity.
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
                Ok(Some(Request::AppendMemory(origin, mutations, reply))) => {
                    let result = self.append_memory(origin, mutations);
                    self.refresh_diagnostics();
                    if let Err(error) = &result {
                        self.diagnostics.error(error);
                        if matches!(error, AofError::Format(FormatError::Limit)) {
                            self.diagnostics.update(|state| {
                                state.record_rejections_total =
                                    state.record_rejections_total.saturating_add(1);
                            });
                        }
                    }
                    let fatal = result.is_err()
                        && !matches!(result, Err(AofError::Format(FormatError::Limit)));
                    let _ = reply.send(result);
                    if fatal {
                        return Err(AofError::Unavailable);
                    }
                }
                Ok(Some(Request::Replicate(sequence, batch, reply))) => {
                    if self.sequence.checked_add(1) != Some(sequence)
                        || !self
                            .replication
                            .is_some_and(|metadata| metadata.role == Role::Replica)
                    {
                        let _ = reply.send(Err(AofError::Sequence));
                        continue;
                    }
                    let result = self.append(batch);
                    if result.is_err()
                        && !matches!(result, Err(AofError::Format(FormatError::Limit)))
                    {
                        let _ = reply.send(result);
                        return Err(AofError::Unavailable);
                    }
                    let _ = reply.send(result);
                }
                Ok(Some(Request::ReplicateMemory(sequence, origin, mutations, reply))) => {
                    if self.sequence.checked_add(1) != Some(sequence)
                        || !self
                            .replication
                            .is_some_and(|metadata| metadata.role == Role::Replica)
                    {
                        let _ = reply.send(Err(AofError::Sequence));
                        continue;
                    }
                    let result = self.append_memory(origin, mutations);
                    if result.is_err()
                        && !matches!(result, Err(AofError::Format(FormatError::Limit)))
                    {
                        let _ = reply.send(result);
                        return Err(AofError::Unavailable);
                    }
                    let _ = reply.send(result);
                }
                Ok(Some(Request::Append(batch, reply))) => {
                    let result = self.append(batch);
                    self.refresh_diagnostics();
                    if let Err(error) = &result {
                        self.diagnostics.error(error);
                        if matches!(error, AofError::Format(FormatError::Limit)) {
                            self.diagnostics.update(|state| {
                                state.record_rejections_total =
                                    state.record_rejections_total.saturating_add(1)
                            });
                        }
                    }
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
                        self.diagnostics.error(&error);
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
                Ok(Some(Request::Install {
                    snapshot,
                    sequence,
                    metadata,
                    reply,
                })) => {
                    let result = self.install_snapshot(snapshot, sequence, metadata);
                    if let Err(error) = &result {
                        self.diagnostics.error(error);
                    }
                    let fatal = matches!(result, Err(AofError::Unavailable));
                    let _ = reply.send(result);
                    if fatal {
                        return Err(AofError::Unavailable);
                    }
                }
                Ok(Some(Request::InstallMemory {
                    snapshot,
                    sequence,
                    metadata,
                    reply,
                })) => {
                    let result = self.install_memory_snapshot(snapshot, sequence, metadata);
                    if let Err(error) = &result {
                        self.diagnostics.error(error);
                    }
                    let fatal = matches!(result, Err(AofError::Unavailable));
                    let _ = reply.send(result);
                    if fatal {
                        return Err(AofError::Unavailable);
                    }
                }
                Ok(Some(Request::Journal(journal, reply))) => {
                    let valid = journal.status().is_ok_and(|status| {
                        status.head.sequence == self.sequence
                            && self.replication
                                == Some(ReplicationMetadata {
                                    role: Role::Primary,
                                    epoch: status.head.epoch,
                                })
                    });
                    if valid {
                        if let Some(previous) = self.journal.replace(journal) {
                            previous.close();
                        }
                        let _ = reply.send(Ok(()));
                    } else {
                        let _ = reply.send(Err(AofError::Sequence));
                    }
                }
                Ok(None) => {
                    self.sync()?;
                    if let Some(journal) = &self.journal {
                        journal.close();
                    }
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
            self.refresh_diagnostics();
        }
    }

    fn append(&mut self, batch: ResolvedBatch) -> Result<u64, AofError> {
        if self.profile == Profile::ComposedLegacyMixed {
            for mutation in &batch.mutations {
                self.config.layout.shard_for(mutation.key())?;
            }
        } else {
            batch_shard(&batch.mutations, self.config.layout)?;
        }
        let sequence = self.sequence.checked_add(1).ok_or(AofError::Sequence)?;
        let replication_frame = if self.journal.is_some() {
            Some(
                crate::replication::protocol::encode(
                    &crate::replication::protocol::Message::Batch {
                        sequence,
                        batch: batch.clone(),
                    },
                    crate::replication::protocol::Limits::default(),
                )
                .map_err(|error| match error {
                    crate::replication::protocol::Error::Record(error) => AofError::Format(error),
                    _ => AofError::Sequence,
                })?,
            )
        } else {
            None
        };
        let encoded = self
            .profile
            .encode_legacy(Record::Batch { sequence, batch }, self.config.limits)?;
        self.append_encoded(sequence, encoded, replication_frame)
    }

    fn append_memory(
        &mut self,
        origin: crate::storage::MutationOrigin,
        mutations: Vec<memory_format::Mutation>,
    ) -> Result<u64, AofError> {
        if !self.config.memory_profile {
            return Err(FormatError::Version(2).into());
        }
        let mut selected = None;
        for mutation in &mutations {
            let shard = self.config.layout.shard_for(mutation.key())?;
            if selected.is_some_and(|previous| previous != shard) {
                return Err(AofError::CrossShard);
            }
            selected = Some(shard);
        }
        let sequence = self.sequence.checked_add(1).ok_or(AofError::Sequence)?;
        let replication_frame = if self.journal.is_some() {
            Some(
                crate::replication::protocol::encode_memory(
                    &crate::replication::protocol::Message::MemoryBatch {
                        sequence,
                        origin,
                        mutations: mutations.clone(),
                    },
                    crate::replication::protocol::Limits {
                        max_frame_bytes: self.config.limits.max_record_bytes
                            + crate::replication::protocol::HEADER_BYTES
                            + 12,
                        record: self.config.limits,
                    },
                )
                .map_err(|error| match error {
                    crate::replication::protocol::Error::Record(error) => AofError::Format(error),
                    _ => AofError::Sequence,
                })?,
            )
        } else {
            None
        };
        let encoded = memory_format::encode(
            &memory_format::Record::Batch {
                sequence,
                origin,
                mutations,
            },
            self.config.limits,
        )?;
        self.append_encoded(sequence, encoded, replication_frame)
    }

    fn append_encoded(
        &mut self,
        sequence: u64,
        encoded: Vec<u8>,
        replication_frame: Option<bytes::Bytes>,
    ) -> Result<u64, AofError> {
        self.faults.hit("before_append")?;
        self.faults.write_append(&mut self.file, &encoded)?;
        self.faults.hit("after_append")?;
        self.sequence = sequence;
        self.bytes_since_compact = self
            .bytes_since_compact
            .saturating_add(encoded.len() as u64);
        self.dirty = true;
        self.diagnostics.update(|state| {
            state.records_written_total = state.records_written_total.saturating_add(1)
        });
        if let Some(compaction) = &mut self.compaction {
            if compaction.aborted {
                // The previous producer is still ending; do not open another snapshot in parallel.
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
        if let (Some(journal), Some(frame)) = (&self.journal, replication_frame) {
            journal
                .publish(sequence, frame)
                .map_err(|_| AofError::Unavailable)?;
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
        self.diagnostics.update(|state| {
            state.written_sequence = self.sequence;
            state.synced_sequence = self.sequence;
            state.syncs_total = state.syncs_total.saturating_add(1);
        });
        self.refresh_diagnostics();
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        snapshot: Vec<Mutation>,
        sequence: u64,
        metadata: ReplicationMetadata,
    ) -> Result<(), AofError> {
        if self.config.memory_profile {
            return Err(FormatError::Version(2).into());
        }
        self.install_profile_snapshot(
            snapshot
                .into_iter()
                .map(memory_format::Mutation::from_storage)
                .collect(),
            sequence,
            metadata,
        )
    }

    fn install_memory_snapshot(
        &mut self,
        snapshot: Vec<memory_format::Mutation>,
        sequence: u64,
        metadata: ReplicationMetadata,
    ) -> Result<(), AofError> {
        if !self.config.memory_profile {
            return Err(FormatError::Version(2).into());
        }
        self.install_profile_snapshot(snapshot, sequence, metadata)
    }

    fn install_profile_snapshot(
        &mut self,
        snapshot: Vec<memory_format::Mutation>,
        sequence: u64,
        metadata: ReplicationMetadata,
    ) -> Result<(), AofError> {
        if self.compaction.is_some() {
            return Err(AofError::Compacting);
        }
        let generation = self.generation.checked_add(1).ok_or(AofError::Sequence)?;
        // Validates all post-images and quotas before creating the candidate generation.
        let mut checked = Store::with_config(self.store_config, self.clock.clone())?;
        let mut usage = vec![0; self.config.layout.shard_count as usize];
        let mut previous = None;
        for mutation in &snapshot {
            if !matches!(
                mutation,
                memory_format::Mutation::Image { .. }
                    | memory_format::Mutation::Legacy(Mutation::Put { .. })
            ) || previous.as_ref().is_some_and(|key| key >= mutation.key())
            {
                return Err(AofError::Sequence);
            }
            previous = Some(mutation.key().clone());
            replay_routed_memory(
                &mut checked,
                std::slice::from_ref(mutation),
                self.config.layout,
                self.store_config.max_dataset_bytes,
                &mut usage,
                self.profile == Profile::ComposedLegacyMixed,
            )?;
        }
        drop(checked);
        let temporary = temporary_path(&self.config.directory, "replication-install");
        let destination = generation_path(&self.config.directory, generation);
        let profile = self.profile;
        let result = (|| {
            self.faults.hit("replication_before_install")?;
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            profile.write_header(&mut file, sequence, self.config.layout, Some(metadata))?;
            let mut entries = 0u64;
            let mut digest = 0;
            for mutation in snapshot {
                let encoded = profile.encode(
                    &memory_format::Record::Snapshot(mutation),
                    self.config.limits,
                )?;
                digest = format::snapshot_digest(digest, &encoded);
                entries += 1;
                file.write_all(&encoded)?;
            }
            file.write_all(&profile.encode(
                &memory_format::Record::Seal {
                    sequence,
                    entries,
                    digest,
                },
                self.config.limits,
            )?)?;
            file.sync_all()?;
            self.faults.hit("replication_before_publish")?;
            fs::rename(&temporary, &destination)?;
            sync_directory(&self.config.directory)?;
            self.faults.hit("replication_after_publish")?;
            self.file = file;
            self.generation = generation;
            self.sequence = sequence;
            self.replication = Some(metadata);
            if let Some(journal) = self.journal.take() {
                journal.close();
            }
            self.bytes_since_compact = 0;
            self.dirty = false;
            self.synced = Instant::now();
            self.diagnostics
                .update(|state| state.synced_sequence = sequence);
            self.refresh_diagnostics();
            Ok(())
        })();
        if let Err(error) = result {
            if destination.exists() {
                self.diagnostics.error(&error);
                return Err(AofError::Unavailable);
            }
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        if let Some(obsolete) = generation.checked_sub(2) {
            let path = generation_path(&self.config.directory, obsolete);
            if path.exists()
                && let Err(error) = fs::remove_file(path)
            {
                tracing::warn!(%error, "could not remove generation prior to installation");
            }
        }
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
        let replication = self.replication;
        let profile = self.profile;
        std::thread::spawn(move || {
            let result = (|| {
                faults.hit("compact_before_snapshot")?;
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&path)?;
                profile.write_header(&mut file, sequence, layout, replication)?;
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
                    let encoded = profile.encode_legacy(Record::Snapshot(mutation), limits)?;
                    entries += 1;
                    digest = format::snapshot_digest(digest, &encoded);
                    file.write_all(&encoded)?;
                }
                file.write_all(&profile.encode_legacy(
                    Record::Seal {
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
            self.diagnostics.error(&AofError::DeltaLimit);
            self.diagnostics.update(|state| {
                state.compaction_failures_total = state.compaction_failures_total.saturating_add(1)
            });
            return Ok(());
        }
        let result = (|| {
            let mut file = ready?;
            for delta in &compaction.delta {
                file.write_all(delta)?;
            }
            file.sync_all()?;
            self.faults.hit("compact_before_publish")?;
            // Unique destination: does not replace an open file. The old generation remains recoverable.
            fs::rename(&compaction.temporary, &compaction.destination)?;
            sync_directory(&self.config.directory)?;
            self.faults.hit("compact_after_publish")?;
            self.file = file;
            self.generation += 1;
            self.bytes_since_compact = 0;
            self.dirty = false;
            self.synced = Instant::now();
            // Keeps one complete prior generation, removing only the obsolete one.
            if let Some(obsolete) = self.generation.checked_sub(2) {
                let path = generation_path(&self.config.directory, obsolete);
                if path.exists()
                    && let Err(error) = fs::remove_file(path)
                {
                    tracing::warn!(%error, "could not remove old AOF generation");
                }
            }
            Ok(())
        })();
        // After publishing, failure is fatal: continuing in the old file would lose appends on next replay.
        let published = compaction.destination.exists();
        if result.is_err() && !published {
            let _ = fs::remove_file(&compaction.temporary);
        }
        let failed = result.is_err();
        self.diagnostics.update(|state| {
            if failed {
                state.compaction_failures_total = state.compaction_failures_total.saturating_add(1);
            } else {
                state.compactions_total = state.compactions_total.saturating_add(1);
                state.synced_sequence = self.sequence;
            }
        });
        if let Err(error) = &result {
            self.diagnostics.error(error);
        }
        self.refresh_diagnostics();
        let _ = compaction.reply.send(result);
        if failed && published {
            return Err(AofError::Unavailable);
        }
        Ok(())
    }

    fn refresh_diagnostics(&self) {
        self.diagnostics.update(|state| {
            state.written_sequence = self.sequence;
            state.generation = self.generation;
            state.bytes_since_compaction = self.bytes_since_compact;
            state.dirty = self.dirty;
            state.compacting = self.compaction.is_some();
        });
    }
}

#[cfg(test)]
mod lock_tests {
    use super::*;

    #[test]
    fn explicit_unlock_releases_ownership_while_duplicate_descriptor_survives() {
        let directory = temporary_path(&std::env::temp_dir(), "sider-lock-regression");
        fs::create_dir(&directory).unwrap();
        let config = AofConfig::new(directory.clone());
        let recovered = recover(
            config.clone(),
            StoreConfig::default(),
            Arc::new(crate::storage::SystemClock),
        )
        .unwrap();
        let duplicate = recovered.writer._lock.0.try_clone().unwrap();
        drop(recovered);
        let reopened = recover(
            config,
            StoreConfig::default(),
            Arc::new(crate::storage::SystemClock),
        )
        .unwrap();
        drop((reopened, duplicate));
        fs::remove_dir_all(directory).unwrap();
    }
}

#[cfg(test)]
mod routed_replay_tests {
    use super::*;
    use crate::storage::{SystemClock, Value};
    use bytes::Bytes;

    fn put(key: &'static [u8], bytes: usize) -> memory_format::Mutation {
        memory_format::Mutation::Legacy(Mutation::Put {
            key: Bytes::from_static(key),
            value: Value::String(Bytes::from(vec![b'x'; bytes])),
            expires_at_unix_ms: None,
        })
    }

    #[test]
    fn routed_replay_prepares_every_owner_before_a_mixed_apply() {
        let layout = DurableLayout {
            shard_count: 2,
            ..DurableLayout::default()
        };
        assert_ne!(
            layout.shard_for(b"a").unwrap(),
            layout.shard_for(b"b").unwrap()
        );
        let mut store = Store::with_config(
            StoreConfig {
                max_dataset_bytes: 600,
            },
            Arc::new(SystemClock),
        )
        .unwrap();
        let mut usage = vec![0; 2];
        let batch = [put(b"a", 10), put(b"b", 10)];

        assert!(matches!(
            replay_routed_memory(&mut store, &batch, layout, 600, &mut usage, false),
            Err(AofError::CrossShard)
        ));
        assert!(store.is_empty());
        assert_eq!(usage, [0, 0]);

        replay_routed_memory(&mut store, &batch, layout, 600, &mut usage, true).unwrap();
        assert_eq!(store.len(), 2);
        assert_eq!(usage, [139, 139]);
        let snapshot = store.snapshot();

        let rejected = [put(b"a", 20), put(b"b", 200)];
        assert!(matches!(
            replay_routed_memory(&mut store, &rejected, layout, 600, &mut usage, true),
            Err(AofError::ShardQuota { .. })
        ));
        assert_eq!(store.snapshot(), snapshot);
        assert_eq!(usage, [139, 139]);

        let replacement = [put(b"a", 20), put(b"b", 5)];
        replay_routed_memory(&mut store, &replacement, layout, 600, &mut usage, true).unwrap();
        assert_eq!(usage[layout.shard_for(b"a").unwrap()], 149);
        assert_eq!(usage[layout.shard_for(b"b").unwrap()], 134);
    }

    #[test]
    fn routed_replay_rejects_last_owner_quota_before_any_initial_write() {
        let layout = DurableLayout {
            shard_count: 2,
            ..DurableLayout::default()
        };
        let mut store = Store::with_config(
            StoreConfig {
                max_dataset_bytes: 600,
            },
            Arc::new(SystemClock),
        )
        .unwrap();
        let mut usage = vec![0; 2];
        assert!(matches!(
            replay_routed_memory(
                &mut store,
                &[put(b"a", 10), put(b"b", 200)],
                layout,
                600,
                &mut usage,
                true,
            ),
            Err(AofError::ShardQuota { .. })
        ));
        assert!(store.is_empty());
        assert_eq!(usage, [0, 0]);
    }
}

#[cfg(test)]
mod composed_local_tests {
    use super::*;
    use crate::storage::{MutationOrigin, SystemClock, Value};
    use bytes::Bytes;

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = temporary_path(&std::env::temp_dir(), "sider-composed-aof");
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let resolved = fs::canonicalize(&self.0).ok();
            let root = fs::canonicalize(std::env::temp_dir()).ok();
            if root.is_some()
                && resolved.as_ref().and_then(|path| path.parent()) == root.as_deref()
                && self
                    .0
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("sider-composed-aof-"))
            {
                fs::remove_dir_all(&self.0).unwrap();
            }
        }
    }

    fn config(directory: &Directory) -> (AofConfig, StoreConfig) {
        let mut aof = AofConfig::new(directory.0.clone());
        aof.layout = DurableLayout {
            shard_count: 2,
            ..DurableLayout::default()
        };
        (
            aof,
            StoreConfig {
                max_dataset_bytes: 600,
            },
        )
    }

    fn mixed_batch(right_size: usize) -> ResolvedBatch {
        ResolvedBatch {
            origin: MutationOrigin::Client,
            mutations: [(b"a".as_slice(), 10), (b"b".as_slice(), right_size)]
                .into_iter()
                .map(|(key, size)| Mutation::Put {
                    key: Bytes::copy_from_slice(key),
                    value: Value::String(Bytes::from(vec![b'x'; size])),
                    expires_at_unix_ms: None,
                })
                .collect(),
        }
    }

    fn recover_composed(
        config: AofConfig,
        store: StoreConfig,
        read_only: bool,
    ) -> Result<Recovered, AofError> {
        load_profile(
            config,
            store,
            Arc::new(SystemClock),
            Arc::new(NoFaults),
            read_only,
            Profile::ComposedLegacyMixed,
        )
    }

    #[tokio::test]
    async fn composed_local_append_compaction_and_restart_keep_one_mixed_decision() {
        let directory = Directory::new();
        let (config, store_config) = config(&directory);
        let recovered = recover_composed(config.clone(), store_config, false).unwrap();
        assert_eq!(recovered.metadata.format_version, 7);
        let (mut store, handle, task) = recovered.start();
        let batch = mixed_batch(10);
        assert_eq!(handle.append(batch.clone()).await.unwrap(), 1);
        store.replay(&batch.mutations).unwrap();
        assert_eq!(handle.flush().await.unwrap(), 1);
        assert!(matches!(
            handle.append_expected(2, batch.clone()).await,
            Err(AofError::Sequence)
        ));
        drop(handle);
        task.await.unwrap().unwrap();

        let replayed = recover_composed(config.clone(), store_config, false).unwrap();
        assert_eq!(replayed.metadata.sequence, 1);
        assert_eq!(replayed.store.snapshot(), store.snapshot());
        let (replayed_store, handle, task) = replayed.start();
        handle.compact(replayed_store.snapshot()).await.unwrap();
        let (sequence, _, _) = handle.status().await.unwrap();
        assert_eq!(sequence, 1);
        drop(handle);
        task.await.unwrap().unwrap();

        let current = generation_path(&config.directory, 1);
        let bytes = fs::read(&current).unwrap();
        let header = super::super::composed_format::read_header(bytes.as_slice()).unwrap();
        assert_eq!(
            header.record_version,
            super::super::composed_format::RecordVersion::Legacy
        );
        assert!(header.mixed_owner_batch);
        assert!(header.replication.is_none());
        assert!(matches!(
            format::read_header_with_layout(bytes.as_slice()),
            Err(FormatError::Version(7))
        ));
        assert!(matches!(
            recover(config.clone(), store_config, Arc::new(SystemClock)),
            Err(AofError::Format(FormatError::Version(7)))
        ));
        assert_eq!(fs::read(&current).unwrap(), bytes);

        let recovered = recover_composed(config, store_config, false).unwrap();
        assert_eq!(recovered.metadata.sequence, 1);
        assert_eq!(recovered.metadata.format_version, 7);
        assert_eq!(recovered.store.snapshot(), store.snapshot());
    }

    #[test]
    fn composed_local_recovery_rejects_hot_owner_without_repair() {
        let directory = Directory::new();
        let (config, store_config) = config(&directory);
        let recovered = recover_composed(config.clone(), store_config, false).unwrap();
        drop(recovered);
        let path = generation_path(&config.directory, 0);
        let mut bytes = fs::read(&path).unwrap();
        bytes.extend_from_slice(
            &format::encode(
                &Record::Batch {
                    sequence: 1,
                    batch: mixed_batch(200),
                },
                config.limits,
            )
            .unwrap(),
        );
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            recover_composed(config, store_config, false),
            Err(AofError::ShardQuota { .. })
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert!(!fs::read_dir(&directory.0).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("tail-")
        }));
    }

    #[test]
    fn composed_local_torn_mixed_record_never_replays_a_participant_prefix() {
        let directory = Directory::new();
        let (config, store_config) = config(&directory);
        drop(recover_composed(config.clone(), store_config, false).unwrap());
        let path = generation_path(&config.directory, 0);
        let sealed = fs::read(&path).unwrap();
        let batch = format::encode(
            &Record::Batch {
                sequence: 1,
                batch: mixed_batch(10),
            },
            config.limits,
        )
        .unwrap();
        for prefix in 1..batch.len() {
            let bytes = [sealed.as_slice(), &batch[..prefix]].concat();
            fs::write(&path, &bytes).unwrap();
            let recovered = recover_composed(config.clone(), store_config, true).unwrap();
            assert!(recovered.store.is_empty(), "prefix {prefix}");
            assert_eq!(recovered.metadata.sequence, 0, "prefix {prefix}");
            assert_eq!(recovered.metadata.incomplete_tail_bytes, prefix as u64);
            drop(recovered);
            assert_eq!(fs::read(&path).unwrap(), bytes, "prefix {prefix}");
        }
        let complete = [sealed.as_slice(), batch.as_slice()].concat();
        fs::write(&path, &complete).unwrap();
        let recovered = recover_composed(config.clone(), store_config, true).unwrap();
        assert_eq!(recovered.metadata.sequence, 1);
        assert_eq!(recovered.store.len(), 2);
        drop(recovered);

        let torn = [sealed.as_slice(), &batch[..3]].concat();
        fs::write(&path, &torn).unwrap();
        assert!(matches!(
            recover(config.clone(), store_config, Arc::new(SystemClock)),
            Err(AofError::Format(FormatError::Version(7)))
        ));
        assert_eq!(fs::read(&path).unwrap(), torn);
        let repaired = recover_composed(config, store_config, false).unwrap();
        assert!(repaired.store.is_empty());
        assert_eq!(repaired.metadata.incomplete_tail_bytes, 3);
        drop(repaired);
        assert_eq!(fs::read(&path).unwrap(), sealed);
        let saved: Vec<_> = fs::read_dir(&directory.0)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("tail-"))
            })
            .collect();
        assert_eq!(saved.len(), 1);
        assert_eq!(fs::read(&saved[0]).unwrap(), torn);
    }

    #[test]
    fn composed_local_reader_refuses_other_profiles_before_tail_repair() {
        use super::super::composed_format::{self, Header, RecordVersion};

        let directory = Directory::new();
        let (config, store_config) = config(&directory);
        drop(recover_composed(config.clone(), store_config, false).unwrap());
        let path = generation_path(&config.directory, 0);
        let header = Header {
            sequence: 0,
            layout: config.layout,
            record_version: RecordVersion::Legacy,
            mixed_owner_batch: true,
            replication: None,
        };
        for unsupported in [
            Header {
                record_version: RecordVersion::Memory,
                ..header
            },
            Header {
                mixed_owner_batch: false,
                ..header
            },
            Header {
                replication: Some(ReplicationMetadata {
                    role: Role::Primary,
                    epoch: [1; 16],
                }),
                ..header
            },
        ] {
            let mut bytes = composed_format::encode_header(unsupported)
                .unwrap()
                .to_vec();
            bytes.extend_from_slice(b"torn-first-record");
            fs::write(&path, &bytes).unwrap();
            assert!(matches!(
                recover_composed(config.clone(), store_config, false),
                Err(AofError::Format(FormatError::Corrupt(
                    "unsupported composed profile"
                )))
            ));
            assert_eq!(fs::read(&path).unwrap(), bytes);
            assert!(!fs::read_dir(&directory.0).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("tail-")
            }));
        }
    }
}
