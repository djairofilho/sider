use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::{CHECKSUMS, Error, Limits, MANIFEST, Manifest, Profile, RestoreOptions, SNAPSHOT};
use crate::persistence::DurableLayout;
use crate::persistence::format;
use crate::persistence::memory_format::{Next, Record};
use crate::persistence::writer::{DirectoryLock, sync_directory};
use crate::storage::{Clock, Mutation, Store, StoreConfig};

const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const GENERATION: &str = "generation-00000000000000000000.aof";

pub struct VerifiedBackup {
    pub manifest: Manifest,
    pub shard_usage: Vec<usize>,
    pub live_entries: u64,
}

pub(super) struct OwnedDirectory {
    pub path: PathBuf,
    pub committed: bool,
}

pub(super) fn destination(path: &Path) -> Result<PathBuf, Error> {
    let name = path
        .file_name()
        .ok_or(Error::Invalid("destination must name a new directory"))?;
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    Ok(parent.canonicalize()?.join(name))
}

impl OwnedDirectory {
    pub fn new(path: &Path) -> Result<Self, Error> {
        let path = destination(path)?;
        fs::create_dir(&path)?;
        Ok(Self {
            path,
            committed: false,
        })
    }
    pub fn finish(&mut self) -> Result<(), Error> {
        sync_directory(&self.path)?;
        sync_directory(
            self.path
                .parent()
                .ok_or(Error::Invalid("destination parent"))?,
        )?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for OwnedDirectory {
    fn drop(&mut self) {
        if !self.committed {
            for name in [
                SNAPSHOT,
                MANIFEST,
                CHECKSUMS,
                GENERATION,
                "restore.tmp",
                "writer.lock",
            ] {
                let _ = fs::remove_file(self.path.join(name));
            }
            let _ = fs::remove_dir(&self.path);
        }
    }
}

pub(super) fn write_new(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn regular(path: &Path, limit: u64) -> Result<File, Error> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > limit
    {
        return Err(Error::Invalid("required regular, non-empty, bounded file"));
    }
    Ok(File::open(path)?)
}

pub(super) fn hash_file(file: &mut File) -> Result<(String, u64), Error> {
    file.seek(SeekFrom::Start(0))?;
    let mut hash = Sha256::new();
    let mut count = 0;
    let mut buffer = [0u8; 8192];
    loop {
        let bytes = file.read(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        count += bytes as u64;
        if count > super::MAX_BACKUP_BYTES {
            return Err(Error::Invalid("file exceeds backup ceiling"));
        }
        hash.update(&buffer[..bytes]);
    }
    Ok((format!("{:x}", hash.finalize()), count))
}

pub(super) fn checksums(manifest: &[u8], snapshot_hash: &str) -> String {
    format!(
        "{:x}  {MANIFEST}\n{snapshot_hash}  {SNAPSHOT}\n",
        Sha256::digest(manifest)
    )
}

pub(super) struct Quotas {
    store: Store,
    layout: DurableLayout,
    total: usize,
    pub usage: Vec<usize>,
    pub live_entries: u64,
    unix_ms: i64,
}

impl Quotas {
    pub fn new(layout: DurableLayout, total: usize, clock: Arc<dyn Clock>) -> Result<Self, Error> {
        layout.quota(total, 0)?;
        let unix_ms = clock.unix_millis();
        Ok(Self {
            store: Store::with_config(
                StoreConfig {
                    max_dataset_bytes: isize::MAX as usize,
                },
                clock,
            )?,
            layout,
            total,
            usage: vec![0; layout.shard_count as usize],
            live_entries: 0,
            unix_ms,
        })
    }
    pub fn add(&mut self, mutation: &Mutation) -> Result<(), Error> {
        let shard = self.layout.shard_for(mutation.key())?;
        self.store
            .replay(std::slice::from_ref(mutation))
            .map_err(crate::persistence::AofError::from)?;
        self.usage[shard] = self.usage[shard]
            .checked_add(self.store.used_bytes())
            .ok_or(Error::Invalid("quota overflow"))?;
        if self.usage[shard] > self.layout.quota(self.total, shard)? {
            return Err(Error::Invalid("snapshot exceeds one shard's quota"));
        }
        let live = match mutation {
            Mutation::Put {
                expires_at_unix_ms, ..
            } => expires_at_unix_ms.is_none_or(|deadline| deadline > self.unix_ms),
            Mutation::Delete { .. } => false,
        };
        self.live_entries += u64::from(live && !self.store.is_empty());
        self.store
            .replay(&[Mutation::Delete {
                key: mutation.key().clone(),
            }])
            .map_err(crate::persistence::AofError::from)?;
        Ok(())
    }
}

struct FrozenClock {
    now: tokio::time::Instant,
    unix_ms: i64,
}
impl Clock for FrozenClock {
    fn now(&self) -> tokio::time::Instant {
        self.now
    }
    fn unix_millis(&self) -> i64 {
        self.unix_ms
    }
}
pub(super) fn freeze(clock: Arc<dyn Clock>) -> Arc<dyn Clock> {
    Arc::new(FrozenClock {
        now: clock.now(),
        unix_ms: clock.unix_millis(),
    })
}

pub(super) fn validate_file(
    file: &mut File,
    manifest: &Manifest,
    limits: Limits,
    clock: Arc<dyn Clock>,
) -> Result<(Vec<usize>, u64), Error> {
    file.seek(SeekFrom::Start(0))?;
    let codec = crate::persistence::profile::Profile::selected(manifest.profile == Profile::Memory);
    let header = codec.read_header(&mut *file)?;
    if header.format_version != manifest.profile.versions().1
        || header.replication.is_some()
        || header.sequence != manifest.cursor.sequence
        || header.layout != manifest.layout
    {
        return Err(Error::Invalid("snapshot header identity"));
    }
    if hash_file(file)? != (manifest.snapshot_sha256.clone(), manifest.snapshot_bytes) {
        return Err(Error::Invalid("snapshot checksum or size"));
    }
    file.seek(SeekFrom::Start(header.bytes as u64))?;
    let mut previous = None;
    let mut digest = 0;
    let mut transport_digest = 0;
    let mut quotas = Quotas::new(manifest.layout, limits.max_dataset_bytes, freeze(clock))?;
    let mut transfer_bytes = 0u64;
    for _ in 0..manifest.entries {
        let Next::Record(record) = codec.read_record(&mut *file, limits.record())? else {
            return Err(Error::Invalid("missing snapshot entry"));
        };
        let Record::Snapshot(image) = *record else {
            return Err(Error::Invalid("missing snapshot entry"));
        };
        let mutation = super::memory::storage_image(&image)?;
        if previous.as_ref().is_some_and(|key| key >= mutation.key()) {
            return Err(Error::Invalid("key order or duplicate"));
        }
        previous = Some(mutation.key().clone());
        quotas.add(&mutation)?;
        let encoded = codec.encode(&Record::Snapshot(image), limits.record())?;
        let framed = if manifest.profile == Profile::Memory {
            super::memory::snapshot_frame(&encoded, limits)?
        } else {
            crate::replication::protocol::encode(
                &crate::replication::protocol::Message::SnapshotEntry(mutation.clone()),
                limits.transport(),
            )?
            .to_vec()
        };
        transfer_bytes = transfer_bytes
            .checked_add(framed.len() as u64)
            .ok_or(Error::Invalid("snapshot transfer size overflow"))?;
        if manifest.profile == Profile::Memory && transfer_bytes > limits.max_snapshot_bytes {
            return Err(Error::Invalid("snapshot exceeds transfer budget"));
        }
        transport_digest = format::snapshot_digest(transport_digest, &framed);
        digest = format::snapshot_digest(digest, &encoded);
    }
    if codec.read_record(&mut *file, limits.record())?
        != Next::Record(Box::new(Record::Seal {
            sequence: manifest.cursor.sequence,
            entries: manifest.entries,
            digest,
        }))
        || codec.read_record(&mut *file, limits.record())? != Next::End
        || transport_digest != manifest.transport_digest
        || hash_file(file)? != (manifest.snapshot_sha256.clone(), manifest.snapshot_bytes)
    {
        return Err(Error::Invalid("snapshot seal, EOF, or integrity"));
    }
    Ok((quotas.usage, quotas.live_entries))
}

/// Verifies all bytes and quotas without creating or changing data directories.
pub fn verify(
    source: &Path,
    layout: DurableLayout,
    limits: Limits,
    clock: Arc<dyn Clock>,
) -> Result<VerifiedBackup, Error> {
    verify_profile(source, layout, limits, clock, Profile::Legacy)
}

/// Verifies only the frozen schema-2/header-4/record-2 archive profile.
pub fn verify_memory(
    source: &Path,
    layout: DurableLayout,
    limits: Limits,
    clock: Arc<dyn Clock>,
) -> Result<VerifiedBackup, Error> {
    verify_profile(source, layout, limits, clock, Profile::Memory)
}

fn verify_profile(
    source: &Path,
    layout: DurableLayout,
    limits: Limits,
    clock: Arc<dyn Clock>,
    profile: Profile,
) -> Result<VerifiedBackup, Error> {
    limits.validate()?;
    layout.validate()?;
    let metadata = fs::symlink_metadata(source)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::Invalid("backup must be a real directory"));
    }
    let mut bytes = Vec::new();
    regular(&source.join(MANIFEST), MAX_MANIFEST_BYTES)?
        .take(MAX_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(Error::Invalid("manifest exceeds limit"));
    }
    let manifest = Manifest::parse(&bytes, limits, profile)?;
    if manifest.layout != layout {
        return Err(Error::Invalid("requested layout differs from backup"));
    }
    let mut sums = Vec::new();
    regular(&source.join(CHECKSUMS), 512)?
        .take(513)
        .read_to_end(&mut sums)?;
    if sums != checksums(&bytes, &manifest.snapshot_sha256).as_bytes() {
        return Err(Error::Invalid("manifest checksums"));
    }
    let mut snapshot = regular(&source.join(SNAPSHOT), limits.max_snapshot_bytes)?;
    let (shard_usage, live_entries) = validate_file(&mut snapshot, &manifest, limits, clock)?;
    Ok(VerifiedBackup {
        manifest,
        shard_usage,
        live_entries,
    })
}

/// Rejects an existing destination. Publishes only after checking the complete copy and sync.
pub fn restore(options: RestoreOptions, clock: Arc<dyn Clock>) -> Result<VerifiedBackup, Error> {
    restore_profile(
        options,
        clock,
        Profile::Legacy,
        Arc::new(crate::persistence::NoFaults),
    )
}

/// Restores only schema 2 into a new header-4 directory; never upgrades legacy bytes.
pub fn restore_memory(
    options: RestoreOptions,
    clock: Arc<dyn Clock>,
) -> Result<VerifiedBackup, Error> {
    restore_memory_with_faults(options, clock, Arc::new(crate::persistence::NoFaults))
}

/// Deterministic copy/publication failure boundaries for the memory restore path.
pub fn restore_memory_with_faults(
    options: RestoreOptions,
    clock: Arc<dyn Clock>,
    faults: Arc<dyn crate::persistence::FaultInjector>,
) -> Result<VerifiedBackup, Error> {
    restore_profile(options, clock, Profile::Memory, faults)
}

fn restore_profile(
    options: RestoreOptions,
    clock: Arc<dyn Clock>,
    profile: Profile,
    faults: Arc<dyn crate::persistence::FaultInjector>,
) -> Result<VerifiedBackup, Error> {
    let clock = freeze(clock);
    let verified = verify_profile(
        &options.source,
        options.layout,
        options.limits,
        clock.clone(),
        profile,
    )?;
    let source = options.source.canonicalize()?;
    if destination(&options.destination)?.starts_with(&source) {
        return Err(Error::Invalid("destination must be outside backup"));
    }
    let mut owned = OwnedDirectory::new(&options.destination)?;
    let _lock = DirectoryLock::acquire(&owned.path, true)?;
    let input = regular(&source.join(SNAPSHOT), options.limits.max_snapshot_bytes)?;
    let temporary = owned.path.join("restore.tmp");
    let mut copied = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let mut input = input.take(options.limits.max_snapshot_bytes + 1);
    let mut buffer = [0; 8192];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        faults.write_append(&mut copied, &buffer[..count])?;
    }
    copied.sync_all()?;
    faults.hit("backup_restore_after_copy_sync")?;
    let (shard_usage, live_entries) =
        validate_file(&mut copied, &verified.manifest, options.limits, clock)?;
    drop(copied);
    fs::rename(&temporary, owned.path.join(GENERATION))?;
    faults.hit("backup_restore_after_generation_publish")?;
    owned.finish()?;
    Ok(VerifiedBackup {
        manifest: verified.manifest,
        shard_usage,
        live_entries,
    })
}
