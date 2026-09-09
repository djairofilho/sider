use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::{CHECKSUMS, Error, Limits, MANIFEST, Manifest, RestoreOptions, SNAPSHOT};
use crate::persistence::DurableLayout;
use crate::persistence::format::{self, Next, Record};
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
        .ok_or(Error::Invalid("destino precisa nomear diretório novo"))?;
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
        sync_directory(self.path.parent().ok_or(Error::Invalid("pai do destino"))?)?;
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
        return Err(Error::Invalid(
            "arquivo obrigatório regular, não vazio e limitado",
        ));
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
            return Err(Error::Invalid("arquivo excede teto de backup"));
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
}

impl Quotas {
    pub fn new(layout: DurableLayout, total: usize, clock: Arc<dyn Clock>) -> Result<Self, Error> {
        layout.quota(total, 0)?;
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
        })
    }
    pub fn add(&mut self, mutation: &Mutation) -> Result<(), Error> {
        let shard = self.layout.shard_for(mutation.key())?;
        self.store
            .replay(std::slice::from_ref(mutation))
            .map_err(crate::persistence::AofError::from)?;
        self.usage[shard] = self.usage[shard]
            .checked_add(self.store.used_bytes())
            .ok_or(Error::Invalid("overflow da quota"))?;
        if self.usage[shard] > self.layout.quota(self.total, shard)? {
            return Err(Error::Invalid("snapshot excede quota de um shard"));
        }
        self.live_entries += u64::from(!self.store.is_empty());
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
    if hash_file(file)? != (manifest.snapshot_sha256.clone(), manifest.snapshot_bytes) {
        return Err(Error::Invalid("checksum ou tamanho do snapshot"));
    }
    file.seek(SeekFrom::Start(0))?;
    let header = format::read_header_with_layout(&mut *file)?;
    if header.format_version != format::LAYOUT_VERSION
        || header.sequence != manifest.cursor.sequence
        || header.layout != manifest.layout
    {
        return Err(Error::Invalid("identidade do cabeçalho do snapshot"));
    }
    let mut previous = None;
    let mut digest = 0;
    let mut transport_digest = 0;
    let mut quotas = Quotas::new(manifest.layout, limits.max_dataset_bytes, freeze(clock))?;
    for _ in 0..manifest.entries {
        let Next::Record(Record::Snapshot(mutation @ Mutation::Put { .. })) =
            format::read_record(&mut *file, limits.record())?
        else {
            return Err(Error::Invalid("entrada de snapshot ausente"));
        };
        if previous.as_ref().is_some_and(|key| key >= mutation.key()) {
            return Err(Error::Invalid("ordem ou duplicata de chave"));
        }
        previous = Some(mutation.key().clone());
        quotas.add(&mutation)?;
        transport_digest = format::snapshot_digest(
            transport_digest,
            &crate::replication::protocol::encode(
                &crate::replication::protocol::Message::SnapshotEntry(mutation.clone()),
                limits.transport(),
            )?,
        );
        digest = format::snapshot_digest(
            digest,
            &format::encode(&Record::Snapshot(mutation), limits.record())?,
        );
    }
    if format::read_record(&mut *file, limits.record())?
        != Next::Record(Record::Seal {
            sequence: manifest.cursor.sequence,
            entries: manifest.entries,
            digest,
        })
        || format::read_record(&mut *file, limits.record())? != Next::End
        || transport_digest != manifest.transport_digest
        || hash_file(file)? != (manifest.snapshot_sha256.clone(), manifest.snapshot_bytes)
    {
        return Err(Error::Invalid("selo, EOF ou integridade do snapshot"));
    }
    Ok((quotas.usage, quotas.live_entries))
}

/// Verifica todos os bytes e quotas sem criar ou alterar diretórios de dados.
pub fn verify(
    source: &Path,
    layout: DurableLayout,
    limits: Limits,
    clock: Arc<dyn Clock>,
) -> Result<VerifiedBackup, Error> {
    limits.validate()?;
    layout.validate()?;
    let metadata = fs::symlink_metadata(source)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::Invalid("backup precisa ser diretório real"));
    }
    let mut bytes = Vec::new();
    regular(&source.join(MANIFEST), MAX_MANIFEST_BYTES)?
        .take(MAX_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(Error::Invalid("manifesto excede limite"));
    }
    let manifest = Manifest::parse(&bytes, limits)?;
    if manifest.layout != layout {
        return Err(Error::Invalid("layout solicitado difere do backup"));
    }
    let mut sums = Vec::new();
    regular(&source.join(CHECKSUMS), 512)?
        .take(513)
        .read_to_end(&mut sums)?;
    if sums != checksums(&bytes, &manifest.snapshot_sha256).as_bytes() {
        return Err(Error::Invalid("checksums do manifesto"));
    }
    let mut snapshot = regular(&source.join(SNAPSHOT), limits.max_snapshot_bytes)?;
    let (shard_usage, live_entries) = validate_file(&mut snapshot, &manifest, limits, clock)?;
    Ok(VerifiedBackup {
        manifest,
        shard_usage,
        live_entries,
    })
}

/// Recusa destino existente. Publica somente após conferir a cópia completa e o sync.
pub fn restore(options: RestoreOptions, clock: Arc<dyn Clock>) -> Result<VerifiedBackup, Error> {
    let verified = verify(
        &options.source,
        options.layout,
        options.limits,
        clock.clone(),
    )?;
    let source = options.source.canonicalize()?;
    if destination(&options.destination)?.starts_with(&source) {
        return Err(Error::Invalid("destino deve ficar fora do backup"));
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
    std::io::copy(
        &mut input.take(options.limits.max_snapshot_bytes + 1),
        &mut copied,
    )?;
    copied.sync_all()?;
    let (shard_usage, live_entries) =
        validate_file(&mut copied, &verified.manifest, options.limits, clock)?;
    drop(copied);
    fs::rename(&temporary, owned.path.join(GENERATION))?;
    owned.finish()?;
    Ok(VerifiedBackup {
        manifest: verified.manifest,
        shard_usage,
        live_entries,
    })
}
