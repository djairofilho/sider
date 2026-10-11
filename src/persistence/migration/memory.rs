//! Explicit offline codec upgrade; the existing migrator and CLI remain legacy.

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::{Destination, FrozenClock, MigrationOptions, MigrationReport};
use crate::persistence::writer::{DirectoryLock, recover_read_only, sync_directory};
use crate::persistence::{AofError, FaultInjector, NoFaults, format, memory_format};
use crate::storage::{Clock, Mutation, Value};

#[derive(Clone, Debug)]
pub struct MemoryMigrationReport {
    pub migration: MigrationReport,
    pub source_record_version: u32,
    pub destination_header_version: u32,
    pub destination_record_version: u32,
    /// Hash identifies the selected complete source generation, including an incomplete tail.
    pub source_generation: OsString,
    pub source_sha256: String,
    pub destination_sha256: String,
    /// Physical retained counts, including elapsed state; migration does not prune memory.
    pub memory_collections: usize,
    pub memory_members: usize,
}

/// Explicitly migrates a stopped legacy or memory source into a new header-4 directory.
/// Requires `destination.with_memory_profile()` and an explicitly configured source profile.
/// Source files are never repaired or upgraded; incomplete tails are reported unchanged.
pub fn migrate_memory_offline(
    options: MigrationOptions,
    clock: Arc<dyn Clock>,
) -> Result<MemoryMigrationReport, AofError> {
    migrate_memory_offline_with_faults(options, clock, Arc::new(NoFaults))
}

/// Injectable append, sync and generation-publication failures; production uses `NoFaults`.
pub fn migrate_memory_offline_with_faults(
    options: MigrationOptions,
    clock: Arc<dyn Clock>,
    faults: Arc<dyn FaultInjector>,
) -> Result<MemoryMigrationReport, AofError> {
    if !options.destination.is_memory_profile() {
        return Err(AofError::Migration(
            "explicit record-2 destination required; downgrade refused",
        ));
    }
    options.source.validate()?;
    options.destination.validate()?;
    options.source_store.validate()?;
    options.destination_store.validate()?;
    options
        .source
        .layout
        .quota(options.source_store.max_dataset_bytes, 0)?;
    options
        .destination
        .layout
        .quota(options.destination_store.max_dataset_bytes, 0)?;
    let source = options.source.directory.canonicalize()?;
    let target = super::destination_path(&options.destination.directory)?;
    if target.starts_with(&source) {
        return Err(AofError::Migration(
            "destination must be outside the source tree",
        ));
    }
    if target.exists() {
        return Err(AofError::Migration("destination already exists"));
    }
    let clock: Arc<dyn Clock> = Arc::new(FrozenClock {
        now: clock.now(),
        unix_ms: clock.unix_millis(),
    });
    // Keeps the source lock until all destination checks and publication have finished.
    let recovered = recover_read_only(options.source.clone(), options.source_store, clock.clone())?;
    let source_generation_path = recovered.generation_path();
    let generation = source_generation_path
        .file_name()
        .ok_or(AofError::Sequence)?
        .to_owned();
    let source_sha256 = hash(&source_generation_path)?;
    let snapshot = recovered.store.snapshot();
    let usage = super::validate_snapshot(
        &snapshot,
        options.destination.layout,
        options.destination_store,
        clock,
    )?;
    let sequence = recovered.metadata.sequence;
    let header = memory_format::encode_header(memory_format::Header {
        sequence,
        layout: options.destination.layout,
        replication: None,
    })?;
    let (mut collections, mut members) = (0usize, 0usize);
    // Preflight every complete output record before reserving the new directory.
    // Only one encoded image is materialized at a time; immutable values remain shared.
    for mutation in &snapshot {
        memory_format::encode(&image(mutation), options.destination.limits)?;
        if let Mutation::Put {
            value: Value::Memory(collection),
            ..
        } = mutation
        {
            collections += 1;
            members =
                members
                    .checked_add(collection.retained_count())
                    .ok_or(AofError::Migration(
                        "retained count exceeds representable size",
                    ))?;
        }
    }
    let entries = u64::try_from(snapshot.len())
        .map_err(|_| AofError::Migration("entry count exceeds representable size"))?;
    memory_format::encode(
        &memory_format::Record::Seal {
            sequence,
            entries,
            digest: 0,
        },
        options.destination.limits,
    )?;

    fs::create_dir(&target)?;
    let mut destination = Destination {
        temporary: target.join("migration.tmp"),
        published: target.join("generation-00000000000000000000.aof"),
        directory: target,
        committed: false,
    };
    let lock = DirectoryLock::acquire(&destination.directory, true)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&destination.temporary)?;
    faults.hit("memory_migration_before_append")?;
    faults.write_append(&mut file, &header)?;
    let mut digest = 0;
    for mutation in &snapshot {
        let encoded = memory_format::encode(&image(mutation), options.destination.limits)?;
        digest = format::snapshot_digest(digest, &encoded);
        faults.write_append(&mut file, &encoded)?;
    }
    let seal = memory_format::Record::Seal {
        sequence,
        entries,
        digest,
    };
    faults.write_append(
        &mut file,
        &memory_format::encode(&seal, options.destination.limits)?,
    )?;
    faults.hit("memory_migration_before_sync")?;
    file.sync_all()?;
    faults.hit("memory_migration_after_sync")?;
    drop(file);
    verify(&destination.temporary, &snapshot, &seal, &options, sequence)?;
    let destination_sha256 = hash(&destination.temporary)?;
    if hash(&source_generation_path)? != source_sha256 {
        return Err(AofError::Migration(
            "source generation changed during migration",
        ));
    }
    faults.hit("memory_migration_before_publication")?;
    fs::rename(&destination.temporary, &destination.published)?;
    faults.hit("memory_migration_after_publication")?;
    sync_directory(&destination.directory)?;
    if let Some(parent) = destination.directory.parent() {
        sync_directory(parent)?;
    }
    faults.hit("memory_migration_after_directory_sync")?;
    destination.committed = true;
    drop(lock);
    Ok(MemoryMigrationReport {
        migration: MigrationReport {
            source: recovered.metadata.clone(),
            destination_layout: options.destination.layout,
            sequence,
            entries: snapshot.len(),
            destination_shard_usage: usage,
        },
        source_record_version: if options.source.is_memory_profile() {
            2
        } else {
            1
        },
        destination_header_version: memory_format::HEADER_VERSION,
        destination_record_version: memory_format::RECORD_VERSION,
        source_generation: generation,
        source_sha256,
        destination_sha256,
        memory_collections: collections,
        memory_members: members,
    })
}

fn image(mutation: &Mutation) -> memory_format::Record {
    memory_format::Record::Snapshot(memory_format::Mutation::from_storage(mutation.clone()))
}

fn verify(
    path: &Path,
    snapshot: &[Mutation],
    seal: &memory_format::Record,
    options: &MigrationOptions,
    sequence: u64,
) -> Result<(), AofError> {
    let mut file = File::open(path)?;
    let header = memory_format::read_header(&mut file)?;
    if header.sequence != sequence
        || header.layout != options.destination.layout
        || header.replication.is_some()
    {
        return Err(AofError::Migration("destination header diverged"));
    }
    for mutation in snapshot {
        if memory_format::read_record(&mut file, options.destination.limits)?
            != memory_format::Next::Record(Box::new(image(mutation)))
        {
            return Err(AofError::Migration("destination snapshot diverged"));
        }
    }
    if memory_format::read_record(&mut file, options.destination.limits)?
        != memory_format::Next::Record(Box::new(seal.clone()))
        || memory_format::read_record(&mut file, options.destination.limits)?
            != memory_format::Next::End
    {
        return Err(AofError::Migration("destination seal or end diverged"));
    }
    Ok(())
}

fn hash(path: &Path) -> Result<String, AofError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(AofError::Migration("generation must be a regular file"));
    }
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 8192];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
