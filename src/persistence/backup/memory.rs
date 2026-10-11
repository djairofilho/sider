//! Explicit offline schema-2 export and shared image validation.

use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;

use super::archive::{
    OwnedDirectory, Quotas, checksums, destination, freeze, hash_file, validate_file, write_new,
};
use super::{CHECKSUMS, Error, Limits, MANIFEST, Manifest, Profile, SNAPSHOT};
use crate::persistence::{AofConfig, FaultInjector, NoFaults, format, memory_format};
use crate::replication::Cursor;
use crate::storage::{Clock, StoreConfig};

#[derive(Clone, Debug)]
pub struct OfflineMemoryExportOptions {
    /// Must explicitly select record 2 and match the stopped source's layout.
    pub source: AofConfig,
    pub destination: PathBuf,
    /// Operator declaration, not an attestation of the stopped source's binary.
    pub source_sha: String,
    pub limits: Limits,
}

/// Exports a stopped memory AOF under its exclusive lock, without repair or source writes.
/// Output is sealed header 4; role/epoch and authentication are never installed.
pub fn export_memory_offline(
    options: OfflineMemoryExportOptions,
    clock: Arc<dyn Clock>,
) -> Result<Manifest, Error> {
    export_memory_offline_with_faults(options, clock, Arc::new(NoFaults))
}

/// Injectable write/sync/publication boundaries; production uses `NoFaults`.
pub fn export_memory_offline_with_faults(
    options: OfflineMemoryExportOptions,
    clock: Arc<dyn Clock>,
    faults: Arc<dyn FaultInjector>,
) -> Result<Manifest, Error> {
    options.limits.validate()?;
    options.source.validate()?;
    super::manifest::require_hex(&options.source_sha, 40)?;
    if !options.source.is_memory_profile()
        || options.source.limits.max_record_bytes > options.limits.max_record_bytes
        || options.source.limits.max_mutations > options.limits.max_mutations
    {
        return Err(Error::Invalid("memory export profile or capacity"));
    }
    let source = options.source.directory.canonicalize()?;
    let target = destination(&options.destination)?;
    if target.starts_with(&source) {
        return Err(Error::Invalid("destination must be outside source AOF"));
    }
    if target.exists() {
        return Err(Error::Invalid("destination already exists"));
    }
    let clock = freeze(clock);
    let recovered = crate::persistence::writer::recover_read_only(
        options.source.clone(),
        StoreConfig {
            max_dataset_bytes: options.limits.max_dataset_bytes,
        },
        clock.clone(),
    )?;
    if recovered.metadata.incomplete_tail_bytes != 0 {
        return Err(Error::Invalid("source has incomplete AOF tail"));
    }
    // Snapshot shares immutable values; materialize only one record-2 image at a time.
    let mutations = recovered.store.snapshot();
    let sequence = recovered.metadata.sequence;
    let layout = recovered.metadata.layout;
    let cursor = Cursor {
        epoch: recovered
            .metadata
            .replication
            .map_or([0; 16], |metadata| metadata.epoch),
        sequence,
    };
    let mut directory = OwnedDirectory::new(&target)?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(directory.path.join(SNAPSHOT))?;
    let header = memory_format::encode_header(memory_format::Header {
        sequence,
        layout,
        replication: None,
    })?;
    faults.write_append(&mut file, &header)?;
    let mut snapshot_bytes = header.len() as u64;
    let mut transfer_bytes = 0u64;
    let mut digest = 0;
    let mut transport_digest = 0;
    let mut quotas = Quotas::new(layout, options.limits.max_dataset_bytes, clock.clone())?;
    for mutation in &mutations {
        let record = memory_format::encode(
            &memory_format::Record::Snapshot(memory_format::Mutation::from_storage(
                mutation.clone(),
            )),
            options.limits.record(),
        )?;
        let framed = snapshot_frame(&record, options.limits)?;
        snapshot_bytes = snapshot_bytes
            .checked_add(record.len() as u64)
            .filter(|size| {
                size.checked_add(33)
                    .is_some_and(|sealed| sealed <= options.limits.max_snapshot_bytes)
            })
            .ok_or(Error::Invalid("snapshot exceeds archive budget"))?;
        transfer_bytes = transfer_bytes
            .checked_add(framed.len() as u64)
            .filter(|size| *size <= options.limits.max_snapshot_bytes)
            .ok_or(Error::Invalid("snapshot exceeds transfer budget"))?;
        quotas.add(mutation)?;
        digest = format::snapshot_digest(digest, &record);
        transport_digest = format::snapshot_digest(transport_digest, &framed);
        faults.write_append(&mut file, &record)?;
    }
    let seal = memory_format::encode(
        &memory_format::Record::Seal {
            sequence,
            entries: mutations.len() as u64,
            digest,
        },
        options.limits.record(),
    )?;
    if snapshot_bytes
        .checked_add(seal.len() as u64)
        .is_none_or(|size| size > options.limits.max_snapshot_bytes)
    {
        return Err(Error::Invalid("snapshot exceeds archive budget"));
    }
    faults.write_append(&mut file, &seal)?;
    faults.hit("backup_export_before_snapshot_sync")?;
    file.sync_all()?;
    faults.hit("backup_export_after_snapshot_sync")?;
    let (snapshot_sha256, snapshot_bytes) = hash_file(&mut file)?;
    let manifest = Manifest {
        profile: Profile::Memory,
        sider_version: env!("CARGO_PKG_VERSION").into(),
        source_sha: options.source_sha,
        cursor,
        layout,
        source_max_record_bytes: options.source.limits.max_record_bytes as u32,
        source_max_mutations: options.source.limits.max_mutations as u32,
        source_max_snapshot_bytes: options.limits.max_snapshot_bytes,
        required_capabilities: None,
        validation_max_dataset_bytes: options.limits.max_dataset_bytes as u64,
        entries: mutations.len() as u64,
        snapshot_bytes,
        snapshot_sha256,
        transport_digest,
        received_at_unix_ms: clock.unix_millis(),
    };
    manifest.validate(options.limits)?;
    validate_file(&mut file, &manifest, options.limits, clock)?;
    drop(file);
    let mut encoded = serde_json::to_vec_pretty(&manifest.json())?;
    encoded.push(b'\n');
    write_new(&directory.path.join(MANIFEST), &encoded)?;
    faults.hit("backup_export_after_manifest_sync")?;
    write_new(
        &directory.path.join(CHECKSUMS),
        checksums(&encoded, &manifest.snapshot_sha256).as_bytes(),
    )?;
    faults.hit("backup_export_before_publication")?;
    directory.finish()?;
    Ok(manifest)
}

pub(super) fn storage_image(
    mutation: &memory_format::Mutation,
) -> Result<crate::storage::Mutation, Error> {
    Ok(match mutation {
        memory_format::Mutation::Legacy(mutation @ crate::storage::Mutation::Put { .. }) => {
            mutation.clone()
        }
        memory_format::Mutation::Image {
            key,
            collection,
            key_deadline,
        } => crate::storage::Mutation::Put {
            key: key.clone(),
            value: crate::storage::Value::Memory(Arc::new(collection.clone())),
            expires_at_unix_ms: *key_deadline,
        },
        _ => return Err(Error::Invalid("snapshot must contain complete key images")),
    })
}

/// Frozen transport-2 tag-4 envelope used to verify the archive digest.
pub(super) fn snapshot_frame(record: &[u8], limits: Limits) -> Result<Vec<u8>, Error> {
    use crate::replication::protocol::{HEADER_BYTES, MAGIC};
    if record.len() > limits.max_record_bytes + 12 {
        return Err(Error::Invalid("snapshot record exceeds transfer budget"));
    }
    let size = u32::try_from(record.len()).map_err(|_| Error::Invalid("snapshot frame size"))?;
    let mut frame = Vec::with_capacity(HEADER_BYTES + record.len());
    frame.extend_from_slice(MAGIC);
    frame.extend_from_slice(&2u16.to_le_bytes());
    frame.extend_from_slice(&[4, 0]);
    frame.extend_from_slice(&size.to_le_bytes());
    frame.extend_from_slice(&(!size).to_le_bytes());
    let digest = format::snapshot_digest(format::checksum(&frame), record);
    frame.extend_from_slice(&digest.to_le_bytes());
    frame.extend_from_slice(record);
    Ok(frame)
}
