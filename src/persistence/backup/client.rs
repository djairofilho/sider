use std::fs::OpenOptions;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout};

use super::archive::{
    OwnedDirectory, Quotas, checksums, freeze, hash_file, validate_file, write_new,
};
use super::manifest::require_hex;
use super::{CHECKSUMS, Error, Limits, MANIFEST, Manifest, SNAPSHOT};
use crate::persistence::DurableLayout;
use crate::persistence::format::{self, Record};
use crate::replication::protocol::{self, Message};
use crate::security::{InternalPeer, InternalPhase};
use crate::storage::{Clock, Mutation};

#[derive(Clone, Debug)]
pub struct ExportOptions {
    pub source: SocketAddr,
    pub destination: PathBuf,
    pub source_sha: String,
    pub limits: Limits,
}

impl ExportOptions {
    fn validate(&self) -> Result<(), Error> {
        self.limits.validate()?;
        require_hex(&self.source_sha, 40)?;
        if self.source.port() == 0 {
            return Err(Error::Invalid("source port must be explicit"));
        }
        Ok(())
    }
}

fn remaining(deadline: Instant) -> Result<Duration, Error> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|value| !value.is_zero())
        .ok_or(Error::Timeout)
}

/// Connects to the internal listener and receives only one snapshot, without ACK or incremental flow.
pub async fn export(options: ExportOptions, clock: Arc<dyn Clock>) -> Result<Manifest, Error> {
    options.validate()?;
    let deadline = Instant::now() + options.limits.timeout;
    let mut stream = timeout(remaining(deadline)?, TcpStream::connect(options.source))
        .await
        .map_err(|_| Error::Timeout)??;
    receive(
        &mut stream,
        options,
        clock,
        deadline,
        None,
        super::Profile::Legacy,
    )
    .await
}

/// Exports through authenticated mutual TLS, checking both IP SAN and expected URI.
pub async fn export_secure(
    options: ExportOptions,
    clock: Arc<dyn Clock>,
    transport: &crate::security::InternalTransport,
) -> Result<Manifest, Error> {
    options.validate()?;
    let deadline = Instant::now() + options.limits.timeout;
    let (mut stream, peer) = transport
        .connect_with_peer(options.source)
        .await
        .map_err(|_| Error::Invalid("internal TLS authentication failed"))?;
    receive(
        &mut stream,
        options,
        clock,
        deadline,
        Some(&peer),
        super::Profile::Legacy,
    )
    .await
}

/// Injectable transport for tests; applies the same limits and contract as the TCP client.
pub async fn export_stream(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    options: ExportOptions,
    clock: Arc<dyn Clock>,
) -> Result<Manifest, Error> {
    options.validate()?;
    let deadline = Instant::now() + options.limits.timeout;
    receive(
        stream,
        options,
        clock,
        deadline,
        None,
        super::Profile::Legacy,
    )
    .await
}

/// Explicit live record-2 backup consumer over an injected stream.
pub async fn export_memory_stream(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    options: ExportOptions,
    clock: Arc<dyn Clock>,
) -> Result<Manifest, Error> {
    options.validate()?;
    let deadline = Instant::now() + options.limits.timeout;
    receive(
        stream,
        options,
        clock,
        deadline,
        None,
        super::Profile::Memory,
    )
    .await
}

/// Connects to an explicitly configured native transport-2 source.
pub async fn export_memory(
    options: ExportOptions,
    clock: Arc<dyn Clock>,
) -> Result<Manifest, Error> {
    options.validate()?;
    let deadline = Instant::now() + options.limits.timeout;
    let mut stream = timeout(remaining(deadline)?, TcpStream::connect(options.source))
        .await
        .map_err(|_| Error::Timeout)??;
    receive(
        &mut stream,
        options,
        clock,
        deadline,
        None,
        super::Profile::Memory,
    )
    .await
}

/// Exports record-2 data through authenticated internal mutual TLS.
pub async fn export_memory_secure(
    options: ExportOptions,
    clock: Arc<dyn Clock>,
    transport: &crate::security::InternalTransport,
) -> Result<Manifest, Error> {
    options.validate()?;
    let deadline = Instant::now() + options.limits.timeout;
    let (mut stream, peer) = transport
        .connect_with_peer(options.source)
        .await
        .map_err(|_| Error::Invalid("internal TLS authentication failed"))?;
    receive(
        &mut stream,
        options,
        clock,
        deadline,
        Some(&peer),
        super::Profile::Memory,
    )
    .await
}

/// Explicit header-7/record-1 backup consumer over an injected stream.
pub async fn export_composed_stream(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    options: ExportOptions,
    clock: Arc<dyn Clock>,
) -> Result<Manifest, Error> {
    options.validate()?;
    let deadline = Instant::now() + options.limits.timeout;
    receive(
        stream,
        options,
        clock,
        deadline,
        None,
        super::Profile::Composed,
    )
    .await
}

/// Connects to an explicitly selected transport-3 source.
pub async fn export_composed(
    options: ExportOptions,
    clock: Arc<dyn Clock>,
) -> Result<Manifest, Error> {
    options.validate()?;
    let deadline = Instant::now() + options.limits.timeout;
    let mut stream = timeout(remaining(deadline)?, TcpStream::connect(options.source))
        .await
        .map_err(|_| Error::Timeout)??;
    receive(
        &mut stream,
        options,
        clock,
        deadline,
        None,
        super::Profile::Composed,
    )
    .await
}

/// Exports a composed snapshot through authenticated internal mutual TLS.
pub async fn export_composed_secure(
    options: ExportOptions,
    clock: Arc<dyn Clock>,
    transport: &crate::security::InternalTransport,
) -> Result<Manifest, Error> {
    options.validate()?;
    let deadline = Instant::now() + options.limits.timeout;
    let (mut stream, peer) = transport
        .connect_with_peer(options.source)
        .await
        .map_err(|_| Error::Invalid("internal TLS authentication failed"))?;
    receive(
        &mut stream,
        options,
        clock,
        deadline,
        Some(&peer),
        super::Profile::Composed,
    )
    .await
}

async fn read_profile(
    stream: &mut (impl AsyncRead + Unpin),
    limits: protocol::Limits,
    deadline: Duration,
    phase: InternalPhase,
    profile: super::Profile,
) -> Result<(Bytes, Message), protocol::Error> {
    match profile {
        super::Profile::Legacy => protocol::read_in_phase(stream, limits, deadline, phase).await,
        super::Profile::Memory => {
            protocol::read_memory_in_phase(stream, limits, deadline, phase).await
        }
        super::Profile::Composed => {
            protocol::read_composed_in_phase(stream, limits, deadline, phase).await
        }
    }
}

async fn receive(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    options: ExportOptions,
    clock: Arc<dyn Clock>,
    deadline: Instant,
    peer: Option<&InternalPeer>,
    profile: super::Profile,
) -> Result<Manifest, Error> {
    let limits = options.limits;
    let transport = limits.transport();
    let request = match profile {
        super::Profile::Legacy => Message::Export {
            sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
            max_record_bytes: limits.max_record_bytes as u32,
            max_snapshot_bytes: limits.max_snapshot_bytes,
        },
        super::Profile::Memory => Message::MemoryExport {
            sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
            record_version: crate::persistence::memory_format::RECORD_VERSION,
            max_record_bytes: limits.max_record_bytes as u32,
            max_snapshot_bytes: limits.max_snapshot_bytes,
        },
        super::Profile::Composed => Message::ComposedExport {
            sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
            record_version: crate::persistence::composed_format::RecordVersion::Legacy as u32,
            aof_header_version: crate::persistence::composed_format::HEADER_VERSION,
            required_capabilities: crate::persistence::composed_format::MIXED_OWNER_BATCH,
            max_record_bytes: limits.max_record_bytes as u32,
            max_snapshot_bytes: limits.max_snapshot_bytes,
        },
    };
    let frame = match profile {
        super::Profile::Legacy => protocol::encode(&request, transport)?,
        super::Profile::Memory => protocol::encode_memory(&request, transport)?,
        super::Profile::Composed => protocol::encode_composed(&request, transport)?,
    };
    use tokio::io::AsyncWriteExt;
    timeout(remaining(deadline)?, stream.write_all(&frame))
        .await
        .map_err(|_| Error::Timeout)??;
    timeout(remaining(deadline)?, stream.flush())
        .await
        .map_err(|_| Error::Timeout)??;
    let response = read_profile(
        stream,
        transport,
        remaining(deadline)?,
        InternalPhase::Hello,
        profile,
    )
    .await?
    .1;
    let (hello, required_capabilities) = match (profile, response) {
        (super::Profile::Legacy | super::Profile::Memory, Message::Hello(hello)) => (hello, None),
        (super::Profile::Composed, Message::ComposedHello(composed))
            if composed.aof_header_version
                == crate::persistence::composed_format::HEADER_VERSION
                && composed.required_capabilities
                    == crate::persistence::composed_format::MIXED_OWNER_BATCH =>
        {
            (composed.base, Some(composed.required_capabilities))
        }
        _ => {
            return Err(Error::Invalid(
                "source rejected export or did not send Hello",
            ));
        }
    };
    if hello.sider_version.as_ref() != env!("CARGO_PKG_VERSION").as_bytes()
        || hello.record_version != profile.versions().2
        || hello.max_record_bytes as usize > limits.max_record_bytes
        || hello.max_mutations as usize > limits.max_mutations
        || hello.max_snapshot_bytes > limits.max_snapshot_bytes
    {
        return Err(Error::Invalid(
            "version or capacity incompatible with the source",
        ));
    }
    let layout = DurableLayout {
        shard_count: hello.shard_count,
        routing_version: hello.routing_version,
    };
    layout.validate()?;
    let Message::FullStart { cursor, entries } = read_profile(
        stream,
        transport,
        remaining(deadline)?,
        InternalPhase::BackupStart,
        profile,
    )
    .await?
    .1
    else {
        return Err(Error::Invalid("missing FullStart"));
    };
    if hello.cursor != Some(cursor)
        || entries > limits.max_snapshot_bytes / (protocol::HEADER_BYTES as u64 + 12)
    {
        return Err(Error::Invalid("incompatible cursor or entry count"));
    }
    let mut directory = OwnedDirectory::new(&options.destination)?;
    let path = directory.path.join(SNAPSHOT);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;
    let codec = match profile {
        super::Profile::Legacy => crate::persistence::profile::Profile::Legacy,
        super::Profile::Memory => crate::persistence::profile::Profile::Memory,
        super::Profile::Composed => crate::persistence::profile::Profile::ComposedLegacyMixed,
    };
    codec.write_header(&mut file, cursor.sequence, layout, None)?;
    let mut previous = None;
    let mut digest = 0;
    let mut aof_digest = 0;
    let mut transport_bytes = 0u64;
    let mut quotas = Quotas::new(layout, limits.max_dataset_bytes, freeze(clock.clone()))?;
    for _ in 0..entries {
        let (raw, message) = read_profile(
            stream,
            transport,
            remaining(deadline)?,
            InternalPhase::SnapshotEntry,
            profile,
        )
        .await?;
        let mutation = match message {
            Message::SnapshotEntry(mutation @ Mutation::Put { .. })
                if profile != super::Profile::Memory =>
            {
                mutation
            }
            Message::MemorySnapshotEntry(mutation) if profile == super::Profile::Memory => {
                super::memory::storage_image(&mutation)?
            }
            _ => return Err(Error::Invalid("incomplete snapshot or unexpected message")),
        };
        if previous.as_ref().is_some_and(|key| key >= mutation.key()) {
            return Err(Error::Invalid(
                "snapshot out of order or with a duplicate key",
            ));
        }
        previous = Some(mutation.key().clone());
        transport_bytes = transport_bytes
            .checked_add(raw.len() as u64)
            .filter(|bytes| *bytes <= limits.max_snapshot_bytes)
            .ok_or(Error::Invalid("snapshot exceeds transfer budget"))?;
        quotas.add(&mutation)?;
        digest = format::snapshot_digest(digest, &raw);
        // The frame body already contains exactly one AOF record validated by the codec.
        let record = &raw[protocol::HEADER_BYTES..];
        aof_digest = format::snapshot_digest(aof_digest, record);
        file.write_all(record)?;
    }
    if read_profile(
        stream,
        transport,
        remaining(deadline)?,
        InternalPhase::SnapshotEnd,
        profile,
    )
    .await?
    .1 != (Message::FullEnd {
        cursor,
        entries,
        digest,
    }) {
        return Err(Error::Invalid("divergent cursor, count, or final digest"));
    }
    let mut extra = [0u8; 1];
    if timeout(remaining(deadline)?, stream.read(&mut extra))
        .await
        .map_err(|_| Error::Timeout)??
        != 0
    {
        return Err(Error::Invalid("data after FullEnd"));
    }
    file.write_all(&codec.encode_legacy(
        Record::Seal {
            sequence: cursor.sequence,
            entries,
            digest: aof_digest,
        },
        limits.record(),
    )?)?;
    file.sync_all()?;
    let (snapshot_sha256, snapshot_bytes) = hash_file(&mut file)?;
    let manifest = Manifest {
        profile,
        sider_version: env!("CARGO_PKG_VERSION").into(),
        source_sha: options.source_sha,
        cursor,
        layout,
        source_max_record_bytes: hello.max_record_bytes,
        source_max_mutations: hello.max_mutations,
        source_max_snapshot_bytes: hello.max_snapshot_bytes,
        required_capabilities,
        validation_max_dataset_bytes: limits.max_dataset_bytes as u64,
        entries,
        snapshot_bytes,
        snapshot_sha256,
        transport_digest: digest,
        received_at_unix_ms: clock.unix_millis(),
    };
    manifest.validate(limits)?;
    validate_file(&mut file, &manifest, limits, clock)?;
    drop(file);
    let mut encoded = serde_json::to_vec_pretty(&manifest.json())?;
    encoded.push(b'\n');
    write_new(&directory.path.join(MANIFEST), &encoded)?;
    write_new(
        &directory.path.join(CHECKSUMS),
        checksums(&encoded, &manifest.snapshot_sha256).as_bytes(),
    )?;
    if peer.is_some_and(|peer| !peer.current()) {
        return Err(Error::Invalid("internal security expired"));
    }
    directory.finish()?;
    Ok(manifest)
}
