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
            return Err(Error::Invalid("porta de origem precisa ser explícita"));
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

/// Conecta ao listener interno e recebe somente um snapshot, sem ACK ou fluxo incremental.
pub async fn export(options: ExportOptions, clock: Arc<dyn Clock>) -> Result<Manifest, Error> {
    options.validate()?;
    let deadline = Instant::now() + options.limits.timeout;
    let mut stream = timeout(remaining(deadline)?, TcpStream::connect(options.source))
        .await
        .map_err(|_| Error::Timeout)??;
    receive(&mut stream, options, clock, deadline).await
}

/// Transporte injetável para testes; aplica os mesmos limites e contrato do cliente TCP.
pub async fn export_stream(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    options: ExportOptions,
    clock: Arc<dyn Clock>,
) -> Result<Manifest, Error> {
    options.validate()?;
    let deadline = Instant::now() + options.limits.timeout;
    receive(stream, options, clock, deadline).await
}

async fn receive(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    options: ExportOptions,
    clock: Arc<dyn Clock>,
    deadline: Instant,
) -> Result<Manifest, Error> {
    let limits = options.limits;
    let transport = limits.transport();
    protocol::write(
        stream,
        &Message::Export {
            sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
            max_record_bytes: limits.max_record_bytes as u32,
            max_snapshot_bytes: limits.max_snapshot_bytes,
        },
        transport,
        remaining(deadline)?,
    )
    .await?;
    let Message::Hello(hello) = protocol::read(stream, transport, remaining(deadline)?).await?
    else {
        return Err(Error::Invalid(
            "origem recusou exportação ou não enviou Hello",
        ));
    };
    if hello.sider_version.as_ref() != env!("CARGO_PKG_VERSION").as_bytes()
        || hello.record_version != format::VERSION
        || hello.max_record_bytes as usize > limits.max_record_bytes
        || hello.max_mutations as usize > limits.max_mutations
        || hello.max_snapshot_bytes > limits.max_snapshot_bytes
    {
        return Err(Error::Invalid(
            "versão ou capacidade incompatível com a origem",
        ));
    }
    let layout = DurableLayout {
        shard_count: hello.shard_count,
        routing_version: hello.routing_version,
    };
    layout.validate()?;
    let Message::FullStart { cursor, entries } =
        protocol::read(stream, transport, remaining(deadline)?).await?
    else {
        return Err(Error::Invalid("FullStart ausente"));
    };
    if hello.cursor != Some(cursor)
        || entries > limits.max_snapshot_bytes / (protocol::HEADER_BYTES as u64 + 12)
    {
        return Err(Error::Invalid(
            "cursor ou quantidade de entradas incompatível",
        ));
    }
    let mut directory = OwnedDirectory::new(&options.destination)?;
    let path = directory.path.join(SNAPSHOT);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;
    format::write_header_with_layout(&mut file, cursor.sequence, layout)?;
    let mut previous = None;
    let mut digest = 0;
    let mut aof_digest = 0;
    let mut transport_bytes = 0u64;
    let mut quotas = Quotas::new(layout, limits.max_dataset_bytes, freeze(clock.clone()))?;
    for _ in 0..entries {
        let (raw, message) =
            protocol::read_with_frame(stream, transport, remaining(deadline)?).await?;
        let Message::SnapshotEntry(mutation @ Mutation::Put { .. }) = message else {
            return Err(Error::Invalid("snapshot incompleto ou mensagem inesperada"));
        };
        if previous.as_ref().is_some_and(|key| key >= mutation.key()) {
            return Err(Error::Invalid(
                "snapshot fora de ordem ou com chave duplicada",
            ));
        }
        previous = Some(mutation.key().clone());
        transport_bytes = transport_bytes
            .checked_add(raw.len() as u64)
            .filter(|bytes| *bytes <= limits.max_snapshot_bytes)
            .ok_or(Error::Invalid("snapshot excede orçamento de transferência"))?;
        quotas.add(&mutation)?;
        digest = format::snapshot_digest(digest, &raw);
        // O corpo do frame já contém exatamente um registro AOF validado pelo codec.
        let record = &raw[protocol::HEADER_BYTES..];
        aof_digest = format::snapshot_digest(aof_digest, record);
        file.write_all(record)?;
    }
    if protocol::read(stream, transport, remaining(deadline)?).await?
        != (Message::FullEnd {
            cursor,
            entries,
            digest,
        })
    {
        return Err(Error::Invalid(
            "cursor, contagem ou digest final divergente",
        ));
    }
    let mut extra = [0u8; 1];
    if timeout(remaining(deadline)?, stream.read(&mut extra))
        .await
        .map_err(|_| Error::Timeout)??
        != 0
    {
        return Err(Error::Invalid("dados depois do FullEnd"));
    }
    file.write_all(&format::encode(
        &Record::Seal {
            sequence: cursor.sequence,
            entries,
            digest: aof_digest,
        },
        limits.record(),
    )?)?;
    file.sync_all()?;
    let (snapshot_sha256, snapshot_bytes) = hash_file(&mut file)?;
    let manifest = Manifest {
        sider_version: env!("CARGO_PKG_VERSION").into(),
        source_sha: options.source_sha,
        cursor,
        layout,
        source_max_record_bytes: hello.max_record_bytes,
        source_max_mutations: hello.max_mutations,
        source_max_snapshot_bytes: hello.max_snapshot_bytes,
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
    directory.finish()?;
    Ok(manifest)
}
