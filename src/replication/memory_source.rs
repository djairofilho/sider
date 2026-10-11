//! Live transport-2 export from an owning-worker AOF snapshot.

use std::sync::Arc;

use bytes::Bytes;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::{Semaphore, watch},
    task::JoinSet,
    time::timeout,
};

use super::{
    Cursor, Error,
    config::Config,
    protocol::{self, Hello, Message, Reject},
    security::{self, Access, Authority},
    session::stopped,
};
use crate::{
    persistence::{AofConfig, AofHandle, backup, format, memory_format},
    storage::{StoreConfig, worker::DbHandle},
};

#[derive(Clone)]
pub(crate) struct Source {
    pub(crate) database: DbHandle,
    pub(crate) aof: AofHandle,
    pub(crate) aof_config: AofConfig,
    pub(crate) store_config: StoreConfig,
    pub(crate) config: Config,
}

pub(crate) async fn serve_plain(
    listener: TcpListener,
    source: Source,
    shutdown: watch::Receiver<bool>,
) -> Result<(), Error> {
    serve_inner(listener, source, shutdown, None).await
}

pub(crate) async fn serve_secure(
    listener: TcpListener,
    source: Source,
    shutdown: watch::Receiver<bool>,
    security: crate::security::InternalSecurity,
) -> Result<(), Error> {
    serve_inner(listener, source, shutdown, Some(Access::Static(security))).await
}

pub(crate) async fn serve_live(
    listener: TcpListener,
    source: Source,
    shutdown: watch::Receiver<bool>,
    security: crate::security::InternalRuntime,
) -> Result<(), Error> {
    serve_inner(listener, source, shutdown, Some(Access::Live(security))).await
}

async fn serve_inner(
    listener: TcpListener,
    source: Source,
    mut shutdown: watch::Receiver<bool>,
    security: Option<Access>,
) -> Result<(), Error> {
    let slots = Arc::new(Semaphore::new(source.config.max_connections));
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = stopped(&mut shutdown) => break,
            Some(result) = sessions.join_next() => match result {
                Ok(Ok(())) => {},
                Ok(Err(error)) => tracing::debug!(%error, "memory export closed"),
                Err(error) => return Err(error.into()),
            },
            accepted = listener.accept() => {
                let (socket, peer) = accepted?;
                if security.is_none() && !peer.ip().is_loopback() {
                    drop(socket);
                    continue;
                }
                let Ok(slot) = slots.clone().try_acquire_owned() else { drop(socket); continue; };
                let source = source.clone();
                let security = security.clone();
                sessions.spawn(async move {
                    let _slot = slot;
                    socket.set_nodelay(true)?;
                    let (mut socket, mut authority): (Box<dyn crate::security::NetworkStream>, _) =
                        if let Some(security) = security {
                            let (socket, authority) = security.accept(socket).await?;
                            (socket, Some(authority))
                        } else {
                            (Box::new(socket), None)
                        };
                    timeout(source.config.sync_timeout, export_connection(
                        &mut socket, &source.database, &source.aof, &source.aof_config,
                        source.store_config, &source.config, authority.as_mut(),
                    ))
                    .await
                    .map_err(|_| protocol::Error::Timeout)?
                });
            }
        }
    }
    sessions.abort_all();
    while sessions.join_next().await.is_some() {}
    Ok(())
}

fn limits(aof: &AofConfig) -> protocol::Limits {
    protocol::Limits {
        max_frame_bytes: aof.limits.max_record_bytes + protocol::HEADER_BYTES + 12,
        record: aof.limits,
    }
}

fn snapshot_ceiling(store_config: StoreConfig) -> u64 {
    (store_config.max_dataset_bytes as u64)
        .saturating_mul(2)
        .max(backup::Limits::default().max_snapshot_bytes)
        .min(backup::MAX_BACKUP_BYTES)
}

fn hello(aof: &AofConfig, store_config: StoreConfig, cursor: Cursor) -> Hello {
    Hello {
        sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
        record_version: memory_format::RECORD_VERSION,
        shard_count: aof.layout.shard_count,
        routing_version: aof.layout.routing_version,
        max_record_bytes: aof.limits.max_record_bytes as u32,
        max_mutations: aof.limits.max_mutations as u32,
        max_snapshot_bytes: snapshot_ceiling(store_config),
        cursor: Some(cursor),
    }
}

async fn export_connection(
    socket: &mut (impl AsyncRead + AsyncWrite + Unpin),
    database: &DbHandle,
    aof: &AofHandle,
    aof_config: &AofConfig,
    store_config: StoreConfig,
    config: &Config,
    mut authority: Option<&mut Authority>,
) -> Result<(), Error> {
    let (_, request, request_lease) = security::read_memory(
        socket,
        limits(aof_config),
        config.frame_timeout,
        authority.as_deref(),
    )
    .await?;
    if let Some(authority) = &mut authority {
        authority.set_request(&request);
    }
    drop(request_lease);
    let Message::MemoryExport {
        sider_version,
        record_version,
        max_record_bytes,
        max_snapshot_bytes,
    } = request
    else {
        return Err(protocol::Error::Invalid("expected memory export").into());
    };
    let source = hello(
        aof_config,
        store_config,
        Cursor {
            epoch: [0; 16],
            sequence: 0,
        },
    );
    let rejection =
        if sider_version != source.sider_version || record_version != source.record_version {
            Some(Reject::IncompatibleVersion)
        } else if max_record_bytes < source.max_record_bytes
            || max_snapshot_bytes < source.max_snapshot_bytes
        {
            Some(Reject::ResourceLimit)
        } else {
            None
        };
    if let Some(reason) = rejection {
        security::write_memory(
            socket,
            &Message::Reject(reason),
            limits(aof_config),
            config.frame_timeout,
            authority.as_deref(),
        )
        .await?;
        socket.shutdown().await?;
        return Ok(());
    }
    let operation_lease = match authority.as_deref() {
        Some(authority) => authority.operation(config.frame_timeout).await?,
        None => None,
    };
    let snapshot = database
        .authorized_internal(operation_lease)
        .snapshot(Some(aof))
        .await?;
    let cursor = Cursor {
        epoch: [0; 16],
        sequence: snapshot.sequence.ok_or(Error::Sequence)?,
    };
    let source = hello(aof_config, store_config, cursor);
    let mutations: Vec<_> = snapshot
        .mutations
        .into_iter()
        .map(memory_format::Mutation::from_storage)
        .collect();
    let mut previous = None;
    let mut transfer_bytes = 0u64;
    for mutation in &mutations {
        if previous.is_some_and(|key| key >= mutation.key()) {
            return Err(Error::Sequence);
        }
        previous = Some(mutation.key());
        let frame = protocol::encode_memory(
            &Message::MemorySnapshotEntry(mutation.clone()),
            limits(aof_config),
        )?;
        transfer_bytes = transfer_bytes
            .checked_add(frame.len() as u64)
            .filter(|bytes| *bytes <= source.max_snapshot_bytes)
            .ok_or(Error::Limit)?;
    }
    security::write_memory(
        socket,
        &Message::Hello(source),
        limits(aof_config),
        config.frame_timeout,
        authority.as_deref(),
    )
    .await?;
    security::write_memory(
        socket,
        &Message::FullStart {
            cursor,
            entries: mutations.len() as u64,
        },
        limits(aof_config),
        config.frame_timeout,
        authority.as_deref(),
    )
    .await?;
    let entries = mutations.len() as u64;
    let mut digest = 0;
    for mutation in mutations {
        let frame =
            protocol::encode_memory(&Message::MemorySnapshotEntry(mutation), limits(aof_config))?;
        digest = format::snapshot_digest(digest, &frame);
        security::write_frame(socket, &frame, config.frame_timeout, authority.as_deref()).await?;
    }
    security::write_memory(
        socket,
        &Message::FullEnd {
            cursor,
            entries,
            digest,
        },
        limits(aof_config),
        config.frame_timeout,
        authority.as_deref(),
    )
    .await?;
    socket.shutdown().await?;
    Ok(())
}
