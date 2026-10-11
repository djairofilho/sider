//! Record-2 follower with exact-sequence durable apply and bounded reconnect.

use std::time::Duration;

use bytes::Bytes;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::watch,
    time::timeout,
};

use super::{
    Cursor, Error,
    config::Config,
    memory_source::{hello, limits, snapshot_ceiling},
    protocol::{self, Message},
    security::{self, Access, Authority},
    session::stopped,
};
use crate::{
    persistence::{AofConfig, format, memory_format},
    security::{InternalLease, InternalPhase},
    storage::{replication::Context, worker::DbHandle},
};

pub(crate) async fn follow_plain(
    database: DbHandle,
    context: Context,
    aof_config: AofConfig,
    config: Config,
    shutdown: watch::Receiver<bool>,
) -> Result<(), Error> {
    follow_inner(database, context, aof_config, config, shutdown, None).await
}

pub(crate) async fn follow_secure(
    database: DbHandle,
    context: Context,
    aof_config: AofConfig,
    config: Config,
    shutdown: watch::Receiver<bool>,
    security: crate::security::InternalSecurity,
) -> Result<(), Error> {
    follow_inner(
        database,
        context,
        aof_config,
        config,
        shutdown,
        Some(Access::Static(security)),
    )
    .await
}

pub(crate) async fn follow_live(
    database: DbHandle,
    context: Context,
    aof_config: AofConfig,
    config: Config,
    shutdown: watch::Receiver<bool>,
    security: crate::security::InternalRuntime,
) -> Result<(), Error> {
    follow_inner(
        database,
        context,
        aof_config,
        config,
        shutdown,
        Some(Access::Live(security)),
    )
    .await
}

async fn follow_inner(
    database: DbHandle,
    context: Context,
    aof_config: AofConfig,
    config: Config,
    mut shutdown: watch::Receiver<bool>,
    security: Option<Access>,
) -> Result<(), Error> {
    let mut backoff = config.reconnect_min;
    loop {
        if !context.runtime.readonly() {
            stopped(&mut shutdown).await;
            return Ok(());
        }
        let generation = context.runtime.begin_session();
        let result = tokio::select! {
            biased;
            () = stopped(&mut shutdown) => return Ok(()),
            () = context.runtime.cancelled(generation) => continue,
            result = receive_session(&database, &context, &aof_config, &config, generation, security.as_ref()) => result,
        };
        context.runtime.disconnected(generation);
        if let Err(error) = &result {
            tracing::warn!(%error, "record-2 upstream disconnected; reconnection bounded");
            if matches!(
                error,
                Error::Database(crate::storage::worker::DbError::Unavailable)
                    | Error::Persistence(crate::persistence::AofError::Unavailable)
            ) {
                return result;
            }
        }
        tokio::select! {
            biased;
            () = stopped(&mut shutdown) => return Ok(()),
            () = context.runtime.cancelled(generation) => {},
            () = tokio::time::sleep(backoff) => {},
        }
        backoff = backoff.saturating_mul(2).min(config.reconnect_max);
    }
}

async fn operation_lease(
    authority: Option<&Authority>,
    deadline: Duration,
) -> Result<Option<InternalLease>, Error> {
    match authority {
        Some(authority) => authority.operation(deadline).await,
        None => Ok(None),
    }
}

async fn receive_session(
    database: &DbHandle,
    context: &Context,
    aof_config: &AofConfig,
    config: &Config,
    generation: u64,
    security: Option<&Access>,
) -> Result<(), Error> {
    let mut position = database.replication_position(&context.runtime).await?;
    let address = config.upstream.ok_or(Error::Stale)?;
    let (mut socket, authority): (Box<dyn crate::security::NetworkStream>, _) =
        if let Some(security) = security {
            let (socket, authority) = security.connect(address).await?;
            (socket, Some(authority))
        } else {
            let socket = timeout(config.frame_timeout, TcpStream::connect(address))
                .await
                .map_err(|_| protocol::Error::Timeout)??;
            socket.set_nodelay(true)?;
            (Box::new(socket), None)
        };
    let authority = authority.as_ref();
    let wire_limits = limits(aof_config);
    let cursor = (position.epoch != [0; 16]).then_some(position);
    security::write_memory(
        &mut socket,
        &Message::Hello(super::protocol::Hello {
            cursor,
            ..hello(aof_config, context.store_config, position)
        }),
        wire_limits,
        config.frame_timeout,
        authority,
    )
    .await?;
    let Message::Hello(source) = security::read_memory_phase(
        &mut socket,
        wire_limits,
        config.frame_timeout,
        InternalPhase::Hello,
        authority,
    )
    .await?
    .1
    else {
        return Err(Error::Sequence);
    };
    hello(aof_config, context.store_config, position)
        .accepts(&source)
        .map_err(|_| Error::Limit)?;
    let head = source.cursor.ok_or(Error::Sequence)?;
    match security::read_memory_phase(
        &mut socket,
        wire_limits,
        config.frame_timeout,
        InternalPhase::ReplicaStart,
        authority,
    )
    .await?
    .1
    {
        Message::Continue(cursor)
            if cursor == position
                && head.epoch == position.epoch
                && head.sequence >= position.sequence =>
        {
            context.runtime.connected(generation, head.sequence, false);
        }
        Message::FullStart { cursor, entries } if cursor == head => {
            let mutations = timeout(
                config.sync_timeout,
                receive_snapshot(
                    &mut socket,
                    cursor,
                    entries,
                    context,
                    aof_config,
                    config,
                    authority,
                ),
            )
            .await
            .map_err(|_| protocol::Error::Timeout)??;
            database
                .authorized_internal(operation_lease(authority, config.sync_timeout).await?)
                .install_memory_replica(context.clone(), generation, cursor, mutations)
                .await?;
            position = cursor;
            context.runtime.connected(generation, head.sequence, true);
        }
        _ => return Err(Error::Sequence),
    }
    security::write_memory(
        &mut socket,
        &Message::Ack(position),
        wire_limits,
        config.frame_timeout,
        authority,
    )
    .await?;
    let mut previous: Option<Bytes> = None;
    loop {
        let (frame, message) = security::read_memory_phase(
            &mut socket,
            wire_limits,
            config.frame_timeout,
            InternalPhase::ReplicaStream,
            authority,
        )
        .await?;
        match message {
            Message::Heartbeat(head)
                if head.epoch == position.epoch && head.sequence >= position.sequence =>
            {
                context.runtime.upstream_head(generation, head.sequence);
            }
            Message::MemoryBatch {
                sequence,
                origin,
                mutations,
            } => {
                if sequence == position.sequence && previous.as_ref() == Some(&frame) {
                    // Exact retransmission of the last batch does not apply twice.
                } else {
                    if position.sequence.checked_add(1) != Some(sequence) {
                        return Err(Error::Sequence);
                    }
                    let cursor = Cursor {
                        sequence,
                        ..position
                    };
                    database
                        .authorized_internal(
                            operation_lease(authority, config.frame_timeout).await?,
                        )
                        .apply_memory_replica(
                            context.clone(),
                            generation,
                            cursor,
                            origin,
                            mutations,
                        )
                        .await?;
                    position = cursor;
                    previous = Some(frame);
                }
            }
            _ => return Err(Error::Sequence),
        }
        security::write_memory(
            &mut socket,
            &Message::Ack(position),
            wire_limits,
            config.frame_timeout,
            authority,
        )
        .await?;
    }
}

async fn receive_snapshot(
    socket: &mut (impl AsyncRead + AsyncWrite + Unpin),
    cursor: Cursor,
    entries: u64,
    context: &Context,
    aof_config: &crate::persistence::AofConfig,
    config: &Config,
    authority: Option<&Authority>,
) -> Result<Vec<memory_format::Mutation>, Error> {
    let ceiling = snapshot_ceiling(context.store_config);
    if entries > ceiling / protocol::HEADER_BYTES as u64 {
        return Err(Error::Limit);
    }
    let mut mutations = Vec::new();
    let mut bytes = 0u64;
    let mut digest = 0;
    let mut previous = None;
    loop {
        let phase = if mutations.len() < entries as usize {
            InternalPhase::SnapshotEntry
        } else {
            InternalPhase::SnapshotEnd
        };
        let (frame, message) = security::read_memory_phase(
            socket,
            limits(aof_config),
            config.frame_timeout,
            phase,
            authority,
        )
        .await?;
        match message {
            Message::MemorySnapshotEntry(mutation) if mutations.len() < entries as usize => {
                if previous.as_ref().is_some_and(|key| key >= mutation.key()) {
                    return Err(Error::Sequence);
                }
                previous = Some(mutation.key().clone());
                bytes = bytes
                    .checked_add(frame.len() as u64)
                    .filter(|bytes| *bytes <= ceiling)
                    .ok_or(Error::Limit)?;
                digest = format::snapshot_digest(digest, &frame);
                mutations.push(mutation);
            }
            Message::FullEnd {
                cursor: end,
                entries: count,
                digest: checksum,
            } if end == cursor
                && count == entries
                && mutations.len() == entries as usize
                && checksum == digest =>
            {
                return Ok(mutations);
            }
            _ => return Err(Error::Sequence),
        }
    }
}
