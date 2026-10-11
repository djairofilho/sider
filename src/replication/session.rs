//! FULL/CONTINUE, confirmation after durable apply, and reconnection of a single session.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;

use super::{
    Cursor, Error,
    config::Config,
    journal::Subscriber,
    protocol::{self, Hello, Message, Reject},
};
use crate::persistence::format;
use crate::security::InternalPhase;
use crate::storage::{
    Mutation,
    replication::{Context, Snapshot},
    worker::DbHandle,
};

fn limits(context: &Context) -> protocol::Limits {
    protocol::Limits {
        max_frame_bytes: (context.aof_limits.max_record_bytes + protocol::HEADER_BYTES + 12)
            .max(128),
        record: context.aof_limits,
    }
}

fn hello(context: &Context, cursor: Option<Cursor>) -> Hello {
    Hello {
        sider_version: Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()),
        record_version: format::VERSION,
        shard_count: context.layout.shard_count,
        routing_version: context.layout.routing_version,
        max_record_bytes: context.aof_limits.max_record_bytes as u32,
        max_mutations: context.aof_limits.max_mutations as u32,
        max_snapshot_bytes: context.store_config.max_dataset_bytes as u64,
        cursor,
    }
}

pub async fn stopped(shutdown: &mut watch::Receiver<bool>) {
    while !*shutdown.borrow_and_update() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

pub async fn serve(
    listener: TcpListener,
    database: DbHandle,
    context: Context,
    config: Config,
    shutdown: watch::Receiver<bool>,
) -> Result<(), Error> {
    serve_inner(listener, database, context, config, shutdown, None).await
}

pub async fn serve_secure(
    listener: TcpListener,
    database: DbHandle,
    context: Context,
    config: Config,
    shutdown: watch::Receiver<bool>,
    security: crate::security::InternalSecurity,
) -> Result<(), Error> {
    serve_inner(
        listener,
        database,
        context,
        config,
        shutdown,
        Some(security),
    )
    .await
}

async fn serve_inner(
    listener: TcpListener,
    database: DbHandle,
    context: Context,
    config: Config,
    mut shutdown: watch::Receiver<bool>,
    security: Option<crate::security::InternalSecurity>,
) -> Result<(), Error> {
    let slots = Arc::new(Semaphore::new(config.max_connections));
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = stopped(&mut shutdown) => break,
            Some(result) = sessions.join_next() => {
                match result {
                    Ok(Ok(())) => {},
                    Ok(Err(error)) => tracing::debug!(%error, "internal session closed"),
                    Err(error) => return Err(error.into()),
                }
            }
            accepted = listener.accept() => {
                let (socket, peer) = accepted?;
                let Ok(slot) = slots.clone().try_acquire_owned() else { drop(socket); continue; };
                let database = database.clone();
                let context = context.clone();
                let config = config.clone();
                let security = security.clone();
                sessions.spawn(async move {
                    let _slot = slot;
                    socket.set_nodelay(true)?;
                    let (socket, authority): (Box<dyn crate::security::NetworkStream>, _) = if let Some(security) = security {
                        let (socket, proof) = security.transport.accept_with_peer(socket).await?;
                        (socket, Some((security.policy, proof)))
                    } else { (Box::new(socket), None) };
                    serve_connection(socket, peer.ip().is_loopback(), database, context, config, authority).await
                });
            }
        }
    }
    sessions.abort_all();
    while sessions.join_next().await.is_some() {}
    Ok(())
}

async fn reject(
    socket: &mut (impl AsyncRead + AsyncWrite + Unpin),
    reason: Reject,
    context: &Context,
    config: &Config,
) -> Result<(), Error> {
    protocol::write(
        socket,
        &Message::Reject(reason),
        limits(context),
        config.frame_timeout,
    )
    .await?;
    Ok(())
}

async fn serve_connection(
    mut socket: Box<dyn crate::security::NetworkStream>,
    local: bool,
    database: DbHandle,
    context: Context,
    config: Config,
    authority: Option<(
        Arc<crate::security::InternalPolicy>,
        crate::security::InternalPeer,
    )>,
) -> Result<(), Error> {
    let request = protocol::read_authorized(
        &mut socket,
        limits(&context),
        config.frame_timeout,
        |kind| {
            let Some((policy, peer)) = &authority else {
                return true;
            };
            peer.current() && crate::security::authorize_internal_request(policy, peer.uri(), kind)
        },
    )
    .await?
    .1;
    if authority.as_ref().is_some_and(|(_, peer)| !peer.current()) {
        return Err(crate::security::Error::Transport.into());
    }
    if database.check_admission().is_err() {
        return reject(&mut socket, Reject::Unavailable, &context, &config).await;
    }
    match request {
        Message::StatusRequest => {
            let status = database.replication_status(&context.runtime).await?;
            let bytes = context
                .runtime
                .journal()
                .and_then(|journal| journal.status().ok())
                .map_or(0, |status| status.bytes as u64);
            protocol::write(
                &mut socket,
                &Message::Status {
                    readonly: status.role == crate::persistence::Role::Replica,
                    cursor: status.applied,
                    upstream_sequence: status.upstream_sequence,
                    connected: status.connected,
                    backlog_bytes: bytes,
                    full_syncs: status.full_syncs,
                    partial_syncs: status.partial_syncs,
                },
                limits(&context),
                config.frame_timeout,
            )
            .await?;
            return Ok(());
        }
        Message::Promote if local || authority.is_some() => {
            let cursor = if context.runtime.readonly() {
                database
                    .promote_replica(context.clone(), super::new_epoch()?)
                    .await?
            } else {
                database.replication_position(&context.runtime).await?
            };
            tracing::info!(
                sequence = cursor.sequence,
                "manual promotion persisted; previous upstream stopped"
            );
            protocol::write(
                &mut socket,
                &Message::Promoted(cursor),
                limits(&context),
                config.frame_timeout,
            )
            .await?;
            return Ok(());
        }
        Message::Promote => {
            return reject(&mut socket, Reject::Unavailable, &context, &config).await;
        }
        _ => {}
    }
    if context.runtime.readonly() {
        return reject(&mut socket, Reject::Unavailable, &context, &config).await;
    }
    let source = hello(&context, None);
    match request {
        Message::Export {
            sider_version,
            max_record_bytes,
            max_snapshot_bytes,
        } => {
            if sider_version != source.sider_version {
                return reject(&mut socket, Reject::IncompatibleVersion, &context, &config).await;
            }
            if max_record_bytes < source.max_record_bytes
                || max_snapshot_bytes < source.max_snapshot_bytes
            {
                return reject(&mut socket, Reject::ResourceLimit, &context, &config).await;
            }
            let snapshot = timeout(
                config.sync_timeout,
                database.replication_snapshot(&context, false),
            )
            .await
            .map_err(|_| protocol::Error::Timeout)??;
            protocol::write(
                &mut socket,
                &Message::Hello(hello(&context, Some(snapshot.cursor))),
                limits(&context),
                config.frame_timeout,
            )
            .await?;
            timeout(
                config.sync_timeout,
                send_snapshot(&mut socket, &snapshot, &context, &config),
            )
            .await
            .map_err(|_| protocol::Error::Timeout)??;
            socket.shutdown().await?;
            Ok(())
        }
        Message::Hello(peer) => {
            if let Err(reason) = peer.accepts(&source) {
                return reject(&mut socket, reason, &context, &config).await;
            }
            let journal = context.runtime.journal().ok_or(Error::Stale)?;
            let resume = peer.cursor.and_then(|cursor| {
                journal
                    .subscribe(cursor)
                    .ok()
                    .map(|subscription| (cursor, subscription))
            });
            let (cursor, subscription) = if let Some((cursor, subscription)) = resume {
                let head = database.replication_position(&context.runtime).await?;
                protocol::write(
                    &mut socket,
                    &Message::Hello(hello(&context, Some(head))),
                    limits(&context),
                    config.frame_timeout,
                )
                .await?;
                protocol::write(
                    &mut socket,
                    &Message::Continue(cursor),
                    limits(&context),
                    config.frame_timeout,
                )
                .await?;
                tracing::info!(sequence = cursor.sequence, "replication CONTINUE");
                (cursor, subscription)
            } else {
                let mut snapshot = timeout(
                    config.sync_timeout,
                    database.replication_snapshot(&context, true),
                )
                .await
                .map_err(|_| protocol::Error::Timeout)??;
                protocol::write(
                    &mut socket,
                    &Message::Hello(hello(&context, Some(snapshot.cursor))),
                    limits(&context),
                    config.frame_timeout,
                )
                .await?;
                timeout(
                    config.sync_timeout,
                    send_snapshot(&mut socket, &snapshot, &context, &config),
                )
                .await
                .map_err(|_| protocol::Error::Timeout)??;
                tracing::info!(sequence = snapshot.cursor.sequence, "replication FULL");
                (
                    snapshot.cursor,
                    snapshot.subscription.take().ok_or(Error::Sequence)?,
                )
            };
            expect_ack(&mut socket, cursor, &context, &config).await?;
            stream(&mut socket, cursor, subscription, &context, &config).await
        }
        _ => reject(&mut socket, Reject::Unavailable, &context, &config).await,
    }
}

async fn send_snapshot(
    socket: &mut (impl AsyncRead + AsyncWrite + Unpin),
    snapshot: &Snapshot,
    context: &Context,
    config: &Config,
) -> Result<(), Error> {
    protocol::write(
        socket,
        &Message::FullStart {
            cursor: snapshot.cursor,
            entries: snapshot.mutations.len() as u64,
        },
        limits(context),
        config.frame_timeout,
    )
    .await?;
    let mut digest = 0;
    let mut bytes = 0usize;
    for mutation in &snapshot.mutations {
        let frame = protocol::encode(&Message::SnapshotEntry(mutation.clone()), limits(context))?;
        bytes = bytes
            .checked_add(frame.len())
            .filter(|bytes| *bytes <= context.store_config.max_dataset_bytes)
            .ok_or(Error::Limit)?;
        digest = format::snapshot_digest(digest, &frame);
        timeout(config.frame_timeout, socket.write_all(&frame))
            .await
            .map_err(|_| protocol::Error::Timeout)??;
    }
    protocol::write(
        socket,
        &Message::FullEnd {
            cursor: snapshot.cursor,
            entries: snapshot.mutations.len() as u64,
            digest,
        },
        limits(context),
        config.frame_timeout,
    )
    .await?;
    Ok(())
}

async fn expect_ack(
    socket: &mut (impl AsyncRead + AsyncWrite + Unpin),
    cursor: Cursor,
    context: &Context,
    config: &Config,
) -> Result<(), Error> {
    if protocol::read_in_phase(
        socket,
        limits(context),
        config.frame_timeout,
        InternalPhase::ReplicaAck,
    )
    .await?
    .1 != Message::Ack(cursor)
    {
        return Err(Error::Sequence);
    }
    Ok(())
}

async fn stream(
    socket: &mut (impl AsyncRead + AsyncWrite + Unpin),
    mut sent: Cursor,
    mut subscription: Subscriber,
    context: &Context,
    config: &Config,
) -> Result<(), Error> {
    let heartbeat = config.frame_timeout.min(Duration::from_secs(1)) / 2;
    loop {
        match timeout(heartbeat, subscription.next()).await {
            Ok(Ok(entry)) => {
                let head = context
                    .runtime
                    .journal()
                    .ok_or(Error::Stale)?
                    .status()?
                    .head;
                protocol::write(
                    socket,
                    &Message::Heartbeat(head),
                    limits(context),
                    config.frame_timeout,
                )
                .await?;
                timeout(config.frame_timeout, socket.write_all(&entry.frame))
                    .await
                    .map_err(|_| protocol::Error::Timeout)??;
                timeout(config.frame_timeout, socket.flush())
                    .await
                    .map_err(|_| protocol::Error::Timeout)??;
                expect_ack(socket, sent, context, config).await?;
                expect_ack(socket, entry.cursor, context, config).await?;
                sent = entry.cursor;
            }
            Ok(Err(error)) => {
                reject(socket, Reject::FullRequired, context, config).await?;
                return Err(error.into());
            }
            Err(_) => {
                let head = context
                    .runtime
                    .journal()
                    .ok_or(Error::Stale)?
                    .status()?
                    .head;
                protocol::write(
                    socket,
                    &Message::Heartbeat(head),
                    limits(context),
                    config.frame_timeout,
                )
                .await?;
                expect_ack(socket, sent, context, config).await?;
            }
        }
    }
}

pub async fn follow(
    database: DbHandle,
    context: Context,
    config: Config,
    shutdown: watch::Receiver<bool>,
) -> Result<(), Error> {
    follow_inner(database, context, config, shutdown, None).await
}

pub async fn follow_secure(
    database: DbHandle,
    context: Context,
    config: Config,
    shutdown: watch::Receiver<bool>,
    security: crate::security::InternalSecurity,
) -> Result<(), Error> {
    follow_inner(database, context, config, shutdown, Some(security)).await
}

async fn follow_inner(
    database: DbHandle,
    context: Context,
    config: Config,
    mut shutdown: watch::Receiver<bool>,
    security: Option<crate::security::InternalSecurity>,
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
            result = receive_session(&database, &context, &config, generation, security.as_ref()) => result,
        };
        context.runtime.disconnected(generation);
        if let Err(error) = &result {
            tracing::warn!(%error, "upstream disconnected; reconnection bounded");
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

async fn receive_session(
    database: &DbHandle,
    context: &Context,
    config: &Config,
    generation: u64,
    security: Option<&crate::security::InternalSecurity>,
) -> Result<(), Error> {
    let mut position = database.replication_position(&context.runtime).await?;
    let address = config.upstream.ok_or(Error::Stale)?;
    let (mut socket, peer): (Box<dyn crate::security::NetworkStream>, _) =
        if let Some(security) = security {
            let (socket, peer) = security.transport.connect_with_peer(address).await?;
            (socket, Some(peer))
        } else {
            let socket = timeout(config.frame_timeout, TcpStream::connect(address))
                .await
                .map_err(|_| protocol::Error::Timeout)??;
            socket.set_nodelay(true)?;
            (Box::new(socket), None)
        };
    let cursor = (position.epoch != [0; 16]).then_some(position);
    protocol::write(
        &mut socket,
        &Message::Hello(hello(context, cursor)),
        limits(context),
        config.frame_timeout,
    )
    .await?;
    let Message::Hello(source) = protocol::read_in_phase(
        &mut socket,
        limits(context),
        config.frame_timeout,
        InternalPhase::Hello,
    )
    .await?
    .1
    else {
        return Err(Error::Sequence);
    };
    hello(context, None)
        .accepts(&source)
        .map_err(|_| Error::Limit)?;
    let head = source.cursor.ok_or(Error::Sequence)?;
    match protocol::read_in_phase(
        &mut socket,
        limits(context),
        config.frame_timeout,
        InternalPhase::ReplicaStart,
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
                receive_snapshot(&mut socket, cursor, entries, context, config),
            )
            .await
            .map_err(|_| protocol::Error::Timeout)??;
            if peer.as_ref().is_some_and(|peer| !peer.current()) {
                return Err(crate::security::Error::Transport.into());
            }
            database
                .install_replica(context.clone(), generation, cursor, mutations)
                .await?;
            position = cursor;
            context.runtime.connected(generation, head.sequence, true);
        }
        _ => return Err(Error::Sequence),
    }
    protocol::write(
        &mut socket,
        &Message::Ack(position),
        limits(context),
        config.frame_timeout,
    )
    .await?;
    let mut previous: Option<Bytes> = None;
    loop {
        let (frame, message) = protocol::read_in_phase(
            &mut socket,
            limits(context),
            config.frame_timeout,
            InternalPhase::ReplicaStream,
        )
        .await?;
        match message {
            Message::Heartbeat(head)
                if head.epoch == position.epoch && head.sequence >= position.sequence =>
            {
                context.runtime.upstream_head(generation, head.sequence);
            }
            Message::Batch { sequence, batch } => {
                if sequence == position.sequence && previous.as_ref() == Some(&frame) {
                    // Exact retransmission of the last batch: neither reapplies nor renews timeout.
                } else {
                    if position.sequence.checked_add(1) != Some(sequence) {
                        return Err(Error::Sequence);
                    }
                    let cursor = Cursor {
                        sequence,
                        ..position
                    };
                    if peer.as_ref().is_some_and(|peer| !peer.current()) {
                        return Err(crate::security::Error::Transport.into());
                    }
                    database
                        .apply_replica(context.clone(), generation, cursor, batch)
                        .await?;
                    position = cursor;
                    previous = Some(frame);
                }
            }
            _ => return Err(Error::Sequence),
        }
        protocol::write(
            &mut socket,
            &Message::Ack(position),
            limits(context),
            config.frame_timeout,
        )
        .await?;
    }
}

async fn receive_snapshot(
    socket: &mut (impl AsyncRead + AsyncWrite + Unpin),
    cursor: Cursor,
    entries: u64,
    context: &Context,
    config: &Config,
) -> Result<Vec<Mutation>, Error> {
    if entries > context.store_config.max_dataset_bytes as u64 / 128 {
        return Err(Error::Limit);
    }
    let mut mutations = Vec::new();
    let mut bytes = 0usize;
    let mut digest = 0;
    let mut previous = None;
    loop {
        let phase = if mutations.len() < entries as usize {
            InternalPhase::SnapshotEntry
        } else {
            InternalPhase::SnapshotEnd
        };
        let (frame, message) =
            protocol::read_in_phase(socket, limits(context), config.frame_timeout, phase).await?;
        match message {
            Message::SnapshotEntry(mutation) if mutations.len() < entries as usize => {
                if previous.as_ref().is_some_and(|key| key >= mutation.key()) {
                    return Err(Error::Sequence);
                }
                previous = Some(mutation.key().clone());
                bytes = bytes
                    .checked_add(frame.len())
                    .filter(|bytes| *bytes <= context.store_config.max_dataset_bytes)
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
