//! FULL/CONTINUE, confirmation after durable apply, and reconnection of a single session.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;

use super::security::{self, Access, Authority};
use super::{
    Cursor, Error,
    config::Config,
    journal::Subscriber,
    protocol::{self, ComposedHello, Hello, Message, Reject},
};
use crate::persistence::{composed_format, format};
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

fn composed(context: &Context) -> bool {
    context.aof.is_composed_legacy_mixed()
}

fn hello_message(context: &Context, cursor: Option<Cursor>) -> Message {
    let base = hello(context, cursor);
    if composed(context) {
        Message::ComposedHello(ComposedHello {
            base,
            aof_header_version: composed_format::HEADER_VERSION,
            required_capabilities: composed_format::MIXED_OWNER_BATCH,
        })
    } else {
        Message::Hello(base)
    }
}

fn accept_hello(context: &Context, request: Message) -> Result<Hello, Reject> {
    match (composed(context), request) {
        (false, Message::Hello(peer)) => {
            peer.accepts(&hello(context, None))?;
            Ok(peer)
        }
        (true, Message::ComposedHello(peer)) => {
            let Message::ComposedHello(source) = hello_message(context, None) else {
                unreachable!()
            };
            peer.accepts(&source)?;
            Ok(peer.base)
        }
        _ => Err(Reject::IncompatibleVersion),
    }
}

async fn write_profile(
    socket: &mut (impl AsyncWrite + Unpin),
    message: &Message,
    context: &Context,
    deadline: Duration,
    authority: Option<&Authority>,
) -> Result<(), Error> {
    if composed(context) {
        security::write_composed(socket, message, limits(context), deadline, authority).await
    } else {
        security::write(socket, message, limits(context), deadline, authority).await
    }
}

async fn read_profile(
    socket: &mut (impl AsyncRead + Unpin),
    context: &Context,
    deadline: Duration,
    authority: Option<&Authority>,
) -> Result<(Bytes, Message, Option<crate::security::InternalLease>), Error> {
    if composed(context) {
        security::read_composed(socket, limits(context), deadline, authority).await
    } else {
        security::read(socket, limits(context), deadline, authority, None).await
    }
}

async fn read_phase_profile(
    socket: &mut (impl AsyncRead + Unpin),
    context: &Context,
    deadline: Duration,
    phase: InternalPhase,
    authority: Option<&Authority>,
) -> Result<(Bytes, Message), Error> {
    if composed(context) {
        security::read_composed_phase(socket, limits(context), deadline, phase, authority).await
    } else {
        security::read_phase(socket, limits(context), deadline, phase, authority).await
    }
}

fn encode_profile(message: &Message, context: &Context) -> Result<Bytes, protocol::Error> {
    if composed(context) {
        protocol::encode_composed(message, limits(context))
    } else {
        protocol::encode(message, limits(context))
    }
}

async fn operation_lease(
    authority: Option<&Authority>,
    deadline: Duration,
) -> Result<Option<crate::security::InternalLease>, Error> {
    match authority {
        Some(authority) => authority.operation(deadline).await,
        None => Ok(None),
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
        Some(Access::Static(security)),
    )
    .await
}

/// Embedding API using the shared live generation registry. Protected executable
/// startup remains gated until the complete operational evidence is available.
pub async fn serve_live(
    listener: TcpListener,
    database: DbHandle,
    context: Context,
    config: Config,
    shutdown: watch::Receiver<bool>,
    security: crate::security::InternalRuntime,
) -> Result<(), Error> {
    serve_inner(
        listener,
        database,
        context,
        config,
        shutdown,
        Some(Access::Live(security)),
    )
    .await
}

async fn serve_inner(
    listener: TcpListener,
    database: DbHandle,
    context: Context,
    config: Config,
    mut shutdown: watch::Receiver<bool>,
    security: Option<Access>,
) -> Result<(), Error> {
    if composed(&context) && security.is_none() && !listener.local_addr()?.ip().is_loopback() {
        return Err(Error::Sequence);
    }
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
                        let (socket, authority) = security.accept(socket).await?;
                        (socket, Some(authority))
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
    authority: Option<&Authority>,
) -> Result<(), Error> {
    write_profile(
        socket,
        &Message::Reject(reason),
        context,
        config.frame_timeout,
        authority,
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
    mut authority: Option<Authority>,
) -> Result<(), Error> {
    let (_, request, mut request_lease) = read_profile(
        &mut socket,
        &context,
        config.frame_timeout,
        authority.as_ref(),
    )
    .await?;
    if let Some(authority) = &mut authority {
        authority.set_request(&request);
    }
    drop(request_lease.take());
    let authority = authority.as_ref();
    if database.check_admission().is_err() {
        return reject(
            &mut socket,
            Reject::Unavailable,
            &context,
            &config,
            authority,
        )
        .await;
    }
    match request {
        Message::StatusRequest => {
            let status = database
                .authorized_internal(operation_lease(authority, config.frame_timeout).await?)
                .replication_status(&context.runtime)
                .await?;
            let bytes = context
                .runtime
                .journal()
                .and_then(|journal| journal.status().ok())
                .map_or(0, |status| status.bytes as u64);
            write_profile(
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
                &context,
                config.frame_timeout,
                authority,
            )
            .await?;
            return Ok(());
        }
        Message::Promote if local || authority.is_some() => {
            let cursor = if context.runtime.readonly() {
                database
                    .authorized_internal(operation_lease(authority, config.frame_timeout).await?)
                    .promote_replica(context.clone(), super::new_epoch()?)
                    .await?
            } else {
                database
                    .authorized_internal(operation_lease(authority, config.frame_timeout).await?)
                    .replication_position(&context.runtime)
                    .await?
            };
            tracing::info!(
                sequence = cursor.sequence,
                "manual promotion persisted; previous upstream stopped"
            );
            write_profile(
                &mut socket,
                &Message::Promoted(cursor),
                &context,
                config.frame_timeout,
                authority,
            )
            .await?;
            return Ok(());
        }
        Message::Promote => {
            return reject(
                &mut socket,
                Reject::Unavailable,
                &context,
                &config,
                authority,
            )
            .await;
        }
        _ => {}
    }
    if context.runtime.readonly() {
        return reject(
            &mut socket,
            Reject::Unavailable,
            &context,
            &config,
            authority,
        )
        .await;
    }
    let source = hello(&context, None);
    match request {
        Message::Export {
            sider_version,
            max_record_bytes,
            max_snapshot_bytes,
        } => {
            if sider_version != source.sider_version {
                return reject(
                    &mut socket,
                    Reject::IncompatibleVersion,
                    &context,
                    &config,
                    authority,
                )
                .await;
            }
            if max_record_bytes < source.max_record_bytes
                || max_snapshot_bytes < source.max_snapshot_bytes
            {
                return reject(
                    &mut socket,
                    Reject::ResourceLimit,
                    &context,
                    &config,
                    authority,
                )
                .await;
            }
            let snapshot = timeout(
                config.sync_timeout,
                database
                    .authorized_internal(operation_lease(authority, config.frame_timeout).await?)
                    .replication_snapshot(&context, false),
            )
            .await
            .map_err(|_| protocol::Error::Timeout)??;
            write_profile(
                &mut socket,
                &hello_message(&context, Some(snapshot.cursor)),
                &context,
                config.frame_timeout,
                authority,
            )
            .await?;
            timeout(
                config.sync_timeout,
                send_snapshot(&mut socket, &snapshot, &context, &config, authority),
            )
            .await
            .map_err(|_| protocol::Error::Timeout)??;
            socket.shutdown().await?;
            Ok(())
        }
        request @ (Message::Hello(_) | Message::ComposedHello(_)) => {
            let peer = match accept_hello(&context, request) {
                Ok(peer) => peer,
                Err(reason) => {
                    return reject(&mut socket, reason, &context, &config, authority).await;
                }
            };
            let journal = context.runtime.journal().ok_or(Error::Stale)?;
            let journal_lease = operation_lease(authority, config.frame_timeout).await?;
            let resume = peer.cursor.and_then(|cursor| {
                journal
                    .subscribe(cursor)
                    .ok()
                    .map(|subscription| (cursor, subscription))
            });
            drop(journal_lease);
            let (cursor, subscription) = if let Some((cursor, subscription)) = resume {
                let head = database
                    .authorized_internal(operation_lease(authority, config.frame_timeout).await?)
                    .replication_position(&context.runtime)
                    .await?;
                write_profile(
                    &mut socket,
                    &hello_message(&context, Some(head)),
                    &context,
                    config.frame_timeout,
                    authority,
                )
                .await?;
                write_profile(
                    &mut socket,
                    &Message::Continue(cursor),
                    &context,
                    config.frame_timeout,
                    authority,
                )
                .await?;
                tracing::info!(sequence = cursor.sequence, "replication CONTINUE");
                (cursor, subscription)
            } else {
                let mut snapshot = timeout(
                    config.sync_timeout,
                    database
                        .authorized_internal(
                            operation_lease(authority, config.frame_timeout).await?,
                        )
                        .replication_snapshot(&context, true),
                )
                .await
                .map_err(|_| protocol::Error::Timeout)??;
                write_profile(
                    &mut socket,
                    &hello_message(&context, Some(snapshot.cursor)),
                    &context,
                    config.frame_timeout,
                    authority,
                )
                .await?;
                timeout(
                    config.sync_timeout,
                    send_snapshot(&mut socket, &snapshot, &context, &config, authority),
                )
                .await
                .map_err(|_| protocol::Error::Timeout)??;
                tracing::info!(sequence = snapshot.cursor.sequence, "replication FULL");
                (
                    snapshot.cursor,
                    snapshot.subscription.take().ok_or(Error::Sequence)?,
                )
            };
            expect_ack(&mut socket, cursor, &context, &config, authority).await?;
            stream(
                &mut socket,
                cursor,
                subscription,
                &context,
                &config,
                authority,
            )
            .await
        }
        _ => {
            reject(
                &mut socket,
                Reject::Unavailable,
                &context,
                &config,
                authority,
            )
            .await
        }
    }
}

async fn send_snapshot(
    socket: &mut (impl AsyncRead + AsyncWrite + Unpin),
    snapshot: &Snapshot,
    context: &Context,
    config: &Config,
    authority: Option<&Authority>,
) -> Result<(), Error> {
    write_profile(
        socket,
        &Message::FullStart {
            cursor: snapshot.cursor,
            entries: snapshot.mutations.len() as u64,
        },
        context,
        config.frame_timeout,
        authority,
    )
    .await?;
    let mut digest = 0;
    let mut bytes = 0usize;
    for mutation in &snapshot.mutations {
        let frame = encode_profile(&Message::SnapshotEntry(mutation.clone()), context)?;
        bytes = bytes
            .checked_add(frame.len())
            .filter(|bytes| *bytes <= context.store_config.max_dataset_bytes)
            .ok_or(Error::Limit)?;
        digest = format::snapshot_digest(digest, &frame);
        security::write_frame(socket, &frame, config.frame_timeout, authority).await?;
    }
    write_profile(
        socket,
        &Message::FullEnd {
            cursor: snapshot.cursor,
            entries: snapshot.mutations.len() as u64,
            digest,
        },
        context,
        config.frame_timeout,
        authority,
    )
    .await?;
    Ok(())
}

async fn expect_ack(
    socket: &mut (impl AsyncRead + AsyncWrite + Unpin),
    cursor: Cursor,
    context: &Context,
    config: &Config,
    authority: Option<&Authority>,
) -> Result<(), Error> {
    if read_phase_profile(
        socket,
        context,
        config.frame_timeout,
        InternalPhase::ReplicaAck,
        authority,
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
    authority: Option<&Authority>,
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
                write_profile(
                    socket,
                    &Message::Heartbeat(head),
                    context,
                    config.frame_timeout,
                    authority,
                )
                .await?;
                security::write_frame(socket, &entry.frame, config.frame_timeout, authority)
                    .await?;
                expect_ack(socket, sent, context, config, authority).await?;
                expect_ack(socket, entry.cursor, context, config, authority).await?;
                sent = entry.cursor;
            }
            Ok(Err(error)) => {
                reject(socket, Reject::FullRequired, context, config, authority).await?;
                return Err(error.into());
            }
            Err(_) => {
                let head = context
                    .runtime
                    .journal()
                    .ok_or(Error::Stale)?
                    .status()?
                    .head;
                write_profile(
                    socket,
                    &Message::Heartbeat(head),
                    context,
                    config.frame_timeout,
                    authority,
                )
                .await?;
                expect_ack(socket, sent, context, config, authority).await?;
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
    follow_inner(
        database,
        context,
        config,
        shutdown,
        Some(Access::Static(security)),
    )
    .await
}

/// Outgoing reconnects and final effects use the current shared generation.
pub async fn follow_live(
    database: DbHandle,
    context: Context,
    config: Config,
    shutdown: watch::Receiver<bool>,
    security: crate::security::InternalRuntime,
) -> Result<(), Error> {
    follow_inner(
        database,
        context,
        config,
        shutdown,
        Some(Access::Live(security)),
    )
    .await
}

async fn follow_inner(
    database: DbHandle,
    context: Context,
    config: Config,
    mut shutdown: watch::Receiver<bool>,
    security: Option<Access>,
) -> Result<(), Error> {
    if composed(&context)
        && security.is_none()
        && !config
            .upstream
            .is_some_and(|address| address.ip().is_loopback())
    {
        return Err(Error::Sequence);
    }
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
    let cursor = (position.epoch != [0; 16]).then_some(position);
    write_profile(
        &mut socket,
        &hello_message(context, cursor),
        context,
        config.frame_timeout,
        authority,
    )
    .await?;
    let source_message = read_phase_profile(
        &mut socket,
        context,
        config.frame_timeout,
        InternalPhase::Hello,
        authority,
    )
    .await?
    .1;
    let source = match (composed(context), source_message) {
        (false, Message::Hello(source)) => {
            hello(context, None)
                .accepts(&source)
                .map_err(|_| Error::Limit)?;
            source
        }
        (true, Message::ComposedHello(source)) => {
            let Message::ComposedHello(local) = hello_message(context, None) else {
                unreachable!()
            };
            local.accepts(&source).map_err(|_| Error::Limit)?;
            source.base
        }
        _ => return Err(Error::Sequence),
    };
    let head = source.cursor.ok_or(Error::Sequence)?;
    match read_phase_profile(
        &mut socket,
        context,
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
                receive_snapshot(&mut socket, cursor, entries, context, config, authority),
            )
            .await
            .map_err(|_| protocol::Error::Timeout)??;
            database
                .authorized_internal(operation_lease(authority, config.sync_timeout).await?)
                .install_replica(context.clone(), generation, cursor, mutations)
                .await?;
            position = cursor;
            context.runtime.connected(generation, head.sequence, true);
        }
        _ => return Err(Error::Sequence),
    }
    write_profile(
        &mut socket,
        &Message::Ack(position),
        context,
        config.frame_timeout,
        authority,
    )
    .await?;
    let mut previous: Option<Bytes> = None;
    loop {
        let (frame, message) = read_phase_profile(
            &mut socket,
            context,
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
                    database
                        .authorized_internal(
                            operation_lease(authority, config.frame_timeout).await?,
                        )
                        .apply_replica(context.clone(), generation, cursor, batch)
                        .await?;
                    position = cursor;
                    previous = Some(frame);
                }
            }
            _ => return Err(Error::Sequence),
        }
        write_profile(
            &mut socket,
            &Message::Ack(position),
            context,
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
    config: &Config,
    authority: Option<&Authority>,
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
            read_phase_profile(socket, context, config.frame_timeout, phase, authority).await?;
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
