//! One request in flight per connection, with bounded input and output.

use std::io;
use std::net::IpAddr;

use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until, timeout_at};

use crate::ServerConfig;
use crate::command::{Command, command_resp3, parse, push, resp3};
use crate::metrics::Counter;
use crate::pubsub::{Hub, Message, PubSubError, Subscription};
use crate::resp::{Decoder, EncodeError, Frame, ProtocolError, RespLimits, encode};
use crate::storage::worker::{DbError, DbHandle};

mod transaction;

fn negotiate(frame: &Frame, protocol: &mut u8, readonly: bool, id: i64) -> Frame {
    match crate::command::parse_hello(frame) {
        Ok(Some(version)) => *protocol = version,
        Ok(None) => {}
        Err(error) => return error.into_frame(),
    }
    hello_reply(*protocol, readonly, id)
}

fn hello_reply(protocol: u8, readonly: bool, id: i64) -> Frame {
    let mut fields = Vec::new();
    for (key, value) in [
        (
            b"server".as_slice(),
            Frame::Bulk(Some(Bytes::from_static(b"sider"))),
        ),
        (
            b"version".as_slice(),
            Frame::Bulk(Some(Bytes::from_static(
                env!("CARGO_PKG_VERSION").as_bytes(),
            ))),
        ),
        (b"proto".as_slice(), Frame::Integer(i64::from(protocol))),
        (b"id".as_slice(), Frame::Integer(id)),
        (
            b"mode".as_slice(),
            Frame::Bulk(Some(Bytes::from_static(b"standalone"))),
        ),
        (
            b"role".as_slice(),
            Frame::Bulk(Some(Bytes::from_static(if readonly {
                b"slave"
            } else {
                b"master"
            }))),
        ),
        (b"modules".as_slice(), Frame::Array(Some(Vec::new()))),
    ] {
        fields.push(Frame::Bulk(Some(Bytes::copy_from_slice(key))));
        fields.push(value);
    }
    if protocol == 3 {
        Frame::Map(fields)
    } else {
        Frame::Array(Some(fields))
    }
}

#[derive(Debug, Error)]
pub(crate) enum ConnectionError {
    #[error("connection I/O failure: {0}")]
    Io(#[from] io::Error),
    #[error("invalid protocol: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("invalid request format")]
    InvalidRequest,
    #[error("frame truncated at EOF")]
    Truncated,
    #[error("input buffer limit exceeded")]
    InputLimit,
    #[error("frame-formation timeout exceeded")]
    FrameTimeout,
    #[error("write timeout exceeded")]
    WriteTimeout,
    #[error("reply encoding failure: {0}")]
    Encode(#[from] EncodeError),
    #[error("worker failure: {0}")]
    Database(#[from] DbError),
    #[error("invalid connection configuration: {0}")]
    Config(#[from] crate::ConfigError),
    #[error("subscriber failure: {0}")]
    PubSub(#[from] PubSubError),
}

pub(crate) async fn stopped(shutdown: &mut watch::Receiver<bool>) {
    // A dropped sender also terminates consumers, without an infinite wait.
    let _ = shutdown.wait_for(|stopping| *stopping).await;
}

#[cfg(test)]
pub(crate) async fn run<S>(
    stream: S,
    config: ServerConfig,
    database: DbHandle,
    shutdown: watch::Receiver<bool>,
) -> Result<(), ConnectionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let hub = Hub::with_metrics(database.metrics.clone());
    run_with_pubsub(stream, config, database, shutdown, hub).await
}

pub(crate) async fn run_with_pubsub<S>(
    stream: S,
    config: ServerConfig,
    database: DbHandle,
    shutdown: watch::Receiver<bool>,
    hub: Hub,
) -> Result<(), ConnectionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    run_with_security(stream, config, database, shutdown, hub, None).await
}

pub(crate) async fn run_with_security<S>(
    stream: S,
    config: ServerConfig,
    database: DbHandle,
    shutdown: watch::Receiver<bool>,
    hub: Hub,
    security: Option<(crate::security::ClientSecurity, IpAddr)>,
) -> Result<(), ConnectionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    run_with_registered_security(stream, config, database, shutdown, hub, security, None).await
}

pub(crate) async fn run_with_registered_security<S>(
    stream: S,
    config: ServerConfig,
    database: DbHandle,
    shutdown: watch::Receiver<bool>,
    hub: Hub,
    security: Option<(crate::security::ClientSecurity, IpAddr)>,
    registered: Option<crate::security::Session>,
) -> Result<(), ConnectionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let metrics = database.metrics.clone();
    let _connection = metrics.connection();
    let mut session = match (registered, &security) {
        (Some(session), _) => Some(session),
        (None, Some((security, _))) => security.register().await,
        (None, None) => None,
    };
    let result = run_inner(
        stream,
        config,
        database,
        shutdown,
        hub,
        security,
        &mut session,
    )
    .await;
    // The inner future has dropped the stream before close acknowledgements fire.
    drop(session);
    if let Err(error) = &result {
        metrics.add(Counter::ConnectionFailures, 1);
        match error {
            ConnectionError::WriteTimeout => metrics.add(Counter::WriteTimeouts, 1),
            ConnectionError::Encode(_) => metrics.add(Counter::ResponseLimitFailures, 1),
            _ => {}
        }
        if matches!(
            error,
            ConnectionError::Protocol(_)
                | ConnectionError::InvalidRequest
                | ConnectionError::Truncated
                | ConnectionError::InputLimit
                | ConnectionError::FrameTimeout
        ) {
            metrics.add(Counter::ProtocolErrors, 1);
        }
    }
    result
}

#[allow(clippy::too_many_arguments)] // The session guard must outlive the socket future for close acknowledgements.
async fn run_inner<S>(
    mut stream: S,
    config: ServerConfig,
    database: DbHandle,
    mut shutdown: watch::Receiver<bool>,
    hub: Hub,
    security: Option<(crate::security::ClientSecurity, IpAddr)>,
    session: &mut Option<crate::security::Session>,
) -> Result<(), ConnectionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    config.validate()?;
    let mut subscription = hub.connect(config.pubsub_max_channels, config.pubsub_queue_capacity)?;
    let mut transaction = transaction::Transaction::default();
    let mut decoder = Decoder::new(config.resp_limits)?;
    let mut input = BytesMut::new();
    let mut output = BytesMut::new();
    let mut protocol = 2_u8;
    let connection_id = subscription.id();
    let mut scratch = [0_u8; 8192];
    let mut frame_started = None;
    let mut last_read_at = None;
    let authentication_deadline = Instant::now() + std::time::Duration::from_secs(5);
    let mut identity: Option<crate::security::CredentialProof> = None;
    let mut ever_authenticated = false;
    let mut authentication_failures = 0_u8;

    loop {
        let lease = tokio::select! {
            biased;
            update = next_update(session) => {
                if apply_update(update, session, &security, &mut identity, &mut transaction, &mut subscription, &hub, &config, protocol)? { return Ok(()); }
                continue;
            }
            _ = stopped(&mut shutdown) => return Ok(()),
            lease = async { match &security { Some((security, _)) => Some(security.lease().await), None => None } } => lease,
        };
        if lease
            .as_ref()
            .zip(security.as_ref())
            .is_some_and(|(lease, (security, _))| {
                lease.transport.digest != security.transport.digest || !lease.transport.current()
            })
        {
            return Ok(());
        }
        let policy = lease.as_ref().map(|lease| lease.policy.clone());
        let database = database.authorized(lease.clone().map(|lease| match &identity {
            Some(proof) => lease.authenticated(proof.clone()),
            None => lease,
        }));
        if security.is_some() && !ever_authenticated && Instant::now() >= authentication_deadline {
            return Ok(());
        }
        if let Some(policy) = &policy
            && identity
                .as_ref()
                .is_some_and(|proof| !proof.valid(policy, time::OffsetDateTime::now_utc()))
        {
            identity = None;
            transaction = transaction::Transaction::default();
            subscription = hub.connect(config.pubsub_max_channels, config.pubsub_queue_capacity)?;
            subscription.resp3 = protocol == 3;
        }
        if *shutdown.borrow() || shutdown.has_changed().is_err() {
            return Ok(());
        }
        if subscription.is_evicted() {
            return Err(PubSubError::Closed.into());
        }
        // One notification per round keeps commands and messages progressing.
        if let Some(message) = subscription.try_message() {
            if let Some(policy) = &policy {
                let permitted = identity.as_ref().is_some_and(|proof| {
                    policy
                        .authorize(
                            proof.username(),
                            b"SUBSCRIBE",
                            &Command::Subscribe {
                                channels: vec![message.channel.clone()],
                            },
                        )
                        .is_ok()
                });
                if !permitted {
                    continue;
                }
            }
            write_subscriber_response(
                &mut stream,
                &mut output,
                push(message.into_frame(), protocol),
                &config,
                &mut subscription,
                protocol,
            )
            .await?;
        }
        match decoder.decode(&mut input) {
            Ok(Some(frame)) => {
                database.metrics.add(Counter::Requests, 1);
                // We read only while the prior frame was incomplete. Therefore,
                // any new suffix necessarily came from the most recent read.
                frame_started = if input.is_empty() { None } else { last_read_at };
                let command_name = match &frame {
                    Frame::Array(Some(parts)) => match parts.first() {
                        Some(Frame::Bulk(Some(name))) => name.to_ascii_lowercase(),
                        _ => Vec::new(),
                    },
                    _ => Vec::new(),
                };
                let memory_command = crate::command::memory::name(&command_name).is_some();
                // A bounded notification write may have crossed the credential's
                // UTC deadline before this next command reached admission.
                if let Some(policy) = &policy
                    && identity
                        .as_ref()
                        .is_some_and(|proof| !proof.valid(policy, time::OffsetDateTime::now_utc()))
                {
                    identity = None;
                    transaction = transaction::Transaction::default();
                    subscription =
                        hub.connect(config.pubsub_max_channels, config.pubsub_queue_capacity)?;
                    subscription.resp3 = protocol == 3;
                }
                if command_name == b"hello" && security.is_none() {
                    let invalid_request = !matches!(&frame, Frame::Array(Some(parts)) if parts.iter().all(|part| matches!(part, Frame::Bulk(Some(_)))));
                    let response = if transaction.active() {
                        transaction.poison();
                        Frame::Error(Bytes::from_static(
                            b"ERR HELLO is not supported inside MULTI",
                        ))
                    } else {
                        negotiate(&frame, &mut protocol, database.readonly(), connection_id)
                    };
                    subscription.resp3 = protocol == 3;
                    write_subscriber_response(
                        &mut stream,
                        &mut output,
                        response,
                        &config,
                        &mut subscription,
                        protocol,
                    )
                    .await?;
                    if invalid_request {
                        return Err(ConnectionError::InvalidRequest);
                    }
                    continue;
                }
                let request_bytes = transaction::request_bytes(&frame);
                if let Some((security, peer)) = &security {
                    if command_name.eq_ignore_ascii_case(b"auth")
                        || command_name.eq_ignore_ascii_case(b"hello")
                    {
                        let hello = command_name.eq_ignore_ascii_case(b"hello");
                        let control = if hello {
                            crate::command::parse_hello_auth(&frame)
                                .map(|request| (request.version, request.authentication))
                        } else {
                            crate::command::parse_auth(&frame)
                                .map(|credentials| (None, Some(credentials)))
                        };
                        let fatal =
                            matches!(&control, Err(crate::command::RequestError::InvalidFormat));
                        let response = if transaction.active() {
                            transaction.poison();
                            Frame::Error(Bytes::from_static(
                                b"ERR authentication/protocol control inside MULTI is not allowed",
                            ))
                        } else {
                            match control {
                                Err(error) => error.into_frame(),
                                Ok((version, None)) => {
                                    if identity.is_none() {
                                        Frame::Error(Bytes::from_static(
                                            b"NOAUTH Authentication required",
                                        ))
                                    } else {
                                        if let Some(version) = version {
                                            protocol = version;
                                        }
                                        hello_reply(protocol, database.readonly(), connection_id)
                                    }
                                }
                                Ok((version, Some((username, password)))) => {
                                    let deadline = if ever_authenticated {
                                        Instant::now() + std::time::Duration::from_secs(5)
                                    } else {
                                        authentication_deadline
                                    };
                                    match security
                                        .authentication
                                        .verify_peer(
                                            *peer,
                                            policy
                                                .as_ref()
                                                .expect("protected policy lease")
                                                .clone(),
                                            username,
                                            password,
                                            deadline,
                                        )
                                        .await
                                    {
                                        Ok(proof) => {
                                            // Commit identity, protocol and local state only after complete verification.
                                            identity = Some(proof);
                                            ever_authenticated = true;
                                            authentication_failures = 0;
                                            transaction = transaction::Transaction::default();
                                            subscription = hub.connect(
                                                config.pubsub_max_channels,
                                                config.pubsub_queue_capacity,
                                            )?;
                                            if let Some(version) = version {
                                                protocol = version;
                                            }
                                            if hello {
                                                hello_reply(
                                                    protocol,
                                                    database.readonly(),
                                                    connection_id,
                                                )
                                            } else {
                                                Frame::Simple(Bytes::from_static(b"OK"))
                                            }
                                        }
                                        Err(_) => {
                                            database
                                                .metrics
                                                .add(Counter::AuthenticationFailures, 1);
                                            authentication_failures += 1;
                                            Frame::Error(Bytes::from_static(
                                                b"WRONGPASS Authentication failed",
                                            ))
                                        }
                                    }
                                }
                            }
                        };
                        subscription.resp3 = protocol == 3;
                        write_subscriber_response(
                            &mut stream,
                            &mut output,
                            response,
                            &config,
                            &mut subscription,
                            protocol,
                        )
                        .await?;
                        if fatal {
                            return Err(ConnectionError::InvalidRequest);
                        }
                        if authentication_failures >= 3
                            || security.authentication.peer_exhausted(*peer)
                        {
                            return Ok(());
                        }
                        continue;
                    }
                    if identity.is_none() {
                        write_response(
                            &mut stream,
                            &mut output,
                            Frame::Error(Bytes::from_static(b"NOAUTH Authentication required")),
                            &config,
                        )
                        .await?;
                        continue;
                    }
                }
                if policy.is_some()
                    && command_name == b"info"
                    && matches!(&frame, Frame::Array(Some(parts)) if parts.iter().all(|part| matches!(part, Frame::Bulk(Some(_)))) && parts.iter().skip(1).any(|part| matches!(part, Frame::Bulk(Some(name)) if !crate::command::InfoSections::valid_name(name))))
                {
                    transaction.poison();
                    write_response(
                        &mut stream,
                        &mut output,
                        crate::command::RequestError::Syntax.into_frame(),
                        &config,
                    )
                    .await?;
                    continue;
                }
                let response = match parse(frame) {
                    Ok(command) => {
                        // Connection-local controls must also stop after failed publication.
                        database.check_admission()?;
                        if let Some(policy) = &policy {
                            let allowed = identity.as_ref().is_some_and(|proof| {
                                proof.valid(policy, time::OffsetDateTime::now_utc())
                                    && policy
                                        .authorize(proof.username(), &command_name, &command)
                                        .is_ok()
                            });
                            if !allowed {
                                let expired = identity.as_ref().is_some_and(|proof| {
                                    !proof.valid(policy, time::OffsetDateTime::now_utc())
                                });
                                if expired {
                                    identity = None;
                                    transaction = transaction::Transaction::default();
                                    subscription = hub.connect(
                                        config.pubsub_max_channels,
                                        config.pubsub_queue_capacity,
                                    )?;
                                    subscription.resp3 = protocol == 3;
                                } else {
                                    database.metrics.add(Counter::AuthorizationDenials, 1);
                                    transaction.poison();
                                }
                                write_response(
                                    &mut stream,
                                    &mut output,
                                    Frame::Error(Bytes::from_static(if expired {
                                        b"NOAUTH Authentication required"
                                    } else {
                                        b"NOPERM Permission denied"
                                    })),
                                    &config,
                                )
                                .await?;
                                continue;
                            }
                            if transaction.active()
                                && matches!(
                                    command,
                                    Command::Subscribe { .. }
                                        | Command::Unsubscribe { .. }
                                        | Command::Publish { .. }
                                )
                            {
                                transaction.poison();
                                write_response(
                                    &mut stream,
                                    &mut output,
                                    Frame::Error(Bytes::from_static(
                                        b"ERR Pub/Sub inside MULTI is not allowed",
                                    )),
                                    &config,
                                )
                                .await?;
                                continue;
                            }
                        }
                        let response_command = (protocol == 3).then(|| command.clone());
                        let action = match transaction
                            .handle(
                                command,
                                request_bytes,
                                &config,
                                &database,
                                subscription.active() && protocol == 2,
                            )
                            .await
                        {
                            Ok(action) => action,
                            Err(DbError::SecurityExpired) => {
                                if lease
                                    .as_ref()
                                    .is_some_and(|lease| !lease.transport.current())
                                {
                                    return Ok(());
                                }
                                identity = None;
                                transaction = transaction::Transaction::default();
                                subscription = hub.connect(
                                    config.pubsub_max_channels,
                                    config.pubsub_queue_capacity,
                                )?;
                                subscription.resp3 = protocol == 3;
                                write_response(
                                    &mut stream,
                                    &mut output,
                                    Frame::Error(Bytes::from_static(
                                        b"NOAUTH Authentication required",
                                    )),
                                    &config,
                                )
                                .await?;
                                continue;
                            }
                            Err(error) => return Err(error.into()),
                        };
                        let command = match action {
                            transaction::Action::Exec { commands, watched } => {
                                if let Some(policy) = &policy
                                    && identity.as_ref().is_none_or(|proof| {
                                        commands.iter().any(|command| {
                                            policy
                                                .authorize(
                                                    proof.username(),
                                                    crate::security::command_name(command)
                                                        .as_bytes(),
                                                    command,
                                                )
                                                .is_err()
                                        })
                                    })
                                {
                                    write_response(
                                        &mut stream,
                                        &mut output,
                                        Frame::Error(Bytes::from_static(
                                            b"NOPERM Permission denied",
                                        )),
                                        &config,
                                    )
                                    .await?;
                                    continue;
                                }
                                let result = match database
                                    .execute_transaction(
                                        commands,
                                        watched,
                                        subscription,
                                        response_limits(&config),
                                    )
                                    .await
                                {
                                    Ok(result) => result,
                                    Err(DbError::SecurityExpired) => {
                                        if lease
                                            .as_ref()
                                            .is_some_and(|lease| !lease.transport.current())
                                        {
                                            return Ok(());
                                        }
                                        identity = None;
                                        transaction = transaction::Transaction::default();
                                        subscription = hub.connect(
                                            config.pubsub_max_channels,
                                            config.pubsub_queue_capacity,
                                        )?;
                                        subscription.resp3 = protocol == 3;
                                        write_response(
                                            &mut stream,
                                            &mut output,
                                            Frame::Error(Bytes::from_static(
                                                b"NOAUTH Authentication required",
                                            )),
                                            &config,
                                        )
                                        .await?;
                                        continue;
                                    }
                                    Err(error) => return Err(error.into()),
                                };
                                subscription = result.subscription;
                                let bytes = result.output?;
                                tokio::select! {
                                    biased;
                                    _ = subscription.evicted() => return Err(PubSubError::Closed.into()),
                                    result = write_buffer(&mut stream, &bytes, &config) => result?,
                                }
                                continue;
                            }
                            transaction::Action::Execute(command) => command,
                            transaction::Action::Reply(frame) => {
                                write_subscriber_response(
                                    &mut stream,
                                    &mut output,
                                    frame,
                                    &config,
                                    &mut subscription,
                                    protocol,
                                )
                                .await?;
                                continue;
                            }
                        };
                        match command {
                            Command::Subscribe { channels } => {
                                match subscription.subscribe(channels) {
                                    Ok(frames) => {
                                        for frame in frames {
                                            write_subscriber_response(
                                                &mut stream,
                                                &mut output,
                                                push(frame, protocol),
                                                &config,
                                                &mut subscription,
                                                protocol,
                                            )
                                            .await?;
                                        }
                                        continue;
                                    }
                                    Err(error) => Frame::Error(Bytes::from(error.to_string())),
                                }
                            }
                            Command::Unsubscribe { channels } => {
                                for frame in subscription.unsubscribe(channels)? {
                                    write_subscriber_response(
                                        &mut stream,
                                        &mut output,
                                        push(frame, protocol),
                                        &config,
                                        &mut subscription,
                                        protocol,
                                    )
                                    .await?;
                                }
                                continue;
                            }
                            Command::Ping(payload) if subscription.active() && protocol == 2 => {
                                Frame::Array(Some(vec![
                                    Frame::Bulk(Some(Bytes::from_static(b"pong"))),
                                    Frame::Bulk(Some(payload.unwrap_or_default())),
                                ]))
                            }
                            _ if subscription.active() && protocol == 2 => {
                                Frame::Error(Bytes::from(format!(
                                    "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                                    String::from_utf8_lossy(&command_name),
                                )))
                            }
                            Command::Info(sections) => match database.info(sections).await {
                                Ok(info) => Frame::Bulk(Some(info)),
                                Err(DbError::ShuttingDown) => return Ok(()),
                                Err(error) => return Err(error.into()),
                            },
                            Command::Publish { channel, message } => {
                                let message = Message {
                                    channel,
                                    payload: message,
                                };
                                output.clear();
                                if encode(
                                    &message.clone().into_frame(),
                                    &mut output,
                                    response_limits(&config),
                                )
                                .is_err()
                                {
                                    Frame::Error(Bytes::from_static(
                                        b"ERR pubsub message exceeds response limit",
                                    ))
                                } else {
                                    Frame::Integer(hub.publish(message))
                                }
                            }
                            command => match database.execute(command).await {
                                Ok(reply) => {
                                    if protocol == 3 {
                                        command_resp3(
                                            Frame::from(reply),
                                            response_command
                                                .as_ref()
                                                .expect("RESP3 command context"),
                                        )
                                    } else {
                                        Frame::from(reply)
                                    }
                                }
                                Err(DbError::ShuttingDown) => return Ok(()),
                                Err(DbError::SecurityExpired) => {
                                    if lease
                                        .as_ref()
                                        .is_some_and(|lease| !lease.transport.current())
                                    {
                                        return Ok(());
                                    }
                                    identity = None;
                                    transaction = transaction::Transaction::default();
                                    subscription = hub.connect(
                                        config.pubsub_max_channels,
                                        config.pubsub_queue_capacity,
                                    )?;
                                    subscription.resp3 = protocol == 3;
                                    Frame::Error(Bytes::from_static(
                                        b"NOAUTH Authentication required",
                                    ))
                                }
                                Err(error) => return Err(error.into()),
                            },
                        }
                    }
                    Err(error) => {
                        transaction.poison();
                        let fatal = error.is_fatal();
                        write_subscriber_response(
                            &mut stream,
                            &mut output,
                            error.into_frame(),
                            &config,
                            &mut subscription,
                            protocol,
                        )
                        .await?;
                        if fatal {
                            return Err(ConnectionError::InvalidRequest);
                        }
                        continue;
                    }
                };
                // Shutdown does not cancel the reply attempt for an accepted request.
                if memory_command {
                    let response = if protocol == 3 {
                        resp3(response)
                    } else {
                        response
                    };
                    output.clear();
                    if encode(&response, &mut output, memory_response_limits(&config)).is_err() {
                        database.metrics.add(Counter::ResponseLimitFailures, 1);
                        write_subscriber_response(
                            &mut stream,
                            &mut output,
                            Frame::Error(Bytes::from_static(b"ERR memory reply limit exceeded")),
                            &config,
                            &mut subscription,
                            protocol,
                        )
                        .await?;
                    } else {
                        subscription.metrics().response(&response);
                        tokio::select! {
                            biased;
                            _ = subscription.evicted() => return Err(PubSubError::Closed.into()),
                            result = write_buffer(&mut stream, &output, &config) => result?,
                        }
                    }
                } else {
                    write_subscriber_response(
                        &mut stream,
                        &mut output,
                        response,
                        &config,
                        &mut subscription,
                        protocol,
                    )
                    .await?;
                }
            }
            Err(error) => {
                let _ = write_response(
                    &mut stream,
                    &mut output,
                    Frame::Error(Bytes::from_static(b"ERR invalid protocol")),
                    &config,
                )
                .await;
                return Err(error.into());
            }
            Ok(None) => {
                let available = config.max_input_buffer_bytes.saturating_sub(input.len());
                if available == 0 {
                    let _ = write_response(
                        &mut stream,
                        &mut output,
                        Frame::Error(Bytes::from_static(b"ERR input buffer limit exceeded")),
                        &config,
                    )
                    .await;
                    return Err(ConnectionError::InputLimit);
                }
                let deadline = frame_started
                    .map(|start: Instant| {
                        start
                            .checked_add(config.frame_timeout)
                            .ok_or(ConnectionError::FrameTimeout)
                    })
                    .transpose()?;
                let deadline = if security.is_some() && !ever_authenticated {
                    Some(deadline.map_or(authentication_deadline, |deadline| {
                        deadline.min(authentication_deadline)
                    }))
                } else if security.is_some() {
                    Some(deadline.map_or(
                        Instant::now() + std::time::Duration::from_secs(1),
                        |deadline| deadline.min(Instant::now() + std::time::Duration::from_secs(1)),
                    ))
                } else {
                    deadline
                };
                let capacity = available.min(scratch.len());
                let notifier = subscription.notifier();
                // Idle input and notification waits never hold a policy lease.
                drop(database);
                drop(lease);
                let length = tokio::select! {
                    biased;
                    update = next_update(session) => {
                        if apply_update(update, session, &security, &mut identity, &mut transaction, &mut subscription, &hub, &config, protocol)? { return Ok(()); }
                        continue;
                    }
                    _ = stopped(&mut shutdown) => return Ok(()),
                    _ = async {
                        match deadline {
                            Some(deadline) => sleep_until(deadline).await,
                            None => std::future::pending().await,
                        }
                    } => {
                        if security.is_some() && (frame_started.is_none() || Instant::now() < frame_started.unwrap() + config.frame_timeout) { continue; }
                        return Err(ConnectionError::FrameTimeout);
                    },
                    result = stream.read(&mut scratch[..capacity]) => result?,
                    _ = notifier.notified() => continue,
                };
                if length == 0 {
                    return if input.is_empty() {
                        Ok(())
                    } else {
                        Err(ConnectionError::Truncated)
                    };
                }
                let now = Instant::now();
                frame_started.get_or_insert(now);
                last_read_at = Some(now);
                input.extend_from_slice(&scratch[..length]);
            }
        }
    }
}

async fn next_update(
    session: &mut Option<crate::security::Session>,
) -> Option<crate::security::Update> {
    match session {
        Some(session) => session.updates.recv().await,
        None => std::future::pending().await,
    }
}

#[allow(clippy::too_many_arguments)] // Reload updates the connection's canonical local state under the writer barrier.
fn apply_update(
    update: Option<crate::security::Update>,
    session: &mut Option<crate::security::Session>,
    security: &Option<(crate::security::ClientSecurity, IpAddr)>,
    identity: &mut Option<crate::security::CredentialProof>,
    transaction: &mut transaction::Transaction,
    subscription: &mut Subscription,
    hub: &Hub,
    config: &ServerConfig,
    protocol: u8,
) -> Result<bool, ConnectionError> {
    let Some(update) = update else {
        return Ok(true);
    };
    // Any cleanup failure must acknowledge only after the socket drops.
    session
        .as_ref()
        .expect("registered reload session")
        .acknowledge_after_close(update.acknowledged);
    let close = security
        .as_ref()
        .is_some_and(|(security, _)| security.transport.digest != update.transport_digest)
        || identity
            .as_ref()
            .is_some_and(|proof| !proof.present(&update.policy));
    if close
        || identity
            .as_ref()
            .is_some_and(|proof| !proof.valid(&update.policy, time::OffsetDateTime::now_utc()))
    {
        *identity = None;
        *transaction = transaction::Transaction::default();
        *subscription = hub.connect(config.pubsub_max_channels, config.pubsub_queue_capacity)?;
        subscription.resp3 = protocol == 3;
    } else if let Some(proof) = identity {
        transaction.revalidate(&update.policy, proof.username());
        subscription.revalidate(&update.policy, proof.username());
    }
    if !close {
        session
            .as_ref()
            .expect("registered reload session")
            .acknowledge_cleanup();
    }
    Ok(close)
}

async fn write_response<S: AsyncWrite + Unpin>(
    stream: &mut S,
    output: &mut BytesMut,
    response: Frame,
    config: &ServerConfig,
) -> Result<(), ConnectionError> {
    output.clear();
    encode(&response, output, response_limits(config))?;
    write_buffer(stream, output, config).await
}

async fn write_buffer<S: AsyncWrite + Unpin>(
    stream: &mut S,
    output: &[u8],
    config: &ServerConfig,
) -> Result<(), ConnectionError> {
    let deadline = Instant::now()
        .checked_add(config.write_timeout)
        .ok_or(ConnectionError::WriteTimeout)?;
    timeout_at(deadline, async {
        stream.write_all(output).await?;
        stream.flush().await
    })
    .await
    .map_err(|_| ConnectionError::WriteTimeout)??;
    Ok(())
}

fn response_limits(config: &ServerConfig) -> RespLimits {
    RespLimits {
        max_frame_bytes: config.max_response_bytes,
        max_bulk_bytes: config.max_response_bytes,
        max_line_bytes: config.max_response_bytes,
        ..config.resp_limits
    }
}

fn memory_response_limits(config: &ServerConfig) -> RespLimits {
    let maximum = config
        .max_response_bytes
        .min(crate::command::memory::MAX_REPLY_BYTES);
    RespLimits {
        max_frame_bytes: config.resp_limits.max_frame_bytes.min(maximum),
        max_bulk_bytes: config.resp_limits.max_bulk_bytes.min(maximum),
        max_line_bytes: config.resp_limits.max_line_bytes.min(maximum),
        ..config.resp_limits
    }
}

async fn write_subscriber_response<S: AsyncWrite + Unpin>(
    stream: &mut S,
    output: &mut BytesMut,
    response: Frame,
    config: &ServerConfig,
    subscription: &mut Subscription,
    protocol: u8,
) -> Result<(), ConnectionError> {
    let response = if protocol == 3 {
        resp3(response)
    } else {
        response
    };
    subscription.metrics().response(&response);
    tokio::select! {
        biased;
        _ = subscription.evicted() => Err(PubSubError::Closed.into()),
        result = write_response(stream, output, response, config) => result,
    }
}

#[cfg(test)]
mod tests;
