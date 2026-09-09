//! Uma requisição em voo por conexão, com entrada e saída limitadas.

use std::io;

use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until, timeout_at};

use crate::ServerConfig;
use crate::command::{Command, parse};
use crate::pubsub::{Hub, Message, PubSubError, Subscription};
use crate::resp::{Decoder, EncodeError, Frame, ProtocolError, RespLimits, encode};
use crate::storage::worker::{DbError, DbHandle};

mod transaction;

#[derive(Debug, Error)]
pub(crate) enum ConnectionError {
    #[error("falha de I/O da conexão: {0}")]
    Io(#[from] io::Error),
    #[error("protocolo inválido: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("formato de requisição inválido")]
    InvalidRequest,
    #[error("frame truncado no EOF")]
    Truncated,
    #[error("limite do buffer de entrada excedido")]
    InputLimit,
    #[error("prazo de formação do frame excedido")]
    FrameTimeout,
    #[error("prazo de escrita excedido")]
    WriteTimeout,
    #[error("falha de codificação da resposta: {0}")]
    Encode(#[from] EncodeError),
    #[error("falha do worker: {0}")]
    Database(#[from] DbError),
    #[error("configuração da conexão inválida: {0}")]
    Config(#[from] crate::ConfigError),
    #[error("falha do assinante: {0}")]
    PubSub(#[from] PubSubError),
}

pub(crate) async fn stopped(shutdown: &mut watch::Receiver<bool>) {
    // Um emissor descartado também encerra consumidores, sem espera infinita.
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
    run_with_pubsub(stream, config, database, shutdown, Hub::default()).await
}

pub(crate) async fn run_with_pubsub<S>(
    mut stream: S,
    config: ServerConfig,
    database: DbHandle,
    mut shutdown: watch::Receiver<bool>,
    hub: Hub,
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
    let mut scratch = [0_u8; 8192];
    let mut frame_started = None;
    let mut last_read_at = None;

    loop {
        if *shutdown.borrow() || shutdown.has_changed().is_err() {
            return Ok(());
        }
        if subscription.is_evicted() {
            return Err(PubSubError::Closed.into());
        }
        // Uma notificação por volta mantém comandos e mensagens progredindo.
        if let Some(message) = subscription.try_message() {
            write_subscriber_response(
                &mut stream,
                &mut output,
                message.into_frame(),
                &config,
                &mut subscription,
            )
            .await?;
        }
        match decoder.decode(&mut input) {
            Ok(Some(frame)) => {
                // Só lemos enquanto o frame anterior estava incompleto. Logo,
                // qualquer sufixo novo veio necessariamente da última leitura.
                frame_started = if input.is_empty() { None } else { last_read_at };
                let command_name = match &frame {
                    Frame::Array(Some(parts)) => match parts.first() {
                        Some(Frame::Bulk(Some(name))) => name.to_ascii_lowercase(),
                        _ => Vec::new(),
                    },
                    _ => Vec::new(),
                };
                let request_bytes = transaction::request_bytes(&frame);
                let response = match parse(frame) {
                    Ok(command) => {
                        let command = match transaction
                            .handle(
                                command,
                                request_bytes,
                                &config,
                                &database,
                                subscription.active(),
                            )
                            .await?
                        {
                            transaction::Action::Exec { commands, watched } => {
                                let result = database
                                    .execute_transaction(
                                        commands,
                                        watched,
                                        subscription,
                                        response_limits(&config),
                                    )
                                    .await?;
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
                                                frame,
                                                &config,
                                                &mut subscription,
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
                                        frame,
                                        &config,
                                        &mut subscription,
                                    )
                                    .await?;
                                }
                                continue;
                            }
                            Command::Ping(payload) if subscription.active() => {
                                Frame::Array(Some(vec![
                                    Frame::Bulk(Some(Bytes::from_static(b"pong"))),
                                    Frame::Bulk(Some(payload.unwrap_or_default())),
                                ]))
                            }
                            _ if subscription.active() => Frame::Error(Bytes::from(format!(
                                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                                String::from_utf8_lossy(&command_name),
                            ))),
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
                                Ok(reply) => Frame::from(reply),
                                Err(DbError::ShuttingDown) => return Ok(()),
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
                        )
                        .await?;
                        if fatal {
                            return Err(ConnectionError::InvalidRequest);
                        }
                        continue;
                    }
                };
                // Parada não cancela a tentativa de resposta de um pedido aceito.
                write_subscriber_response(
                    &mut stream,
                    &mut output,
                    response,
                    &config,
                    &mut subscription,
                )
                .await?;
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
                let capacity = available.min(scratch.len());
                let length = tokio::select! {
                    biased;
                    _ = stopped(&mut shutdown) => return Ok(()),
                    _ = async {
                        match deadline {
                            Some(deadline) => sleep_until(deadline).await,
                            None => std::future::pending().await,
                        }
                    } => return Err(ConnectionError::FrameTimeout),
                    result = stream.read(&mut scratch[..capacity]) => result?,
                    message = subscription.message() => {
                        let message = message.ok_or(PubSubError::Closed)?;
                        write_subscriber_response(&mut stream, &mut output, message.into_frame(), &config, &mut subscription).await?;
                        continue;
                    }
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
    timeout_at(deadline, stream.write_all(output))
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

async fn write_subscriber_response<S: AsyncWrite + Unpin>(
    stream: &mut S,
    output: &mut BytesMut,
    response: Frame,
    config: &ServerConfig,
    subscription: &mut Subscription,
) -> Result<(), ConnectionError> {
    tokio::select! {
        biased;
        _ = subscription.evicted() => Err(PubSubError::Closed.into()),
        result = write_response(stream, output, response, config) => result,
    }
}

#[cfg(test)]
mod tests;
