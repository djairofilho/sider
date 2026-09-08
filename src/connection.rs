//! Uma requisição em voo por conexão, com entrada e saída limitadas.

use std::io;

use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until, timeout_at};

use crate::ServerConfig;
use crate::command::parse;
use crate::resp::{Decoder, EncodeError, Frame, ProtocolError, RespLimits, encode};
use crate::storage::worker::{DbError, DbHandle};

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
}

pub(crate) async fn stopped(shutdown: &mut watch::Receiver<bool>) {
    // Um emissor descartado também encerra consumidores, sem espera infinita.
    let _ = shutdown.wait_for(|stopping| *stopping).await;
}

pub(crate) async fn run<S>(
    mut stream: S,
    config: ServerConfig,
    database: DbHandle,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ConnectionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    config.validate()?;
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
        match decoder.decode(&mut input) {
            Ok(Some(frame)) => {
                // Só lemos enquanto o frame anterior estava incompleto. Logo,
                // qualquer sufixo novo veio necessariamente da última leitura.
                frame_started = if input.is_empty() { None } else { last_read_at };
                let response = match parse(frame) {
                    Ok(command) => match database.execute(command).await {
                        Ok(reply) => Frame::from(reply),
                        Err(DbError::ShuttingDown) => return Ok(()),
                        Err(error) => return Err(error.into()),
                    },
                    Err(error) => {
                        let fatal = error.is_fatal();
                        write_response(&mut stream, &mut output, error.into_frame(), &config)
                            .await?;
                        if fatal {
                            return Err(ConnectionError::InvalidRequest);
                        }
                        continue;
                    }
                };
                // Parada não cancela a tentativa de resposta de um pedido aceito.
                write_response(&mut stream, &mut output, response, &config).await?;
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
    let limits = RespLimits {
        max_frame_bytes: config.max_response_bytes,
        max_bulk_bytes: config.max_response_bytes,
        max_line_bytes: config.max_response_bytes,
        ..config.resp_limits
    };
    encode(&response, output, limits)?;
    let deadline = Instant::now()
        .checked_add(config.write_timeout)
        .ok_or(ConnectionError::WriteTimeout)?;
    timeout_at(deadline, stream.write_all(output))
        .await
        .map_err(|_| ConnectionError::WriteTimeout)??;
    Ok(())
}

#[cfg(test)]
mod tests;
