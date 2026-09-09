//! Listener e supervisão do worker e das conexões, com drenagem limitada.

use std::future::Future;
use std::io;
use std::sync::Arc;

use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until};

use crate::connection::{self, ConnectionError};
use crate::storage::worker::{self, DbHandle};
use crate::storage::{Store, StoreConfig, SystemClock};
use crate::{ConfigError, ServerConfig};

/// Falha que impede o servidor de continuar atendendo.
#[derive(Debug, Error)]
pub enum ServerError {
    #[error("configuração inválida: {0}")]
    Config(#[from] ConfigError),
    #[error("falha do listener: {0}")]
    Io(#[from] io::Error),
    #[error("worker terminou inesperadamente")]
    WorkerStopped,
    #[error("worker falhou: {0}")]
    WorkerFailed(#[source] tokio::task::JoinError),
    #[error("prazo de encerramento excedido; tarefas restantes foram abortadas")]
    ShutdownTimeout,
    #[error("persistência indisponível: {0}")]
    Persistence(#[from] crate::persistence::AofError),
}

/// Estado recuperado antes de abrir o listener do binário.
pub struct PreparedServer {
    store: Option<Store>,
    recovered: Option<crate::persistence::Recovered>,
}

pub async fn prepare(config: &ServerConfig) -> Result<PreparedServer, ServerError> {
    config.validate()?;
    if config.aof.is_some() && config.shards != 1 {
        return Err(ConfigError::InvalidServerLimits {
            reason: "AOF v1 requer um shard; migra??o dur?vel ? expl?cita",
        }
        .into());
    }
    let store_config = StoreConfig {
        max_dataset_bytes: config.max_dataset_bytes,
    };
    if let Some(aof) = config.aof.clone() {
        let recovered = tokio::task::spawn_blocking(move || {
            crate::persistence::recover(aof, store_config, Arc::new(SystemClock))
        })
        .await
        .map_err(ServerError::WorkerFailed)??;
        Ok(PreparedServer {
            store: None,
            recovered: Some(recovered),
        })
    } else {
        Ok(PreparedServer {
            store: Some(Store::with_config(store_config, Arc::new(SystemClock))?),
            recovered: None,
        })
    }
}

/// Atende o listener já aberto até o sinal de parada ou uma falha do worker.
///
/// O cancelamento desta future aborta as tarefas que ela possui. A parada normal
/// drena pedidos aceitos, mas não oferece durabilidade ou rollback de timeout.
pub async fn serve(
    listener: TcpListener,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), ServerError> {
    let prepared = prepare(&config).await?;
    serve_prepared(listener, config, shutdown, prepared).await
}

/// Atende somente depois da recuperação. `prepare` pode executar antes do bind.
pub async fn serve_prepared(
    listener: TcpListener,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
    prepared: PreparedServer,
) -> Result<(), ServerError> {
    config.validate()?;
    let (stop, receiver) = watch::channel(false);
    let (store, persistence) = match prepared.recovered {
        Some(recovered) => {
            let (store, handle, task) = recovered.start();
            (store, Some((handle, task)))
        }
        None => (prepared.store.expect("estado preparado"), None),
    };
    let stores = if persistence.is_some() {
        vec![store]
    } else {
        (0..config.shards)
            .map(|index| {
                Store::with_config(
                    StoreConfig {
                        max_dataset_bytes: config.max_dataset_bytes / config.shards
                            + usize::from(index < config.max_dataset_bytes % config.shards),
                    },
                    Arc::new(SystemClock),
                )
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let (database, shard_workers) = worker::channel_with_stores(
        config.worker_queue_capacity,
        config.request_timeout,
        receiver,
        stores,
    )?;
    let mut workers = JoinSet::new();
    for worker in shard_workers {
        let worker = match &persistence {
            Some((handle, _)) => worker.with_aof(
                handle.clone(),
                config.aof.as_ref().map_or(0, |aof| aof.compact_after_bytes),
            ),
            None => worker,
        };
        workers.spawn(worker.run());
    }
    let timeout = config.shutdown_timeout;
    let result = supervise_workers(listener, config, shutdown, database, workers, stop).await;
    if let Some((handle, writer)) = persistence {
        drop(handle);
        tokio::time::timeout(timeout, writer)
            .await
            .map_err(|_| ServerError::ShutdownTimeout)?
            .map_err(ServerError::WorkerFailed)??;
    }
    result
}

#[cfg(test)]
async fn supervise(
    listener: TcpListener,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
    database: DbHandle,
    worker: impl Future<Output = ()> + Send + 'static,
    stop: watch::Sender<bool>,
) -> Result<(), ServerError> {
    let mut workers = JoinSet::new();
    workers.spawn(worker);
    supervise_workers(listener, config, shutdown, database, workers, stop).await
}

async fn supervise_workers(
    listener: TcpListener,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
    database: DbHandle,
    mut workers: JoinSet<()>,
    stop: watch::Sender<bool>,
) -> Result<(), ServerError> {
    let mut connections = JoinSet::new();
    let slots = Arc::new(Semaphore::new(config.max_connections));
    let pubsub = crate::pubsub::Hub::default();
    tokio::pin!(shutdown);
    tracing::info!(address = %listener.local_addr()?, "servidor TCP iniciado");

    let failure = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break None,
            worker = workers.join_next() => {
                break Some(match worker {
                    Some(Err(error)) => ServerError::WorkerFailed(error),
                    _ => ServerError::WorkerStopped,
                });
            }
            result = connections.join_next(), if !connections.is_empty() => log_connection(result),
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Some(error.into()),
                };
                let Ok(slot) = slots.clone().try_acquire_owned() else {
                    tracing::warn!("conexão excedente recusada");
                    continue;
                };
                if let Err(error) = stream.set_nodelay(true) {
                    tracing::warn!(%error, "falha ao configurar conexão");
                    continue;
                }
                let config = config.clone();
                let database = database.clone();
                let shutdown = stop.subscribe();
                let pubsub = pubsub.clone();
                connections.spawn(async move {
                    let _slot = slot;
                    connection::run_with_pubsub(stream, config, database, shutdown, pubsub).await
                });
            }
        }
    };

    drop(listener);
    stop.send_replace(true);
    drop(database);
    tracing::info!("iniciando drenagem dos pedidos aceitos");
    let deadline = Instant::now()
        .checked_add(config.shutdown_timeout)
        .ok_or(ServerError::ShutdownTimeout)?;
    let mut failure = failure;
    while !workers.is_empty() || !connections.is_empty() {
        tokio::select! {
            biased;
            _ = sleep_until(deadline) => {
                tracing::error!(connections = connections.len(), workers = workers.len(), "encerramento forçado");
                workers.abort_all();
                connections.abort_all();
                while workers.join_next().await.is_some() {}
                while connections.join_next().await.is_some() {}
                return Err(ServerError::ShutdownTimeout);
            }
            result = workers.join_next(), if !workers.is_empty() => {
                if let Some(Err(error)) = result {
                    failure.get_or_insert(ServerError::WorkerFailed(error));
                }
            }
            result = connections.join_next(), if !connections.is_empty() => log_connection(result),
        }
    }
    tracing::info!("servidor encerrado");
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn log_connection(result: Option<Result<Result<(), ConnectionError>, tokio::task::JoinError>>) {
    match result {
        Some(Ok(Err(error))) => tracing::warn!(%error, "conexão encerrada com erro"),
        Some(Err(error)) => tracing::error!(%error, "tarefa de conexão falhou"),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    use super::*;

    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn invalid_configuration_is_rejected_and_listener_is_released() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let config = ServerConfig {
            max_connections: 0,
            ..ServerConfig::default()
        };
        assert!(matches!(
            serve(listener, config, pending()).await,
            Err(ServerError::Config(_))
        ));
        assert!(TcpStream::connect(address).await.is_err());
    }

    #[tokio::test]
    async fn unexpected_worker_exit_closes_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let config = ServerConfig::default();
        let (stop, rx) = watch::channel(false);
        let (db, worker) = worker::channel(1, config.request_timeout, rx).unwrap();
        let result = timeout(
            Duration::from_secs(5),
            supervise(
                listener,
                config,
                pending(),
                db,
                async move {
                    drop(worker);
                },
                stop,
            ),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(ServerError::WorkerStopped)));
        assert!(TcpStream::connect(address).await.is_err());
    }

    #[tokio::test]
    async fn worker_panic_is_reported_and_listener_is_closed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let config = ServerConfig::default();
        let (stop, rx) = watch::channel(false);
        let (db, worker) = worker::channel(1, config.request_timeout, rx).unwrap();
        let result = timeout(
            Duration::from_secs(5),
            supervise(
                listener,
                config,
                pending(),
                db,
                async move {
                    let _worker = worker;
                    panic!("falha injetada do worker");
                },
                stop,
            ),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(ServerError::WorkerFailed(error)) if error.is_panic()));
        assert!(TcpStream::connect(address).await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_deadline_aborts_and_joins_pending_worker() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ServerConfig {
            shutdown_timeout: Duration::from_secs(1),
            ..ServerConfig::default()
        };
        let (stop, rx) = watch::channel(false);
        let (db, worker) = worker::channel(1, config.request_timeout, rx).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Dropped(dropped.clone());
        let started = Instant::now();
        let result = supervise(
            listener,
            config,
            async {},
            db,
            async move {
                let _guard = guard;
                let _worker = worker;
                pending::<()>().await;
            },
            stop,
        )
        .await;
        assert!(matches!(result, Err(ServerError::ShutdownTimeout)));
        assert_eq!(Instant::now() - started, Duration::from_secs(1));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancelling_server_does_not_detach_worker_or_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let config = ServerConfig::default();
        let (stop, rx) = watch::channel(false);
        let (db, worker) = worker::channel(1, config.request_timeout, rx).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Dropped(dropped.clone());
        let (finished, waited) = tokio::sync::oneshot::channel();
        struct Finished(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Finished {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        let finished = Finished(Some(finished));
        let running = tokio::spawn(supervise(
            listener,
            config,
            pending(),
            db,
            async move {
                let _finished = finished;
                let _guard = guard;
                worker.run().await;
            },
            stop,
        ));
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let mut response = [0_u8; 7];
        timeout(Duration::from_secs(5), client.read_exact(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&response, b"+PONG\r\n");
        running.abort();
        assert!(running.await.unwrap_err().is_cancelled());
        timeout(Duration::from_secs(5), waited)
            .await
            .unwrap()
            .unwrap();
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(
            timeout(Duration::from_secs(5), client.read(&mut response))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert!(TcpStream::connect(address).await.is_err());
    }
}
