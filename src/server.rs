//! Listener e supervisão do worker e das conexões, com drenagem limitada.

use std::future::Future;
use std::io;
use std::sync::Arc;

use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until, timeout_at};

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
    #[error("replicação indisponível: {0}")]
    Replication(#[from] crate::replication::Error),
}

/// Estado recuperado antes de abrir o listener do binário.
pub struct PreparedServer {
    stores: Vec<Store>,
    recovered: Option<crate::persistence::Recovered>,
    config: ServerConfig,
    replication_listener: Option<TcpListener>,
}

pub async fn prepare(config: &ServerConfig) -> Result<PreparedServer, ServerError> {
    config.validate()?;
    let store_config = StoreConfig {
        max_dataset_bytes: config.max_dataset_bytes,
    };
    let mut stores = (0..config.shards)
        .map(|index| {
            Store::with_config(
                StoreConfig {
                    max_dataset_bytes: config.max_dataset_bytes / config.shards
                        + usize::from(index < config.max_dataset_bytes % config.shards),
                },
                Arc::new(SystemClock),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let recovered = if let Some(aof) = config.aof.clone() {
        let mut recovered = tokio::task::spawn_blocking(move || {
            crate::persistence::recover(aof, store_config, Arc::new(SystemClock))
        })
        .await
        .map_err(ServerError::WorkerFailed)??;
        let router = crate::storage::routing::ShardRouter::new(config.shards)?;
        let mut partitions = vec![Vec::new(); config.shards];
        for mutation in recovered.store.snapshot() {
            partitions[router.shard_for(mutation.key())].push(mutation);
        }
        for (store, partition) in stores.iter_mut().zip(partitions) {
            store
                .replay(&partition)
                .map_err(crate::persistence::AofError::from)?;
        }
        recovered.store = Store::new();
        Some(recovered)
    } else {
        None
    };
    if recovered
        .as_ref()
        .and_then(|state| state.metadata.replication)
        .is_some_and(|metadata| metadata.role == crate::persistence::Role::Replica)
        && config
            .replication
            .as_ref()
            .and_then(|replication| replication.upstream)
            .is_none()
    {
        return Err(ConfigError::InvalidServerLimits { reason: "réplica persistida exige upstream configurado; use promoção local para trocar o papel" }.into());
    }
    let replication_listener = match &config.replication {
        Some(replication) => Some(TcpListener::bind(replication.listen).await?),
        None => None,
    };
    Ok(PreparedServer {
        stores,
        recovered,
        config: config.clone(),
        replication_listener,
    })
}

/// Atende o listener já aberto até o sinal de parada ou uma falha do worker.
///
/// O cancelamento desta future aborta as tarefas que ela possui. A parada normal
/// drena pedidos aceitos. A durabilidade segue a política AOF configurada;
/// cancelar ou exceder o timeout não desfaz pedidos já aceitos.
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
    if prepared.config != config {
        return Err(ConfigError::InvalidServerLimits {
            reason: "configuração difere do estado recuperado",
        }
        .into());
    }
    let (stop, receiver) = watch::channel(false);
    let metadata = prepared
        .recovered
        .as_ref()
        .and_then(|recovered| recovered.metadata.replication);
    let recovered_sequence = prepared
        .recovered
        .as_ref()
        .map_or(0, |recovered| recovered.metadata.sequence);
    let runtime = config.replication.as_ref().map(|replication| {
        let role = if metadata
            .is_some_and(|metadata| metadata.role == crate::persistence::Role::Primary)
            || replication.upstream.is_none()
        {
            crate::persistence::Role::Primary
        } else {
            crate::persistence::Role::Replica
        };
        crate::replication::state::Runtime::new(
            role,
            crate::replication::Cursor {
                epoch: metadata.map_or([0; 16], |metadata| metadata.epoch),
                sequence: recovered_sequence,
            },
        )
    });
    let persistence = match prepared.recovered {
        Some(recovered) => {
            let (_, handle, task) = recovered.start();
            Some((handle, task))
        }
        None => None,
    };
    let (mut database, shard_workers) = worker::channel_with_stores(
        config.worker_queue_capacity,
        config.request_timeout,
        receiver.clone(),
        prepared.stores,
    )?;
    if let Some(runtime) = &runtime {
        database = database.with_replication(runtime.clone());
    }
    let mut workers = JoinSet::new();
    for worker in shard_workers {
        let worker = match &persistence {
            Some((handle, _)) => worker.with_aof(handle.clone(), 0),
            None => worker,
        };
        let worker = match &runtime {
            Some(runtime) => worker.with_replication(runtime.clone()),
            None => worker,
        };
        workers.spawn(worker.run());
    }
    let replication_addr = prepared
        .replication_listener
        .as_ref()
        .map(TcpListener::local_addr)
        .transpose()?;
    if let (Some(listener), Some(replication), Some(runtime), Some((aof, _))) = (
        prepared.replication_listener,
        config.replication.as_ref(),
        runtime.as_ref(),
        persistence.as_ref(),
    ) {
        let aof_config = config.aof.as_ref().expect("AOF validada para replicação");
        let context = crate::storage::replication::Context {
            aof: aof.clone(),
            runtime: runtime.clone(),
            layout: aof_config.layout,
            store_config: StoreConfig {
                max_dataset_bytes: config.max_dataset_bytes,
            },
            clock: Arc::new(SystemClock),
            journal_limits: crate::replication::journal::Limits {
                max_bytes: replication.backlog_bytes,
                max_batches: replication.backlog_batches,
                max_frame_bytes: aof_config.limits.max_record_bytes
                    + crate::replication::protocol::HEADER_BYTES
                    + 12,
            },
            aof_limits: aof_config.limits,
        };
        if !runtime.readonly() {
            database
                .promote_replica(context.clone(), crate::replication::new_epoch()?)
                .await?;
        } else if metadata.is_none() {
            let image = database
                .snapshot(Some(aof))
                .await
                .map_err(crate::replication::Error::from)?;
            database
                .install_replica(
                    context.clone(),
                    0,
                    runtime.status().applied,
                    image.mutations,
                )
                .await?;
        }
        let source_db = database.clone();
        let source_context = context.clone();
        let source_config = replication.clone();
        let source_shutdown = receiver.clone();
        workers.spawn(async move {
            if let Err(error) = crate::replication::session::serve(
                listener,
                source_db,
                source_context,
                source_config,
                source_shutdown,
            )
            .await
            {
                tracing::error!(%error, "listener interno encerrou com falha");
            }
        });
        if runtime.readonly() {
            let replica_db = database.clone();
            let replica_config = replication.clone();
            let replica_shutdown = receiver.clone();
            workers.spawn(async move {
                if let Err(error) = crate::replication::session::follow(
                    replica_db,
                    context,
                    replica_config,
                    replica_shutdown,
                )
                .await
                {
                    tracing::error!(%error, "réplica encerrou com falha");
                }
            });
        }
    }
    if let Some((handle, _)) = &persistence {
        workers.spawn(database.clone().run_compaction(
            handle.clone(),
            config.aof.as_ref().map_or(0, |aof| aof.compact_after_bytes),
        ));
    }
    let timeout = config.shutdown_timeout;
    let _replication_ready = config
        .replication
        .as_ref()
        .and_then(|replication| replication.ready_file.as_ref())
        .zip(replication_addr)
        .map(|(path, address)| crate::readiness::ReadyFile::create(path, address))
        .transpose()?;
    let _ready = config
        .ready_file
        .as_ref()
        .map(|path| crate::readiness::ReadyFile::create(path, listener.local_addr()?))
        .transpose()?;
    let mut shutdown_deadline = None;
    let result = supervise_workers(
        listener,
        config,
        shutdown,
        database,
        workers,
        stop,
        &mut shutdown_deadline,
    )
    .await;
    if let Some((handle, writer)) = persistence {
        drop(handle);
        let deadline = shutdown_deadline.unwrap_or_else(|| Instant::now() + timeout);
        timeout_at(deadline, writer)
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
    supervise_workers(
        listener, config, shutdown, database, workers, stop, &mut None,
    )
    .await
}

async fn supervise_workers(
    listener: TcpListener,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
    database: DbHandle,
    mut workers: JoinSet<()>,
    stop: watch::Sender<bool>,
    shutdown_deadline: &mut Option<Instant>,
) -> Result<(), ServerError> {
    let mut connections = JoinSet::new();
    let slots = Arc::new(Semaphore::new(config.max_connections));
    let metrics = database.metrics.clone();
    metrics.configure(&config, listener.local_addr()?.port());
    let pubsub = crate::pubsub::Hub::with_metrics(metrics.clone());
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
                    metrics.add(crate::metrics::Counter::RejectedConnections, 1);
                    tracing::warn!("conexão excedente recusada");
                    continue;
                };
                if let Err(error) = stream.set_nodelay(true) {
                    metrics.add(crate::metrics::Counter::ConnectionFailures, 1);
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
    *shutdown_deadline = Some(deadline);
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
