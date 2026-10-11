//! Listener and supervision for the worker and connections, with bounded draining.

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

/// Failure that prevents the server from continuing to serve.
#[derive(Debug, Error)]
pub enum ServerError {
    #[error("invalid configuration: {0}")]
    Config(#[from] ConfigError),
    #[error("listener failure: {0}")]
    Io(#[from] io::Error),
    #[error("worker ended unexpectedly")]
    WorkerStopped,
    #[error("worker failed: {0}")]
    WorkerFailed(#[source] tokio::task::JoinError),
    #[error("shutdown timeout exceeded; remaining tasks were aborted")]
    ShutdownTimeout,
    #[error("persistence unavailable: {0}")]
    Persistence(#[from] crate::persistence::AofError),
    #[error("security validation unavailable: {0}")]
    Security(#[from] crate::security::Error),
    #[error("replication unavailable: {0}")]
    Replication(#[from] crate::replication::Error),
}

/// State recovered before opening the binary listener.
pub struct PreparedServer {
    stores: Vec<Store>,
    recovered: Option<crate::persistence::Recovered>,
    config: ServerConfig,
    replication_listener: Option<TcpListener>,
}

pub async fn prepare(config: &ServerConfig) -> Result<PreparedServer, ServerError> {
    prepare_inner(config, None).await
}
async fn prepare_inner(
    config: &ServerConfig,
    security: Option<&crate::security::ServerPreflight>,
) -> Result<PreparedServer, ServerError> {
    if let Some(security) = security {
        security.validate(config).await?;
    }
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
        let composed = config.composed_aof;
        let mut recovered = tokio::task::spawn_blocking(move || {
            if composed {
                crate::persistence::recover_composed(aof, store_config, Arc::new(SystemClock))
            } else {
                crate::persistence::recover(aof, store_config, Arc::new(SystemClock))
            }
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
        return Err(ConfigError::InvalidServerLimits { reason: "persisted replica requires configured upstream; use local promotion to change the role" }.into());
    }
    if let Some(security) = security {
        security.validate(config).await?;
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

/// Serves the already open listener until the shutdown signal or a worker failure.
///
/// Cancelling this future aborts the tasks it owns. Normal shutdown
/// drains accepted requests. Durability follows the configured AOF policy;
/// cancellation or timeout does not undo requests already accepted.
pub async fn serve(
    listener: TcpListener,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), ServerError> {
    let prepared = prepare(&config).await?;
    serve_prepared(listener, config, shutdown, prepared).await
}

/// Serves only after recovery. `prepare` may run before bind.
pub async fn serve_prepared(
    listener: TcpListener,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
    prepared: PreparedServer,
) -> Result<(), ServerError> {
    serve_prepared_inner(listener, config, shutdown, prepared, None, None).await
}

/// Serves RESP with TLS, named authentication, and default-deny authorization.
///
/// Internal networking requires its separate mutual TLS integration; this entry
/// point rejects a configured plaintext internal listener before preparing state.
pub async fn serve_secure(
    listener: TcpListener,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
    security: crate::security::ClientSecurity,
) -> Result<(), ServerError> {
    if config.replication.is_some() {
        return Err(ConfigError::InvalidServerLimits {
            reason: "secure client listener requires secure internal transport integration",
        }
        .into());
    }
    let prepared = prepare(&config).await?;
    serve_prepared_inner(listener, config, shutdown, prepared, Some(security), None).await
}

/// Serves both listeners and outgoing replica sessions using separately validated identities.
pub async fn serve_secure_full(
    listener: TcpListener,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
    client: crate::security::ClientSecurity,
    internal: Option<crate::security::InternalSecurity>,
) -> Result<(), ServerError> {
    if config.replication.is_some() != internal.is_some() {
        return Err(ConfigError::InvalidServerLimits {
            reason: "internal networking requires validated mutual TLS identity and roles",
        }
        .into());
    }
    let prepared = prepare(&config).await?;
    serve_secure_prepared(listener, config, shutdown, prepared, client, internal).await
}

/// Starts protected networking after validated generation loading and recovery.
pub async fn serve_secure_prepared(
    listener: TcpListener,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
    prepared: PreparedServer,
    client: crate::security::ClientSecurity,
    internal: Option<crate::security::InternalSecurity>,
) -> Result<(), ServerError> {
    if config.replication.is_some() != internal.is_some() {
        return Err(ConfigError::InvalidServerLimits {
            reason: "internal networking requires validated mutual TLS identity and roles",
        }
        .into());
    }
    serve_prepared_inner(
        listener,
        config,
        shutdown,
        prepared,
        Some(client),
        internal.map(InternalAdmission::Static),
    )
    .await
}

#[derive(Clone)]
enum InternalAdmission {
    Static(crate::security::InternalSecurity),
    Live(crate::security::InternalRuntime),
}

/// Validates the live runtime before recovery and each listener bind.
/// In-memory preflight remains an embedding API; executable permission gates apply
/// through `StartupSecurity::preflight` before this function is called.
pub async fn serve_live(
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
    preflight: crate::security::ServerPreflight,
) -> Result<(), ServerError> {
    let prepared = prepare_inner(&config, Some(&preflight)).await?;
    preflight.validate(&config).await?;
    let listener = TcpListener::bind(config.bind_addr).await?;
    serve_live_prepared(listener, config, shutdown, prepared, preflight).await
}

/// Uses one validated runtime for both lanes of an already prepared server.
/// Callers opening listeners themselves must validate preflight before those binds.
pub async fn serve_live_prepared(
    listener: TcpListener,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
    prepared: PreparedServer,
    preflight: crate::security::ServerPreflight,
) -> Result<(), ServerError> {
    preflight.validate(&config).await?;
    let runtime = preflight.runtime();
    let client = runtime.client().await;
    let internal = runtime.internal().await.map(InternalAdmission::Live);
    serve_prepared_inner(listener, config, shutdown, prepared, Some(client), internal).await
}

async fn serve_prepared_inner(
    listener: TcpListener,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
    prepared: PreparedServer,
    security: Option<crate::security::ClientSecurity>,
    internal_security: Option<InternalAdmission>,
) -> Result<(), ServerError> {
    config.validate()?;
    if prepared.config != config {
        return Err(ConfigError::InvalidServerLimits {
            reason: "configuration differs from recovered state",
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
    let memory_profile = config
        .aof
        .as_ref()
        .is_some_and(|aof| aof.is_memory_profile());
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
    if config.composed_aof {
        let (aof, _) = persistence.as_ref().expect("composed AOF validated");
        database = database.with_composed_aof(aof.clone())?;
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
    let replication_listener = prepared.replication_listener;
    if memory_profile {
        if let (Some(listener), Some(replication), Some((aof, _)), Some(aof_config)) = (
            replication_listener,
            config.replication.as_ref(),
            persistence.as_ref(),
            config.aof.as_ref(),
        ) {
            let runtime = runtime.as_ref().expect("replication runtime configured");
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
                    .install_memory_replica(
                        context.clone(),
                        0,
                        runtime.status().applied,
                        image
                            .mutations
                            .into_iter()
                            .map(crate::persistence::memory_format::Mutation::from_storage)
                            .collect(),
                    )
                    .await?;
            }
            let source = crate::replication::memory_source::Source {
                database: database.clone(),
                aof: aof.clone(),
                aof_config: aof_config.clone(),
                store_config: StoreConfig {
                    max_dataset_bytes: config.max_dataset_bytes,
                },
                config: replication.clone(),
                context: context.clone(),
            };
            let source_shutdown = receiver.clone();
            let source_internal = internal_security.clone();
            workers.spawn(async move {
                let result = match source_internal {
                    Some(InternalAdmission::Live(security)) => {
                        crate::replication::memory_source::serve_live(
                            listener,
                            source,
                            source_shutdown,
                            security,
                        )
                        .await
                    }
                    Some(InternalAdmission::Static(security)) => {
                        crate::replication::memory_source::serve_secure(
                            listener,
                            source,
                            source_shutdown,
                            security,
                        )
                        .await
                    }
                    None => {
                        crate::replication::memory_source::serve_plain(
                            listener,
                            source,
                            source_shutdown,
                        )
                        .await
                    }
                };
                if let Err(error) = result {
                    tracing::error!(%error, "memory export listener terminated with failure");
                }
            });
            if runtime.readonly() {
                let replica_db = database.clone();
                let replica_config = replication.clone();
                let replica_aof = aof_config.clone();
                let replica_shutdown = receiver.clone();
                let replica_internal = internal_security.clone();
                workers.spawn(async move {
                    let result = match replica_internal {
                        Some(InternalAdmission::Live(security)) => {
                            crate::replication::memory_follow::follow_live(
                                replica_db,
                                context,
                                replica_aof,
                                replica_config,
                                replica_shutdown,
                                security,
                            )
                            .await
                        }
                        Some(InternalAdmission::Static(security)) => {
                            crate::replication::memory_follow::follow_secure(
                                replica_db,
                                context,
                                replica_aof,
                                replica_config,
                                replica_shutdown,
                                security,
                            )
                            .await
                        }
                        None => {
                            crate::replication::memory_follow::follow_plain(
                                replica_db,
                                context,
                                replica_aof,
                                replica_config,
                                replica_shutdown,
                            )
                            .await
                        }
                    };
                    if let Err(error) = result {
                        tracing::error!(%error, "memory replica follower terminated with failure");
                    }
                });
            }
        }
    } else if let (Some(listener), Some(replication), Some(runtime), Some((aof, _))) = (
        replication_listener,
        config.replication.as_ref(),
        runtime.as_ref(),
        persistence.as_ref(),
    ) {
        let aof_config = config.aof.as_ref().expect("AOF validated for replication");
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
        let source_internal = internal_security.clone();
        workers.spawn(async move {
            let result = match source_internal {
                Some(InternalAdmission::Live(security)) => {
                    crate::replication::session::serve_live(
                        listener,
                        source_db,
                        source_context,
                        source_config,
                        source_shutdown,
                        security,
                    )
                    .await
                }
                Some(InternalAdmission::Static(security)) => {
                    crate::replication::session::serve_secure(
                        listener,
                        source_db,
                        source_context,
                        source_config,
                        source_shutdown,
                        security,
                    )
                    .await
                }
                None => {
                    crate::replication::session::serve(
                        listener,
                        source_db,
                        source_context,
                        source_config,
                        source_shutdown,
                    )
                    .await
                }
            };
            if let Err(error) = result {
                tracing::error!(%error, "internal listener terminated with failure");
            }
        });
        if runtime.readonly() {
            let replica_db = database.clone();
            let replica_config = replication.clone();
            let replica_shutdown = receiver.clone();
            let replica_internal = internal_security.clone();
            workers.spawn(async move {
                let result = match replica_internal {
                    Some(InternalAdmission::Live(security)) => {
                        crate::replication::session::follow_live(
                            replica_db,
                            context,
                            replica_config,
                            replica_shutdown,
                            security,
                        )
                        .await
                    }
                    Some(InternalAdmission::Static(security)) => {
                        crate::replication::session::follow_secure(
                            replica_db,
                            context,
                            replica_config,
                            replica_shutdown,
                            security,
                        )
                        .await
                    }
                    None => {
                        crate::replication::session::follow(
                            replica_db,
                            context,
                            replica_config,
                            replica_shutdown,
                        )
                        .await
                    }
                };
                if let Err(error) = result {
                    tracing::error!(%error, "replica terminated with failure");
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
        .map(|(path, address)| {
            crate::readiness::ReadyFile::create_with_security(
                path,
                address,
                internal_security.is_some(),
            )
        })
        .transpose()?;
    let _ready = config
        .ready_file
        .as_ref()
        .map(|path| {
            crate::readiness::ReadyFile::create_with_security(
                path,
                listener.local_addr()?,
                security.is_some(),
            )
        })
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
        security,
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
        listener, config, shutdown, database, workers, stop, &mut None, None,
    )
    .await
}

#[allow(clippy::too_many_arguments)] // Explicit ownership of workers, shutdown, and optional TLS admission.
async fn supervise_workers(
    listener: TcpListener,
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send,
    database: DbHandle,
    mut workers: JoinSet<()>,
    stop: watch::Sender<bool>,
    shutdown_deadline: &mut Option<Instant>,
    security: Option<crate::security::ClientSecurity>,
) -> Result<(), ServerError> {
    let mut connections = JoinSet::new();
    let slots = Arc::new(Semaphore::new(config.max_connections));
    let metrics = database.metrics.clone();
    metrics.configure(&config, listener.local_addr()?.port());
    metrics.configure_security(security.is_some());
    let pubsub = crate::pubsub::Hub::with_metrics(metrics.clone());
    tokio::pin!(shutdown);
    tracing::info!(address = %listener.local_addr()?, "TCP server started");

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
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Some(error.into()),
                };
                let Ok(slot) = slots.clone().try_acquire_owned() else {
                    metrics.add(crate::metrics::Counter::RejectedConnections, 1);
                    tracing::warn!("excess connection rejected");
                    continue;
                };
                if let Err(error) = stream.set_nodelay(true) {
                    metrics.add(crate::metrics::Counter::ConnectionFailures, 1);
                    tracing::warn!(%error, "failed to configure connection");
                    continue;
                }
                let config = config.clone();
                let database = database.clone();
                let shutdown = stop.subscribe();
                let pubsub = pubsub.clone();
                let security = security.clone();
                connections.spawn(async move {
                    let _slot = slot;
                    if let Some(security) = security {
                        let (stream, security, registered) = match security.accept(stream).await {
                            Ok(stream) => stream,
                            Err(_) => { tracing::warn!("client TLS authentication failed"); return Ok(()); }
                        };
                        connection::run_with_registered_security(stream, config, database, shutdown, pubsub, Some((security, peer.ip())), registered).await
                    } else {
                        connection::run_with_pubsub(stream, config, database, shutdown, pubsub).await
                    }
                });
            }
        }
    };

    drop(listener);
    stop.send_replace(true);
    drop(database);
    tracing::info!("starting accepted-request drain");
    let deadline = Instant::now()
        .checked_add(config.shutdown_timeout)
        .ok_or(ServerError::ShutdownTimeout)?;
    *shutdown_deadline = Some(deadline);
    let mut failure = failure;
    while !workers.is_empty() || !connections.is_empty() {
        tokio::select! {
            biased;
            _ = sleep_until(deadline) => {
                tracing::error!(connections = connections.len(), workers = workers.len(), "forced shutdown");
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
    tracing::info!("server stopped");
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn log_connection(result: Option<Result<Result<(), ConnectionError>, tokio::task::JoinError>>) {
    match result {
        Some(Ok(Err(error))) => tracing::warn!(%error, "connection terminated with error"),
        Some(Err(error)) => tracing::error!(%error, "connection task failed"),
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
                    panic!("injected worker failure");
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
