//! Barreira global para observar workers proprietários sem compartilhar seus mapas.

use std::time::Duration;

use tokio::sync::{OwnedRwLockWriteGuard, oneshot};
use tokio::time::{MissedTickBehavior, interval, timeout};

use crate::persistence::{AofError, AofHandle};

use super::Mutation;
use super::worker::{DbError, DbHandle};

/// Estado ordenado de todos os shards e sequência durável no mesmo instante lógico.
#[derive(Debug)]
pub struct Snapshot {
    pub mutations: Vec<Mutation>,
    pub sequence: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("snapshot indisponível: {0}")]
    Database(#[from] DbError),
    #[error("snapshot durável: {0}")]
    Persistence(#[from] AofError),
}

impl DbHandle {
    async fn freeze(&self) -> Result<(OwnedRwLockWriteGuard<()>, Vec<Mutation>), DbError> {
        if *self.shutdown.borrow() {
            return Err(DbError::ShuttingDown);
        }
        // Nenhum worker espera por esta trava. Pedidos normais já aceitos possuem
        // read guards e terminam antes daqui; expiração apenas tenta a admissão.
        let guard = self.barrier.clone().write_owned().await;
        let mut responses = Vec::with_capacity(self.snapshots.len());
        for sender in &self.snapshots {
            let (reply, response) = oneshot::channel();
            sender.send(reply).await.map_err(|_| DbError::Unavailable)?;
            responses.push(response);
        }
        let mut mutations = Vec::new();
        for response in responses {
            mutations.extend(response.await.map_err(|_| DbError::Unavailable)?);
        }
        mutations.sort_unstable_by(|left, right| left.key().cmp(right.key()));
        Ok((guard, mutations))
    }

    /// Aguarda pedidos aceitos e captura todos os shards, incluindo deadlines.
    /// O mesmo prazo total limita a barreira, coleta e flush do escritor.
    pub async fn snapshot(&self, aof: Option<&AofHandle>) -> Result<Snapshot, SnapshotError> {
        timeout(self.request_timeout, async {
            let (_guard, mutations) = self.freeze().await?;
            let sequence = match aof {
                Some(aof) => Some(aof.flush().await?),
                None => None,
            };
            Ok(Snapshot {
                mutations,
                sequence,
            })
        })
        .await
        .map_err(|_| DbError::Timeout)?
    }

    /// Enfileira a barreira AOF ainda sob exclusão de mutações. Depois permite
    /// novos pedidos; o escritor captura o delta durante a compactação.
    pub async fn begin_compaction(
        &self,
        aof: &AofHandle,
    ) -> Result<oneshot::Receiver<Result<(), AofError>>, SnapshotError> {
        timeout(self.request_timeout, async {
            let (_guard, mutations) = self.freeze().await?;
            Ok(aof.begin_compaction(mutations).await?)
        })
        .await
        .map_err(|_| DbError::Timeout)?
    }

    /// Coordena compactação automática global. Zero desabilita o agendamento.
    pub async fn run_compaction(self, aof: AofHandle, threshold: u64) {
        let mut shutdown = self.shutdown.clone();
        let mut ticker = interval(Duration::from_millis(100));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut completion: Option<oneshot::Receiver<Result<(), AofError>>> = None;
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                () = aof.failed() => break,
                _ = ticker.tick(), if threshold > 0 => {
                    if let Some(pending) = &mut completion {
                        match pending.try_recv() {
                            Ok(Ok(())) => tracing::info!("compactação global concluída"),
                            Ok(Err(error)) => tracing::warn!(%error, "compactação global abortada"),
                            Err(oneshot::error::TryRecvError::Empty) => continue,
                            Err(oneshot::error::TryRecvError::Closed) => break,
                        }
                        completion = None;
                    }
                    match aof.status().await {
                        Ok((_, bytes, false)) if bytes >= threshold => {
                            match self.begin_compaction(&aof).await {
                                Ok(pending) => completion = Some(pending),
                                Err(error) => tracing::warn!(%error, "snapshot global não iniciado"),
                            }
                        }
                        Err(_) => break,
                        _ => {}
                    }
                }
            }
        }
        if let Some(pending) = completion {
            let _ = pending.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::pin::pin;
    use std::task::Poll;

    use bytes::Bytes;
    use tokio::sync::watch;
    use tokio::time::advance;

    use super::*;
    use crate::command::{Command, Reply, SetExpiry, SetOptions};
    use crate::storage::{Store, worker};

    fn set(key: &'static [u8]) -> Command {
        Command::Set {
            key: Bytes::from_static(key),
            value: Bytes::from_static(b"accepted"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn admission_barrier_uses_total_deadline_and_rejection_has_no_effect() {
        let (stop, shutdown) = watch::channel(false);
        let (database, worker) = worker::channel(1, Duration::from_secs(5), shutdown).unwrap();
        let running = tokio::spawn(worker.run());
        let exclusive = database.barrier.clone().write_owned().await;
        let mut command = pin!(database.execute(set(b"key")));
        poll_fn(|cx| {
            assert!(command.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        advance(Duration::from_secs(5)).await;
        assert_eq!(command.await, Err(DbError::Timeout));
        drop(exclusive);
        assert!(database.snapshot(None).await.unwrap().mutations.is_empty());
        stop.send_replace(true);
        running.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn abandoned_accepted_request_is_in_snapshot_after_drain() {
        let (stop, shutdown) = watch::channel(false);
        let (database, worker) = worker::channel(1, Duration::from_secs(5), shutdown).unwrap();
        {
            let mut command = pin!(database.execute(set(b"key")));
            poll_fn(|cx| {
                assert!(command.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        // Quem mantém a admissão é o envelope já aceito, não a future do cliente.
        assert!(database.barrier.try_write().is_err());
        let running = tokio::spawn(worker.run());
        let snapshot = database.snapshot(None).await.unwrap();
        assert_eq!(snapshot.mutations.len(), 1);
        assert_eq!(snapshot.mutations[0].key().as_ref(), b"key");
        stop.send_replace(true);
        running.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn expiration_never_blocks_snapshot_requests_behind_exclusive_barrier() {
        let mut store = Store::new();
        store.execute(Command::SetWithOptions {
            key: Bytes::from_static(b"expiring"),
            value: Bytes::from_static(b"v"),
            options: SetOptions {
                expiry: SetExpiry::After(Duration::from_millis(10)),
                ..SetOptions::default()
            },
        });
        let (stop, shutdown) = watch::channel(false);
        let (database, worker) =
            worker::channel_with_store(1, Duration::from_secs(5), shutdown, store).unwrap();
        let exclusive = database.barrier.clone().write_owned().await;
        advance(Duration::from_millis(100)).await;
        let running = tokio::spawn(worker.run());
        let (reply, snapshot) = oneshot::channel();
        database.snapshots[0].send(reply).await.unwrap();
        let snapshot = timeout(Duration::from_secs(1), snapshot)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.len(), 1);
        assert!(matches!(
            snapshot[0],
            Mutation::Put {
                expires_at_unix_ms: Some(_),
                ..
            }
        ));
        drop(exclusive);
        stop.send_replace(true);
        running.await.unwrap();
    }

    #[tokio::test]
    async fn snapshots_preserve_batches_while_distinct_workers_write() {
        let (stop, shutdown) = watch::channel(false);
        let (database, workers) = worker::channel_with_stores(
            8,
            Duration::from_secs(5),
            shutdown,
            (0..4).map(|_| Store::new()).collect(),
        )
        .unwrap();
        let mut running = tokio::task::JoinSet::new();
        for worker in workers {
            running.spawn(worker.run());
        }
        let mut writes = tokio::task::JoinSet::new();
        for tag in 0..4 {
            let database = database.clone();
            writes.spawn(async move {
                for n in 0..40 {
                    let value = Bytes::from(n.to_string());
                    assert_eq!(
                        database
                            .execute(Command::MSet {
                                entries: vec![
                                    (Bytes::from(format!("{{{tag}}}:a")), value.clone()),
                                    (Bytes::from(format!("{{{tag}}}:b")), value),
                                ]
                            })
                            .await
                            .unwrap(),
                        Reply::Ok
                    );
                }
            });
        }
        for _ in 0..30 {
            let snapshot = database.snapshot(None).await.unwrap();
            assert_eq!(snapshot.mutations.len() % 2, 0);
            for pair in snapshot.mutations.chunks_exact(2) {
                let [
                    Mutation::Put { value: left, .. },
                    Mutation::Put { value: right, .. },
                ] = pair
                else {
                    panic!("snapshot contém remoção")
                };
                assert_eq!(left, right);
            }
        }
        while let Some(result) = writes.join_next().await {
            result.unwrap();
        }
        assert_eq!(database.snapshot(None).await.unwrap().mutations.len(), 8);
        stop.send_replace(true);
        while let Some(result) = running.join_next().await {
            result.unwrap();
        }
    }
}
