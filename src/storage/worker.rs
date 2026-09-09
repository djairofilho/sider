//! Worker proprietário do armazenamento, com fila limitada e respostas individuais.

use std::time::Duration;

use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::time::{Instant, MissedTickBehavior, interval, sleep_until};

use crate::command::{Command, Reply};
use crate::error::ConfigError;

use super::{Store, routing::ShardRouter};

/// Falha de transporte ou de ciclo de vida, distinta de uma resposta do banco.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum DbError {
    /// O worker ou o canal de resposta terminou inesperadamente.
    #[error("worker indisponível")]
    Unavailable,
    /// O prazo total expirou; um comando aceito pode ter sido aplicado.
    #[error("prazo do worker excedido; resultado da operação desconhecido")]
    Timeout,
    /// A parada começou antes de aceitar este pedido.
    #[error("servidor em encerramento")]
    ShuttingDown,
}

/// Envelope interno; o enum de comandos continua independente dos canais.
pub(crate) struct Request {
    pub(crate) command: Command,
    pub(crate) reply: oneshot::Sender<Result<Reply, DbError>>,
}

/// Acesso clonável ao mesmo armazenamento, sem compartilhar o mapa diretamente.
#[derive(Clone)]
pub struct DbHandle {
    requests: Vec<mpsc::Sender<Request>>,
    router: ShardRouter,
    request_timeout: Duration,
    shutdown: watch::Receiver<bool>,
}

/// Proprietário único do mapa e do lado receptor da fila.
pub struct Worker {
    store: Store,
    aof: Option<crate::persistence::AofHandle>,
    compact_after_bytes: u64,
    compaction: Option<oneshot::Receiver<Result<(), crate::persistence::AofError>>>,
    requests: mpsc::Receiver<Request>,
    shutdown: watch::Receiver<bool>,
}

/// Cria um mapa vazio e seu canal limitado, sem iniciar uma tarefa.
///
/// O chamador deve executar [`Worker::run`] em um runtime Tokio com timers.
/// `true` no canal de parada, ou o fechamento de todos os seus emissores, inicia
/// o encerramento. A capacidade conta mensagens, não bytes ou memória do dataset.
///
/// # Erros
///
/// Recusa capacidade zero ou acima do limite de canais Tokio, timeout zero e
/// duração que não pode ser representada como prazo do relógio monotônico.
pub fn channel(
    capacity: usize,
    request_timeout: Duration,
    shutdown: watch::Receiver<bool>,
) -> Result<(DbHandle, Worker), ConfigError> {
    channel_with_store(capacity, request_timeout, shutdown, Store::new())
}

/// Cria o canal para um armazenamento já configurado ou recuperado.
pub fn channel_with_store(
    capacity: usize,
    request_timeout: Duration,
    shutdown: watch::Receiver<bool>,
    store: Store,
) -> Result<(DbHandle, Worker), ConfigError> {
    let (handle, mut workers) =
        channel_with_stores(capacity, request_timeout, shutdown, vec![store])?;
    Ok((handle, workers.remove(0)))
}

/// Cada Store é movido para um único worker; filas e mapas permanecem separados.
pub fn channel_with_stores(
    capacity: usize,
    request_timeout: Duration,
    shutdown: watch::Receiver<bool>,
    stores: Vec<Store>,
) -> Result<(DbHandle, Vec<Worker>), ConfigError> {
    let router = ShardRouter::new(stores.len())?;
    if capacity == 0 || capacity > Semaphore::MAX_PERMITS {
        return Err(ConfigError::InvalidServerLimits {
            reason: "capacidade da fila deve estar entre 1 e Semaphore::MAX_PERMITS",
        });
    }
    if request_timeout.is_zero()
        || std::time::Instant::now()
            .checked_add(request_timeout)
            .is_none()
    {
        return Err(ConfigError::InvalidServerLimits {
            reason: "timeout do worker deve ser positivo e representável pelo relógio",
        });
    }

    let mut senders = Vec::with_capacity(stores.len());
    let mut workers = Vec::with_capacity(stores.len());
    for store in stores {
        let (sender, receiver) = mpsc::channel(capacity);
        senders.push(sender);
        workers.push(Worker {
            store,
            aof: None,
            compact_after_bytes: 0,
            compaction: None,
            requests: receiver,
            shutdown: shutdown.clone(),
        });
    }
    Ok((
        DbHandle {
            requests: senders,
            router,
            request_timeout,
            shutdown: shutdown.clone(),
        },
        workers,
    ))
}

impl DbHandle {
    /// Envia um comando e espera sua resposta dentro de um único prazo total.
    ///
    /// A conclusão do envio à fila é a fronteira de aceitação. Cancelar antes
    /// disso não modifica o banco. Depois da aceitação, descartar esta future,
    /// expirar o prazo ou iniciar a parada não cancela o comando no worker.
    /// Falha na entrega da resposta não desfaz operações e não provoca repetição.
    /// A drenagem só é garantida enquanto o worker não for abortado pelo supervisor.
    ///
    /// # Erros
    ///
    /// [`DbError::Timeout`] nunca promete ausência de efeitos. A parada cancela
    /// somente envios ainda não aceitos; respostas aceitas mantêm seu prazo original.
    pub async fn execute(&self, command: Command) -> Result<Reply, DbError> {
        let shard = match self.router.route(&command) {
            Ok(shard) => shard,
            Err(error) => return Ok(Reply::Error(error)),
        };
        let deadline = Instant::now()
            .checked_add(self.request_timeout)
            .ok_or(DbError::Timeout)?;
        let mut shutdown = self.shutdown.clone();
        let (reply, response) = oneshot::channel();
        let request = Request { command, reply };

        tokio::select! {
            biased;
            () = sleep_until(deadline) => return Err(DbError::Timeout),
            () = stopping(&mut shutdown) => return Err(DbError::ShuttingDown),
            sent = self.requests[shard].send(request) => {
                sent.map_err(|_| DbError::Unavailable)?;
            }
        }

        // Não observar shutdown aqui: o pedido já pertence ao worker. O prazo
        // é o mesmo usado no envio, incluindo toda a espera por capacidade.
        tokio::select! {
            biased;
            () = sleep_until(deadline) => Err(DbError::Timeout),
            result = response => result.map_err(|_| DbError::Unavailable)?,
        }
    }
}

impl Worker {
    pub fn with_aof(
        mut self,
        aof: crate::persistence::AofHandle,
        compact_after_bytes: u64,
    ) -> Self {
        self.aof = Some(aof);
        self.compact_after_bytes = compact_after_bytes;
        self
    }
    /// Processa comandos em ordem de recepção, sem suspender uma mutação.
    ///
    /// Na parada, fecha a admissão e drena tudo que foi aceito. Também termina
    /// naturalmente quando todos os handles são descartados e a fila esvazia.
    /// Uma resposta sem destinatário não impede a execução nem encerra o worker.
    pub async fn run(mut self) {
        let mut expiration = interval(Duration::from_millis(100));
        expiration.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = stopping(&mut self.shutdown) => {
                    self.requests.close();
                    break;
                }
                () = async { match &self.aof { Some(aof) => aof.failed().await, None => std::future::pending().await } } => return,
                _ = expiration.tick() => {
                    if self.expire().await.is_err() { return; }
                    self.compact_if_due().await;
                }
                request = self.requests.recv() => {
                    match request {
                        Some(request) => if !self.apply(request).await { return; },
                        None => break,
                    }
                }
            }
        }

        while let Some(request) = self.requests.recv().await {
            if !self.apply(request).await {
                return;
            }
        }
        if let Some(compaction) = self.compaction.take() {
            let _ = compaction.await;
        }
        if let Some(aof) = &self.aof
            && let Err(error) = aof.flush().await
        {
            tracing::error!(%error, "falha ao sincronizar AOF na parada");
        }
    }

    async fn apply(&mut self, request: Request) -> bool {
        let prepared = self.store.prepare(request.command);
        let result = self.commit(prepared).await;
        let healthy = result.is_ok();
        let _ = request.reply.send(result);
        healthy
    }

    async fn commit(&mut self, prepared: super::Prepared) -> Result<Reply, DbError> {
        if let Some(aof) = &self.aof
            && !prepared.batch.mutations.is_empty()
            && let Err(error) = aof.append(prepared.batch.clone()).await
        {
            if matches!(
                error,
                crate::persistence::AofError::Format(
                    crate::persistence::format::FormatError::Limit
                )
            ) {
                return Ok(Reply::Error(crate::command::ExecutionError::AofRecordLimit));
            }
            tracing::error!(%error, "mutação não aplicada por falha do AOF");
            self.requests.close();
            return Err(DbError::Unavailable);
        }
        Ok(self.store.apply(prepared))
    }

    async fn expire(&mut self) -> Result<(), DbError> {
        let mut budget = 64;
        loop {
            let prepared = self.store.prepare_expiration(budget);
            match self.commit(prepared).await? {
                Reply::Error(crate::command::ExecutionError::AofRecordLimit) if budget > 1 => {
                    budget /= 2
                }
                _ => return Ok(()),
            }
        }
    }

    async fn compact_if_due(&mut self) {
        if let Some(completion) = &mut self.compaction {
            match completion.try_recv() {
                Ok(Ok(())) => tracing::info!("compactação AOF concluída"),
                Ok(Err(error)) => tracing::warn!(%error, "compactação AOF abortada"),
                Err(oneshot::error::TryRecvError::Empty) => return,
                Err(oneshot::error::TryRecvError::Closed) => {
                    tracing::warn!("compactador AOF indisponível")
                }
            }
            self.compaction = None;
        }
        if self.compact_after_bytes == 0 {
            return;
        }
        if let Some(aof) = &self.aof
            && let Ok((_, bytes, false)) = aof.status().await
            && bytes >= self.compact_after_bytes
        {
            // Enfileira a barreira antes de aceitar outra mutação neste worker.
            match aof.begin_compaction(self.store.snapshot()).await {
                Ok(completion) => self.compaction = Some(completion),
                Err(error) => {
                    tracing::warn!(%error, "não foi possível iniciar compactação AOF")
                }
            }
        }
    }
}

async fn stopping(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::pin::Pin;
    use std::task::Poll;

    use bytes::Bytes;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::time::advance;

    use super::*;

    fn setup(capacity: usize) -> (watch::Sender<bool>, DbHandle, Worker) {
        let (stop, shutdown) = watch::channel(false);
        let (handle, worker) = channel(capacity, Duration::from_secs(5), shutdown).unwrap();
        (stop, handle, worker)
    }

    fn set(key: &'static [u8], value: &'static [u8]) -> Command {
        Command::Set {
            key: Bytes::from_static(key),
            value: Bytes::from_static(value),
        }
    }

    fn get(key: &'static [u8]) -> Command {
        Command::Get {
            key: Bytes::from_static(key),
        }
    }

    fn bulk(value: &'static [u8]) -> Reply {
        Reply::Bulk(Some(Bytes::from_static(value)))
    }

    async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
        poll_fn(|context| {
            assert!(future.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
    }

    fn enqueue(handle: &DbHandle, command: Command) -> oneshot::Receiver<Result<Reply, DbError>> {
        let (reply, response) = oneshot::channel();
        assert!(
            handle.requests[0]
                .try_send(Request { command, reply })
                .is_ok()
        );
        response
    }

    #[test]
    fn rejects_invalid_capacity_and_timeout_without_runtime() {
        let (_stop, shutdown) = watch::channel(false);
        for capacity in [0, Semaphore::MAX_PERMITS + 1, usize::MAX] {
            assert!(matches!(
                channel(capacity, Duration::from_secs(1), shutdown.clone()),
                Err(ConfigError::InvalidServerLimits { .. })
            ));
        }
        for duration in [Duration::ZERO, Duration::MAX] {
            assert!(matches!(
                channel(1, duration, shutdown.clone()),
                Err(ConfigError::InvalidServerLimits { .. })
            ));
        }
        assert!(channel(1, Duration::from_nanos(1), shutdown).is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn saturated_shard_does_not_block_an_independent_worker() {
        let (stop, shutdown) = watch::channel(false);
        let (handle, mut workers) = channel_with_stores(
            1,
            Duration::from_secs(5),
            shutdown,
            vec![Store::new(), Store::new()],
        )
        .unwrap();
        // FNV-1a(a) termina em bit zero; FNV-1a(b) termina em bit um.
        assert_eq!(handle.router.shard_for(b"a"), 0);
        assert_eq!(handle.router.shard_for(b"b"), 1);
        let cold = workers.pop().unwrap();
        let hot = workers.pop().unwrap();
        let mut first = Box::pin(handle.execute(set(b"a", b"first")));
        let mut second = Box::pin(handle.execute(set(b"a", b"second")));
        assert_pending(first.as_mut()).await;
        assert_pending(second.as_mut()).await;
        assert_eq!(hot.requests.len(), 1);
        let cold_task = tokio::spawn(cold.run());
        assert_eq!(
            handle.execute(set(b"b", b"independent")).await,
            Ok(Reply::Ok)
        );
        // O shard quente progride assim que seu worker volta a ser escalonado.
        let hot_task = tokio::spawn(hot.run());
        assert_eq!(first.await, Ok(Reply::Ok));
        assert_eq!(second.await, Ok(Reply::Ok));
        stop.send_replace(true);
        cold_task.await.unwrap();
        hot_task.await.unwrap();
    }

    #[tokio::test]
    async fn cross_shard_commands_are_rejected_before_any_enqueue() {
        let (_stop, shutdown) = watch::channel(false);
        let (handle, workers) = channel_with_stores(
            1,
            Duration::from_secs(5),
            shutdown,
            vec![Store::new(), Store::new()],
        )
        .unwrap();
        let keys = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
        for command in [
            Command::Del { keys: keys.clone() },
            Command::Exists { keys: keys.clone() },
            Command::MGet { keys: keys.clone() },
            Command::MSet {
                entries: keys.iter().map(|key| (key.clone(), Bytes::new())).collect(),
            },
        ] {
            assert_eq!(
                handle.execute(command).await,
                Ok(Reply::Error(crate::command::ExecutionError::CrossShard))
            );
            assert!(workers.iter().all(|worker| worker.requests.is_empty()));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cloned_handles_share_binary_state_and_all_commands_use_worker() {
        let (_stop, first, worker) = setup(1);
        let second = first.clone();
        let running = tokio::spawn(worker.run());

        assert_eq!(
            first.execute(set(b"\xff\0", b"\x80\r\n")).await,
            Ok(Reply::Ok)
        );
        assert_eq!(second.execute(get(b"\xff\0")).await, Ok(bulk(b"\x80\r\n")));
        assert_eq!(second.execute(Command::Ping(None)).await, Ok(Reply::Pong));
        assert_eq!(
            first.execute(Command::Ping(Some(Bytes::new()))).await,
            Ok(bulk(b""))
        );
        assert_eq!(
            second
                .execute(Command::Echo(Bytes::from_static(b"\xff\0")))
                .await,
            Ok(bulk(b"\xff\0"))
        );
        assert_eq!(
            first
                .execute(Command::Del {
                    keys: vec![Bytes::from_static(b"\xff\0"), Bytes::from_static(b"\xff\0")],
                })
                .await,
            Ok(Reply::Integer(1))
        );
        assert_eq!(second.execute(get(b"\xff\0")).await, Ok(Reply::Bulk(None)));
        drop((first, second));
        running.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn ping_and_echo_wait_for_the_same_worker_queue() {
        let (stop, handle, worker) = setup(2);
        let mut ping = Box::pin(handle.execute(Command::Ping(None)));
        let mut echo = Box::pin(handle.execute(Command::Echo(Bytes::from_static(b"\xff\0"))));
        assert_pending(ping.as_mut()).await;
        assert_pending(echo.as_mut()).await;
        assert_eq!(worker.requests.len(), 2);
        stop.send(true).unwrap();
        worker.run().await;
        assert_eq!(ping.await, Ok(Reply::Pong));
        assert_eq!(echo.await, Ok(bulk(b"\xff\0")));
    }

    #[tokio::test(start_paused = true)]
    async fn one_slot_applies_backpressure_and_preserves_received_order() {
        let (_stop, handle, mut worker) = setup(1);
        let mut first = Box::pin(handle.execute(set(b"key", b"first")));
        assert_pending(first.as_mut()).await;
        assert_eq!(worker.requests.len(), 1);
        let mut second = Box::pin(handle.execute(set(b"key", b"second")));
        assert_pending(second.as_mut()).await;
        assert_eq!(worker.requests.len(), 1);

        let request = worker.requests.try_recv().unwrap();
        assert_eq!(request.command, set(b"key", b"first"));
        worker.apply(request).await;
        assert_eq!(first.await, Ok(Reply::Ok));
        assert_pending(second.as_mut()).await;
        assert_eq!(worker.requests.len(), 1);
        let request = worker.requests.try_recv().unwrap();
        assert_eq!(request.command, set(b"key", b"second"));
        worker.apply(request).await;
        assert_eq!(second.await, Ok(Reply::Ok));
        assert_eq!(worker.store.execute(get(b"key")), bulk(b"second"));
        drop(handle);
        worker.run().await;
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_before_acceptance_does_not_change_state_or_consume_capacity() {
        let (_stop, handle, mut worker) = setup(1);
        let first = enqueue(&handle, set(b"key", b"kept"));
        let mut cancelled = Box::pin(handle.execute(set(b"key", b"cancelled")));
        assert_pending(cancelled.as_mut()).await;
        drop(cancelled);

        let request = worker.requests.try_recv().unwrap();
        worker.apply(request).await;
        assert_eq!(first.await.unwrap(), Ok(Reply::Ok));
        assert!(matches!(
            worker.requests.try_recv(),
            Err(TryRecvError::Empty)
        ));
        let observed = enqueue(&handle, get(b"key"));
        drop(handle);
        worker.run().await;
        assert_eq!(observed.await.unwrap(), Ok(bulk(b"kept")));
    }

    #[tokio::test(start_paused = true)]
    async fn discarded_response_after_acceptance_does_not_cancel_mutation() {
        let (_stop, handle, worker) = setup(2);
        let mut abandoned = Box::pin(handle.execute(set(b"key", b"accepted")));
        assert_pending(abandoned.as_mut()).await;
        assert_eq!(worker.requests.len(), 1);
        drop(abandoned);

        let observed = enqueue(&handle, get(b"key"));
        drop(handle);
        worker.run().await;
        assert_eq!(observed.await.unwrap(), Ok(bulk(b"accepted")));
    }

    #[tokio::test(start_paused = true)]
    async fn r02_discarded_mset_reply_still_applies_the_complete_batch() {
        let (_stop, handle, worker) = setup(2);
        let mut abandoned = Box::pin(handle.execute(Command::MSet {
            entries: vec![
                (Bytes::from_static(b"a"), Bytes::from_static(b"first")),
                (Bytes::from_static(b"b"), Bytes::from_static(b"second")),
            ],
        }));
        assert_pending(abandoned.as_mut()).await;
        assert_eq!(worker.requests.len(), 1);
        drop(abandoned);
        let observed = enqueue(
            &handle,
            Command::MGet {
                keys: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            },
        );
        drop(handle);
        worker.run().await;
        assert_eq!(
            observed.await.unwrap(),
            Ok(Reply::Array(vec![bulk(b"first"), bulk(b"second")]))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_while_waiting_for_capacity_has_no_effect() {
        let (_stop, handle, mut worker) = setup(1);
        let ping = enqueue(&handle, Command::Ping(None));
        let mut waiting = Box::pin(handle.execute(set(b"key", b"not accepted")));
        assert_pending(waiting.as_mut()).await;
        advance(Duration::from_secs(5)).await;
        assert_eq!(waiting.await, Err(DbError::Timeout));

        let request = worker.requests.try_recv().unwrap();
        worker.apply(request).await;
        assert_eq!(ping.await.unwrap(), Ok(Reply::Pong));
        assert!(matches!(
            worker.requests.try_recv(),
            Err(TryRecvError::Empty)
        ));
        assert_eq!(worker.store.execute(get(b"key")), Reply::Bulk(None));
    }

    #[tokio::test(start_paused = true)]
    async fn total_deadline_is_not_restarted_after_queue_wait() {
        let (_stop, handle, mut worker) = setup(1);
        let first = enqueue(&handle, Command::Ping(None));
        let began = Instant::now();
        let mut waiting = Box::pin(handle.execute(set(b"key", b"accepted")));
        assert_pending(waiting.as_mut()).await;
        advance(Duration::from_secs(3)).await;

        let request = worker.requests.try_recv().unwrap();
        worker.apply(request).await;
        assert_eq!(first.await.unwrap(), Ok(Reply::Pong));
        assert_pending(waiting.as_mut()).await;
        assert_eq!(worker.requests.len(), 1);
        advance(Duration::from_secs(2)).await;
        assert_eq!(waiting.await, Err(DbError::Timeout));
        assert_eq!(Instant::now() - began, Duration::from_secs(5));

        let request = worker.requests.try_recv().unwrap();
        assert!(request.reply.is_closed());
        worker.apply(request).await;
        assert_eq!(worker.store.execute(get(b"key")), bulk(b"accepted"));
    }

    #[tokio::test(start_paused = true)]
    async fn elapsed_deadline_wins_over_newly_available_queue_space() {
        let (_stop, handle, mut worker) = setup(1);
        let first = enqueue(&handle, Command::Ping(None));
        let mut waiting = Box::pin(handle.execute(set(b"key", b"late")));
        assert_pending(waiting.as_mut()).await;
        advance(Duration::from_secs(5)).await;
        let request = worker.requests.try_recv().unwrap();
        worker.apply(request).await;
        assert_eq!(first.await.unwrap(), Ok(Reply::Pong));

        assert_eq!(waiting.await, Err(DbError::Timeout));
        assert!(matches!(
            worker.requests.try_recv(),
            Err(TryRecvError::Empty)
        ));
        assert_eq!(worker.store.execute(get(b"key")), Reply::Bulk(None));
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_rejects_unaccepted_requests_and_drains_accepted_in_order() {
        let (stop, handle, worker) = setup(2);
        let mut accepted = Box::pin(handle.execute(set(b"key", b"kept")));
        assert_pending(accepted.as_mut()).await;
        let mut observed = Box::pin(handle.execute(get(b"key")));
        assert_pending(observed.as_mut()).await;
        assert_eq!(worker.requests.len(), 2);
        let mut waiting = Box::pin(handle.execute(set(b"key", b"not accepted")));
        assert_pending(waiting.as_mut()).await;

        stop.send(true).unwrap();
        assert_eq!(waiting.await, Err(DbError::ShuttingDown));
        assert_pending(accepted.as_mut()).await;
        assert_pending(observed.as_mut()).await;
        // O handle continua vivo: fechar a admissão deve bastar para drenar.
        worker.run().await;
        assert_eq!(accepted.await, Ok(Reply::Ok));
        assert_eq!(observed.await, Ok(bulk(b"kept")));
        assert_eq!(
            handle.execute(Command::Ping(None)).await,
            Err(DbError::ShuttingDown)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_with_free_queue_rejects_before_acceptance() {
        let (stop, handle, mut worker) = setup(1);
        stop.send(true).unwrap();
        assert_eq!(
            handle.execute(set(b"key", b"rejected")).await,
            Err(DbError::ShuttingDown)
        );
        assert!(matches!(
            worker.requests.try_recv(),
            Err(TryRecvError::Empty)
        ));
        worker.run().await;
    }

    #[tokio::test(start_paused = true)]
    async fn closed_shutdown_channel_stops_admission_and_drains() {
        let (stop, handle, worker) = setup(2);
        let stored = enqueue(&handle, set(b"key", b"kept"));
        let observed = enqueue(&handle, get(b"key"));
        drop(stop);
        assert_eq!(
            handle.execute(Command::Ping(None)).await,
            Err(DbError::ShuttingDown)
        );
        worker.run().await;
        assert_eq!(stored.await.unwrap(), Ok(Reply::Ok));
        assert_eq!(observed.await.unwrap(), Ok(bulk(b"kept")));
    }

    #[tokio::test(start_paused = true)]
    async fn false_shutdown_update_does_not_stop_worker_or_handle() {
        let (stop, handle, worker) = setup(1);
        stop.send(false).unwrap();
        let running = tokio::spawn(worker.run());
        assert_eq!(handle.execute(Command::Ping(None)).await, Ok(Reply::Pong));
        stop.send(true).unwrap();
        running.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_worker_before_acceptance_returns_without_waiting() {
        let (_stop, handle, worker) = setup(1);
        drop(worker);
        let began = Instant::now();
        assert_eq!(
            handle.execute(Command::Ping(None)).await,
            Err(DbError::Unavailable)
        );
        assert_eq!(Instant::now(), began);
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_response_after_acceptance_returns_without_waiting() {
        let (_stop, handle, worker) = setup(1);
        let began = Instant::now();
        let mut accepted = Box::pin(handle.execute(set(b"key", b"unknown")));
        assert_pending(accepted.as_mut()).await;
        assert_eq!(worker.requests.len(), 1);
        drop(worker);
        assert_eq!(accepted.await, Err(DbError::Unavailable));
        assert_eq!(Instant::now(), began);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_all_handles_drains_and_finishes_without_shutdown_signal() {
        let (_stop, handle, worker) = setup(3);
        let stored = enqueue(&handle, set(b"key", b"kept"));
        let observed = enqueue(&handle, get(b"key"));
        let ping = enqueue(&handle, Command::Ping(None));
        drop(handle);
        worker.run().await;
        assert_eq!(stored.await.unwrap(), Ok(Reply::Ok));
        assert_eq!(observed.await.unwrap(), Ok(bulk(b"kept")));
        assert_eq!(ping.await.unwrap(), Ok(Reply::Pong));
    }
}
