//! Worker proprietário do armazenamento, com fila limitada e respostas individuais.

use std::time::Duration;

use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::time::{Instant, sleep_until};

use crate::command::{Command, Reply};
use crate::error::ConfigError;

use super::Store;

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
    requests: mpsc::Sender<Request>,
    request_timeout: Duration,
    shutdown: watch::Receiver<bool>,
}

/// Proprietário único do mapa e do lado receptor da fila.
pub struct Worker {
    store: Store,
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

    let (sender, receiver) = mpsc::channel(capacity);
    Ok((
        DbHandle {
            requests: sender,
            request_timeout,
            shutdown: shutdown.clone(),
        },
        Worker {
            store: Store::new(),
            requests: receiver,
            shutdown,
        },
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
            sent = self.requests.send(request) => {
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
    /// Processa comandos em ordem de recepção, sem suspender uma mutação.
    ///
    /// Na parada, fecha a admissão e drena tudo que foi aceito. Também termina
    /// naturalmente quando todos os handles são descartados e a fila esvazia.
    /// Uma resposta sem destinatário não impede a execução nem encerra o worker.
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                () = stopping(&mut self.shutdown) => {
                    self.requests.close();
                    break;
                }
                request = self.requests.recv() => {
                    match request {
                        Some(request) => self.apply(request),
                        None => return,
                    }
                }
            }
        }

        while let Some(request) = self.requests.recv().await {
            self.apply(request);
        }
    }

    fn apply(&mut self, request: Request) {
        let result = self.store.execute(request.command);
        let _ = request.reply.send(Ok(result));
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
        assert!(handle.requests.try_send(Request { command, reply }).is_ok());
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
        worker.apply(request);
        assert_eq!(first.await, Ok(Reply::Ok));
        assert_pending(second.as_mut()).await;
        assert_eq!(worker.requests.len(), 1);
        let request = worker.requests.try_recv().unwrap();
        assert_eq!(request.command, set(b"key", b"second"));
        worker.apply(request);
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
        worker.apply(request);
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
    async fn timeout_while_waiting_for_capacity_has_no_effect() {
        let (_stop, handle, mut worker) = setup(1);
        let ping = enqueue(&handle, Command::Ping(None));
        let mut waiting = Box::pin(handle.execute(set(b"key", b"not accepted")));
        assert_pending(waiting.as_mut()).await;
        advance(Duration::from_secs(5)).await;
        assert_eq!(waiting.await, Err(DbError::Timeout));

        let request = worker.requests.try_recv().unwrap();
        worker.apply(request);
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
        worker.apply(request);
        assert_eq!(first.await.unwrap(), Ok(Reply::Pong));
        assert_pending(waiting.as_mut()).await;
        assert_eq!(worker.requests.len(), 1);
        advance(Duration::from_secs(2)).await;
        assert_eq!(waiting.await, Err(DbError::Timeout));
        assert_eq!(Instant::now() - began, Duration::from_secs(5));

        let request = worker.requests.try_recv().unwrap();
        assert!(request.reply.is_closed());
        worker.apply(request);
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
        worker.apply(request);
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
