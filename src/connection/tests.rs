use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{DuplexStream, ReadBuf, duplex};
use tokio::time::{advance, timeout};

use crate::command::{Command, Reply};
use crate::storage::worker;

use super::*;

const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const LATER_SET: &[u8] = b"*3\r\n$3\r\nSET\r\n$5\r\nlater\r\n$5\r\nvalue\r\n";

const SUBSCRIBE: &[u8] = b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\na\r\n";
const SUBSCRIBED: &[u8] = b"*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n";

#[tokio::test(start_paused = true)]
async fn pubsub_eviction_interrupts_blocked_write_and_cleans_every_channel() {
    let config = ServerConfig {
        pubsub_queue_capacity: 1,
        ..ServerConfig::default()
    };
    let (stop, database, _worker) = setup(&config);
    let hub = Hub::default();
    let (mut client, stream) = duplex(64);
    let mut connection = Box::pin(run_with_pubsub(
        stream,
        config,
        database,
        stop.subscribe(),
        hub.clone(),
    ));
    feed(&mut client, connection.as_mut(), SUBSCRIBE).await;
    read_exact_ready(&mut client, SUBSCRIBED).await;
    let message = || Message {
        channel: Bytes::from_static(b"a"),
        payload: Bytes::from(vec![0xff; 128]),
    };
    assert_eq!(hub.publish(message()), 1);
    assert_pending(connection.as_mut()).await; // A escrita de 128 bytes bloqueia no duplex de 64.
    assert_eq!(hub.publish(message()), 1);
    assert_eq!(hub.publish(message()), 0); // Fila cheia remove o assinante sem esperar I/O.
    assert!(matches!(
        ready(connection.as_mut()).await,
        Err(ConnectionError::PubSub(PubSubError::Closed))
    ));
    drop(connection);
    assert_eq!(hub.publish(message()), 0);
    let mut remaining = Vec::new();
    client.read_to_end(&mut remaining).await.unwrap();
    assert_eq!(remaining.len(), 64);
}

#[tokio::test(start_paused = true)]
async fn pubsub_write_timeout_and_future_cancellation_release_subscriptions() {
    for cancel in [false, true] {
        let config = ServerConfig {
            write_timeout: Duration::from_secs(1),
            ..ServerConfig::default()
        };
        let (stop, database, _worker) = setup(&config);
        let hub = Hub::default();
        let (mut client, stream) = duplex(64);
        let mut connection = Box::pin(run_with_pubsub(
            stream,
            config,
            database,
            stop.subscribe(),
            hub.clone(),
        ));
        feed(&mut client, connection.as_mut(), SUBSCRIBE).await;
        read_exact_ready(&mut client, SUBSCRIBED).await;
        let message = || Message {
            channel: Bytes::from_static(b"a"),
            payload: Bytes::from(vec![0; 128]),
        };
        assert_eq!(hub.publish(message()), 1);
        assert_pending(connection.as_mut()).await;
        if !cancel {
            advance(Duration::from_secs(1)).await;
            assert!(matches!(
                ready(connection.as_mut()).await,
                Err(ConnectionError::WriteTimeout)
            ));
        }
        drop(connection);
        assert_eq!(hub.publish(message()), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn pubsub_shutdown_and_eof_remove_subscriptions() {
    for eof in [false, true] {
        let config = ServerConfig::default();
        let (stop, database, _worker) = setup(&config);
        let hub = Hub::default();
        let (mut client, stream) = duplex(128);
        let mut connection = Box::pin(run_with_pubsub(
            stream,
            config,
            database,
            stop.subscribe(),
            hub.clone(),
        ));
        feed(&mut client, connection.as_mut(), SUBSCRIBE).await;
        read_exact_ready(&mut client, SUBSCRIBED).await;
        if eof {
            client.shutdown().await.unwrap();
        } else {
            stop.send_replace(true);
        }
        ready(connection.as_mut()).await.unwrap();
        drop(connection);
        assert_eq!(
            hub.publish(Message {
                channel: Bytes::from_static(b"a"),
                payload: Bytes::new()
            }),
            0
        );
    }
}

#[tokio::test(start_paused = true)]
async fn pubsub_limit_is_atomic_and_intercepted_without_polling_worker() {
    let config = ServerConfig {
        pubsub_max_channels: 1,
        ..ServerConfig::default()
    };
    let (stop, database, _worker) = setup(&config);
    let hub = Hub::default();
    let (mut client, stream) = duplex(256);
    let mut connection = Box::pin(run_with_pubsub(
        stream,
        config,
        database,
        stop.subscribe(),
        hub.clone(),
    ));
    feed(&mut client, connection.as_mut(), SUBSCRIBE).await;
    read_exact_ready(&mut client, SUBSCRIBED).await;
    feed(
        &mut client,
        connection.as_mut(),
        b"*3\r\n$9\r\nSUBSCRIBE\r\n$1\r\nb\r\n$1\r\na\r\n",
    )
    .await;
    read_exact_ready(&mut client, b"-ERR pubsub channel limit exceeded\r\n").await;
    feed(&mut client, connection.as_mut(), PING).await;
    read_exact_ready(&mut client, b"*2\r\n$4\r\npong\r\n$0\r\n\r\n").await;
    assert_eq!(
        hub.publish(Message {
            channel: Bytes::from_static(b"b"),
            payload: Bytes::new()
        }),
        0
    );
    assert_eq!(
        hub.publish(Message {
            channel: Bytes::from_static(b"a"),
            payload: Bytes::new()
        }),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn pubsub_oversized_notification_is_rejected_before_fanout() {
    let config = ServerConfig {
        resp_limits: RespLimits {
            max_bulk_bytes: 64,
            ..RespLimits::default()
        },
        max_response_bytes: 128,
        ..ServerConfig::default()
    };
    let (stop, database, _worker) = setup(&config);
    let hub = Hub::default();
    let channel = Bytes::from(vec![b'c'; 64]);
    let mut subscriber = hub.connect(1, 1).unwrap();
    subscriber.subscribe(vec![channel.clone()]).unwrap();
    let (mut client, stream) = duplex(512);
    let mut connection = Box::pin(run_with_pubsub(
        stream,
        config,
        database,
        stop.subscribe(),
        hub,
    ));
    let request = Frame::Array(Some(vec![
        Frame::Bulk(Some(Bytes::from_static(b"PUBLISH"))),
        Frame::Bulk(Some(channel)),
        Frame::Bulk(Some(Bytes::from(vec![0xff; 64]))),
    ]));
    let mut bytes = BytesMut::new();
    encode(&request, &mut bytes, RespLimits::default()).unwrap();
    feed(&mut client, connection.as_mut(), &bytes).await;
    read_exact_ready(
        &mut client,
        b"-ERR pubsub message exceeds response limit\r\n",
    )
    .await;
    assert!(subscriber.try_message().is_none());
    assert!(subscriber.active());
}

#[tokio::test(start_paused = true)]
async fn pubsub_slow_socket_does_not_block_fast_socket_or_database() {
    let config = ServerConfig {
        pubsub_queue_capacity: 1,
        ..ServerConfig::default()
    };
    let (stop, database, owner) = setup(&config);
    let mut worker = Box::pin(owner.run());
    let hub = Hub::default();
    let (mut slow_client, slow_stream) = duplex(64);
    let (mut fast_client, fast_stream) = duplex(512);
    let mut slow = Box::pin(run_with_pubsub(
        slow_stream,
        config.clone(),
        database.clone(),
        stop.subscribe(),
        hub.clone(),
    ));
    let mut fast = Box::pin(run_with_pubsub(
        fast_stream,
        config,
        database.clone(),
        stop.subscribe(),
        hub.clone(),
    ));
    feed(&mut slow_client, slow.as_mut(), SUBSCRIBE).await;
    read_exact_ready(&mut slow_client, SUBSCRIBED).await;
    feed(&mut fast_client, fast.as_mut(), SUBSCRIBE).await;
    read_exact_ready(&mut fast_client, SUBSCRIBED).await;
    for index in 0..3_u8 {
        let message = Message {
            channel: Bytes::from_static(b"a"),
            payload: Bytes::from(vec![index; 128]),
        };
        let mut expected = BytesMut::new();
        encode(
            &message.clone().into_frame(),
            &mut expected,
            RespLimits::default(),
        )
        .unwrap();
        assert_eq!(hub.publish(message), if index == 2 { 1 } else { 2 });
        if index == 0 {
            assert_pending(slow.as_mut()).await;
        }
        assert_pending(fast.as_mut()).await;
        read_exact_ready(&mut fast_client, &expected).await;
        let mut request = Box::pin(database.execute(Command::Ping(None)));
        assert_pending(request.as_mut()).await;
        assert_pending(worker.as_mut()).await;
        assert_eq!(ready(request.as_mut()).await.unwrap(), Reply::Pong);
    }
    assert!(matches!(
        ready(slow.as_mut()).await,
        Err(ConnectionError::PubSub(PubSubError::Closed))
    ));
}

async fn guard(test: impl Future<Output = ()>) {
    timeout(Duration::from_secs(600), test)
        .await
        .expect("connection test exceeded its outer deadline");
}

// Uma única sondagem sempre termina imediatamente: não dá ao runtime a chance
// de avançar o relógio até um timer futuro para disfarçar falta de progresso.
async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}

async fn assert_pending<F: Future>(future: Pin<&mut F>) {
    assert!(poll_once(future).await.is_pending());
}

async fn ready<F: Future>(future: Pin<&mut F>) -> F::Output {
    match poll_once(future).await {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future should be ready without advancing time"),
    }
}

async fn feed<F>(client: &mut DuplexStream, mut connection: Pin<&mut F>, bytes: &[u8])
where
    F: Future<Output = Result<(), ConnectionError>>,
{
    let mut writing = Box::pin(client.write_all(bytes));
    // Há no máximo um avanço por byte, mesmo com um duplex de capacidade 1.
    for _ in 0..=bytes.len() {
        match poll_once(writing.as_mut()).await {
            Poll::Ready(result) => {
                result.unwrap();
                assert_pending(connection.as_mut()).await;
                return;
            }
            Poll::Pending => assert_pending(connection.as_mut()).await,
        }
    }
    panic!("connection stopped consuming input before accepting a request");
}

async fn read_exact_ready(client: &mut DuplexStream, expected: &[u8]) {
    let mut actual = vec![0; expected.len()];
    let mut reading = Box::pin(client.read_exact(&mut actual));
    ready(reading.as_mut()).await.unwrap();
    drop(reading);
    assert_eq!(actual, expected);
}

async fn assert_eof(client: &mut DuplexStream) {
    let mut byte = [0];
    let mut reading = Box::pin(client.read(&mut byte));
    assert_eq!(ready(reading.as_mut()).await.unwrap(), 0);
}

fn setup(config: &ServerConfig) -> (watch::Sender<bool>, DbHandle, worker::Worker) {
    let (stop, shutdown) = watch::channel(false);
    let (database, owner) = worker::channel(
        config.worker_queue_capacity,
        config.request_timeout,
        shutdown,
    )
    .unwrap();
    (stop, database, owner)
}

fn get_later() -> Command {
    Command::Get {
        key: Bytes::from_static(b"later"),
    }
}

struct ReadBudgetProbe {
    stream: DuplexStream,
    capacities: Arc<Mutex<Vec<usize>>>,
}

impl AsyncRead for ReadBudgetProbe {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.capacities.lock().unwrap().push(buffer.remaining());
        Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for ReadBudgetProbe {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }
}

#[tokio::test(start_paused = true)]
async fn incomplete_frame_deadline_starts_at_first_byte_and_does_not_renew() {
    guard(async {
        let config = ServerConfig::default();
        let (stop, database, _owner) = setup(&config);
        let (server, mut client) = duplex(64);
        let mut connection = Box::pin(run(server, config, database, stop.subscribe()));
        let began = Instant::now();

        feed(&mut client, connection.as_mut(), b"*").await;
        advance(Duration::from_secs(6)).await;
        feed(&mut client, connection.as_mut(), b"1\r").await;
        advance(Duration::from_secs(4)).await;
        assert!(matches!(
            ready(connection.as_mut()).await,
            Err(ConnectionError::FrameTimeout)
        ));
        assert_eq!(Instant::now() - began, Duration::from_secs(10));
        assert_eof(&mut client).await;
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn idle_connection_has_no_frame_deadline_before_its_first_byte() {
    guard(async {
        let config = ServerConfig::default();
        let (stop, database, owner) = setup(&config);
        let (server, mut client) = duplex(64);
        let mut connection = Box::pin(run(server, config, database, stop.subscribe()));
        let mut owner = Box::pin(owner.run());

        assert_pending(connection.as_mut()).await;
        advance(Duration::from_secs(100)).await;
        assert_pending(connection.as_mut()).await;
        feed(&mut client, connection.as_mut(), PING).await;
        assert_pending(owner.as_mut()).await;
        assert_pending(connection.as_mut()).await;
        read_exact_ready(&mut client, b"+PONG\r\n").await;
        stop.send(true).unwrap();
        ready(connection.as_mut()).await.unwrap();
        ready(owner.as_mut()).await;
        assert_eof(&mut client).await;
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn partial_pipeline_suffix_keeps_its_age_while_previous_reply_waits() {
    guard(async {
        let config = ServerConfig {
            request_timeout: Duration::from_secs(20),
            ..ServerConfig::default()
        };
        let (stop, database, owner) = setup(&config);
        let (server, mut client) = duplex(64);
        let mut connection = Box::pin(run(server, config, database, stop.subscribe()));
        let mut owner = Box::pin(owner.run());
        let request = [PING, b"*"].concat();

        feed(&mut client, connection.as_mut(), &request).await;
        advance(Duration::from_secs(11)).await;
        assert_pending(owner.as_mut()).await;
        assert!(matches!(
            ready(connection.as_mut()).await,
            Err(ConnectionError::FrameTimeout)
        ));
        read_exact_ready(&mut client, b"+PONG\r\n").await;
        assert_eof(&mut client).await;
        stop.send(true).unwrap();
        ready(owner.as_mut()).await;
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn complete_pipeline_suffix_does_not_expire_while_previous_reply_waits() {
    guard(async {
        let config = ServerConfig {
            request_timeout: Duration::from_secs(20),
            ..ServerConfig::default()
        };
        let (stop, database, owner) = setup(&config);
        let (server, mut client) = duplex(64);
        let mut connection = Box::pin(run(server, config, database, stop.subscribe()));
        let mut owner = Box::pin(owner.run());

        feed(&mut client, connection.as_mut(), &[PING, PING].concat()).await;
        advance(Duration::from_secs(11)).await;
        assert_pending(owner.as_mut()).await;
        assert_pending(connection.as_mut()).await;
        assert_pending(owner.as_mut()).await;
        assert_pending(connection.as_mut()).await;
        read_exact_ready(&mut client, b"+PONG\r\n+PONG\r\n").await;
        stop.send(true).unwrap();
        ready(connection.as_mut()).await.unwrap();
        ready(owner.as_mut()).await;
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn next_partial_frame_uses_its_own_read_time_not_the_previous_frame_start() {
    guard(async {
        let config = ServerConfig::default();
        let (stop, database, owner) = setup(&config);
        let (server, mut client) = duplex(64);
        let mut connection = Box::pin(run(server, config, database, stop.subscribe()));
        let mut owner = Box::pin(owner.run());

        feed(&mut client, connection.as_mut(), b"*1\r\n$4\r\nPI").await;
        advance(Duration::from_secs(9)).await;
        feed(&mut client, connection.as_mut(), b"NG\r\n*1\r\n$4\r\nPI").await;
        assert_pending(owner.as_mut()).await;
        assert_pending(connection.as_mut()).await;
        read_exact_ready(&mut client, b"+PONG\r\n").await;
        advance(Duration::from_secs(8)).await;
        feed(&mut client, connection.as_mut(), b"NG\r\n").await;
        assert_pending(owner.as_mut()).await;
        assert_pending(connection.as_mut()).await;
        read_exact_ready(&mut client, b"+PONG\r\n").await;
        stop.send(true).unwrap();
        ready(connection.as_mut()).await.unwrap();
        ready(owner.as_mut()).await;
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn write_timeout_closes_connection_without_dispatching_the_next_set() {
    guard(async {
        let config = ServerConfig::default();
        let (stop, database, owner) = setup(&config);
        let observer = database.clone();
        let (server, mut client) = duplex(64);
        let mut connection = Box::pin(run(server, config, database, stop.subscribe()));
        let mut owner = Box::pin(owner.run());
        let request = [
            b"*2\r\n$4\r\nECHO\r\n$512\r\n".as_slice(),
            &[b'x'; 512],
            b"\r\n",
        ]
        .concat();

        feed(&mut client, connection.as_mut(), &request).await;
        // Já existe outro pedido no socket, mas a primeira resposta é maior que
        // o duplex e o cliente não a lê antes do prazo.
        let mut later = Box::pin(client.write_all(LATER_SET));
        ready(later.as_mut()).await.unwrap();
        drop(later);
        assert_pending(owner.as_mut()).await;
        assert_pending(connection.as_mut()).await;
        advance(Duration::from_secs(5)).await;
        assert!(matches!(
            ready(connection.as_mut()).await,
            Err(ConnectionError::WriteTimeout)
        ));

        let mut observed = Box::pin(observer.execute(get_later()));
        assert_pending(observed.as_mut()).await;
        assert_pending(owner.as_mut()).await;
        assert_eq!(ready(observed.as_mut()).await, Ok(Reply::Bulk(None)));
        let mut partial = Vec::new();
        let mut reading = Box::pin(client.read_to_end(&mut partial));
        assert_eq!(ready(reading.as_mut()).await.unwrap(), 64);
        drop(reading);
        assert!(partial.starts_with(b"$512\r\n"));
        stop.send(true).unwrap();
        ready(owner.as_mut()).await;
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn partial_pipeline_suffix_keeps_its_age_during_a_blocked_write() {
    guard(async {
        let config = ServerConfig {
            frame_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(20),
            ..ServerConfig::default()
        };
        let (stop, database, owner) = setup(&config);
        let (server, mut client) = duplex(64);
        let mut connection = Box::pin(run(server, config, database, stop.subscribe()));
        let mut owner = Box::pin(owner.run());
        let payload = [b'x'; 512];
        let request = [
            b"*2\r\n$4\r\nECHO\r\n$512\r\n".as_slice(),
            &payload,
            b"\r\n*",
        ]
        .concat();
        let expected = [b"$512\r\n".as_slice(), &payload, b"\r\n"].concat();

        feed(&mut client, connection.as_mut(), &request).await;
        assert_pending(owner.as_mut()).await;
        assert_pending(connection.as_mut()).await;
        advance(Duration::from_secs(6)).await;

        let mut actual = Vec::new();
        let mut finished = false;
        for _ in 0..expected.len() {
            let mut buffer = [0; 64];
            let mut reading = Box::pin(client.read(&mut buffer));
            let count = ready(reading.as_mut()).await.unwrap();
            assert!(count > 0);
            drop(reading);
            actual.extend_from_slice(&buffer[..count]);
            match poll_once(connection.as_mut()).await {
                Poll::Pending => continue,
                Poll::Ready(result) => {
                    assert!(matches!(result, Err(ConnectionError::FrameTimeout)));
                    finished = true;
                    break;
                }
            }
        }
        assert!(
            finished,
            "partial suffix should expire after the full response"
        );
        let mut reading = Box::pin(client.read_to_end(&mut actual));
        ready(reading.as_mut()).await.unwrap();
        drop(reading);
        assert_eq!(actual, expected);
        stop.send(true).unwrap();
        ready(owner.as_mut()).await;
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn accepted_request_delivers_its_response_after_shutdown() {
    guard(async {
        let config = ServerConfig::default();
        let (stop, database, owner) = setup(&config);
        let (server, mut client) = duplex(128);
        let mut connection = Box::pin(run(server, config, database, stop.subscribe()));

        feed(
            &mut client,
            connection.as_mut(),
            &[LATER_SET, PING].concat(),
        )
        .await;
        stop.send(true).unwrap();
        let mut owner = Box::pin(owner.run());
        ready(owner.as_mut()).await;
        ready(connection.as_mut()).await.unwrap();
        read_exact_ready(&mut client, b"+OK\r\n").await;
        assert_eof(&mut client).await;
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_during_a_blocked_write_delivers_the_complete_accepted_response() {
    guard(async {
        let config = ServerConfig::default();
        let (stop, database, owner) = setup(&config);
        let (server, mut client) = duplex(64);
        let mut connection = Box::pin(run(server, config, database, stop.subscribe()));
        let mut owner = Box::pin(owner.run());
        let payload = [b'x'; 512];
        let request = [
            b"*2\r\n$4\r\nECHO\r\n$512\r\n".as_slice(),
            &payload,
            b"\r\n",
        ]
        .concat();
        let expected = [b"$512\r\n".as_slice(), &payload, b"\r\n"].concat();

        feed(&mut client, connection.as_mut(), &request).await;
        assert_pending(owner.as_mut()).await;
        assert_pending(connection.as_mut()).await;
        // A resposta já começou e não cabe no duplex. A parada não deve abortar
        // essa escrita nem despachar outro comando depois de terminá-la.
        stop.send(true).unwrap();
        assert_pending(connection.as_mut()).await;
        ready(owner.as_mut()).await;
        let mut actual = Vec::new();
        let mut finished = false;
        for _ in 0..expected.len() {
            let mut buffer = [0; 64];
            let mut reading = Box::pin(client.read(&mut buffer));
            let count = ready(reading.as_mut()).await.unwrap();
            assert!(count > 0);
            drop(reading);
            actual.extend_from_slice(&buffer[..count]);
            match poll_once(connection.as_mut()).await {
                Poll::Pending => continue,
                Poll::Ready(result) => {
                    result.unwrap();
                    finished = true;
                    break;
                }
            }
        }
        assert!(
            finished,
            "shutdown should finish after the accepted response"
        );
        let mut reading = Box::pin(client.read_to_end(&mut actual));
        ready(reading.as_mut()).await.unwrap();
        drop(reading);
        assert_eq!(actual, expected);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn every_read_is_bounded_by_the_remaining_input_budget() {
    guard(async {
        let config = ServerConfig {
            resp_limits: RespLimits {
                max_frame_bytes: 64,
                max_bulk_bytes: 16,
                max_line_bytes: 16,
                ..RespLimits::default()
            },
            max_input_buffer_bytes: 64,
            max_response_bytes: 128,
            ..ServerConfig::default()
        };
        let (stop, database, owner) = setup(&config);
        let (server, mut client) = duplex(128);
        let capacities = Arc::new(Mutex::new(Vec::new()));
        let server = ReadBudgetProbe {
            stream: server,
            capacities: Arc::clone(&capacities),
        };
        let mut connection = Box::pin(run(server, config, database, stop.subscribe()));
        let mut owner = Box::pin(owner.run());
        let prefix = b"*2\r\n$4\r\nECHO\r\n$16\r\n";

        feed(&mut client, connection.as_mut(), &prefix[..1]).await;
        assert_eq!(capacities.lock().unwrap().first(), Some(&64));
        assert_eq!(capacities.lock().unwrap().last(), Some(&63));
        feed(&mut client, connection.as_mut(), &prefix[1..]).await;
        assert_eq!(
            capacities.lock().unwrap().last(),
            Some(&(64 - prefix.len()))
        );
        feed(&mut client, connection.as_mut(), b"abcdefghijklmnop\r\n").await;
        assert_pending(owner.as_mut()).await;
        assert_pending(connection.as_mut()).await;
        read_exact_ready(&mut client, b"$16\r\nabcdefghijklmnop\r\n").await;
        assert_eq!(capacities.lock().unwrap().last(), Some(&64));
        assert!(
            capacities
                .lock()
                .unwrap()
                .iter()
                .all(|capacity| (1..=64).contains(capacity))
        );
        stop.send(true).unwrap();
        ready(connection.as_mut()).await.unwrap();
        ready(owner.as_mut()).await;
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn unavailable_worker_closes_connection_without_a_success_response() {
    guard(async {
        let config = ServerConfig::default();
        let (stop, database, owner) = setup(&config);
        drop(owner);
        let (server, mut client) = duplex(64);
        let mut connection = Box::pin(run(server, config, database, stop.subscribe()));
        let mut writing = Box::pin(client.write_all(PING));
        ready(writing.as_mut()).await.unwrap();
        drop(writing);

        assert!(matches!(
            ready(connection.as_mut()).await,
            Err(ConnectionError::Database(DbError::Unavailable))
        ));
        assert_eof(&mut client).await;
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn request_timeout_closes_connection_without_dispatching_a_later_set() {
    guard(async {
        let config = ServerConfig::default();
        let (stop, database, owner) = setup(&config);
        let observer = database.clone();
        let (server, mut client) = duplex(128);
        let mut connection = Box::pin(run(server, config, database, stop.subscribe()));
        feed(
            &mut client,
            connection.as_mut(),
            &[PING, LATER_SET].concat(),
        )
        .await;

        advance(Duration::from_secs(5)).await;
        assert!(matches!(
            ready(connection.as_mut()).await,
            Err(ConnectionError::Database(DbError::Timeout))
        ));
        assert_eof(&mut client).await;
        let mut owner = Box::pin(owner.run());
        assert_pending(owner.as_mut()).await;
        let mut observed = Box::pin(observer.execute(get_later()));
        assert_pending(observed.as_mut()).await;
        assert_pending(owner.as_mut()).await;
        assert_eq!(ready(observed.as_mut()).await, Ok(Reply::Bulk(None)));
        stop.send(true).unwrap();
        ready(owner.as_mut()).await;
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_cancels_a_send_waiting_behind_a_full_worker_queue() {
    guard(async {
        let config = ServerConfig {
            worker_queue_capacity: 1,
            ..ServerConfig::default()
        };
        let (stop, database, owner) = setup(&config);
        let observer = database.clone();
        let mut accepted = Box::pin(observer.execute(Command::Ping(None)));
        assert_pending(accepted.as_mut()).await;
        let (server, mut client) = duplex(128);
        let mut connection = Box::pin(run(server, config, database, stop.subscribe()));
        feed(&mut client, connection.as_mut(), LATER_SET).await;

        // O worker ainda não foi polled, portanto a única vaga continua ocupada
        // pelo PING. A conexão deve terminar sem esperar capacidade ou resposta.
        stop.send(true).unwrap();
        ready(connection.as_mut()).await.unwrap();
        assert_eof(&mut client).await;
        assert_pending(accepted.as_mut()).await;
        let mut owner = Box::pin(owner.run());
        ready(owner.as_mut()).await;
        assert_eq!(ready(accepted.as_mut()).await, Ok(Reply::Pong));
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn response_encoding_uses_output_limits_independent_of_input_line_limits() {
    guard(async {
        let config = ServerConfig {
            resp_limits: RespLimits {
                max_line_bytes: 3,
                ..RespLimits::default()
            },
            ..ServerConfig::default()
        };
        let (mut server, mut client) = duplex(128);
        let mut output = BytesMut::new();
        let mut writing = Box::pin(write_response(
            &mut server,
            &mut output,
            Frame::Error(Bytes::from_static(b"ERR invalid protocol")),
            &config,
        ));
        ready(writing.as_mut()).await.unwrap();
        drop(writing);
        read_exact_ready(&mut client, b"-ERR invalid protocol\r\n").await;
    })
    .await;
}
