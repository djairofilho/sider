use super::*;
use crate::{
    command::{self, Command, Reply, SetExpiry, SetOptions},
    pubsub::{Hub, Message},
    resp::{Decoder, RespLimits, encode},
    storage::{Store, StoreConfig, SystemClock, worker},
};
use bytes::BytesMut;
use std::{collections::BTreeMap, future::poll_fn, task::Poll, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
};

fn fields(bytes: &[u8]) -> BTreeMap<String, String> {
    std::str::from_utf8(bytes)
        .unwrap()
        .lines()
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}
fn metric(metrics: &Metrics, name: &str) -> u64 {
    fields(&metrics.render(InfoSections::ALL))[name]
        .parse()
        .unwrap()
}
fn request(args: &[&[u8]]) -> BytesMut {
    let mut out = BytesMut::new();
    encode(
        &Frame::Array(Some(
            args.iter()
                .map(|arg| Frame::Bulk(Some(Bytes::copy_from_slice(arg))))
                .collect(),
        )),
        &mut out,
        RespLimits::default(),
    )
    .unwrap();
    out
}
async fn read_frame(stream: &mut tokio::io::DuplexStream) -> Frame {
    let mut decoder = Decoder::new(RespLimits::default()).unwrap();
    let mut input = BytesMut::new();
    loop {
        if let Some(frame) = decoder.decode(&mut input).unwrap() {
            return frame;
        }
        assert_ne!(stream.read_buf(&mut input).await.unwrap(), 0);
    }
}

#[test]
fn metrics_concurrent_connections_and_counters_have_fixed_cardinality() {
    let metrics = Metrics::default();
    let names: Vec<_> = fields(&metrics.render(InfoSections::ALL))
        .into_keys()
        .collect();
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let metrics = &metrics;
            scope.spawn(move || {
                for _ in 0..1000 {
                    let _guard = metrics.connection();
                    metrics.add(Counter::Requests, 1);
                }
            });
        }
    });
    assert_eq!(metric(&metrics, "connected_clients"), 0);
    assert_eq!(metric(&metrics, "total_connections_received"), 8000);
    assert_eq!(metric(&metrics, "commands_received_total"), 8000);
    assert_eq!(
        fields(&metrics.render(InfoSections::ALL))
            .into_keys()
            .collect::<Vec<_>>(),
        names
    );
    metrics.add(Counter::Requests, u64::MAX);
    assert_eq!(metric(&metrics, "commands_received_total"), u64::MAX);
}

#[tokio::test]
async fn metrics_info_honors_response_limit_without_partial_output() {
    let (stop, shutdown) = watch::channel(false);
    let (database, _owner) = worker::channel(1, Duration::from_secs(5), shutdown).unwrap();
    let config = ServerConfig {
        max_response_bytes: 128,
        resp_limits: RespLimits {
            max_bulk_bytes: 32,
            ..RespLimits::default()
        },
        ..ServerConfig::default()
    };
    let (mut client, stream) = tokio::io::duplex(8192);
    let connection = tokio::spawn(crate::connection::run(
        stream,
        config,
        database.clone(),
        stop.subscribe(),
    ));
    client.write_all(&request(&[b"INFO"])).await.unwrap();
    let mut bytes = Vec::new();
    client.read_to_end(&mut bytes).await.unwrap();
    assert!(bytes.is_empty());
    assert!(matches!(
        connection.await.unwrap(),
        Err(crate::connection::ConnectionError::Encode(_))
    ));
    assert_eq!(
        metric(&database.metrics, "response_encoding_failures_total"),
        1
    );
    assert_eq!(metric(&database.metrics, "connected_clients"), 0);
}

#[tokio::test(start_paused = true)]
async fn metrics_slow_client_timeout_and_cancellation_release_connection_gauge() {
    let (stop, shutdown) = watch::channel(false);
    let (database, _owner) = worker::channel(1, Duration::from_secs(5), shutdown).unwrap();
    let (mut client, stream) = tokio::io::duplex(64);
    let connection = tokio::spawn(crate::connection::run(
        stream,
        ServerConfig::default(),
        database.clone(),
        stop.subscribe(),
    ));
    client.write_all(&request(&[b"INFO"])).await.unwrap();
    assert!(matches!(
        connection.await.unwrap(),
        Err(crate::connection::ConnectionError::WriteTimeout)
    ));
    assert_eq!(metric(&database.metrics, "client_write_timeouts_total"), 1);
    assert_eq!(metric(&database.metrics, "connected_clients"), 0);
    let (mut client, stream) = tokio::io::duplex(8192);
    let connection = tokio::spawn(crate::connection::run(
        stream,
        ServerConfig::default(),
        database.clone(),
        stop.subscribe(),
    ));
    client.write_all(&request(&[b"INFO"])).await.unwrap();
    read_frame(&mut client).await;
    assert_eq!(metric(&database.metrics, "connected_clients"), 1);
    connection.abort();
    assert!(connection.await.unwrap_err().is_cancelled());
    assert_eq!(metric(&database.metrics, "connected_clients"), 0);
}

#[test]
fn metrics_info_parser_filters_sections_without_retaining_binary_labels() {
    let decode = |args: &[&[u8]]| {
        let frame = Decoder::new(RespLimits::default())
            .unwrap()
            .decode(&mut request(args))
            .unwrap()
            .unwrap();
        let Command::Info(sections) = command::parse(frame).unwrap() else {
            panic!()
        };
        sections
    };
    assert_eq!(decode(&[b"INFO"]), InfoSections::ALL);
    assert_eq!(decode(&[b"info", b"all", b"everything"]), InfoSections::ALL);
    let selected = decode(&[b"iNfO", b"MeMoRy", b"memory", b"\xff\0secret"]);
    let output = Metrics::default().render(selected);
    assert!(output.starts_with(b"# Memory\r\n"));
    assert!(!output.windows(6).any(|bytes| bytes == b"secret"));
    assert!(!selected.contains(InfoSections::SERVER));
    assert!(
        Metrics::default()
            .render(decode(&[b"INFO", b"\xffsecret"]))
            .is_empty()
    );
    assert_eq!(
        Store::new().execute(Command::Info(selected)),
        Reply::Error(command::ExecutionError::ConnectionOnly)
    );
}

#[tokio::test(start_paused = true)]
async fn metrics_info_remains_available_with_a_saturated_worker_and_records_timeout() {
    let (stop, shutdown) = watch::channel(false);
    let (database, owner) = worker::channel(1, Duration::from_secs(5), shutdown).unwrap();
    let mut waiting = Box::pin(database.execute(Command::Ping(None)));
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let (mut client, stream) = tokio::io::duplex(8192);
    let connection = tokio::spawn(crate::connection::run(
        stream,
        ServerConfig::default(),
        database.clone(),
        stop.subscribe(),
    ));
    client
        .write_all(&request(&[b"INFO", b"stats"]))
        .await
        .unwrap();
    let Frame::Bulk(Some(info)) = read_frame(&mut client).await else {
        panic!()
    };
    let observed = fields(&info);
    assert_eq!(observed["worker_queue_used"], "1");
    assert_eq!(observed["worker_queue_capacity"], "1");
    assert_eq!(observed["worker_requests_accepted_total"], "1");
    assert_eq!(observed["commands_received_total"], "1");
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(waiting.await, Err(worker::DbError::Timeout));
    assert_eq!(metric(&database.metrics, "worker_timeouts_total"), 1);
    drop(client);
    connection.await.unwrap().unwrap();
    assert_eq!(metric(&database.metrics, "connected_clients"), 0);
    drop(owner);
    assert_eq!(
        database.execute(Command::Ping(None)).await,
        Err(worker::DbError::Unavailable)
    );
    assert_eq!(metric(&database.metrics, "worker_failures_total"), 1);
    assert_eq!(
        fields(&database.info(InfoSections::ALL))["worker_queue_used"],
        "0"
    );
}

#[tokio::test(start_paused = true)]
async fn metrics_dataset_tracks_committed_quota_and_expiration() {
    let (stop, shutdown) = watch::channel(false);
    let store = Store::with_config(
        StoreConfig {
            max_dataset_bytes: 300,
        },
        Arc::new(SystemClock),
    )
    .unwrap();
    let (database, owner) =
        worker::channel_with_store(4, Duration::from_secs(5), shutdown, store).unwrap();
    let worker = tokio::spawn(owner.run());
    assert_eq!(
        database
            .execute(Command::SetWithOptions {
                key: Bytes::from_static(b"secret-key"),
                value: Bytes::from_static(b"secret-value"),
                options: SetOptions {
                    expiry: SetExpiry::After(Duration::from_millis(10)),
                    ..SetOptions::default()
                }
            })
            .await
            .unwrap(),
        Reply::Ok
    );
    let committed = fields(&database.info(InfoSections::ALL));
    assert_eq!(committed["dataset_keys"], "1");
    assert_eq!(committed["dataset_expiring_keys"], "1");
    assert_eq!(committed["dataset_quota_bytes"], "300");
    assert_eq!(
        database
            .execute(Command::Set {
                key: Bytes::from_static(b"too-large"),
                value: Bytes::from(vec![1; 500])
            })
            .await
            .unwrap(),
        Reply::Error(command::ExecutionError::OutOfMemory)
    );
    assert_eq!(
        fields(&database.info(InfoSections::ALL))["dataset_logical_bytes"],
        committed["dataset_logical_bytes"]
    );
    tokio::time::advance(Duration::from_millis(100)).await;
    database.execute(Command::Ping(None)).await.unwrap();
    let expired = fields(&database.info(InfoSections::ALL));
    assert_eq!(expired["dataset_keys"], "0");
    assert_eq!(expired["dataset_expiring_keys"], "0");
    assert_eq!(expired["dataset_logical_bytes"], "0");
    assert_eq!(expired["expiration_batches_total"], "1");
    assert_eq!(expired["expiration_batch_keys_removed_total"], "1");
    stop.send_replace(true);
    worker.await.unwrap();
}

#[test]
fn metrics_pubsub_counts_delivery_eviction_and_raii_cleanup_without_labels() {
    let metrics = Metrics::default();
    let hub = Hub::with_metrics(metrics.clone());
    let mut slow = hub.connect(2, 1).unwrap();
    let mut fast = hub.connect(2, 1).unwrap();
    let channel = Bytes::from_static(b"\xff\0secret-channel");
    slow.subscribe(vec![channel.clone(), channel.clone()])
        .unwrap();
    fast.subscribe(vec![channel.clone()]).unwrap();
    assert_eq!(metric(&metrics, "pubsub_channels"), 1);
    assert_eq!(metric(&metrics, "pubsub_subscribers"), 2);
    assert_eq!(metric(&metrics, "pubsub_subscriptions"), 2);
    let message = Message {
        channel,
        payload: Bytes::from_static(b"secret-payload"),
    };
    assert_eq!(hub.publish(message.clone()), 2);
    fast.try_message().unwrap();
    assert_eq!(hub.publish(message), 1);
    assert!(slow.is_evicted());
    assert_eq!(metric(&metrics, "pubsub_deliveries_total"), 3);
    assert_eq!(metric(&metrics, "pubsub_evictions_total"), 1);
    assert_eq!(metric(&metrics, "pubsub_subscribers"), 1);
    fast.unsubscribe(vec![]).unwrap();
    drop((slow, fast));
    assert_eq!(metric(&metrics, "pubsub_channels"), 0);
    assert_eq!(metric(&metrics, "pubsub_subscriptions"), 0);
    assert!(
        !metrics
            .render(InfoSections::ALL)
            .windows(6)
            .any(|bytes| bytes == b"secret")
    );
}

#[tokio::test]
async fn metrics_info_exec_observes_applied_state_and_error_counts_once() {
    let (stop, shutdown) = watch::channel(false);
    let (database, owner) = worker::channel(4, Duration::from_secs(5), shutdown).unwrap();
    let worker = tokio::spawn(owner.run());
    let (mut client, stream) = tokio::io::duplex(8192);
    let connection = tokio::spawn(crate::connection::run(
        stream,
        ServerConfig::default(),
        database.clone(),
        stop.subscribe(),
    ));
    for (args, expected) in [
        (vec![b"MULTI".as_slice()], b"OK".as_slice()),
        (vec![b"SET", b"key", b"not-integer"], b"QUEUED"),
        (vec![b"INCR", b"key"], b"QUEUED"),
        (vec![b"INFO", b"memory"], b"QUEUED"),
    ] {
        client.write_all(&request(&args)).await.unwrap();
        assert_eq!(
            read_frame(&mut client).await,
            Frame::Simple(Bytes::copy_from_slice(expected))
        );
    }
    client.write_all(&request(&[b"EXEC"])).await.unwrap();
    let Frame::Array(Some(replies)) = read_frame(&mut client).await else {
        panic!()
    };
    assert!(matches!(&replies[1], Frame::Error(_)));
    let Frame::Bulk(Some(info)) = &replies[2] else {
        panic!()
    };
    assert_eq!(fields(info)["dataset_keys"], "1");
    client
        .write_all(&request(&[b"INFO", b"stats"]))
        .await
        .unwrap();
    let Frame::Bulk(Some(info)) = read_frame(&mut client).await else {
        panic!()
    };
    assert_eq!(fields(&info)["commands_received_total"], "6");
    assert_eq!(fields(&info)["command_error_replies_total"], "1");
    assert_eq!(fields(&info)["worker_requests_accepted_total"], "1");
    client.write_all(b"!secret-invalid\r\n").await.unwrap();
    assert!(matches!(read_frame(&mut client).await, Frame::Error(_)));
    assert!(connection.await.unwrap().is_err());
    assert_eq!(metric(&database.metrics, "protocol_errors_total"), 1);
    assert_eq!(metric(&database.metrics, "connection_failures_total"), 1);
    assert_eq!(metric(&database.metrics, "connected_clients"), 0);
    stop.send_replace(true);
    worker.await.unwrap();
}
