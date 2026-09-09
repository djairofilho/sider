#![forbid(unsafe_code)]

//! Integração TCP com bytes literais, sem usar o codec do Sider como oráculo.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use sider::ServerConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

#[path = "common/resp_fixtures.rs"]
mod resp_fixtures;

const IO_DEADLINE: Duration = Duration::from_secs(5);
const STOP_DEADLINE: Duration = Duration::from_secs(10);
const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const PONG: &[u8] = b"+PONG\r\n";

struct TestServer {
    address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl TestServer {
    async fn start(config: ServerConfig) -> Self {
        let listener = timeout(IO_DEADLINE, TcpListener::bind("127.0.0.1:0"))
            .await
            .expect("listener bind deadline")
            .expect("ephemeral loopback listener");
        let address = listener.local_addr().expect("listener address");
        let (shutdown, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            sider::server::serve(listener, config, async move {
                let _ = stopped.await;
            })
            .await
            .expect("server shutdown without error");
        });
        Self {
            address,
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    async fn connect(&self) -> TcpStream {
        let stream = timeout(IO_DEADLINE, TcpStream::connect(self.address))
            .await
            .expect("connect deadline")
            .expect("connect to test server");
        stream.set_nodelay(true).expect("set TCP_NODELAY");
        stream
    }

    async fn stop(mut self) {
        self.shutdown
            .take()
            .expect("shutdown sender")
            .send(())
            .expect("server still running");
        timeout(STOP_DEADLINE, self.task.as_mut().expect("server task"))
            .await
            .expect("server shutdown deadline")
            .expect("server task completed without panic");
        self.task.take();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Falhas dos testes não deixam o listener ou tarefas órfãs no runtime.
        self.shutdown.take();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn write(stream: &mut TcpStream, bytes: &[u8]) {
    timeout(IO_DEADLINE, stream.write_all(bytes))
        .await
        .expect("write deadline")
        .expect("write request bytes");
}

async fn expect_bytes(stream: &mut TcpStream, expected: &[u8]) {
    let mut actual = vec![0; expected.len()];
    timeout(IO_DEADLINE, stream.read_exact(&mut actual))
        .await
        .expect("response deadline")
        .expect("complete response bytes");
    assert_eq!(actual, expected);
}

async fn exchange(stream: &mut TcpStream, request: &[u8], expected: &[u8]) {
    write(stream, request).await;
    expect_bytes(stream, expected).await;
}

#[tokio::test]
async fn shard_routing_rejects_crossing_and_preserves_binary_hash_tag_batches() {
    let server = TestServer::start(ServerConfig {
        shards: 2,
        ..ServerConfig::default()
    })
    .await;
    let mut stream = server.connect().await;
    exchange(
        &mut stream,
        b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n",
        b"+OK\r\n",
    )
    .await;
    exchange(
        &mut stream,
        b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n",
        b"+OK\r\n",
    )
    .await;
    for command in [b"MGET".as_slice(), b"MSET", b"DEL", b"EXISTS"] {
        let mut request = if command == b"MSET" {
            b"*5\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\nx\r\n$1\r\nb\r\n$1\r\ny\r\n".to_vec()
        } else {
            format!("*3\r\n${}\r\n", command.len()).into_bytes()
        };
        if command != b"MSET" {
            request.extend_from_slice(command);
            request.extend_from_slice(b"\r\n$1\r\na\r\n$1\r\nb\r\n");
        }
        exchange(
            &mut stream,
            &request,
            b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
        )
        .await;
    }
    exchange(
        &mut stream,
        b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n*2\r\n$3\r\nGET\r\n$1\r\nb\r\n",
        b"$1\r\n1\r\n$1\r\n2\r\n",
    )
    .await;
    exchange(
        &mut stream,
        b"*5\r\n$4\r\nMSET\r\n$4\r\n{\xff}a\r\n$1\r\nx\r\n$4\r\n{\xff}b\r\n$0\r\n\r\n",
        b"+OK\r\n",
    )
    .await;
    exchange(
        &mut stream,
        b"*4\r\n$4\r\nMGET\r\n$4\r\n{\xff}b\r\n$4\r\n{\xff}a\r\n$4\r\n{\xff}a\r\n",
        b"*3\r\n$0\r\n\r\n$1\r\nx\r\n$1\r\nx\r\n",
    )
    .await;
    drop(stream);
    server.stop().await;
}

#[tokio::test]
async fn quota_is_partitioned_without_borrowing_other_shards_budget() {
    let server = TestServer::start(ServerConfig {
        shards: 2,
        max_dataset_bytes: 260,
        ..ServerConfig::default()
    })
    .await;
    let mut stream = server.connect().await;
    exchange(
        &mut stream,
        b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$2\r\n12\r\n",
        b"-OOM dataset memory quota exceeded\r\n",
    )
    .await;
    exchange(
        &mut stream,
        b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n",
        b"+OK\r\n+OK\r\n",
    )
    .await;
    exchange(
        &mut stream,
        b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n*2\r\n$3\r\nGET\r\n$1\r\nb\r\n",
        b"$1\r\n1\r\n$1\r\n2\r\n",
    )
    .await;
    drop(stream);
    server.stop().await;
}

async fn half_close(stream: &mut TcpStream) {
    timeout(IO_DEADLINE, stream.shutdown())
        .await
        .expect("write-half shutdown deadline")
        .expect("write-half shutdown");
}

fn is_connection_closed(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
    )
}

async fn read_until_closed(stream: &mut TcpStream) -> Vec<u8> {
    timeout(IO_DEADLINE, async {
        let mut collected = Vec::new();
        let mut chunk = [0; 256];
        loop {
            match stream.read(&mut chunk).await {
                Ok(0) => return collected,
                Ok(count) => {
                    collected.extend_from_slice(&chunk[..count]);
                    assert!(collected.len() <= 1024, "unexpected response after close");
                }
                // Fechar com bytes não lidos pode produzir reset, especialmente
                // no Windows. Isso não equivale a aceitar erros de I/O genéricos.
                Err(error) if is_connection_closed(&error) => return collected,
                Err(error) => panic!("read after close: {error}"),
            }
        }
    })
    .await
    .expect("connection close deadline")
}

fn arguments(args: &[&[u8]]) -> Vec<u8> {
    let mut output = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        output.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        output.extend_from_slice(arg);
        output.extend_from_slice(b"\r\n");
    }
    output
}

#[tokio::test]
async fn r05_list_response_limit_closes_without_partial_array_or_mutation() {
    let server = TestServer::start(ServerConfig {
        resp_limits: sider::resp::RespLimits {
            max_frame_bytes: 256,
            max_bulk_bytes: 64,
            max_line_bytes: 64,
            max_nodes: 8,
            max_depth: 2,
        },
        max_input_buffer_bytes: 256,
        max_response_bytes: 128,
        ..ServerConfig::default()
    })
    .await;
    let mut client = server.connect().await;
    exchange(
        &mut client,
        &arguments(&[b"RPUSH", b"{r05}list", &[b'x'; 55], &[b'x'; 55]]),
        b":2\r\n",
    )
    .await;
    let mut expected = b"*2\r\n".to_vec();
    for _ in 0..2 {
        expected.extend_from_slice(b"$55\r\n");
        expected.extend_from_slice(&[b'x'; 55]);
        expected.extend_from_slice(b"\r\n");
    }
    assert_eq!(expected.len(), 128);
    exchange(
        &mut client,
        &arguments(&[b"LRANGE", b"{r05}list", b"0", b"-1"]),
        &expected,
    )
    .await;
    exchange(
        &mut client,
        &arguments(&[b"RPUSH", b"{r05}list", b""]),
        b":3\r\n",
    )
    .await;
    write(
        &mut client,
        &arguments(&[b"LRANGE", b"{r05}list", b"0", b"-1"]),
    )
    .await;
    assert!(read_until_closed(&mut client).await.is_empty());
    let mut other = server.connect().await;
    exchange(&mut other, &arguments(&[b"LLEN", b"{r05}list"]), b":3\r\n").await;
    exchange(&mut other, PING, PONG).await;
    server.stop().await;
}

#[tokio::test]
async fn r02_mget_exact_response_limit_and_one_byte_over_close_without_partial_reply() {
    let server = TestServer::start(ServerConfig {
        resp_limits: sider::resp::RespLimits {
            max_frame_bytes: 256,
            max_bulk_bytes: 64,
            max_line_bytes: 64,
            max_nodes: 8,
            max_depth: 2,
        },
        max_input_buffer_bytes: 256,
        max_response_bytes: 128,
        ..ServerConfig::default()
    })
    .await;
    let mut client = server.connect().await;
    for key in [b"a".as_slice(), b"b"] {
        exchange(
            &mut client,
            &arguments(&[b"SET", key, &[b'x'; 55]]),
            b"+OK\r\n",
        )
        .await;
    }
    let mut expected = b"*2\r\n".to_vec();
    for _ in 0..2 {
        expected.extend_from_slice(b"$55\r\n");
        expected.extend_from_slice(&[b'x'; 55]);
        expected.extend_from_slice(b"\r\n");
    }
    assert_eq!(expected.len(), 128);
    exchange(&mut client, &arguments(&[b"MGET", b"a", b"b"]), &expected).await;
    exchange(
        &mut client,
        &arguments(&[b"SET", b"a", &[b'x'; 56]]),
        b"+OK\r\n",
    )
    .await;
    write(&mut client, &arguments(&[b"MGET", b"a", b"b"])).await;
    assert!(read_until_closed(&mut client).await.is_empty());
    let mut other = server.connect().await;
    let mut value = b"$56\r\n".to_vec();
    value.extend_from_slice(&[b'x'; 56]);
    value.extend_from_slice(b"\r\n");
    exchange(&mut other, &arguments(&[b"GET", b"a"]), &value).await;
    server.stop().await;
}

#[tokio::test]
async fn r02_quota_and_integer_errors_preserve_the_connection_and_batch_state() {
    let server = TestServer::start(ServerConfig {
        max_dataset_bytes: 260,
        ..ServerConfig::default()
    })
    .await;
    let mut client = server.connect().await;
    exchange(
        &mut client,
        &arguments(&[b"MSET", b"a", b"9", b"b", b"x"]),
        b"+OK\r\n",
    )
    .await;
    exchange(
        &mut client,
        &arguments(&[b"INCR", b"a"]),
        b"-OOM dataset memory quota exceeded\r\n",
    )
    .await;
    exchange(
        &mut client,
        &arguments(&[b"INCR", b"b"]),
        b"-ERR value is not an integer or out of range\r\n",
    )
    .await;
    exchange(
        &mut client,
        &arguments(&[b"MSET", b"a", b"0", b"c", b"x"]),
        b"-OOM dataset memory quota exceeded\r\n",
    )
    .await;
    exchange(
        &mut client,
        &arguments(&[b"MSET", b"a", b"0", b"b"]),
        b"-ERR wrong number of arguments for 'mset' command\r\n",
    )
    .await;
    exchange(
        &mut client,
        &arguments(&[b"MGET", b"a", b"b", b"c"]),
        b"*3\r\n$1\r\n9\r\n$1\r\nx\r\n$-1\r\n",
    )
    .await;
    exchange(&mut client, PING, PONG).await;
    server.stop().await;
}

#[tokio::test]
async fn r02_mset_is_indivisible_between_clients() {
    let server = TestServer::start(ServerConfig::default()).await;
    let mut writer = server.connect().await;
    let mut reader = server.connect().await;
    exchange(
        &mut writer,
        &arguments(&[b"MSET", b"a", b"0", b"b", b"0"]),
        b"+OK\r\n",
    )
    .await;
    let writes = async {
        for _ in 0..100 {
            for value in [b"1".as_slice(), b"0"] {
                exchange(
                    &mut writer,
                    &arguments(&[b"MSET", b"a", value, b"b", value]),
                    b"+OK\r\n",
                )
                .await;
            }
        }
    };
    let reads = async {
        for _ in 0..200 {
            write(&mut reader, &arguments(&[b"MGET", b"a", b"b"])).await;
            let mut actual = [0; 18];
            timeout(IO_DEADLINE, reader.read_exact(&mut actual))
                .await
                .unwrap()
                .unwrap();
            assert!(
                actual == *b"*2\r\n$1\r\n0\r\n$1\r\n0\r\n"
                    || actual == *b"*2\r\n$1\r\n1\r\n$1\r\n1\r\n"
            );
        }
    };
    tokio::join!(writes, reads);
    server.stop().await;
}

#[tokio::test]
async fn literal_reference_cases_work_sequentially_over_tcp() {
    let server = TestServer::start(ServerConfig::default()).await;
    let mut client = server.connect().await;
    for case in resp_fixtures::CASES {
        for &(request, expected) in case.exchanges {
            exchange(&mut client, request, expected).await;
        }
        // A sentinela detecta respostas excedentes entre os casos.
        exchange(&mut client, PING, PONG).await;
    }
    half_close(&mut client).await;
    assert!(read_until_closed(&mut client).await.is_empty());
    server.stop().await;
}

#[tokio::test]
async fn literal_reference_pipelines_preserve_response_order() {
    let server = TestServer::start(ServerConfig::default()).await;
    let mut client = server.connect().await;
    for case in resp_fixtures::CASES {
        let requests: Vec<u8> = case
            .exchanges
            .iter()
            .flat_map(|(request, _)| request.iter().copied())
            .collect();
        let expected: Vec<u8> = case
            .exchanges
            .iter()
            .flat_map(|(_, response)| response.iter().copied())
            .collect();
        write(&mut client, &requests).await;
        let mut actual = vec![0; expected.len()];
        timeout(IO_DEADLINE, client.read_exact(&mut actual))
            .await
            .expect("pipeline response deadline")
            .expect("complete pipeline response");
        assert_eq!(actual, expected, "pipeline {}", case.name);
        exchange(&mut client, PING, PONG).await;
    }
    half_close(&mut client).await;
    assert!(read_until_closed(&mut client).await.is_empty());
    server.stop().await;
}

#[tokio::test]
async fn clients_share_state_but_not_partial_decoder_buffers() {
    let server = TestServer::start(ServerConfig::default()).await;
    let mut first = server.connect().await;
    let mut second = server.connect().await;
    exchange(&mut first, PING, PONG).await;
    exchange(&mut second, PING, PONG).await;

    // O primeiro cliente interrompe seu pedido dentro do payload. A resposta
    // do segundo sincroniza as operações sem impor uma ordem por relógio.
    write(&mut first, b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$5\r\n\x00\r").await;
    exchange(
        &mut second,
        b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$0\r\n\r\n",
        b"+OK\r\n",
    )
    .await;
    exchange(&mut second, b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n", b"$-1\r\n").await;
    write(&mut first, b"\n\xffA\r\n").await;
    expect_bytes(&mut first, b"+OK\r\n").await;
    exchange(
        &mut second,
        b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n",
        b"$5\r\n\x00\r\n\xffA\r\n",
    )
    .await;
    exchange(&mut first, b"*2\r\n$3\r\nGET\r\n$1\r\nb\r\n", b"$0\r\n\r\n").await;
    exchange(
        &mut first,
        b"*4\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\na\r\n$1\r\nb\r\n",
        b":2\r\n",
    )
    .await;
    exchange(&mut second, b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n", b"$-1\r\n").await;
    server.stop().await;
    assert!(read_until_closed(&mut first).await.is_empty());
    assert!(read_until_closed(&mut second).await.is_empty());
}

#[tokio::test]
async fn recoverable_errors_keep_pipeline_alive_without_mutation() {
    let server = TestServer::start(ServerConfig::default()).await;
    let mut client = server.connect().await;
    exchange(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\nsafe\r\n\
          *1\r\n$3\r\nGET\r\n\
          *2\r\n$7\r\nPRIVATE\r\n$6\r\nsecret\r\n\
          *4\r\n$3\r\nSET\r\n$1\r\nk\r\n$6\r\nunsafe\r\n$3\r\nBAD\r\n\
          *2\r\n$3\r\nGET\r\n$1\r\nk\r\n\
          *1\r\n$4\r\nPING\r\n",
        b"+OK\r\n\
          -ERR wrong number of arguments for 'get' command\r\n\
          -ERR unknown command\r\n\
          -ERR syntax error\r\n\
          $4\r\nsafe\r\n\
          +PONG\r\n",
    )
    .await;
    half_close(&mut client).await;
    assert!(read_until_closed(&mut client).await.is_empty());
    server.stop().await;
}

#[tokio::test]
async fn binary_echo_survives_every_write_split_and_byte_writes() {
    let server = TestServer::start(ServerConfig::default()).await;
    let mut client = server.connect().await;
    const REQUEST: &[u8] = b"*2\r\n$4\r\nECHO\r\n$5\r\n\x00\r\n\xffA\r\n";
    const RESPONSE: &[u8] = b"$5\r\n\x00\r\n\xffA\r\n";
    for split in 1..REQUEST.len() {
        write(&mut client, &REQUEST[..split]).await;
        tokio::task::yield_now().await;
        write(&mut client, &REQUEST[split..]).await;
        expect_bytes(&mut client, RESPONSE).await;
    }
    for byte in REQUEST {
        write(&mut client, std::slice::from_ref(byte)).await;
        tokio::task::yield_now().await;
    }
    expect_bytes(&mut client, RESPONSE).await;
    // TCP pode agregar escritas. A fragmentação exata de cada leitura é coberta
    // pelos testes unitários do codec e por I/O controlada da conexão.
    exchange(&mut client, PING, PONG).await;
    server.stop().await;
}

#[tokio::test]
async fn half_close_executes_complete_pipeline_and_returns_all_responses() {
    let server = TestServer::start(ServerConfig::default()).await;
    let mut client = server.connect().await;
    write(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$0\r\n\r\n$1\r\nx\r\n\
          *2\r\n$3\r\nGET\r\n$0\r\n\r\n\
          *2\r\n$3\r\nDEL\r\n$0\r\n\r\n\
          *2\r\n$3\r\nGET\r\n$0\r\n\r\n\
          *1\r\n$4\r\nPING\r\n",
    )
    .await;
    half_close(&mut client).await;
    expect_bytes(&mut client, b"+OK\r\n$1\r\nx\r\n:1\r\n$-1\r\n+PONG\r\n").await;
    assert!(read_until_closed(&mut client).await.is_empty());
    server.stop().await;
}

#[tokio::test]
async fn truncated_frame_never_executes_but_complete_prefix_does() {
    let server = TestServer::start(ServerConfig::default()).await;
    let mut writer = server.connect().await;
    write(
        &mut writer,
        b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nx\r\n\
          *3\r\n$3\r\nSET\r\n$1\r\nb\r\n$5\r\nshort\r",
    )
    .await;
    half_close(&mut writer).await;
    expect_bytes(&mut writer, b"+OK\r\n").await;
    assert!(read_until_closed(&mut writer).await.is_empty());

    let mut observer = server.connect().await;
    exchange(
        &mut observer,
        b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n\
          *2\r\n$3\r\nGET\r\n$1\r\nb\r\n",
        b"$1\r\nx\r\n$-1\r\n",
    )
    .await;
    server.stop().await;
}

#[tokio::test]
async fn malformed_protocol_closes_connection_without_resynchronizing() {
    let server = TestServer::start(ServerConfig::default()).await;
    for malformed in [
        b"!private\r\n".as_slice(),
        b"*1\r\n$4\r\nPING\rx",
        b"*1\r\n$-2\r\n",
        b"*999999999999999999999999999\r\n",
    ] {
        let mut client = server.connect().await;
        let mut request = malformed.to_vec();
        request.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nx\r\n");
        write(&mut client, &request).await;
        let response = read_until_closed(&mut client).await;
        if !response.is_empty() {
            assert_eq!(response, b"-ERR invalid protocol\r\n");
        }
        let mut observer = server.connect().await;
        exchange(&mut observer, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", b"$-1\r\n").await;
    }
    server.stop().await;
}

#[tokio::test]
async fn invalid_request_shapes_are_fatal_without_pipelined_mutation() {
    let server = TestServer::start(ServerConfig::default()).await;
    for invalid in [
        b"*0\r\n".as_slice(),
        b"*-1\r\n",
        b"+PING\r\n",
        b"*1\r\n$-1\r\n",
        b"*1\r\n+PING\r\n",
        b"*2\r\n$4\r\nECHO\r\n:1\r\n",
        b"*1\r\n*1\r\n$4\r\nPING\r\n",
    ] {
        let mut client = server.connect().await;
        let mut request = invalid.to_vec();
        request.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nx\r\n");
        write(&mut client, &request).await;
        let response = read_until_closed(&mut client).await;
        if !response.is_empty() {
            assert_eq!(response, b"-ERR invalid request format\r\n");
        }
        let mut observer = server.connect().await;
        exchange(&mut observer, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", b"$-1\r\n").await;
    }
    server.stop().await;
}

#[tokio::test]
async fn excess_connection_is_closed_and_disconnection_releases_the_slot() {
    let server = TestServer::start(ServerConfig {
        max_connections: 1,
        ..ServerConfig::default()
    })
    .await;
    let mut first = server.connect().await;
    exchange(&mut first, PING, PONG).await;
    let mut excess = server.connect().await;
    // O PONG anterior comprova que a primeira conexão já ocupa a única vaga.
    assert!(read_until_closed(&mut excess).await.is_empty());
    exchange(&mut first, PING, PONG).await;
    half_close(&mut first).await;
    assert!(read_until_closed(&mut first).await.is_empty());
    drop(first);

    // EOF pode ficar visível antes de a supervisão recolher a tarefa concluída.
    // Cada tentativa é ordenada por I/O real; não usamos sleeps de escalonamento.
    let mut replacement = timeout(IO_DEADLINE, async {
        loop {
            let mut candidate = server.connect().await;
            match candidate.write_all(PING).await {
                Ok(()) => {}
                Err(error) if is_connection_closed(&error) => continue,
                Err(error) => panic!("replacement write: {error}"),
            }
            let mut response = [0; 7];
            match candidate.read_exact(&mut response).await {
                Ok(_) => {
                    assert_eq!(&response, PONG);
                    break candidate;
                }
                Err(error)
                    if is_connection_closed(&error)
                        || error.kind() == io::ErrorKind::UnexpectedEof => {}
                Err(error) => panic!("replacement read: {error}"),
            }
        }
    })
    .await
    .expect("released connection slot deadline");
    exchange(&mut replacement, PING, PONG).await;
    server.stop().await;
    assert!(read_until_closed(&mut replacement).await.is_empty());
}

#[tokio::test]
async fn shutdown_closes_idle_and_partial_connections_and_listener() {
    let server = TestServer::start(ServerConfig::default()).await;
    let address = server.address;
    let mut idle = server.connect().await;
    let mut partial = server.connect().await;
    exchange(&mut idle, PING, PONG).await;
    exchange(&mut partial, PING, PONG).await;
    write(&mut partial, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\nx").await;
    server.stop().await;
    assert!(read_until_closed(&mut idle).await.is_empty());
    assert!(read_until_closed(&mut partial).await.is_empty());
    assert!(
        timeout(IO_DEADLINE, TcpStream::connect(address))
            .await
            .expect("closed listener connect deadline")
            .is_err(),
        "listener must no longer accept clients"
    );
}
