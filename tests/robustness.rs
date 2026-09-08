#![forbid(unsafe_code)]

//! Reutilização observável das vagas de conexão, sem inferir quota ou ausência de leaks.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use sider::ServerConfig;
use sider::resp::RespLimits;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const IO_DEADLINE: Duration = Duration::from_secs(3);
const TEST_DEADLINE: Duration = Duration::from_secs(20);
const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const PONG: &[u8] = b"+PONG\r\n";
const GET: &[u8] = b"*2\r\n$3\r\nGET\r\n$5\r\nstate\r\n";
const PARTIAL_SET: &[u8] = b"*3\r\n$3\r\nSET\r\n$5\r\nstate\r\n$7\r\nco";

struct TestServer {
    address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl TestServer {
    async fn start(max_connections: usize, frame_timeout: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let config = ServerConfig {
            max_connections,
            worker_queue_capacity: 1,
            resp_limits: RespLimits {
                max_frame_bytes: 256,
                max_bulk_bytes: 64,
                max_line_bytes: 64,
                max_nodes: 8,
                max_depth: 2,
            },
            max_input_buffer_bytes: 512,
            max_response_bytes: 128,
            frame_timeout,
            shutdown_timeout: IO_DEADLINE,
            ..ServerConfig::default()
        };
        let (shutdown, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            sider::server::serve(listener, config, async move {
                let _ = stopped.await;
            })
            .await
            .expect("servidor deve encerrar sem erro");
        });
        Self {
            address,
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    async fn admitted(&self) -> TcpStream {
        // EOF pode preceder a liberação do permit. Repetimos somente rejeições
        // observadas por I/O, com um prazo total, sem sleeps de escalonamento.
        timeout(IO_DEADLINE, async {
            loop {
                let mut stream = TcpStream::connect(self.address).await.unwrap();
                stream.set_nodelay(true).unwrap();
                match stream.write_all(PING).await {
                    Ok(()) => {}
                    Err(error) if is_closed(&error) => continue,
                    Err(error) => panic!("escrever PING de admissão: {error}"),
                }
                let mut pong = [0; 7];
                match stream.read(&mut pong[..1]).await {
                    Ok(0) => continue,
                    Ok(1) => {
                        // Uma resposta já iniciada não pode ser descartada como
                        // simples rejeição: truncamento ou bytes errados falham.
                        stream.read_exact(&mut pong[1..]).await.unwrap();
                        assert_eq!(pong, PONG);
                        return stream;
                    }
                    Ok(_) => unreachable!("leitura limitada a um byte"),
                    Err(error) if is_closed(&error) => continue,
                    Err(error) => panic!("ler PONG de admissão: {error}"),
                }
            }
        })
        .await
        .expect("todas as vagas liberadas devem poder ser reocupadas")
    }

    async fn stop(mut self) {
        self.shutdown.take().unwrap().send(()).unwrap();
        timeout(IO_DEADLINE * 2, self.task.as_mut().unwrap())
            .await
            .expect("prazo total de encerramento")
            .expect("tarefa do servidor recolhida sem panic");
        self.task.take();
        let result = timeout(IO_DEADLINE, TcpStream::connect(self.address))
            .await
            .expect("prazo para conferir listener fechado");
        assert!(result.is_err(), "shutdown não pode deixar o listener ativo");
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown.take();
        if let Some(task) = self.task.take() {
            // Cancelar serve também aborta seus JoinSets. O guard cobre panic
            // e o cancelamento da future pelo prazo total do teste.
            task.abort();
        }
    }
}

fn is_closed(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
    )
}

async fn write(stream: &mut TcpStream, bytes: &[u8]) {
    timeout(IO_DEADLINE, stream.write_all(bytes))
        .await
        .expect("prazo de escrita")
        .expect("escrita no cliente admitido");
}

async fn exchange(stream: &mut TcpStream, request: &[u8], expected: &[u8]) {
    timeout(IO_DEADLINE, async {
        stream.write_all(request).await.unwrap();
        let mut response = vec![0; expected.len()];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);
    })
    .await
    .expect("prazo total da troca RESP");
}

async fn expect_closed(stream: &mut TcpStream) {
    match timeout(IO_DEADLINE, stream.read(&mut [0; 1]))
        .await
        .expect("prazo de fechamento da conexão")
    {
        Ok(0) => {}
        Err(error) if is_closed(&error) => {}
        result => panic!("esperado EOF/reset sem resposta excedente: {result:?}"),
    }
}

async fn half_close(stream: &mut TcpStream) {
    timeout(IO_DEADLINE, stream.shutdown())
        .await
        .expect("prazo de half-close")
        .unwrap();
}

#[tokio::test]
async fn mixed_disconnect_waves_reuse_every_slot_and_preserve_confirmed_state() {
    timeout(TEST_DEADLINE, async {
        let server = TestServer::start(3, TEST_DEADLINE).await;
        for wave in 0..24 {
            // Os três PONGs comprovam ocupação simultânea das três vagas.
            let mut idle = server.admitted().await;
            let mut partial = server.admitted().await;
            let mut completed = server.admitted().await;
            if wave > 0 {
                let previous = format!("$7\r\nwave-{:02}\r\n", wave - 1);
                exchange(&mut completed, GET, previous.as_bytes()).await;
            }
            write(&mut partial, PARTIAL_SET).await;
            let request = format!("*3\r\n$3\r\nSET\r\n$5\r\nstate\r\n$7\r\nwave-{wave:02}\r\n");
            exchange(&mut completed, request.as_bytes(), b"+OK\r\n").await;
            let expected = format!("$7\r\nwave-{wave:02}\r\n");
            exchange(&mut idle, GET, expected.as_bytes()).await;

            // Ondas pares drenam EOF; ímpares descartam todos os sockets sem
            // aguardar o servidor. O SET completo só conta após seu +OK.
            if wave % 2 == 0 {
                half_close(&mut idle).await;
                half_close(&mut partial).await;
                half_close(&mut completed).await;
                expect_closed(&mut idle).await;
                expect_closed(&mut partial).await;
                expect_closed(&mut completed).await;
            }
            drop((idle, partial, completed));
        }

        let mut idle = server.admitted().await;
        let mut partial = server.admitted().await;
        let mut observer = server.admitted().await;
        write(&mut partial, PARTIAL_SET).await;
        exchange(&mut observer, GET, b"$7\r\nwave-23\r\n").await;
        server.stop().await;
        expect_closed(&mut idle).await;
        expect_closed(&mut partial).await;
        expect_closed(&mut observer).await;
    })
    .await
    .expect("prazo total das ondas de desconexão");
}

#[tokio::test]
async fn expired_slow_frames_release_all_slots_without_applying_partial_sets() {
    timeout(TEST_DEADLINE, async {
        let server = TestServer::start(2, Duration::from_millis(100)).await;
        for wave in 0..4 {
            let mut first = server.admitted().await;
            let mut second = server.admitted().await;
            if wave == 0 {
                exchange(
                    &mut first,
                    b"*3\r\n$3\r\nSET\r\n$5\r\nstate\r\n$4\r\nsafe\r\n",
                    b"+OK\r\n",
                )
                .await;
            }
            exchange(&mut second, GET, b"$4\r\nsafe\r\n").await;
            write(&mut first, PARTIAL_SET).await;
            write(&mut second, PARTIAL_SET).await;
            // O fechamento é o evento de sincronização. Não avançamos relógio
            // artificial nem dormimos para presumir que o timeout ocorreu.
            tokio::join!(expect_closed(&mut first), expect_closed(&mut second));
        }
        let mut first = server.admitted().await;
        let mut second = server.admitted().await;
        exchange(&mut first, GET, b"$4\r\nsafe\r\n").await;
        exchange(&mut second, PING, PONG).await;
        server.stop().await;
        expect_closed(&mut first).await;
        expect_closed(&mut second).await;
    })
    .await
    .expect("prazo total de recuperação após clientes lentos");
}
