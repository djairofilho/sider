#![forbid(unsafe_code)]

use std::process::ExitCode;

use sider::ServerConfig;
use sider::readiness::ReadyFile;
use sider::server;

fn main() -> ExitCode {
    let args: Vec<_> = std::env::args_os().skip(1).collect();

    match args.as_slice() {
        [flag] if flag == "--help" || flag == "-h" => {
            println!(
                "Sider: servidor de banco de dados em memória em desenvolvimento.\n\n\
                 Uso: sider [--help | --version]\n\n\
                 SIDER_ADDR: IP e porta (padrão: 127.0.0.1:6379).\n\
                 SIDER_READY_FILE: arquivo JSON de prontidão opcional.\n\
                 Limites: SIDER_MAX_CONNECTIONS, SIDER_WORKER_QUEUE_CAPACITY,\n\
                 SIDER_MAX_FRAME_BYTES, SIDER_MAX_BULK_BYTES, SIDER_MAX_LINE_BYTES,\n\
                 SIDER_MAX_NODES, SIDER_MAX_DEPTH, SIDER_MAX_INPUT_BUFFER_BYTES,\n\
                 SIDER_MAX_RESPONSE_BYTES, SIDER_MAX_DATASET_BYTES, SIDER_SHARDS. Prazos em milissegundos:\n\
                 SIDER_FRAME_TIMEOUT_MS, SIDER_REQUEST_TIMEOUT_MS,\n\
                 SIDER_WRITE_TIMEOUT_MS e SIDER_SHUTDOWN_TIMEOUT_MS.\n\
                 Sem argumentos, inicia o servidor TCP. Use Ctrl+C para encerrar."
            );
            ExitCode::SUCCESS
        }
        [flag] if flag == "--version" || flag == "-V" => {
            println!("sider {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        [] => match ServerConfig::from_env() {
            Ok(config) => start(config),
            Err(error) => {
                eprintln!("erro: {error}");
                ExitCode::FAILURE
            }
        },
        _ => {
            eprintln!("argumentos inválidos; uso: sider [--help | --version]");
            ExitCode::FAILURE
        }
    }
}

fn start(config: ServerConfig) -> ExitCode {
    if let Err(error) = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init()
    {
        eprintln!("erro ao iniciar logs: {error}");
        return ExitCode::FAILURE;
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("erro ao iniciar runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "servidor terminou com erro");
            ExitCode::FAILURE
        }
    }
}

async fn run(config: ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Registrar sinais antes de publicar prontidão evita perder uma parada imediata.
    let shutdown = shutdown_signal()?;
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    let address = listener.local_addr()?;
    if !address.ip().is_loopback() {
        tracing::warn!("endereço fora de loopback; servidor sem autenticação ou TLS");
    }
    let _ready = config
        .ready_file
        .as_ref()
        .map(|path| ReadyFile::create(path, address))
        .transpose()?;
    server::serve(listener, config, shutdown).await?;
    Ok(())
}

#[cfg(unix)]
fn shutdown_signal() -> std::io::Result<impl std::future::Future<Output = ()> + Send> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! { _ = interrupt.recv() => {}, _ = terminate.recv() => {} }
    })
}

#[cfg(windows)]
fn shutdown_signal() -> std::io::Result<impl std::future::Future<Output = ()> + Send> {
    let mut interrupt = tokio::signal::windows::ctrl_c()?;
    let mut ctrl_break = tokio::signal::windows::ctrl_break()?;
    let mut close = tokio::signal::windows::ctrl_close()?;
    Ok(async move {
        tokio::select! { _ = interrupt.recv() => {}, _ = ctrl_break.recv() => {}, _ = close.recv() => {} }
    })
}
