#![forbid(unsafe_code)]

use std::process::ExitCode;

use sider::ServerConfig;
use sider::server;

fn main() -> ExitCode {
    let args: Vec<_> = std::env::args_os().skip(1).collect();

    match args.as_slice() {
        [flag] if flag == "--help" || flag == "-h" => {
            println!(
                "Sider: in-memory database server under development.\n\n\
                 Usage: sider [--help | --version | --diagnose]\n\n\
                 --diagnose: validate configuration without starting the server or accessing the AOF.\n\
                 SIDER_ADDR: IP address and port (default: 127.0.0.1:6379).\n\
                 SIDER_READY_FILE: optional readiness JSON file.\n\
                 Limits: SIDER_MAX_CONNECTIONS, SIDER_WORKER_QUEUE_CAPACITY,\n\
                 SIDER_PUBSUB_MAX_CHANNELS, SIDER_PUBSUB_QUEUE_CAPACITY,\n\
                 SIDER_TRANSACTION_MAX_COMMANDS, SIDER_TRANSACTION_MAX_BYTES, SIDER_WATCH_MAX_KEYS,\n\
                 SIDER_MAX_FRAME_BYTES, SIDER_MAX_BULK_BYTES, SIDER_MAX_LINE_BYTES,\n\
                 SIDER_MAX_NODES, SIDER_MAX_DEPTH, SIDER_MAX_INPUT_BUFFER_BYTES,\n\
                 SIDER_MAX_RESPONSE_BYTES, SIDER_MAX_DATASET_BYTES, SIDER_SHARDS. Timeouts in milliseconds:\n\
                 SIDER_FRAME_TIMEOUT_MS, SIDER_REQUEST_TIMEOUT_MS,\n\
                 SIDER_WRITE_TIMEOUT_MS and SIDER_SHUTDOWN_TIMEOUT_MS.\n\
                 With no arguments, start the TCP server. Use Ctrl+C to shut down."
            );
            ExitCode::SUCCESS
        }
        [flag] if flag == "--version" || flag == "-V" => {
            println!("sider {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        [flag] if flag == "--diagnose" => match ServerConfig::from_env() {
            Ok(config) => {
                print!("{}", config.diagnostic());
                ExitCode::SUCCESS
            }
            Err(error) => {
                // A malformed variable's value may contain sensitive data.
                let reason = match &error {
                    sider::ConfigError::InvalidServerLimits { reason }
                    | sider::ConfigError::InvalidRespLimits { reason } => *reason,
                    sider::ConfigError::InvalidInteger { name, .. }
                    | sider::ConfigError::NonUnicodeValue { name } => *name,
                    sider::ConfigError::InvalidAddress { .. }
                    | sider::ConfigError::NonUnicodeAddress => "SIDER_ADDR",
                };
                eprintln!("invalid configuration: {reason}");
                ExitCode::FAILURE
            }
        },
        [] => match ServerConfig::from_env() {
            Ok(config) => start(config),
            Err(error) => {
                eprintln!("error: {error}");
                ExitCode::FAILURE
            }
        },
        _ => {
            eprintln!("invalid arguments; usage: sider [--help | --version | --diagnose]");
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
        eprintln!("failed to initialize logging: {error}");
        return ExitCode::FAILURE;
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("failed to initialize runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "server terminated with an error");
            ExitCode::FAILURE
        }
    }
}

async fn run(config: ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Register signals before publishing readiness to avoid missing an immediate shutdown.
    let shutdown = shutdown_signal()?;
    let prepared = server::prepare(&config).await?;
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    let address = listener.local_addr()?;
    if !address.ip().is_loopback() {
        tracing::warn!("non-loopback address; server has no authentication or TLS");
    }
    server::serve_prepared(listener, config, shutdown, prepared).await?;
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
