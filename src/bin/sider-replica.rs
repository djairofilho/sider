//! Administração explícita da replicação pelo listener interno local.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::time::Duration;

use sider::replication::protocol::{self, Limits, Message};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sider-replica: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if arguments == ["--help"] || arguments.is_empty() {
        println!(
            "Uso: sider-replica --addr IP:PORTA --status|--promote\n--promote persiste o papel primário e interrompe o upstream. Exige loopback."
        );
        return Ok(());
    }
    if arguments.len() != 3 || arguments[0] != "--addr" {
        return Err("argumentos inválidos; consulte --help".into());
    }
    let address: SocketAddr = arguments[1].parse()?;
    let promote = match arguments[2].as_str() {
        "--status" => false,
        "--promote" if address.ip().is_loopback() => true,
        "--promote" => return Err("promoção exige endereço de loopback".into()),
        _ => return Err("operação inválida; consulte --help".into()),
    };
    let deadline = Duration::from_secs(30);
    let mut socket = tokio::time::timeout(deadline, TcpStream::connect(address)).await??;
    protocol::write(
        &mut socket,
        &if promote {
            Message::Promote
        } else {
            Message::StatusRequest
        },
        Limits::default(),
        deadline,
    )
    .await?;
    let response = protocol::read(&mut socket, Limits::default(), deadline).await?;
    let epoch = |bytes: [u8; 16]| {
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let value = match response {
        Message::Promoted(cursor) if promote => {
            serde_json::json!({ "role": "primary", "epoch": epoch(cursor.epoch), "sequence": cursor.sequence })
        }
        Message::Status {
            readonly,
            cursor,
            upstream_sequence,
            connected,
            backlog_bytes,
            full_syncs,
            partial_syncs,
        } if !promote => serde_json::json!({
            "role": if readonly { "replica" } else { "primary" }, "epoch": epoch(cursor.epoch), "sequence": cursor.sequence,
            "upstream_sequence": upstream_sequence, "connected": connected, "backlog_bytes": backlog_bytes,
            "full_syncs": full_syncs, "partial_syncs": partial_syncs
        }),
        response => return Err(format!("resposta administrativa inesperada: {response:?}").into()),
    };
    println!("{}", serde_json::to_string(&value)?);
    Ok(())
}
