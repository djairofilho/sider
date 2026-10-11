//! Explicit replication administration through the local internal listener.

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
            "Usage: sider-replica --addr IP:PORT --status|--promote\n--promote persists the primary role and stops upstream. Fence the former primary first.\nProtected mode requires SIDER_SECURITY_DIR. Explicit legacy-loopback permits only loopback targets."
        );
        return Ok(());
    }
    if arguments.len() != 3 || arguments[0] != "--addr" {
        return Err("invalid arguments; see --help".into());
    }
    let address: SocketAddr = arguments[1].parse()?;
    let promote = match arguments[2].as_str() {
        "--status" => false,
        "--promote" => true,
        _ => return Err("invalid operation; see --help".into()),
    };
    let deadline = Duration::from_secs(30);
    let security = sider::security::StartupSecurity::from_env()?.tool(address)?;
    let (mut socket, peer): (Box<dyn sider::security::NetworkStream>, _) =
        if let Some(generation) = security {
            let (socket, peer) = generation
                .internal
                .ok_or(sider::security::Error::Configuration)?
                .transport
                .connect_with_peer(address)
                .await?;
            (socket, Some(peer))
        } else {
            eprintln!("legacy-loopback profile: plaintext and unauthenticated");
            (
                Box::new(tokio::time::timeout(deadline, TcpStream::connect(address)).await??),
                None,
            )
        };
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
    let phase = if promote {
        sider::security::InternalPhase::Promoted
    } else {
        sider::security::InternalPhase::Status
    };
    let response = protocol::read_in_phase(&mut socket, Limits::default(), deadline, phase)
        .await?
        .1;
    if peer.as_ref().is_some_and(|peer| !peer.current()) {
        return Err(sider::security::Error::Transport.into());
    }
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
        _ => return Err("unexpected administrative reply".into()),
    };
    println!("{}", serde_json::to_string(&value)?);
    Ok(())
}
