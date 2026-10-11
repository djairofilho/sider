//! Explicit operational backup; does not open the server's public listener.
#![forbid(unsafe_code)]

use std::process::ExitCode;
use std::sync::Arc;

use sider::persistence::backup::{self, cli::Action};
use sider::storage::SystemClock;

fn main() -> ExitCode {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if args.len() == 1 && args[0] == "--help" {
        println!(
            "Usage: sider-backup export --source IP:PORT --destination NEW_DIRECTORY --source-sha SHA\n     sider-backup verify --source BACKUP --shards N --routing 1\n     sider-backup restore --source BACKUP --destination NEW_DIRECTORY --shards N --routing 1\nLimits: --max-record-bytes N --max-mutations N --max-snapshot-bytes N --max-dataset-bytes N --timeout-ms N"
        );
        return ExitCode::SUCCESS;
    }
    if args.len() == 1 && args[0] == "--version" {
        println!("sider-backup {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    let result = backup::cli::parse(args).and_then(|action| {
        let clock = Arc::new(SystemClock);
        match action {
            Action::Export(options) => {
                let security = sider::security::StartupSecurity::from_env()
                    .and_then(|profile| profile.tool(options.source))
                    .map_err(|_| backup::Error::Invalid("security configuration unavailable"))?;
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?;
                let result = if let Some(generation) = security {
                    runtime.block_on(backup::export_secure(
                        options,
                        clock,
                        &generation
                            .internal
                            .ok_or(backup::Error::Invalid("security configuration unavailable"))?
                            .transport,
                    ))
                } else {
                    eprintln!("legacy-loopback profile: plaintext and unauthenticated");
                    runtime.block_on(backup::export(options, clock))
                };
                result.map(|manifest| manifest.json())
            }
            Action::Verify {
                source,
                layout,
                limits,
            } => backup::verify(&source, layout, limits, clock).map(report),
            Action::Restore(options) => backup::restore(options, clock).map(report),
        }
    });
    match result {
        Ok(report) => {
            println!("{report}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("backup recusado: {error}");
            ExitCode::FAILURE
        }
    }
}

fn report(verified: backup::VerifiedBackup) -> serde_json::Value {
    serde_json::json!({"status":"verified","manifest":verified.manifest.json(),"live_entries":verified.live_entries,"shard_usage":verified.shard_usage})
}
