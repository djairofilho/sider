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
            "Usage: sider-backup export --source IP:PORT --destination NEW_DIRECTORY --source-sha SHA [--profile legacy|memory|composed]\n     sider-backup verify --source BACKUP --shards N --routing 1 [--profile legacy|memory|composed]\n     sider-backup restore --source BACKUP --destination NEW_DIRECTORY --shards N --routing 1 [--profile legacy|memory|composed]\nProfile defaults to legacy; --record-version 1|2 remains an alternative for legacy or memory.\nLimits: --max-record-bytes N --max-mutations N --max-snapshot-bytes N --max-dataset-bytes N --timeout-ms N"
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
            Action::Export(options) => export(options, ExportKind::Legacy, clock),
            Action::ExportMemory(options) => export(options, ExportKind::Memory, clock),
            Action::ExportComposed(options) => export(options, ExportKind::Composed, clock),
            Action::Verify {
                source,
                layout,
                limits,
            } => backup::verify(&source, layout, limits, clock).map(report),
            Action::VerifyMemory {
                source,
                layout,
                limits,
            } => backup::verify_memory(&source, layout, limits, clock).map(report),
            Action::VerifyComposed {
                source,
                layout,
                limits,
            } => backup::verify_composed(&source, layout, limits, clock).map(report),
            Action::Restore(options) => backup::restore(options, clock).map(report),
            Action::RestoreMemory(options) => backup::restore_memory(options, clock).map(report),
            Action::RestoreComposed(options) => {
                backup::restore_composed(options, clock).map(report)
            }
        }
    });
    match result {
        Ok(report) => {
            println!("{report}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("backup rejected: {error}");
            ExitCode::FAILURE
        }
    }
}

#[derive(Clone, Copy)]
enum ExportKind {
    Legacy,
    Memory,
    Composed,
}

fn export(
    options: backup::ExportOptions,
    kind: ExportKind,
    clock: Arc<SystemClock>,
) -> Result<serde_json::Value, backup::Error> {
    let security = sider::security::StartupSecurity::from_env()
        .and_then(|profile| profile.tool(options.source))
        .map_err(|_| backup::Error::Invalid("security configuration unavailable"))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let manifest = if let Some(generation) = security {
        let transport = &generation
            .internal
            .ok_or(backup::Error::Invalid("security configuration unavailable"))?
            .transport;
        match kind {
            ExportKind::Legacy => {
                runtime.block_on(backup::export_secure(options, clock, transport))?
            }
            ExportKind::Memory => {
                runtime.block_on(backup::export_memory_secure(options, clock, transport))?
            }
            ExportKind::Composed => {
                runtime.block_on(backup::export_composed_secure(options, clock, transport))?
            }
        }
    } else {
        eprintln!("legacy-loopback profile: plaintext and unauthenticated");
        match kind {
            ExportKind::Legacy => runtime.block_on(backup::export(options, clock))?,
            ExportKind::Memory => runtime.block_on(backup::export_memory(options, clock))?,
            ExportKind::Composed => runtime.block_on(backup::export_composed(options, clock))?,
        }
    };
    Ok(manifest.json())
}

fn report(verified: backup::VerifiedBackup) -> serde_json::Value {
    serde_json::json!({"status":"verified","manifest":verified.manifest.json(),"live_entries":verified.live_entries,"shard_usage":verified.shard_usage})
}
