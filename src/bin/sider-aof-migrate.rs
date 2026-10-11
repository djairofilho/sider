//! Explicit offline operation; arguments and paths do not change the global environment.
#![forbid(unsafe_code)]

use std::process::ExitCode;
use std::sync::Arc;

use sider::persistence::migration::{
    MemoryMigrationReport, MigrationReport, migrate_memory_offline, migrate_offline,
    options_from_args,
};
use sider::storage::SystemClock;

enum Report {
    Legacy(MigrationReport),
    Memory(MemoryMigrationReport),
}

fn main() -> ExitCode {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    if arguments.len() == 1 && arguments[0] == "--help" {
        println!(
            "Usage: sider-aof-migrate --source DIR --source-shards N --source-routing 1 --destination NEW_DIR --shards N --routing 1 [--source-record-version 1|2] [--record-version 1|2] [--source-max-dataset-bytes N] [--max-dataset-bytes N] [--source-max-record-bytes N] [--max-record-bytes N]\nRecord versions default to 1. Record 2 requires an explicit --record-version 2; source record 2 also requires --source-record-version 2. No automatic upgrade or downgrade."
        );
        return ExitCode::SUCCESS;
    }
    let result = options_from_args(arguments).and_then(|options| {
        if options.destination.is_memory_profile() {
            migrate_memory_offline(options, Arc::new(SystemClock)).map(Report::Memory)
        } else {
            migrate_offline(options, Arc::new(SystemClock)).map(Report::Legacy)
        }
        .map_err(|error| error.to_string())
    });
    match result {
        Ok(Report::Legacy(report)) => {
            println!(
                "{{\"source_format\":{},\"sequence\":{},\"entries\":{},\"source_shards\":{},\"target_shards\":{},\"routing_version\":{},\"shard_usage\":{:?},\"incomplete_source_tail_bytes\":{}}}",
                report.source.format_version,
                report.sequence,
                report.entries,
                report.source.layout.shard_count,
                report.destination_layout.shard_count,
                report.destination_layout.routing_version,
                report.destination_shard_usage,
                report.source.incomplete_tail_bytes
            );
            ExitCode::SUCCESS
        }
        Ok(Report::Memory(report)) => {
            let migration = &report.migration;
            println!(
                "{}",
                serde_json::json!({
                    "source_format": migration.source.format_version,
                    "target_format": report.destination_header_version,
                    "source_record_version": report.source_record_version,
                    "target_record_version": report.destination_record_version,
                    "sequence": migration.sequence,
                    "entries": migration.entries,
                    "source_shards": migration.source.layout.shard_count,
                    "target_shards": migration.destination_layout.shard_count,
                    "routing_version": migration.destination_layout.routing_version,
                    "source_routing_version": migration.source.layout.routing_version,
                    "target_routing_version": migration.destination_layout.routing_version,
                    "shard_usage": migration.destination_shard_usage,
                    "incomplete_source_tail_bytes": migration.source.incomplete_tail_bytes,
                    "source_generation": report.source_generation.to_string_lossy(),
                    "source_sha256": report.source_sha256,
                    "target_sha256": report.destination_sha256,
                    "retained_memory_collections": report.memory_collections,
                    "retained_memory_members": report.memory_members,
                })
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("AOF migration rejected: {error}");
            ExitCode::FAILURE
        }
    }
}
