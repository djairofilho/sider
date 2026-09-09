//! Explicit offline operation; arguments and paths do not change the global environment.
#![forbid(unsafe_code)]

use std::process::ExitCode;
use std::sync::Arc;

use sider::persistence::migration::{migrate_offline, options_from_args};
use sider::storage::SystemClock;

fn main() -> ExitCode {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    if arguments.len() == 1 && arguments[0] == "--help" {
        println!(
            "Usage: sider-aof-migrate --source DIR --source-shards N --source-routing 1 --destination NEW_DIR --shards N --routing 1 [--source-max-dataset-bytes N] [--max-dataset-bytes N] [--source-max-record-bytes N] [--max-record-bytes N]"
        );
        return ExitCode::SUCCESS;
    }
    let result = options_from_args(arguments).and_then(|options| {
        migrate_offline(options, Arc::new(SystemClock)).map_err(|error| error.to_string())
    });
    match result {
        Ok(report) => {
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
        Err(error) => {
            eprintln!("AOF migration rejected: {error}");
            ExitCode::FAILURE
        }
    }
}
