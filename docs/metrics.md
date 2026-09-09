# Metrics and operational diagnostics

Sider exposes instance indicators through `INFO` and prints effective
configuration with `sider --diagnose`. There is no HTTP listener, exporter,
periodic metrics file, or labels derived from keys or channels.

- [Querying the instance](#querying-the-instance)
- [Validating configuration without starting the database](#validating-configuration-without-starting-the-database)
- [Indicator contract](#indicator-contract)
- [Diagnostic procedures](#diagnostic-procedures)
- [Reproducible verification](#reproducible-verification)

## Querying the instance

```sh
redis-cli -h 127.0.0.1 -p 6379 INFO
redis-cli -h 127.0.0.1 -p 6379 INFO clients stats
redis-cli -h 127.0.0.1 -p 6379 INFO memory persistence
redis-cli -h 127.0.0.1 -p 6379 INFO replication persistence
redis-cli -h 127.0.0.1 -p 6379 INFO config
```

The response is a RESP2 bulk string, with `# Section` headings and
`name:value` lines terminated by CRLF. Available sections are `server`, `clients`,
`stats`, `memory`, `persistence`, `replication`, and `config`. Names are ASCII
case-insensitive; repetitions do not duplicate output. Without arguments, or with
`all`, `default`, or `everything`, all sections are selected. Unknown names
are ignored; an entirely unknown selection produces an empty string.

Selection syntax follows the [Redis INFO interface](https://redis.io/docs/latest/commands/info/).
Content is a Sider-specific contract, identified by `metrics_schema_version:1`;
it does not reproduce all Redis fields or sections.
Tools requiring specific Redis fields need adaptation.

`INFO` does not enter the worker queue on a normal connection. An already admitted
connection can query it during queue saturation. The command remains subject to
connection limits, subscriber mode restrictions, and response/write limits from
the [network contract](network.md). If the whole response does not fit, the
connection closes without writing a partial prefix. If only one section matters,
select it to reduce response size.

Inside `MULTI`, `INFO` is queued and executed with `EXEC`. Diagnostics describe
the committed state when the worker assembles batch responses, after applying
its mutations. It is not a snapshot of each queued command's intermediate point.
The query adds no AOF record. [Transaction](transactions.md) rules, including
aggregate RESP2 limits, still apply.

The server does not yet provide administrative authorization for this command.
Use the instance's existing exposure only in the project's intended controlled
environment. Output contains no keys, values, channel names, file paths, or
invalid configuration values.

## Validating configuration without starting the database

```sh
cargo run --locked -- --diagnose
```

The binary loads and validates the same `SIDER_*` variables as startup, prints
the version, configured address, limits, and AOF policy, and exits with code zero.
Invalid configuration exits with a nonzero code and identifies the option or
constraint without repeating the supplied value.

The `diagnostic_scope:configuration_only` field defines the check's scope:
no runtime, listener, readiness file, or AOF directory is created.
There is no recovery, truncation, persisted-data reading, or connectivity test.
An occupied port or inaccessible directory can therefore coexist with a valid
configuration diagnosis. The command does not attest to readiness, AOF integrity,
disk space, or replica health.

`ready_file_enabled`, `aof_configured`, `replication_configured`,
`replication_upstream_configured`, and `replication_ready_file_enabled` indicate
option presence without printing paths or replication endpoints.
Numeric replication limits are also printed when configured. This does not
determine the instance's persisted role: durable promotion takes precedence over
upstream configuration. On a running instance, `tcp_port` gives the actual port;
in offline diagnostics, `bind_addr` gives the requested address, including port
zero when configured.

## Indicator contract

Names are fixed. There are no per-client, shard, key, or channel labels.
Internal vectors have the configured shard count, limited to 256; they do not
grow with the dataset. Counters use atomic increments saturating at `u64::MAX`
and reset when the instance is recreated. AOF sequences and generation come from
the actual writer and may be restored from a previous run.

Each subsystem provides a short observation, without file or socket I/O.
The entire read is not a global barrier: concurrent commands can change state
between fields or shards. Dataset gauges are published after `apply`;
preparing an operation or rejecting it for quota/AOF does not publish future state.
Queues are measured in requests, excluding the request already removed for execution.
An accepted transaction occupies one request, even when it contains multiple commands.

### Server, connections, and commands

| Field | Unit and definition |
| --- | --- |
| `sider_version` | Binary Cargo version; does not identify release approval |
| `metrics_schema_version` | Metrics contract version, currently `1` |
| `uptime_seconds` | Monotonic seconds since instance collector creation |
| `tcp_port` | Actual listener port |
| `connected_clients` | Active connection tasks; drop, cancellation, and EOF release the gauge |
| `total_connections_received` | Admitted connections that started their serving task |
| `rejected_connections` | Connections rejected by the simultaneous limit |
| `commands_received_total` | Complete frames delivered to the parser, including invalid commands and transaction controls |
| `command_error_replies_total` | Command-error frames produced for replies; includes individual EXEC errors but does not confirm client delivery |
| `protocol_errors_total` | Connections closed for codec errors, invalid format, truncated EOF, input buffer limits, or frame assembly deadlines |
| `connection_failures_total` | Failures ending service, plus socket configuration failures; normal shutdown and external cancellation do not count |
| `client_write_timeouts_total` | Connections closed by the write deadline |
| `response_encoding_failures_total` | Connections closed because the response could not be encoded within limits |

A command received inside `MULTI` counts once upon arrival, even if later discarded.
`EXEC` counts as another request; queued commands are not counted again during execution.
A frame rejected by the codec before completion does not increment
`commands_received_total`. Generic protocol errors are classified in
`protocol_errors_total`, distinct from command execution errors.

### Queues, dataset, expiration, and Pub/Sub

| Field | Unit and definition |
| --- | --- |
| `worker_requests_accepted_total` | Completed sends to worker queues, including batches and WATCH |
| `worker_timeouts_total` | Requests whose caller observed the total deadline exceeded, before or after acceptance |
| `worker_failures_total` | Requests whose caller observed `Unavailable` |
| `worker_queue_used` | Observed sum of requests waiting in channels |
| `worker_queue_capacity` | Sum of those channels' fixed capacities |
| `dataset_keys` | Physical entries, including expired entries not yet removed |
| `dataset_expiring_keys` | Events present in expiration indexes |
| `dataset_logical_bytes` | Sum of logical usage accounted for by Store |
| `dataset_quota_bytes` | Sum of logical quotas assigned to Stores |
| `expiration_batches_total` | Applied nonempty batches originating from `Expiration` |
| `expiration_batch_keys_removed_total` | Tombstones applied in those expiration batches |
| `pubsub_channels` | Channels with at least one subscription |
| `pubsub_subscribers` | Connections with at least one subscription |
| `pubsub_subscriptions` | Unique active connection/channel pairs |
| `pubsub_deliveries_total` | Subscriber queues that accepted notifications |
| `pubsub_evictions_total` | Subscribers removed when `try_send` rejected a notification |

`dataset_logical_bytes` and `dataset_quota_bytes` are not RSS: they do not include
the full cost of sockets, buffers, queues, runtime, allocator, or temporary structures.
A quota error preserves previous usage. Expiration counts cover batches whose
resolved origin is `Expiration`; they do not classify every removal by a command
or mixed `EXEC` batch as expiration.

`worker_timeouts_total` does not imply that the command did not execute.
After acceptance, the worker retains the operation even when the caller gives up.
`pubsub_deliveries_total` confirms queue entry, not socket writing or client receipt.
Subscriber disconnection rules are in the [Pub/Sub guide](pubsub.md).

### Persistence

`aof_enabled` indicates whether a writer is attached to the workers. Other fields
in this section exist only when that actual consumer is present.

| Field | Unit and definition |
| --- | --- |
| `aof_running`, `aof_failed` | Writer state, represented by `0` or `1` |
| `aof_written_sequence` | Last sequence whose record completed writing to the active file |
| `aof_synced_sequence` | Last sequence covered by confirmed writer synchronization |
| `aof_generation` | Active file generation |
| `aof_bytes_since_compaction` | Bytes accounted for by the writer since the last compaction |
| `aof_dirty`, `aof_compacting` | Writes awaiting sync and compaction in progress, as `0`/`1` |
| `aof_queue_used`, `aof_queue_capacity` | Queued requests and the writer's fixed capacity |
| `aof_records_written_total` | Append records written in this run |
| `aof_active_file_syncs_total` | Confirmed active-file syncs; excludes auxiliary snapshot-producer fsyncs |
| `aof_fatal_failures_total` | Writer terminations with error |
| `aof_record_rejections_total` | Appends rejected by the format limit before writing |
| `aof_compactions_total` | Completed compactions |
| `aof_compaction_failures_total` | Compactions started and aborted or completed with error |
| `aof_last_error` | Last static error category, or `none`; not cleared by later success |

Categories include `io`, `record_limit`, `format`, `configuration`, `replay`,
`directory_locked`, `unavailable`, `sequence`, `compaction_busy`,
`compaction_delta_limit`, `layout_mismatch`, `cross_shard`, `shard_quota`, and
`migration`. They retain neither original error text nor paths.
The queue gauge is zero when the observer can no longer find an active channel.

AOF writing and Store application are different boundaries. A failure before
synchronization may leave `written_sequence > synced_sequence` and prevent
applying the mutation to the dataset. With periodic policy, this difference may
also represent the normal window between syncs. Read both fields alongside
`aof_failed`, `aof_dirty`, policy, and logs; a sequence alone does not prove
client acknowledgment.

### Replication

`INFO replication` observes the runtime used by sessions and the AOF writer.
Without replication configured, the section contains only `replication_enabled:0`.
Other lines exist only when the actual observer is attached. Reading does not
send requests to workers, query the upstream, or keep database/writer channels
open after shutdown.

| Field | Unit and definition |
| --- | --- |
| `replication_enabled` | Presence of the replication runtime, as `0`/`1` |
| `replication_role` | Effective `primary` or `replica` role, including persisted promotion |
| `replication_connected` | Established upstream session, as `0`/`1`; on a primary it is `0` and does not count replica connections |
| `replication_epoch_known` | Cursor belongs to an initialized epoch, as `0`/`1` |
| `replication_epoch` | Fixed 32-hex-digit identifier, present when the epoch is known |
| `replication_head_sequence` | Primary only: last batch published to the journal after append; may precede Store apply |
| `replication_applied_sequence` | Replica only: last confirmed position after snapshot installation or batch flush and apply |
| `replication_upstream_sequence` | Replica only: last sequence reported by upstream; may be stale after disconnection |
| `replication_lag_known` | Session is connected, epoch is known, and observed upstream is at least the applied position, as `0`/`1` |
| `replication_lag_batches` | Upstream minus applied, in batches; present only when `replication_lag_known:1` |
| `replication_full_syncs_total` | Full synchronizations that established a session in this run |
| `replication_partial_syncs_total` | Accepted continuations that established a session in this run |
| `replication_reconnects_total` | Session attempts in this run, including the first |
| `replication_backlog_bytes`, `replication_backlog_batches` | Bytes and batches retained in the primary's active journal |
| `replication_oldest_sequence` | First retained sequence; absent when the journal is empty |

Applied/head sequence fields are omitted until the epoch is known.
During initial synchronization, disconnection, or observation of an upstream
position before the local cursor, lag is unknown. Its absence does not mean zero.
Even known zero lag compares against the last upstream observation; it does not
promise instantaneous equality with concurrent writes. The unit is batches,
not commands, bytes, or seconds. A transaction may contain multiple commands
in one sequence. Compare sequences across processes only within the same epoch.

Dataset gauges are updated after replication installation and application controls.
Session counters restart with the process; role, epoch, and cursor may come from
durable state. An old session cannot overwrite a new session's state or undo
promotion. Journal and runtime observations are short and may reflect nearby
instants, without a global barrier.

### Configuration

The `config` section prints numeric limits for the codec, buffers, connections,
queues, shards, dataset, Pub/Sub, transactions, WATCH, and deadlines in milliseconds.
When AOF is configured, it also prints queue capacity, record/mutation/delta limits,
compaction threshold, and synchronization policy. File options are exposed only
through presence flags.

With replication configured, output includes `replication_backlog_limit_bytes`,
`replication_backlog_limit_batches`, `replication_max_connections`, and deadlines
`replication_frame_timeout_ms`, `replication_sync_timeout_ms`,
`replication_reconnect_min_ms`, and `replication_reconnect_max_ms`.
These fields describe configured limits; gauges in the `replication` section measure usage.

`worker_queue_capacity_per_shard` is the configured capacity of each channel;
`worker_queue_capacity`, in `stats`, is their observed sum.
Names remain unique even when all sections are selected.

## Diagnostic procedures

| Situation | Evidence and action |
| --- | --- |
| Full queue | Read `INFO stats config` through an already admitted connection. Compare usage/capacity and timeout changes; reduce producer concurrency and check deadlines. Do not automatically retry a write with an unknown outcome. Increasing the queue requires considering memory and waiting time. |
| Connection limit | Compare `connected_clients`, `rejected_connections`, and `max_connections`; close idle client connections. There is no reserved administrative slot. |
| Disk unavailable | Query `INFO persistence` while the instance is still serving and preserve AOF failure logs. A fatal error terminates the writer and supervision, so INFO may become inaccessible. Correct space/permissions/device issues and perform normal recovery in a controlled procedure; `--diagnose` does not validate or repair the disk. |
| Large record | `aof_record_rejections_total` rises while `aof_failed` stays zero. Reduce the batch or review the record limit within supported bounds. Rejection occurs before batch application. |
| Slow client | Check `client_write_timeouts_total`, `pubsub_evictions_total`, and write-deadline/subscriber-disconnection logs. Have the consumer drain replies and notifications; reconnect and resubscribe after disconnection. A larger queue only increases temporary tolerance. |
| Response exceeds limit | `response_encoding_failures_total` increases. Reduce the requested size, select fewer INFO sections, or adjust consistent limits. The command may already have produced effects before response failure. |
| Logical quota | Compare usage and quota in `INFO memory`; check quota errors. Remove data or increase the budget in a controlled manner, without treating logical quota as process memory. |
| Connected replica with lag | Query `INFO replication persistence` on both sides. Compare epochs, primary head, replica applied position, and last upstream position over successive observations. Check replica queue/disk and backlog growth. Observed zero does not prove receipt of a later write. |
| Disconnected replica | `replication_connected:0` and `replication_lag_known:0` make lag unknown. Check processes, internal readiness, network, and static logs; correct the cause. The session attempts reconnection within configured deadlines. Rising `replication_reconnects_total` shows attempts, not success. |
| Repeated full synchronization | Observe rising `replication_full_syncs_total`, byte/batch retention, and destination apply capacity. If the cursor leaves retention, resumption requires FULL. Correct slowness or size the backlog within the memory budget; merely increasing timeout does not preserve discarded history. |
| Replica AOF failure | Compare local AOF indicators and preserve logs. Do not interpret append without flush/apply as an applied position. Correct storage before recovery; `--diagnose` neither inspects nor repairs the file. |
| Deliberate promotion | Check applied position and epoch before the operational decision. Isolate the old primary and redirect clients through a controlled procedure. `sider-replica --addr IP:PORT --promote`, on the internal loopback listener, persists a new role/epoch and cancels the old session. Confirm `replication_role:primary` and the new epoch. There is no automatic failover or reconciliation of divergent writes. |

To query the existing administrative protocol, use
`sider-replica --addr IP:PORT --status` on the internal endpoint. This differs
from the RESP port used by `redis-cli`. Internal readiness published through
`SIDER_REPLICATION_READY_FILE` identifies the instance listener; its presence
does not establish that a replica has finished synchronizing. Do not remove the
persisted role or AOF to force reconnection after promotion: topology recovery
requires deciding which history to preserve.

Existing events record startup/shutdown, rejected connections, serving failures,
append/sync failures, and automatic compaction completion/abortion.
Indicators identify categories and volumes without adding payloads to logs.
This delivery has no counter reset command.

## Reproducible verification

The [extracted-package operational test](operational-package.md) exercises
diagnostics, quota, connection limits, a slow client, and an actual AOF open error
with the distributed CLIs. Execution is explicit and records JSON; it does not
turn an ignored test into release approval.

```sh
cargo test --locked --lib metrics_ -- --nocapture
cargo test --locked --test metrics -- --nocapture
cargo test --locked --test cli metrics_ -- --nocapture
cargo test --locked --test replication_network metrics_replication -- --nocapture
cargo clippy --locked --lib --test metrics --test cli --test replication_network -- -D warnings
```

Native tests cover concurrency without lost increments, fixed names, unknown/binary
sections, slow/fast Pub/Sub, cancellation, EXEC counts, saturated queues, timeouts,
quota, expiration, and output limits. One uses four shards: it holds an accepted
request without execution, queries INFO while the global snapshot waits for that
request, and checks gauges after apply. Four integration tests check two TCP
scenarios with four shards and AOF diagnostics for append, compaction, fatal disk
failure, and recoverable size rejection. Two CLI tests check exit codes, hiding
invalid values, and absence of effects on the listener, AOF directory, and readiness file.
The TCP replication test starts actual processes with four shards, checks FULL
and delta, queries INFO inside EXEC, stops the primary, and verifies that lag
becomes unknown; it then promotes the replica and checks role, epoch, and head.
The state test covers an old session trying to update a new one, partial resumption,
a still-unknown cursor, and an upstream temporarily behind the applied position.
The offline test also uses an occupied internal port and confirms no AOF or
readiness creation.

These results check the functional contract on Windows. They are neither a benchmark
of instrumentation cost nor a Docker/Linux gate. Integrated milestone validation
should repeat only the paths required by the combined changes and record the actual SHA.
