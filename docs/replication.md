# Sider → Sider replication

Sider asynchronously replicates strings, hashes, lists, sets, sorted sets,
absolute deadlines, and resolved transactional batches. Instances require the
same Sider version, record format, and shard configuration.
The transport is proprietary. It does not support Redis replicas, Redis Cluster,
or automatic failover.

The primary responds to writes without waiting for replicas. Promotion can lose
everything the primary acknowledged that the replica has not yet applied. There
is no guaranteed loss limit in seconds or operations during a disconnection.
Status shows the applied position and the latest progress announced by the upstream;
it cannot predict writes that have not yet been observed.

## Configuration

Replication requires AOF on both instances. Each process uses its own data directory.
The internal listener serves replication, snapshot export, and administration.
It has no authentication or TLS; use loopback or a controlled private network.
The epoch identifies stream continuity without authenticating the operator or machine.

Example primary on Linux with four shards:

```sh
SIDER_ADDR=127.0.0.1:6379 \
SIDER_SHARDS=4 \
SIDER_AOF_DIR=./primary-aof \
SIDER_REPLICATION_ADDR=127.0.0.1:7380 \
./sider
```

In another terminal, configure the replica:

```sh
SIDER_ADDR=127.0.0.1:6380 \
SIDER_SHARDS=4 \
SIDER_AOF_DIR=./replica-aof \
SIDER_REPLICATION_ADDR=127.0.0.1:7381 \
SIDER_REPLICA_OF=127.0.0.1:7380 \
./sider
```

In PowerShell, set these same variables with `$env:NAME = 'value'` in each process's
terminal and run `sider.exe`. `SIDER_REPLICA_OF` must be unset on the initial primary.
The upstream address requires a literal IP address and a nonzero port.

| Variable | Default | Contract |
| --- | --- | --- |
| `SIDER_REPLICATION_ADDR` | Unset | Enables the listener; accepts port zero |
| `SIDER_REPLICA_OF` | Unset | Replica upstream; also enables the local listener at `127.0.0.1:0` |
| `SIDER_REPLICATION_READY_FILE` | Unset | Separate JSON with PID, host, and actual internal port |
| `SIDER_REPLICATION_BACKLOG_BYTES` | `134217728` | Maximum frame bytes retained in the primary's history |
| `SIDER_REPLICATION_BACKLOG_BATCHES` | `4096` | Maximum number of batches in history |
| `SIDER_REPLICATION_MAX_CONNECTIONS` | `4` | Concurrent internal sessions, including export and administration |
| `SIDER_REPLICATION_FRAME_TIMEOUT_MS` | `5000` | Deadline per frame read/write, including a partial header |
| `SIDER_REPLICATION_SYNC_TIMEOUT_MS` | `30000` | Capture deadline and total snapshot transfer deadline |
| `SIDER_REPLICATION_RECONNECT_MIN_MS` | `100` | Initial reconnect interval |
| `SIDER_REPLICATION_RECONNECT_MAX_MS` | `5000` | Exponential backoff ceiling, limited to 60000 ms |

History must fit at least one maximum-sized AOF record with replication framing.
Deadlines and capacities must be positive; the synchronization deadline cannot be
shorter than the frame deadline. Configuration is validated before readiness.
The receiver must accept the primary's advertised record, mutation count, and dataset
limits. Incompatible configuration fails before installing data.

`SIDER_REPLICATION_READY_FILE` uses the same format and atomic publication as the
[RESP readiness file](network.md#readiness-file). The two paths must differ.
The internal JSON is published before the RESP JSON, after initializing the workers
and durable role. Port zero is discovered through this file, without reserving and
closing a port before starting the process. Check the PID of the child you started
and query `sider-replica --status`; send `PING` only to the RESP listener.

## Snapshot and history

A single global sequence comes from the AOF writer. Snapshot capture blocks new
mutations, waits for accepted requests and ongoing expirations, collects every
shard, and synchronizes the AOF at position `S`. The history subscription starts
at `S` under the same barrier. Transmission occurs after primary writes are released.

The receiver validates the version, limits, binary key order, entry count, checksum,
types, and quotas before replacing the dataset. An incomplete or invalid transfer
preserves the previous state. The received snapshot stays in memory bounded by the
advertised budget; the temporary AOF file is created during installation.

Installation publishes a new AOF generation with data, sequence, role, and epoch.
Only then does it replace all maps under the global barrier. An accepted read sees
either the previous state or the complete new state. Canceling the connection does
not abandon an already accepted installation midway through this switch. On restart,
recovery selects the published generation and removes unpublished installation
temporary files.

After the snapshot, the primary sends batches of resolved postimages in sequence.
The replica sends an ACK only after persisting, applying, and synchronizing the batch.
This synchronization also occurs with `SIDER_AOF_SYNC=everysec`. TTL retains its
absolute deadline, and a transaction remains a single batch. An exact repeat of the
last frame in the same session only repeats the ACK; gaps and conflicting duplicates
end the session without applying the invalid batch.

The primary bounds history by bytes and batches. Each subscriber holds at most one
incremental frame outside that history. A slow subscriber does not block writes or
other subscribers. If the required history is lost, it receives `FULL` on reconnect.
Backlog, snapshots, indexes, buffers, and queues consume memory beyond the dataset's
logical quota. That quota is not an RSS guarantee.

## Reconnection and read-only mode

The replica reconnects with the epoch and sequence recovered from its own AOF.
The primary responds `CONTINUE` only if the identity and history still cover that
position. A different epoch or insufficient history requires `FULL`. Each primary
startup generates a new epoch; its in-memory history does not survive restart.
Continuity is not simulated using the sequence number alone.

While the role is replica, client writes return `READONLY`. The check occurs at
the connection and again at the worker, including for `MULTI`/`EXEC`. Reads hide
expired keys, but the replica does not create its own tombstones or run active
expiration. The primary replicates expiration removals in AOF order.
Machine clocks must remain synchronized to interpret absolute deadlines;
local execution uses a monotonic clock.

Pub/Sub remains ephemeral and local to each instance. Published messages and
subscriptions are not part of the dataset, snapshot, or replication history.
`WATCH` is also local; installing a new snapshot invalidates observations of
the replaced state.

## Status and manual promotion

Query the internal listener:

```sh
./sider-replica --addr 127.0.0.1:7381 --status
```

The JSON output contains `role`, `epoch`, `sequence`, `connected`,
`upstream_sequence`, `backlog_bytes`, `full_syncs`, and `partial_syncs`.
On a replica, `sequence` is the applied and synchronized position. On a primary,
it is the journal head, observed after append; it does not prove that all workers
have finished applying at that instant. Lag in batches can be calculated only when
the replica is connected and the upstream position is known within the same epoch.
When disconnected, lag is unknown, even if the last observed number matches the
local position.

For a planned switch, stop writes to the old primary, wait for the positions to
converge, and redirect clients after promotion. For recovery with an unavailable
upstream, explicitly accept the loss of the unapplied suffix. The command requires
a loopback connection:

```sh
./sider-replica --addr 127.0.0.1:7381 --promote
```

The response arrives only after publishing the primary role and a new epoch in the
AOF, disconnecting the old session, and releasing writes. A client timeout can leave
the outcome unknown; query status before deciding the next step. On restart, the
promoted role takes precedence over a stale `SIDER_REPLICA_OF`, preventing the
instance from following that upstream again. An AOF still marked as replica requires
a configured upstream to start; removing the variable does not promote the dataset.

The old primary is not demoted automatically. Remove it from write traffic before
reusing it to avoid two instances accepting independent changes. To recreate it as
a replica, stop the process, preserve a backup of the old directory, and start with
a new AOF directory and `SIDER_REPLICA_OF` pointing to the new primary.
This version provides neither online demotion nor consensus between primaries.

## Durability and validation

The AOF v3 header records role and epoch in the same generation as the snapshot.
Typed records retain their format; legacy v1/v2 files are still accepted. An older
binary that does not recognize v3 must not open this AOF. The primary's synchronization
policy remains the one in the [AOF guide](persistence.md), regardless of replica ACKs.
Forced termination tests demonstrate recovery after process crashes. On Windows,
they do not demonstrate resilience to power failure or directory synchronization
equivalent to that available on Unix.

The focused tests are:

```sh
cargo test --locked --lib replication_config
cargo test --locked --test replication_protocol
cargo test --locked --test replication_journal
cargo test --locked --test replication_storage
cargo test --locked --test replication_persistence
cargo test --locked --test replication_network replication_
```

`replication_network` runs five scenarios with real binaries: types/TTL/transactions
and export; invalid transfer and ACK followed by crash; slow replica and history
loss; primary restart; and lagging promotion with upstream cutoff.
`replication_persistence` includes six controlled kills during installation, at the
three publication points for each role. Storage tests verify the barrier,
cancellation, quota, and absence of mixed state across shards.
An additional test stops a connected primary and replica with a partial internal
frame in progress and checks removal of the readiness files.

The `release_replication_gate` runner executes the five process scenarios and records
observed convergence and lag measurements before promotion. It issues a receipt only
with valid release context at the exact SHA, according to the [release guide](releases.md).
Local tests do not automatically approve candidate gates or replace tests on the
distributed packages and image.
