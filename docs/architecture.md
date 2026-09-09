# Sider architecture

Sider uses one Rust crate with a testable library and four executables: server,
AOF migration, backup, and replica administration. This document summarizes the
current boundaries. The [initial plan](../PLANO.md) preserves the historical
0.1 design; the [1.0 matrix](compatibility-matrix.md) defines the current subset.

## Implemented

| Part | Current responsibility |
| --- | --- |
| Library | Expose reusable configuration for the binary and tests |
| RESP2 codec | Represent, validate, encode, and decode frames within limits |
| Command parser | Validate format/arity and move arguments into typed commands |
| Synchronous storage | Execute strings and typed collections on a private `HashMap<Bytes, Entry>`, with TTL and logical quota |
| Worker | Own the map, receive commands through the bounded queue, and reply through oneshot |
| Connection and server | Coordinate RESP2/TCP, limits, timeouts, ordering, and supervision |
| Configuration | Validate address, limits, deadlines, and optional readiness file |
| Configuration errors | Represent validation failures with explicit types |
| Binary | Process help/version; open the listener, register signals, and publish readiness |
| Persistence | Resolve batches, synchronize AOF, recover data, and compact global snapshots |
| Replication and backup | Transport bounded snapshots and batches, install data, and preserve absolute TTL |
| Metrics | Expose observed state through INFO and configuration diagnostics without starting listeners |
| Development tools | Pin toolchain and dependencies; check formatting, lint, tests, and build |

Normal execution serves TCP with Tokio. The parser and map remain testable without
a runtime. Production dependencies are `thiserror`, `bytes`, `tokio`, `tracing`,
`tracing-subscriber`, `getrandom`, `serde_json`, and `sha2`, with selected features.
Project code uses
`#![forbid(unsafe_code)]`.

`ServerConfig` groups the address, limits, and deadlines. `from_env` reads the
process environment; `from_lookup` allows explicit test values without changing
the global environment. The binary treats unknown arguments and invalid
configuration as failures, with a nonzero exit code.

## Implemented boundaries

| Component | Knows about | Does not need to know about |
| --- | --- | --- |
| RESP2 codec | Frames, bytes, and protocol limits | Sockets, commands, and storage |
| Command parser | Frames and command contracts | I/O and database state |
| Storage | Typed commands and the key/value map | RESP and networking tasks |
| Worker | Storage, request queue, and replies | Framing and connection buffers |
| Connection | Buffer, codec, parser, worker handle, and socket | Direct map access |
| Server | Listener, configuration, tasks, and shutdown | Details of each command |

The worker, connection, and server were added in R01-04. The structure does not
create empty modules ahead of time or multiple crates without independent consumers.

### Storage ownership

Each `Store` owns its `HashMap` and executes commands synchronously, with a single
owning worker. `DbHandle` routes a command by key and checks all keys before enqueue.
Each shard has a bounded `mpsc` channel; replies use `oneshot`.
`PING` and `ECHO` pass through worker zero. The default remains one shard.

Pub/Sub uses a separate hub that protects only metadata and nonblocking sends
to bounded queues. Each connection owns an RAII subscription, also released on
cancellation. The socket has a single writer for acknowledgments/notifications;
PING in subscriber mode does not use the worker. No subscription belongs to the dataset.

The queue defines execution order across connections. Each connection waits for
its response before dispatching the next command. A connection keeps one request
in flight per client and preserves that client's concatenated command order,
without promising strict fairness between clients.

### Protocol and binary data

The decoder is incremental: its input may contain a fragment, a complete frame,
or several frames. It consumes exactly one complete frame, preserves the buffer
suffix, and maintains bounded state while waiting for more bytes.

The codec represents all five RESP2 types. The parser accepts requests only as
nonempty arrays of non-null bulk strings. Keys and values preserve their bytes,
including empty content and non-UTF-8 sequences.

Input payloads are copied into independent allocations after full validation.
This prevents a small key from retaining a large network buffer. The decision may
be revisited with measurements. The [codec guide](resp.md) details contracts and limits.

The parser moves payloads into `Command` without copying their content again.
`Reply` does not depend on channels. Response conversion to `Frame` belongs to
the command layer; the map does not import RESP. `GET` clones the immutable
`Bytes` handle, so an already obtained response remains valid after overwrite or deletion.

R02 adds response arrays and recoverable execution errors. `MGET` preserves order
and shares the same immutable payloads. `MSET` validates the entire batch's net
size change before modifying the map, using the last value for each key.

### Expiration and quota

R05/R06 add `Value` with strings, hashes, lists, sets, and sorted sets.
Collections share immutable snapshots through `Arc`; metadata remains in
`Entry`. Storage prepares complete postimages before applying a batch.
The [collection contracts](collections.md) and [sorted set contracts](sorted-sets.md)
detail accounting, indexes, and AOF representation.

`Entry` stores the value, generation, monotonic deadline, and Unix deadline in milliseconds.
The clock is injectable. Execution uses the monotonic deadline; the absolute
deadline is available for AOF persistence. An ordered index keeps at most one
event per key, removed on rewrites or `PERSIST`; the generation prevents stale
expiration from being applied. The worker processes up to 64 events every 100 ms,
in addition to expiration on access.

`StoreConfig` defines the logical quota. Each entry counts key bytes, value bytes,
and a fixed 128-byte overhead. Growth is rejected before mutation, without eviction.
Buffers, queues, retained responses, and RSS are outside this accounting.
The [strings guide](strings.md) describes commands, invariants, and timing limits.

A format error is fatal to the connection. Arity errors, unknown commands, and
unsupported options are recoverable and never reach the map. Unknown-command
error text does not reproduce client arguments.

### Limits and lifecycle

Limits cover connections, queues, frame sizes, buffers, depth, element count,
and deadlines. The binary validates everything before opening the listener;
`serve` also validates configurations received directly through the API.
The [networking guide](network.md) documents defaults and variables.

A completed send to the queue is the acceptance boundary. After that point, the
worker executes the command even if the client disconnects. Losing the response
may leave the outcome unknown to the client; there is no automatic retry.

Shutdown stops new connections and requests and drains accepted work until the
configured deadline. It then requests cooperative abortion of remaining tasks.
Unexpected worker termination closes the listener. `JoinSet` also aborts
server-owned tasks if the supervision future is canceled.

## Persistence and replication

[R09 replication](replication.md) observes the global AOF writer to assign history
positions. Capture and subscription use the admission barrier at the same point;
network transmission occurs outside it. The replica publishes a complete AOF
generation and swaps maps under global exclusion before ACK. `Runtime` exposes
only role, position, and session state through a short read, without waiting for
disk/network or holding workers. Snapshots, batches, and channels continue to
carry `Bytes` and immutable values.

The database keeps data in memory with independent workers per shard. TTL, quota,
and additional strings were implemented in R02; routing, queues, and multikey
restrictions are in R04. The global AOF writer acknowledges the batch before
applying resolved mutations. An admission barrier captures snapshots from all
workers without sharing maps. The [sharding contract](sharding.md) describes
quota, snapshots, and load evidence; [AOF](persistence.md) describes durability.

Splitting into multiple crates and optimizing copying, allocation, or hashing
will depend on concrete needs and measurements. R04 has local exploratory
measurements without performance promises or speed comparisons with Redis.
