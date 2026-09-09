# Initial Sider plan: version 0.1 record

This document preserves the initial September 2026 design for the version 0.1
RESP2 core. Future-tense wording, the single worker, and the limitations in the
sections below belong to that historical scope; they do not describe every
capability of the current checkout. Stages R01-01 through R01-05 were implemented,
and their evidence remains tied to the SHAs recorded in the [testing guide](docs/testing.md).

The [README](README.md) and [compatibility matrix](docs/compatibility-matrix.md)
describe the current implementation, including collections, TTL, persistence,
shards, transactions, replication, and backup. The [ROADMAP](ROADMAP.md) and its
[versioned manifest](releases/plan.json) define tasks, dependencies, and criteria
through 1.0. The [release guide](docs/releases.md) governs internal checkpoints
and publication; this initial record does not replace those contracts.

CI and automatic publication were deferred until after 1.0. Through and including
1.0, stages advance with proportionate local tests. Milestones 0.1–0.10 do not
require publication. The 1.0 candidate and final release will be promoted manually
with the same SHA and files, preserving functional criteria and actual evidence.

## Contents

- [Starting point](#starting-point)
- [Version 0.1 scope](#version-01-scope)
- [Architecture and Rust decisions](#architecture-and-rust-decisions)
- [Initial structure](#initial-structure)
- [Core contracts](#core-contracts)
- [Limits and lifecycle](#limits-and-lifecycle)
- [Incremental checklist](#incremental-checklist)
- [Completion criteria](#completion-criteria)
- [Risks and evolution](#risks-and-evolution)

## Starting point

Initial inspection on September 7, 2026, before the bootstrap:

- The `NovoRedis` folder is empty, without code or an initialized Git repository.
- Rust and Cargo 1.97.1 are available, with the stable Windows MSVC toolchain.
- The Docker executable is installed. Daemon operation has not been verified.
- `redis-cli` was not found in `PATH`.

The implementation uses a package and binary named `sider`, version `0.1.0`,
and Edition 2024. The current folder keeps its name without affecting the program
name. The bootstrap includes the private `djairofilho/sider` repository, requested
after planning. Crate publication is disabled with `publish = false`.

## Version 0.1 scope

The expected result is to run `sider`, connect with `redis-cli` over RESP2, and
test operations on binary keys and values with a single storage worker.

| Command | Initial contract | Expected evidence |
| --- | --- | --- |
| `PING` | Return `+PONG\r\n` | Byte comparison with Redis |
| `PING message` | Return the message as a bulk string | Test empty and non-UTF-8 bytes |
| `ECHO message` | Return the exact bytes received | Test CRLF within content |
| `GET key` | Bulk string or `$-1\r\n` when absent | Distinguish absence from an empty value |
| `SET key value` | Create or replace; return `+OK\r\n` | Check state with `GET` |
| `DEL key [key ...]` | Count keys actually removed | Test absent and repeated keys |

Behavior references: [PING](https://redis.io/docs/latest/commands/ping/),
[ECHO](https://redis.io/docs/latest/commands/echo/),
[GET](https://redis.io/docs/latest/commands/get/),
[SET](https://redis.io/docs/latest/commands/set/), and
[DEL](https://redis.io/docs/latest/commands/del/).
The exact type of `PONG` was also checked in the
[official Redis source](https://github.com/redis/redis/blob/unstable/src/server.c),
in the `shared.pong` and `pingCommand` definitions.

The following rules resolve scope ambiguities:

- Command names will be compared case-insensitively in ASCII.
  Keys and values will retain every byte, including `NUL` and non-UTF-8 sequences.
- `SET` will accept only its basic form. Additional arguments will produce
  `ERR unsupported SET options` without changing state. This is an intentional
  difference for options valid in Redis and will be in the compatibility matrix.
- Incorrect arity for supported commands will receive a Redis-compatible response.
  Unknown commands will produce `ERR unknown command`. This simplified error
  text will be a documented difference; it will not repeat client arguments.
- Only the default logical database will be provided. There will be no `SELECT`,
  `AUTH`, `HELLO`, `COMMAND`, `CLIENT`, RESP3, or inline command format.
- The codec will represent all five RESP2 types. Executable requests must be
  nonempty arrays of non-null bulk strings.
- Empty/null root arrays and arguments of other types will be rejected as invalid
  request formats. Handling will not be advertised as identical to Redis for
  inputs outside the declared subset.
- `EXISTS` is deferred to 0.2: although listed among the general initial commands,
  the specific 0.1 scope enumerates only the five commands above.

Request forms and types are defined in the
[RESP specification](https://redis.io/docs/latest/develop/reference/protocol-spec/).
The distinction between framing and command errors is supported by
[Redis request processing](https://github.com/redis/redis/blob/unstable/src/networking.c).

Multiple connections and sequential handling of concatenated commands will be
available from 0.1. TCP delivers a byte stream: one read may contain part of a
command or several commands. This provides basic pipeline correctness early,
but does not include batching, multiple executing commands per connection,
or throughput optimizations.

Input limits, backpressure, and basic shutdown are also part of this version.
They are needed to control resources and run reliable tests. TTL, dataset limits,
AOF, shards, replication, new types, and comparative benchmarks come later.

## Architecture and Rust decisions

One crate will provide a testable library and a small binary. Splitting into
multiple crates will be considered when there are independent consumers or
evolution cycles. Initially, modules provide sufficient boundaries.

```text
TCP listener
    |
    +-- connection A: buffer -> decoder -> command parser --+
    |                                                      |
    +-- connection B: buffer -> decoder -> command parser --+--> bounded mpsc
                                                                  |
                                                           single worker
                                                           owns HashMap
                                                                  |
                                                oneshot to the originating connection
                                                                  |
                                                         encoder -> socket
```

### Storage ownership

The worker will own `HashMap<Bytes, Bytes>`. Only that task will access the map.
Each command will be applied synchronously, without `await` while changing state.
A multikey `DEL` will finish before the next command starts.

The worker will be a Tokio task, not a dedicated or core-pinned thread.
The runtime may execute networking tasks in parallel, but map operations will
be serialized. Creating more tasks does not make storage parallel.

We will initially retain the default `HashMap` hasher. Changing the algorithm
requires measurement and analysis of resistance to adversarial input.

### Channels and ordering

Each connection will have a cloneable handle containing an `mpsc::Sender<Request>`.
The envelope will carry a command and a `oneshot` response channel.
The command enum will remain independent of channels so it can be tested and
reused in future replay. The owning-task/message-passing pattern is documented
in the [Tokio channels tutorial](https://tokio.rs/tokio/tutorial/channels).

All valid commands, including `PING` and `ECHO`, will pass through the worker in 0.1.
This simplifies the flow. Channel cost will be evaluated later.

Each connection will wait for execution and response writing before dispatching
its next command. This preserves per-connection ordering and limits each client
to one in-flight request. Across connections, worker receive order applies;
there is no promise of socket-arrival ordering or strict fairness.

### Buffers and binary data

The connection will own its `BytesMut`. The decoder will store indexes and parsing
state, without borrowed references spanning reads or buffer reallocations.

In the first implementation, complete-frame payloads will be copied into independent
`Bytes` before releasing the buffer prefix. The command parser will move those
values into the command without a second content copy.

This deliberate copy prevents a small key from keeping a large network buffer
alive. Retention is a possible consequence of shared storage described in the
[Bytes documentation](https://docs.rs/bytes/latest/bytes/struct.Bytes.html).
`GET` may clone the stored `Bytes` for its response, sharing immutable content.
Zero-copy input will be a later optimization, accompanied by measurements.

### Boundaries and dependencies

The codec will not know about sockets or storage. The command parser will not
change state. The map will not know about RESP. The connection will coordinate these parts.

Production dependencies: `tokio`, `bytes`, `thiserror`, `tracing`, and
`tracing-subscriber`. Enable only the Tokio features needed for networking,
I/O, the multithread runtime, channels, timers, and signals. Tests will use
`proptest` and Tokio's time-control utilities.

Project code will start with `#![forbid(unsafe_code)]`. This does not claim that
all transitive dependencies are free of `unsafe`. The project will not use panics
to handle client-supplied data.

## Initial structure

```text
sider/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── README.md
├── PLAN.md
├── src/
│   ├── lib.rs
│   ├── main.rs
│   ├── config.rs
│   ├── error.rs
│   ├── server.rs
│   ├── connection.rs
│   ├── resp/
│   │   ├── mod.rs
│   │   ├── frame.rs
│   │   ├── decoder.rs
│   │   └── encoder.rs
│   ├── command/
│   │   ├── mod.rs
│   │   ├── parser.rs
│   │   └── reply.rs
│   └── storage/
│       ├── mod.rs
│       ├── store.rs
│       └── worker.rs
├── tests/
│   ├── common/mod.rs
│   ├── tcp.rs
│   ├── robustness.rs
│   ├── differential.rs
│   └── redis_cli.rs
└── docs/
    ├── architecture.md
    └── compatibility.md
```

Files will be created when their stage begins. Unit tests will stay alongside
modules. `store.rs` will contain synchronous execution of the five commands;
there will not be one file per command while these implementations remain small.

We will not create empty expiration, persistence, replication, or routing modules.
The worker handle will be the boundary to preserve when the router arrives in 0.4.

## Core contracts

The snippets below are interface sketches, not complete implementation files.
Every referenced error type will be defined with `thiserror` in its respective stage.

### RESP2 codec

```rust
use bytes::{Bytes, BytesMut};

pub enum Frame {
    Simple(Bytes),
    Error(Bytes),
    Integer(i64),
    Bulk(Option<Bytes>),
    Array(Option<Vec<Frame>>),
}

impl Decoder {
    pub fn new(limits: RespLimits) -> Result<Self, ConfigError>;
    pub fn decode(&mut self, src: &mut BytesMut)
        -> Result<Option<Frame>, ProtocolError>;
}

pub fn encode(frame: &Frame, dst: &mut BytesMut, limits: RespLimits)
    -> Result<(), EncodeError>;
```

We will also use `Bytes` for simple strings and errors to avoid requiring UTF-8
in the codec. Those types will continue to prohibit CR and LF in content, as
required by RESP. Internal replies will use controlled ASCII messages.

Decoder invariants:

1. `Ok(None)` means an incomplete frame. The content of `src` remains intact;
   only the internal scan state advances. The caller can append bytes at the end.
2. `Ok(Some(frame))` consumes exactly one frame, retains the rest of `src`, and
   resets state for the next frame. Each connection will have its own decoder.
3. `Err` means invalid format or an exceeded limit. The connection will close;
   we will not attempt resynchronization after invalid framing.
4. The scanner will preserve its cursor, array stack, and metadata for previously
   read elements. Earlier bytes and elements will not be reprocessed for each fragment.
5. Lengths and offsets will use checked operations. No memory proportional to
   the declared size will be reserved before validating limits.
6. The aggregate limit will include framing bytes. The node count will include
   arrays and their elements, including the root node. Depth and count will be independent.
7. Only after validating the complete frame will the decoder materialize the tree
   and copy payloads. Partial metadata growth will also be bounded.

The encoder will validate total size and simple-type content before changing `dst`.
On error, it will leave no partial response in the buffer. Round trips will be
required only for valid frames within limits; equivalent encodings may be normalized.

### Command and reply

```rust
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Get { key: Bytes },
    Set { key: Bytes, value: Bytes },
    Del { keys: Vec<Bytes> },
}

pub enum Reply {
    Pong,
    Ok,
    Bulk(Option<Bytes>),
    Integer(i64),
}

pub fn parse(frame: Frame) -> Result<Command, RequestError>;
```

`RequestError` will distinguish invalid request structure, which closes the
connection, from command errors, which allow continuation. Arity will be validated
before sending to the worker. `Reply` will be converted to `Frame` in the protocol layer.

The parser will preserve the `DEL` list, including duplicates. The store will count
only successful removals. Input deduplication is not needed to obtain this result.

### Worker and server

```rust
pub struct Request {
    pub command: Command,
    pub reply: tokio::sync::oneshot::Sender<Result<Reply, DbError>>,
}

impl DbHandle {
    pub async fn execute(&self, command: Command) -> Result<Reply, DbError>;
}

impl Store {
    pub fn execute(&mut self, command: Command) -> Reply;
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), ServerError>;
```

`serve` will create the worker and supervise tasks. Accepting an already open
listener will allow tests with `127.0.0.1:0` without competing for fixed ports.
The binary will load configuration, initialize logging, open the listener,
and provide the shutdown signal.

`Store::execute` will be testable without a runtime. The envelope and handle
`Result` will distinguish worker unavailability from a normal missing-key reply.
No contract promises recovery from process out-of-memory conditions.

## Limits and lifecycle

Proposed defaults for local development, adjustable through configuration:

| Parameter | Initial value | Rule |
| --- | --- | --- |
| Address | `127.0.0.1:6379` | Configurable through `SIDER_ADDR` |
| Active connections | 32 | Excess connections close without creating a persistent task |
| Worker queue | 32 messages | Sending waits for available capacity |
| In-flight requests per connection | 1 | Covers queue wait, execution, and response |
| Input frame | 4 MiB | Includes headers, payloads, and CRLF |
| Bulk string | 1 MiB | Applies individually to keys and values |
| Input buffer | 4 MiB | Limit before each read |
| Simple line/header | 1 KiB | Avoid unbounded CRLF searches |
| Nodes per frame | 1,024 | Includes root; up to 1,023 arguments in a flat request |
| Array depth | 16 | Root array counts as level 1 |
| Response buffer | 4 MiB | At most one response being written |
| Incomplete frame assembly | 10 s | From the first byte, without renewing for each fragment |
| Worker send and response wait | 5 s | Total deadline for the `execute` call |
| Response writing | 5 s | Timeout closes the connection |
| Shutdown | 5 s | Bounded drain, followed by forced termination |

`ServerConfig` will group these limits; `RespLimits` will contain the codec portion.
Parameters will be exposed through documented `SIDER_*` variables with strict parsing.
Zero limits, capacities, and timeouts, inconsistent relationships, and overflow
will be rejected before starting the server. Port `0` is an intentional exception:
it allows the system to choose an ephemeral port in network tests.
Tests will use smaller limits to exercise boundaries.

A bounded queue counts messages, not bytes. The per-frame limit, connection count,
and single in-flight request must be analyzed together. As a conservative budget,
consider `connections × (buffer + request + response)`, plus
`queue_capacity × request` and one request executing in the worker, in addition
to metadata and allocation capacity. This may double-count requests from still-active
clients, but covers commands that remain accepted after disconnection, when the
client slot may already have been reused.
The [Tokio mpsc contract](https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html)
documents capacity and backpressure but does not establish a process memory quota.

Dataset size will remain unbounded in 0.1. Multiple valid `SET` commands may still
exhaust available memory. Network limits do not solve this problem; storage accounting
and rejection policy will be implemented in 0.2, without automatic eviction.

Default loopback binding accompanies the unauthenticated scope. Version 0.1 will
be presented as a prototype for local use and testing, without a production-ready
service promise.

### Acceptance, cancellation, and disconnection

A completed send to `mpsc` will be the acceptance boundary. Before that point,
canceling the send attempt does not modify the map. After it, the worker will
execute the accepted command even if the `oneshot` receiver has been dropped.

Any `execute` timeout will close the connection without dispatching another request
from that client. Closing does not cancel already accepted commands. If a timeout
or disconnection occurs after acceptance, the outcome will be unknown to the client.
We will not send a message promising no effects or retry commands automatically.
Failure to deliver a response will not terminate the worker or undo a mutation.

At EOF, already received complete frames will be processed in order. Any remaining
partial frame will be discarded as truncated without execution. A client that closes
only its write side will still be able to receive responses to complete commands.

A command error will produce an error frame and allow reading the next command.
A protocol error will produce a short response, when possible, followed by closing.
An interrupted write will not be resumed on the same connection with a new response.

### Shutdown and supervision

Upon receiving the shutdown signal, the server will stop accepting connections
and new requests. Connections will finish their already accepted request and
attempt to deliver its response. Idle reads and sends not yet accepted will be
canceled. All send handles will be released, the worker will drain what it has
already received, and tasks will be awaited.

The shutdown deadline limits this drain. If it expires, remaining tasks will be
aborted and logged; pending commands will have no completion guarantee during
that forced exit. The server will also monitor unexpected worker failure and
close the listener, avoiding continued client acceptance without functioning storage.

This organization follows the detect, notify, and wait phases described in
[Tokio Graceful Shutdown](https://tokio.rs/tokio/topics/shutdown).
It is basic testable shutdown without durability: the dataset remains entirely in memory.

## Incremental checklist

Each stage ends with compiling code and passing available tests.
Progress depends on these criteria, not a fixed estimate in days.

### 1. Foundation and executable specification

- [x] Create package `sider`, `lib.rs`, a minimal binary, and initial configuration.
- [x] Pin the stable development toolchain, initially 1.97.1, and generate
      `Cargo.lock`. Define an MSRV only if it is also tested.
- [x] Add `forbid(unsafe_code)`, formatting, and lint.
- [x] Write the first configuration and binary integration tests.
- [x] Add literal fixtures for future RESP2 responses.
- [x] Create a compatibility matrix with every item pending.
- [x] Pin Redis and `redis-cli` 8.10.1 and the Linux amd64 digest in the release manifest.
- [x] Prepare the reference infrastructure and verify its execution in `R01-01`.
      A pinned image does not establish compatibility without running tests.

Outcome: `cargo check --locked` and `cargo test --locked` pass. The binary runs
but does not yet offer TCP service. Choosing the reference version precedes
consolidating error fixtures and differential tests.

### 2. Isolated RESP2 codec

- [x] Implement types, limit validation, and encoder.
- [x] Implement an incremental decoder with bounded cursor and stack.
- [x] Test RESP2 types, nulls, empty values, integer extremes, and binary content.
- [x] For each short fixture, test every fragmentation point and byte-by-byte delivery.
      For large payloads, test representative and random splits.
- [x] Test concatenated frames, suffix preservation, and CRLF split across reads.
- [x] Test invalid lengths, overflow, unknown prefixes, invalid CRLF,
      exact limits, and exceeding a limit by one byte/node/level.
- [x] Add `proptest`: valid-frame round trips, equivalent fragmentation,
      correct consumption, and arbitrary input without panics under small limits.

Outcome: codec tested without networking or a database. A test-only work counter
checks approximately linear growth when fragmenting headers and arrays, detecting
quadratic reprocessing without relying on the clock.
Implemented contracts are in the [codec guide](docs/resp.md). Line limits include
the prefix and CRLF; configurable depth is capped at 128 to also protect frame-tree destruction.

### 3. Commands and map semantics

- [x] Implement parsing for the five commands and error classification.
- [x] Implement `Store` with synchronous operations and typed replies.
- [x] Test command-name casing, arity, and rejection of `SET` options.
- [x] Test overwrites, absent keys, empty keys, empty values, and non-UTF-8 bytes.
- [x] Test `DEL a a missing`: count only one removal when `a` exists.
- [x] Test that rejected commands do not change state.

Outcome: complete 0.1 semantics tested without sockets or asynchronous tasks.

### 4. Owning worker and channels

- [x] Implement `Request`, `DbHandle`, and a worker with a bounded queue.
- [x] Test ordering, shared state across handles, and channel closure.
- [x] Test backpressure with a small queue and explicit synchronization.
- [x] Drop the response receiver after accepting `SET` and verify its effect with `GET`.
- [x] Test worker unavailability without panic or infinite waits.

Outcome: no direct concurrent map access; concurrency tested with channels and
barriers, without arbitrary sleeps to determine operation order.

### 5. TCP and the full lifecycle

- [x] Implement `serve`, a per-connection task, bounded buffer, and response encoder.
- [x] Integrate configuration, logging, supervision, and shutdown signal in the binary.
- [x] Test the `SET -> GET -> DEL -> GET` cycle over TCP on an ephemeral port.
- [x] Test two clients sharing state and buffer isolation.
- [x] Test multiple commands sent in one write, with ordered responses.
- [x] Test a recoverable error followed by a valid command on the same connection.
- [x] Test clean EOF, half-close, truncated frames, excess connections, and slow clients.
- [x] Test timeout and shutdown during reads, full queues, and response writing.

Outcome: `cargo run --locked --bin sider` starts the server on loopback. Logs
show startup, errors, and shutdown, without recording keys or values by default.

### 6. Redis and redis-cli compatibility

- [x] Create a differential suite that sends the same bytes to isolated Redis
      and Sider instances. The oracle will not depend solely on the codec under test.
- [x] Compare raw responses, types, codes, and error messages declared compatible,
      as well as state observed through supported commands.
- [x] Run deterministic and generated `SET`, `GET`, and `DEL` sequences,
      with unique key prefixes per case and disposable instances.
- [x] Run all five commands using `redis-cli` in noninteractive RESP2 mode.
- [x] Separate required native tests from the external suite, explicitly marked
      as requiring Redis/CLI. A missing tool does not count as a pass.
- [x] Update the matrix by command, supported form, limitation, test, and version used.

Outcome: external tests pass before declaring 0.1 complete. We will not use
`redis-cli` textual output for binary comparison: it presents values to users,
as described in the [official CLI guide](https://redis.io/docs/latest/develop/tools/cli/).

On Windows, prioritize a Linux test environment with matching Redis and CLI versions
through Docker or WSL. The daemon and connectivity will be checked at that stage.
Docker will provide test infrastructure; a Sider distribution image comes later.
Limitations for clients requiring automatic handshake will be documented.

### 7. Robustness and 0.1 delivery

- [x] Validate resource release after slow clients and multiple disconnections.
- [x] Document architecture, supported commands, limits, and test reproduction.
- [x] Run the final suite and record actual results, including ignored tests.

Quick local stage checks as targets become available:

```powershell
cargo test --locked <filter>
cargo xtask check
```

The check combines formatting, Clippy, the binary build, and native tests.
Changes to interfaces, API documentation, or builds require
`cargo doc --locked --no-deps` and
`cargo build --locked --release`. Record local results in the PR and integrate
with a merge commit without waiting for CI.

External tests have their own documented commands and do not run implicitly
with `cargo test`. Before publication, the build and common tests will be
checked manually on Windows and Linux. Cross-platform CI was verified at bootstrap,
but is disabled in this phase. Workflows were removed; future CI requires
an explicit request after 1.0.

## Completion criteria

Version 0.1 will be ready when:

1. All five commands work through TCP and `redis-cli` within the declared subset.
2. Binary, empty, and absent keys and values have verified behavior.
3. Fragmentation, concatenation, limits, and malformed inputs have passing tests.
4. The differential suite passes against the recorded Redis version.
5. Cancellation, backpressure, slow clients, and shutdown have tested behavior.
6. Formatting, lint, and native test results are recorded.
7. The matrix distinguishes planned behavior from demonstrated compatibility.

This version will not target outperforming Redis. Copying, channel, and allocation
costs will be recorded as hypotheses for future measurement, without performance claims.

## Risks and evolution

| Risk or decision | Planned treatment |
| --- | --- |
| Parser consumes memory or CPU on hostile input | Aggregate limits, checked arithmetic, incremental state, fixtures, and properties |
| Small key retains a large buffer | Copy input payloads into independent allocations |
| Network limits mistaken for database limits | Document unbounded dataset; address accounting in 0.2 |
| Client interprets timeout as an undone operation | Define acceptance at enqueue and unknown outcome afterward |
| Single worker becomes a bottleneck | Measure before partitioning; bound work per command and connection |
| Large command delays others | Limit bytes and arguments; avoid promising strict fairness |
| Differential test repeats the codec's bug | Literal fixtures and independent wire reading for expected responses |
| Version differences mistaken for bugs | Pin Redis, CLI, toolchain, and dependencies; version the matrix |
| Interface grows too early | Keep concrete modules; introduce abstractions when the second case arises |

Evolution decisions are fixed in the manifest; implementation details and evidence
will be produced in the corresponding task, without claiming support ahead of time:

| Version | Contract and deliverable |
| --- | --- |
| 0.2 | Additional strings, `SET` options, passive/active TTL, and quota with growth rejection, without automatic eviction |
| 0.3 | Versioned AOF and checksums, global writer, resolved mutations, fsync policies, recovery, and compaction |
| 0.4 | Stable routing, hash tags, fixed shards, and rejection of cross-shard multikey operations before effects |
| 0.5 | Hashes, lists, and sets with TTL, quota, `WRONGTYPE`, and persistence |
| 0.6 | Sorted sets with basic `ZADD`, `ZREM`, `ZCARD`, `ZSCORE`, and `ZRANGE start stop [WITHSCORES]` |
| 0.7 | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, and `UNWATCH` on the same shard; atomic batch replay |
| 0.8 | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, and `PING` in subscriber mode; bounded queues |
| 0.9 | Asynchronous Sider→Sider replication with the same version/configuration, read-only replicas, and manual promotion |
| 0.10 | Metrics, diagnostics, backup/restoration, and a privately distributed Linux amd64 Docker image |
| 1.0 | Frozen subset, differential audit, one-hour continuous load, migration, and reproducible benchmarks |

AOF will also require validating state-dependent conditions before logging a mutation
and preserving the order of log, application, and response. With periodic fsync,
a success response will not have the same guarantee as per-write fsync.
After a failure, there may also be a durable operation whose response never
reached the client. TTL replay will require a persisted absolute deadline;
`Instant` is not a durable format. Atomic file replacement will need system-specific tests.

For shards, the proposal is parallelism between partitions, not between arbitrary
distinct keys: two independent keys can still fall on the same shard.
Using hash tags does not imply Redis Cluster compatibility. That would require
additional slot, discovery, and redirection contracts.

In 0.4, `DEL`, `MGET`, `MSET`, and other multikey operations will be restricted
to the same shard, with an error before any effect. This restriction changes the
operations accepted by the single worker and must appear in notes and the
compatibility matrix. AOF will initially retain a global writer; 0.4 measurements
will evaluate its cost without changing the recovery guarantee.

In 0.7, queueing errors abort the transaction according to the Redis subset;
individual errors during `EXEC` do not undo other operations. AOF records the
resolved mutation batch so replay cannot apply half a transaction.
Replication in 0.9 preserves that batch and TTL but remains asynchronous,
without automatic failover or support across different versions/configurations.

Redis Cluster, Sentinel, online resharding, RESP3, Lua, blocking operations,
cross-shard transactions, TLS, and ACL come after 1.0. The supported environment
until then is controlled. Every publication requires a candidate and passing
cumulative gates; the bootstrap will not be published as a functional version.

The executable foundation, configuration tests, and Redis/CLI reference fixtures
are implemented. Execution of `R01-01` is documented in the
[testing guide](docs/testing.md). The isolated `R01-02` codec is also implemented,
as are the `R01-03` parser and synchronous storage, and `R01-04` worker and TCP.
`R01-05` integrates differential tests and CLI. `R01-GATE` published the first
candidate, preserved as history. Milestone 0.1 will close as a technical checkpoint
without another publication. The roadmap preserves this plan's IDs.
