# Sider networking and lifecycle

This guide describes the `R01-04` implementation: RESP2/TCP server, owning worker,
configuration, deadlines, and readiness. R02 adds string commands, expiration, and
logical quota to the same network path. See the [compatibility matrix](compatibility.md)
to distinguish implemented support from equivalence already demonstrated against Redis.

## Contents

- [Running locally](#running-locally)
- [Configuration](#configuration)
- [Requests and limits](#requests-and-limits)
- [Acceptance and uncertain outcomes](#acceptance-and-uncertain-outcomes)
- [Read and write deadlines](#read-and-write-deadlines)
- [EOF and errors](#eof-and-errors)
- [Shutdown and supervision](#shutdown-and-supervision)
- [Readiness file](#readiness-file)
- [Code and validation](#code-and-validation)

## Running locally

From the repository root, with the toolchain in `rust-toolchain.toml`:

```sh
cargo run --locked -- --help
cargo run --locked -- --version
cargo run --locked
```

Without arguments, the binary starts the server at `127.0.0.1:6379`. It runs in the
foreground; use `Ctrl+C` to request shutdown. `--help` and `--version` do not open
the listener. Unknown arguments, invalid configuration, and bind failure terminate
the program with an error exit code.

The default address is local. This version has no authentication, ACL, or TLS.
Configuring an IP outside loopback emits a warning but neither blocks the bind nor
adds protection. Use only a controlled environment, without public exposure.
Startup, failure, and shutdown logs go to `stderr`, without logging request keys
or values.

To choose another port in PowerShell:

```powershell
$env:SIDER_ADDR = '127.0.0.1:6380'
try {
    cargo run --locked
} finally {
    Remove-Item Env:SIDER_ADDR
}
```

In a POSIX shell:

```sh
SIDER_ADDR=127.0.0.1:6380 cargo run --locked
```

## Configuration

An unset variable uses its default. Sizes are in bytes, counts are integers,
and deadlines are in milliseconds. `1 MiB` equals `1048576` bytes.

| Variable | Default | Meaning |
| --- | --- | --- |
| `SIDER_ADDR` | `127.0.0.1:6379` | Listener IP literal and port |
| `SIDER_MAX_CONNECTIONS` | `32` | Connections admitted concurrently |
| `SIDER_WORKER_QUEUE_CAPACITY` | `32` | Requests that can wait in each worker's queue; each EXEC occupies one request |
| `SIDER_SHARDS` | `1` | Owning workers, between 1 and 256; fixed configuration |
| `SIDER_PUBSUB_MAX_CHANNELS` | `32` | Distinct channels per connection |
| `SIDER_PUBSUB_QUEUE_CAPACITY` | `32` | Pending notifications per connection; a full queue disconnects |
| `SIDER_TRANSACTION_MAX_COMMANDS` | `128` | Commands retained between MULTI and EXEC |
| `SIDER_TRANSACTION_MAX_BYTES` | `1048576` | Sum of RESP bytes in queued commands |
| `SIDER_WATCH_MAX_KEYS` | `128` | Distinct keys watched per connection |
| `SIDER_MAX_FRAME_BYTES` | `4194304` | Complete input frame, including framing |
| `SIDER_MAX_BULK_BYTES` | `1048576` | Payload of each input bulk string |
| `SIDER_MAX_LINE_BYTES` | `1024` | Input line or header, including prefix and CRLF |
| `SIDER_MAX_NODES` | `1024` | Frame nodes, counting root, arrays, and elements |
| `SIDER_MAX_DEPTH` | `16` | Array levels; the root array counts as level 1 |
| `SIDER_MAX_INPUT_BUFFER_BYTES` | `4194304` | Unconsumed bytes in the connection buffer |
| `SIDER_MAX_RESPONSE_BYTES` | `4194304` | Complete response, including framing |
| `SIDER_MAX_DATASET_BYTES` | `67108864` | Logical dataset usage, including 128 bytes per entry |
| `SIDER_FRAME_TIMEOUT_MS` | `10000` | Frame formation from the first byte read |
| `SIDER_REQUEST_TIMEOUT_MS` | `5000` | Total deadline for queue submission and response waiting |
| `SIDER_WRITE_TIMEOUT_MS` | `5000` | Writing one complete response |
| `SIDER_SHUTDOWN_TIMEOUT_MS` | `5000` | Draining before requesting abortion of remaining tasks |
| `SIDER_READY_FILE` | Unset | Optional path for readiness JSON |

### Validation

The binary validates before opening the listener. The `serve` API also validates
its configuration, although its caller has already opened the listener.

- The address requires a literal IP and port. `localhost:6379` is not accepted;
  use `127.0.0.1:6379`. IPv6 uses brackets, such as `[::1]:6379`. Port `0` is valid
  and requests an ephemeral port from the operating system.
- Numbers accept only ASCII digits. Signs, whitespace, suffixes such as `MiB`,
  empty text, and overflow are rejected. Leading zeros are allowed. Addresses
  and numbers require valid UTF-8 text.
- All limits, counts, and deadlines must be positive. The maximum allowed depth
  is `128`; connections and queue capacity cannot exceed
  `tokio::sync::Semaphore::MAX_PERMITS`.
- `max_bulk_bytes` and `max_line_bytes` cannot exceed `max_frame_bytes`.
  `max_frame_bytes` cannot exceed `max_input_buffer_bytes`.
- Byte limits cannot exceed `isize::MAX` on the target. Counts use `usize`;
  deadlines received from the environment use `u64` milliseconds. Beyond parsing,
  the deadline must fit when added to the monotonic `Instant` clock.
- `max_response_bytes` must hold at least `128` bytes and also the largest
  configured bulk string with framing: `B + decimal_digits(B) + 5`, where `B`
  is `max_bulk_bytes`. The sum is checked for overflow.
- `SIDER_READY_FILE` preserves the native path, including non-UTF-8 paths, but
  rejects an empty path. Directory, permissions, and hard-link support are checked
  by the file operation after binding.

Accepted configuration does not guarantee memory availability for every possible
allocation. It also does not guarantee that a command fits if limits are reduced
too far. For example, the node count includes the root array and every argument.

## Requests and limits

Executable requests are nonempty RESP2 arrays of non-null bulk strings.
Command names are ASCII case-insensitive; keys and values preserve all bytes,
including empty content, `NUL`, and non-UTF-8 data.

Each connection has its own decoder, input buffer, and output buffer. Reads are
bounded by available space before receiving more bytes. The codec validates the
frame before materializing its payloads; details are in the [RESP2 guide](resp.md).

Data commands pass through the chosen shard's bounded queue; `PING` and `ECHO`
use shard zero. Each worker owns its map and applies the command without `await`
during mutation. Across connections, worker receive order applies, with no guarantee
of socket arrival order or strict fairness.

The [sharding guide](sharding.md) defines hash tags, quota division, and rejection
of cross-shard multikey commands before submission. Independent queues allow one
shard to progress even when another is saturated.

Pub/Sub uses a registry separate from the dataset. The connection serializes
acknowledgments and notifications through the same writer, with a bounded queue
and write deadline. In subscriber mode, PING responds without entering the data
queue. SUBSCRIBE, UNSUBSCRIBE, and PUBLISH do not reach the Store.
See [Pub/Sub](pubsub.md).

The client can send multiple commands in one TCP write. The connection decodes,
submits to the worker, receives, and writes a response before dispatching the next
command. There is one in-flight request per connection; pipelining does not mean
parallel execution or batching. Excess connections are closed without creating
a persistent task. Closing a connection releases its slot.

These limits are not a process memory quota. The queue counts messages, buffers
may retain allocated capacity, and the system maintains its own socket buffers.
Connection counts and maximum sizes must be considered together, as in the budget
in the [0.1 plan](../PLAN.md#limits-and-lifecycle).

The R02 dataset has its own logical quota and expiration. `SET`, `MSET`, and
increments reject growth beyond the budget without eviction. Each entry counts
key and value bytes plus a fixed 128-byte charge; this does not bound RSS.
See the [strings guide](strings.md#logical-quota) for accounting and its limitations.

## Acceptance and uncertain outcomes

Completion of the send to the `mpsc` channel is the acceptance boundary:

1. Before it, canceling the send does not modify the map.
2. After it, the request belongs to the worker and will execute even if the
   connection drops or the response receiver is discarded, provided the worker
   continues running and is not aborted.
3. The response confirms the result to the client. Losing the response does not
   undo an already applied mutation.

`SIDER_REQUEST_TIMEOUT_MS` covers waiting for queue capacity and for the response
with one deadline. It does not restart when queue space becomes available. A timeout
closes the connection and prevents that client's next dispatch. Sider neither retries
commands automatically nor sends a response promising the absence of effects.

If a timeout or disconnection occurs after acceptance, the outcome is uncertain
for the client. This rule also applies to worker failure after submission.
Reconnecting does not cancel the previous request.

## Read and write deadlines

An idle connection, with no bytes yet from the next frame, has no idle timeout.
The formation deadline starts when the connection reads the first byte of an
incomplete frame. New fragments do not renew that deadline.

In a pipeline, the first byte of a suffix may have been read alongside the previous
command. If that suffix is incomplete, its deadline keeps running while the previous
response is processed. On returning to reads, an expired deadline closes the
connection before accepting more bytes for that frame.

A frame already complete in the buffer does not expire merely because execution or
writing the previous response took too long. The formation timeout is not a pipeline
execution deadline. A suffix that began in a later read uses that read's timestamp,
not the previous frame's timestamp.

Each response has its own write deadline. The encoder validates the entire response
before changing the output buffer. The input line limit does not limit internal
errors: output uses `SIDER_MAX_RESPONSE_BYTES` for total size and bulk/line sizes,
while preserving node and depth limits.

If writing fails or times out, the connection closes. Even if some bytes have reached
the client, there is no attempt to continue with a new response on that connection.

## EOF and errors

EOF without pending bytes closes the connection normally. If the client closes
only its write half, complete frames already received are processed in order and
their responses can still be read. A partial frame at EOF is discarded as truncated,
without execution.

Arity errors, unknown commands, and unsupported `SET` options produce error responses
and allow the next command. They do not reach storage. Unknown-command text is
simplified and does not include private arguments.

An invalid request shape is fatal even when the RESP2 frame is well formed.
Invalid framing and exceeded limits also close the connection. When possible,
the server attempts to write a short error before closing. It does not search for
a next command to recover synchronization. Frame timeout, truncated EOF, or write
failure does not guarantee an error response to the client.

## Shutdown and supervision

On Unix, the binary handles `SIGINT` and `SIGTERM`. On Windows, it registers
`Ctrl+C`, `Ctrl+Break`, and console-close events. Forced system termination may
prevent draining; it is not equivalent to the normal shutdown flow.

When shutdown is requested, the server:

1. Closes the listener and signals the connections and worker.
2. Cancels idle reads and sends still waiting for acceptance.
3. Closes queue admission and drains already accepted commands.
4. Allows attempted responses for accepted requests, respecting their deadlines.
5. Waits for tasks. If draining exceeds the configured deadline, it requests their
   abortion, logs forced shutdown, and waits for them to terminate.

The `5000` ms default bounds draining; it does not promise to kill a thread or
process in exactly five seconds. Tokio task abortion is cooperative and completes
only when tasks yield execution. Stuck synchronous code is not forcibly interrupted
by this mechanism.
[Tokio cancellation contract](https://docs.rs/tokio/latest/tokio/task/index.html#cancellation).

Unexpected worker termination or panic closes the listener and terminates the server
with an error. Connection failures are logged and do not bring down other connections.
Canceling the `serve` future requests abortion of its tasks without leaving them
detached from the supervisor. This exit path does not offer the draining guarantee
of normal shutdown.

Without AOF configured, the in-memory dataset is lost when the process exits.
With AOF, normal shutdown drains accepted requests and synchronizes the writer;
cancellation or forced termination follows the limits of the `always` or
`everysec` policy described in the [persistence contract](persistence.md).

## Readiness file

`SIDER_READY_FILE` lets tests and packagers discover the actual port without
reserving a port, releasing it, and racing for a new bind. The parent directory
must exist and be writable. For a test process, use a unique path and
`SIDER_ADDR=127.0.0.1:0`.

After configured AOF recovery, signal registration, and binding, the binary
publishes complete JSON. Illustrative example:

```json
{"pid":12345,"host":"127.0.0.1","port":49152}
```

`pid` is Sider's own PID. `host` and `port` come from the address returned by
the listener, including the port chosen by the system. A wildcard bind such as
`0.0.0.0` also appears that way in the JSON; smoke tests use loopback to obtain
an explicit connection destination.

The content is written and synchronized to a temporary file in the same directory.
A hard link publishes the destination only when the JSON is complete. This requires
hard-link support in the filesystem, such as NTFS or ext4. There is no fallback
to a partial write at the destination. The operation fails if the destination
already exists, without replacing it; the binary does not start serving in that case.
[`std::fs::hard_link` contract](https://doc.rust-lang.org/std/fs/fn.hard_link.html).

During normal shutdown, Sider compares the destination's contents with the JSON it
published and attempts to remove it only if they still match. Changed content is
preserved. Use a controlled directory: comparison and removal are not atomic against
concurrent replacements. Removal failure and forced termination can leave stale
files; readiness is not a durable health record.

The consumer must retain the handle of the child process it started, check that
it remains alive, compare its PID with the JSON, and perform a RESP2 `PING` on
the reported port. Merely checking whether some process has the recorded PID is
insufficient because PIDs can be reused. A stale file does not authorize terminating
a process found by that number. JSON publication proves binding; it does not replace
the TCP smoke test or guarantee that the process will remain available.

The [package guide](packages.md#manual-smoke-test) defines
use of this contract for binaries actually extracted from packages.

## Code and validation

The internal [replication](replication.md) listener has its own limits and deadlines.
`SIDER_REPLICATION_READY_FILE` publishes its actual address in separate JSON with
the same `pid`, `host`, and `port` fields. The RESP file retains exactly the original
three fields. Both use `ReadyFile`, after configuring the durable role and starting
the workers; consumers of the internal port query status through its own protocol.

| File | Responsibility |
| --- | --- |
| [src/config.rs](../src/config.rs) | Defaults, injectable parsing, and validation |
| [src/connection.rs](../src/connection.rs) | Buffers, pipelines, framing, responses, and deadlines |
| [src/storage/worker.rs](../src/storage/worker.rs) | Queue, acceptance, and owned map |
| [src/server.rs](../src/server.rs) | Admission, supervision, and draining |
| [src/readiness.rs](../src/readiness.rs) | JSON publication and cleanup |
| [src/main.rs](../src/main.rs) | Arguments, logs, binding, signals, and readiness |

Before integrating changes at this stage, run focused tests and the local suite.
The commands below do not, by themselves, constitute a record of passing results:

```sh
cargo test --locked --lib config::
cargo test --locked --lib storage::worker::
cargo test --locked --lib connection::
cargo test --locked --lib server::
cargo test --locked --lib readiness::
cargo test --locked --test tcp
cargo test --locked --test cli
cargo xtask check
cargo doc --locked --no-deps
cargo build --locked --release
```

Validation of `R01-04` must cover both platforms, exact limits, binary bytes,
fragmentation, pipelining, backpressure, cancellation, slow clients, half-close,
worker failures, shutdown, and readiness. Ordering and deadline tests use explicit
synchronization and controlled I/O; arbitrary pauses do not prove event order.

Record actual results, commands, target, and limitations in the PR and the
[testing guide](testing.md). Differential comparison against the reference,
`redis-cli` usage, and extracted packages have their own gates. Network unit
tests neither replace that evidence nor approve candidate publication.
CI remains disabled through 1.0, according to the [release workflow](releases.md).
