# Pub/Sub

Milestone R08 implements TCP `SUBSCRIBE`, `UNSUBSCRIBE`, and `PUBLISH`, with
binary channels and messages. The registry is shared by connections in one server
instance and is separate from `Store` and worker queues. Publications do not
create keys, have no replay, and disappear when the process exits.

## Commands and transitions

| Command | Normal mode | RESP2 subscriber mode |
| --- | --- | --- |
| `SUBSCRIBE channel [channel ...]` | Subscribes and enters subscriber mode | Adds subscriptions |
| `UNSUBSCRIBE [channel ...]` | Confirms a count of zero | Removes the specified subscriptions; without arguments, removes all |
| `PUBLISH channel message` | Returns the number of queues that accepted the message | Returns a forbidden-command error |
| `PING [message]` | `PONG` or a bulk containing the message | Two-bulk array: `pong` and message, empty when absent |
| Other implemented commands | Follow their normal contracts | Return an error without executing against the database |

The final removal returns the connection to normal mode. Each subscription or
removal argument receives a confirmation, including duplicate or absent channels.
Duplicate subscriptions do not duplicate recipients. The confirmation contains
the operation name, channel, and remaining subscribed-channel count. `UNSUBSCRIBE`
with no active channels returns a null channel and count zero. These formats follow
the [Redis RESP2 contract](https://redis.io/docs/latest/develop/pubsub/).

A message uses the `[message, channel, payload]` array, with three bulk strings.
An empty channel, null bytes, CRLF, and non-UTF-8 bytes are preserved. In
subscriber mode, `PSUBSCRIBE`, `PUNSUBSCRIBE`, sharded commands, `QUIT`, and
`RESET` are not yet implemented. Error text for recognized commands reproduces
Redis and refers to this broader family; the table above delimits the effective
subset.

`UNSUBSCRIBE` without arguments confirms channels in ascending binary order. This
order is a Sider choice; matching Redis order is not required when multiple
channels exist. Differentials compare the empty case and the one-channel-remaining
case.

## Delivery, ordering, and slow clients

`PUBLISH` immediately sends to bounded queues. Its count confirms queue acceptance,
not socket reading. There is no retry, client acknowledgment, or persistence.
Disconnections can lose notifications that were already accepted. Ephemeral delivery
is compatible with Redis [at-most-once semantics](https://redis.io/docs/latest/develop/pubsub/).

The hub mutex orders concurrent publications. It protects only metadata and
`try_send`, without waiting for sockets. Subscribers receive the same publication
order, and one task writes each socket. A complete notification never interleaves
with bytes from a command response. Already accepted messages are drained before
subscription-change confirmations; removal prevents further messages from that
channel after confirmation. Between consecutive commands, the loop allows
notifications to progress.

| Configuration | Default | Effect |
| --- | --- | --- |
| `SIDER_PUBSUB_MAX_CHANNELS` | 32 | Distinct channels per connection; excess rejects the whole command without altering subscriptions |
| `SIDER_PUBSUB_QUEUE_CAPACITY` | 32 | Pending notifications per subscriber |
| `SIDER_WRITE_TIMEOUT_MS` | 5000 | Deadline to write each response or notification |
| `SIDER_MAX_RESPONSE_BYTES` | 4194304 | Complete-notification limit, including framing |

A full queue immediately removes all of that client's subscriptions and signals
connection closure, including when it is blocked on writing. The removed client
is not included in that publication's count. This policy and count limits are
specific to Sider; they do not reproduce Redis buffer limits. Other clients and
the database continue to progress. Timeout, EOF, shutdown, cancellation, and
errors also release subscriptions through the connection guard.

The queue holds up to its configured limit plus one notification being written.
Subscription changes can temporarily retain the bounded drained batch while new
notifications enter the queue. The total remains bounded by capacities, connection
count, and RESP limits. `Bytes` shares immutable payloads among recipients. These
limits are not an RSS quota. A notification exceeding the response limit is
rejected before any send with `ERR pubsub message exceeds response limit`.

## Integration with storage and transactions

`connection::run_with_pubsub` receives the hub owned by the server. The parser
produces typed commands; the connection intercepts the three Pub/Sub commands
before calling `DbHandle::execute`. A direct call to `Store` returns
`ERR command requires connection context`, protecting this boundary.

`MULTI` can queue Pub/Sub commands alongside database commands. The worker approves
the single durable append before applying effects; an AOF error prevents the batch's
publications and subscriptions. Ephemeral execution preserves hub order without
waiting for sockets and does not produce Pub/Sub records in the AOF. In `EXEC`,
messages to the same connection appear after batch responses. Complete output
remains subject to the response limit. See [transactions](transactions.md) for
WATCH, RESP2 framing, limits, and integration evidence.

## Reproduction and evidence

```sh
cargo test --locked --lib pubsub
cargo test --locked --test pubsub pubsub_tcp_contract -- --exact
cargo test --locked --test pubsub pubsub_matches_redis -- --ignored --exact --nocapture
```

The last command requires Linux Docker and verifies the Redis 8.10.1 image pinned
in `releases/plan.json`. On September 8, 2026, it passed on Windows with 245
binary comparisons, 64 messages for two subscribers, and 16 reconnections, using
disposable Sider processes and Redis containers. The native TCP test passed, as
did deterministic scenarios for concurrent publication, a full queue, a slow
socket alongside a fast socket and worker, limits, timeouts, and cleanup.
Connection tests use controlled I/O and a paused clock; they do not depend on
sleeps.

The `pubsub` gate runs native tests with the `pubsub_` filter, requires real cases,
runs the differential, and only then publishes `receipt-pubsub.json`. The receipt
requires the Linux release context and a clean checkout; an isolated local run does
not generate a publication receipt. The gate in the final 1.0 context must still
run at the bundle SHA. See the [release flow](releases.md).
