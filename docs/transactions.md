# Transactions

`MULTI`, `EXEC`, `DISCARD`, `WATCH`, and `UNWATCH` belong to the TCP connection.
A transaction may access one shard. Keys and arguments retain arbitrary bytes;
hash tags can place related keys in the same shard.

## Queueing and execution

`MULTI` starts the queue and replies `OK`. Each accepted command replies `QUEUED`
and has no effect until `EXEC`. `EXEC` returns an array of replies in command order;
an empty queue returns an empty array. `DISCARD` removes the queue and observations.
EOF, cancellation, and shutdown also discard a queue not yet sent to the worker.

An arity or parsing error, an attempt to cross shards, or a queue limit makes the
transaction invalid. `EXEC` then returns `EXECABORT` and no queued command runs.
Nested `MULTI` and `WATCH` inside `MULTI` return an error without invalidating the
previous queue. `EXEC` and `DISCARD` outside `MULTI` return an error.

Execution errors, such as an invalid integer, overflow, `WRONGTYPE`, or quota,
occupy their place in the array; other commands continue. There is no rollback of
valid operations. This distinction between queueing failure and individual failure
follows the [Redis transaction contract](https://redis.io/docs/latest/develop/using-commands/transactions/).

The worker prepares all commands with a frozen clock, resolves conditions, TTL,
and quota, and gathers final post-images in one `ResolvedBatch`. Only touched keys
enter temporary state. No other request from that shard interleaves with the batch.
The global snapshot barrier retains request admission through application and reply,
including when the client cancels its wait after acceptance.

## WATCH and resource release

`WATCH key [key ...]` observes keys until `EXEC`, `DISCARD`, `UNWATCH`, or the end
of the connection. A write by the same connection also invalidates observation.
Writing the same value, creating and removing a key, or reaching its expiration
deadline are conflicts. Removing an already absent key is not a conflict. `EXEC`
with a conflict returns a null array (`*-1\r\n`) and neither writes nor applies the
batch. `UNWATCH` inside `MULTI` is queued, so it does not remove a conflict already
detected before execution. Expiration semantics follow
[Redis WATCH](https://redis.io/docs/latest/commands/watch/).

Each connection keeps exclusively owned tokens. The registry shares a change flag
among observers of the same key and removes it when that generation is invalidated
or its last token released. There is no permanent version map for every key ever
used. A new observation receives its own state when an earlier generation has
already been invalidated.

Before registering WATCH, the worker resolves pending expiration of those keys.
With AOF enabled, tombstones must first be accepted by the writer. A limit rejection
does not create tokens. A deadline for an already watched key is also checked in
`EXEC`, even before active or passive registry cleanup.

## Persistence and Pub/Sub

With AOF enabled, a batch with durable changes produces one append before changing
Store. Replay applies that complete record. An individual error does not prevent
the batch's valid operations from being persisted. An aborted WATCH and a
transaction without durable changes produce no mutation record. fsync limits and
guarantees follow AOF configuration.

`PUBLISH`, `SUBSCRIBE`, and `UNSUBSCRIBE` can be queued in `MULTI`. After the
append is accepted, a short hub section applies database changes and performs
ephemeral effects in command order. It does not await sockets. Pub/Sub is not in
the AOF. An append/fsync failure or record-size rejection prevents both batch
application and pending publications and subscriptions.

A subscription inside `EXEC` changes the format of later `PING`s. Database commands
already queued keep running, and the connection ends in the mode corresponding to
remaining subscriptions. New commands received after `EXEC` obey subscriber-mode
restrictions.

In RESP2, `SUBSCRIBE a b` and `UNSUBSCRIBE a b` generate one confirmation per
argument inside `EXEC`, while the outer header retains command count. A message
published to the same connection appears after all batch replies. The test retains
the literal transcript observed in Redis 8.10.1; this deferral also appears in the
[Redis write path](https://github.com/redis/redis/blob/8.10.1/src/networking.c).

All `EXEC` output, including confirmations and messages for the connection itself,
is encoded in a bounded buffer before the first write. If encoding exceeds the
limit, the connection closes without transmitting an incomplete prefix of that
reply. The already accepted batch may have been applied; timeout or disconnection
after acceptance does not promise no effects or trigger automatic retry.

## Limits

| Configuration | Default | Effect |
| --- | --- | --- |
| `SIDER_TRANSACTION_MAX_COMMANDS` | 128 | Commands retained in the queue |
| `SIDER_TRANSACTION_MAX_BYTES` | 1048576 | Sum of complete RESP sizes of queued commands |
| `SIDER_WATCH_MAX_KEYS` | 128 | Distinct keys watched per connection |

The byte limit includes arguments, lengths, and framing. An invalid queue releases
commands already retained and keeps only the state needed to return `EXECABORT`.
WATCH excess rejects the new command without removing earlier observations. These
limits are specific to Sider and do not simulate Redis memory policy.

RESP input and reply limits, Pub/Sub channels and queue, dataset quota, worker
queue capacity, total request deadline, and AOF record size also apply. Connection
count bounds the sum of client-owned queues and observations; the logical budget
does not represent process RSS.

## Reproduction

```sh
cargo test --locked --lib transactions_
cargo test --locked --test transactions transactions_tcp_contract -- --exact
cargo test --locked --test transactions_persistence transactions_ -- --nocapture
cargo test --locked --test transactions transactions_matches_redis -- --ignored --exact --nocapture
```

The differential uses the Redis 8.10.1 image pinned in `releases/plan.json`, a
disposable Sider server, and byte comparison. Native tests cover the queue, absence
of early effects, WATCH, a paused clock, shards, snapshot, reply limit, and AOF
failures without Pub/Sub effects. Persistence tests use the worker's real batch,
truncation of every record prefix, and nine process-crash points, including
publication of compacted generations.

In the local Windows run, 16 implementation tests, 91 binary comparisons with
Redis, the 274-byte Pub/Sub transcript, 71 record prefixes, and nine process
crashes passed. Replay after compaction preserved hash, list, set, and sorted set
written in the same batch, including with `WRONGTYPE` in another position. This
evidence does not replace the Linux execution required for publication.

The `transactions` gate runs these checks and publishes a receipt only after
success and process cleanup. The release context requires Linux and a clean
checkout. Local validation of the internal milestone does not produce a publication
receipt; the 1.0 gate must still run at the exact bundle SHA.
