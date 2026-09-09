# Strings, expiration, and quota

R02 expands core commands without changing boundaries between protocol, execution,
and networking. All valid commands still pass through an owning worker, which
applies every operation or batch without suspending execution.

## String operations

| Form | Result |
| --- | --- |
| `EXISTS key [key ...]` | Count of existing-key occurrences; duplicates count |
| `INCR key` / `DECR key` | New i64 integer; an absent key starts at zero |
| `MGET key [key ...]` | Ordered array of values or nulls, retaining duplicates |
| `MSET key value [key value ...]` | `OK` after applying the entire batch; the last value for a key wins |

Increments accept only canonical decimal values: `0`, or an optional minus sign
followed by digits with a first digit from `1` to `9`. `+1`, `-0`, leading zeroes,
spaces, empty input, nonnumeric bytes, and values outside i64 are rejected.
Operation overflow has a distinct error from an already-invalid value. Both retain
the value and its expiration deadline.

Complete arity and format are validated before sending to the worker. Under default
limits, `EXISTS` and `MGET` admit up to 1,022 keys and `MSET` up to 511 pairs, also
subject to byte limits. These values derive from the RESP node limit. A batch
rejected for arity or quota does not apply pairs partially.

`MGET` shares immutable `Bytes` payloads. Repeating keys can produce a response
larger than the request. If it exceeds `SIDER_MAX_RESPONSE_BYTES`, the encoder
rejects it before emitting bytes and the connection closes; state remains. Cancelling
the receiver after `MSET` acceptance does not undo the batch.

## SET options

```text
SET key value [NX | XX] [EX seconds | PX milliseconds | KEEPTTL] [GET]
```

- `NX` writes only when absent; `XX`, only when present.
- `GET` returns the previous value, including when a condition prevents the write.
  Without `GET`, an unsatisfied condition returns a null bulk.
- `EX` and `PX` set a positive relative deadline, validated before testing the
  condition. Zero, negative duration, overflow, or an unrepresentable deadline
  are rejected.
- `KEEPTTL` retains the previous deadline. Basic SET, including with `GET`, removes
  the deadline when no time option is present. `MSET` also removes prior deadlines.
- Option order is unrestricted. Repeating `NX`, `XX`, `GET`, or `KEEPTTL` is
  accepted; repeating `EX` or `PX` uses its final argument. `NX` with `XX`, `EX`
  with `PX`, and explicit expiration with `KEEPTTL` return `ERR syntax error`.

`EXAT`, `PXAT`, `IFEQ`, `IFNE`, `IFDEQ`, and `IFDNE` are not in this subset. The
implemented forms were compared against [Redis 8.10.1 string code](https://github.com/redis/redis/blob/8.10.1/src/t_string.c)
and an instance of that version.

## Expiration

| Form | Result |
| --- | --- |
| `EXPIRE key seconds` / `PEXPIRE key milliseconds` | `1` if the deadline was applied or the key removed; `0` when absent |
| `TTL key` / `PTTL key` | Remaining time; `-1` with no deadline; `-2` when absent or expired |
| `PERSIST key` | `1` when removing an existing deadline; `0` with no key or deadline |

A nonpositive `EXPIRE`/`PEXPIRE` deadline removes the key immediately. Commands
accept only these basic forms, without `NX`, `XX`, `GT`, or `LT` options. `TTL`
rounds milliseconds with `(remaining + 500) / 1000`, like the
[Redis implementation](https://github.com/redis/redis/blob/8.10.1/src/expire.c).

`Clock` provides injectable monotonic and Unix clocks. On write, `Entry` retains a
monotonic deadline, absolute millisecond deadline, and generation. The monotonic
clock governs the running process, so later civil-clock jumps do not alter expiry.
The absolute value enables future persistence; replay must convert it again to a
monotonic clock after restart.

Every key access treats `deadline <= now` as expired. Beyond passive expiration,
the worker processes up to 64 events every 100 ms without scanning the entire map.
An ordered index retains at most one event per key: replacement, deletion, and
`PERSIST` remove the old one. Checking generation and deadline prevents an old
event from deleting a new value. Large expired batches can require several rounds.

## Logical quota

`SIDER_MAX_DATASET_BYTES` defaults to 64 MiB and accepts positive values up to
`isize::MAX`. Configuration is checked before opening the server. An entry consumes
`key.len() + value.len() + 128` bytes. The fixed charge includes metadata and the
time index, even when the entry has no expiration.

This is logical accounting, not an RSS measurement. It excludes reserved table
capacity, allocator, network buffers, queued requests, and responses still sharing
removed values. The limit does not guarantee that the process consumes only that
amount of physical memory.

`SET`, `MSET`, and increments calculate final state before applying growth. In
`MSET`, duplicates are reduced to the last value before that calculation. Exceeding
quota returns `OOM dataset memory quota exceeded`, retaining values, deadlines,
and live-key counters. There is no automatic eviction. `DEL`, expiration, and
smaller replacements free consumption; already-due passive expirations can be
collected while inspecting keys of a command later rejected. Unvisited expired
keys count until active or passive cleanup.

## Reproducible validation

```sh
cargo test --locked --test strings --test expiration --test memory
cargo test --locked --test tcp r02
cargo test --locked --lib storage::worker::tests::r02
cargo test --locked --test compatibility -- --ignored --exact sider_matches_redis --nocapture
```

The Windows MSVC run with Redis 8.10.1 in Docker compared 3,588 R01 binary
responses and 461 R02 responses. R02 includes 48 SET combinations, boundary
integers, rejections, binary values, duplicates, and MGET with three 1 MiB payloads.
Reports separate these comparisons from temporal observations: 100 ms tolerance
for `PTTL` and one second for `TTL`, plus polling with a five-second deadline until
expiration was observed in both processes. The number of temporal iterations varies
with scheduling and is not presented as a binary comparison.

Tests with an injected clock verify the exact boundary, old generation, bounded
active cleanup, quota, and state retention. TCP tests verify a 128-byte MGET
response and rejection of 129 bytes with no partial output, as well as indivisible
MSET between clients. This is local implementation evidence; it neither approves
distributed packages nor claims native Linux execution.
