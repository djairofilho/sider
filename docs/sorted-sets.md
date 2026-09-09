# Sorted sets

Sorted sets associate each binary member with a 64-bit IEEE-754 score. Results
follow ascending score and, on ties, member bytes. Ordering is independent of
UTF-8, locale, or platform.

## Commands

| Form | Contract |
| --- | --- |
| `ZADD key score member [score member ...]` | Returns the number of new members. Updating a score does not increase the count; the last pair for a member wins. |
| `ZREM key member [member ...]` | Returns removed members, without counting duplicates; the final removal eliminates the entry. |
| `ZCARD key` | Returns cardinality, or zero when absent. |
| `ZSCORE key member` | Returns the score as a bulk string, or null when the member/key is absent. |
| `ZRANGE key start stop [WITHSCORES]` | Returns members in the inclusive range. `WITHSCORES` interleaves member and score in the RESP2 array. |

Negative indexes count from the end. Inverted or out-of-range intervals return an
empty array, following the same rules as [lists](collections.md). A command on
another type returns `WRONGTYPE` without changing data.

`ZADD` accepts only basic pairs. `NX`, `XX`, `GT`, `LT`, `CH`, and `INCR` are
outside the subset. `ZRANGE` does not accept `BYSCORE`, `BYLEX`, `REV`, or
`LIMIT`. These forms are rejected before execution; rejection text for unsupported
options does not promise equivalence with Redis.

## Scores

The parser accepts decimal, exponential notation, hexadecimal values, and infinities
according to cases verified in Redis 8.10.1. `inf`, `+inf`, and `-inf` are valid;
`Infinity` spellings and case variants are also accepted. Negative zero is
normalized to zero. Spaces, `NaN`, finite overflow, and underflow of a nonzero
value to zero are invalid. Representable subnormals are valid.

Every `ZADD` score is validated before querying or changing the entry. An invalid
score in the final pair also prevents preceding pairs. `ERR value is not a valid
float` allows continued use of the connection.

`ZSCORE` and `WITHSCORES` use the same converter. For example, `1e-7` produces
`1e-7`, `1e-6` produces `0.000001`, and `1e20` produces `1e+20`. The rendering
of `1e23` is `99999999999999990000000`, matching reference rounding. The safe Rust
converter adapts Redis 8.10.1 [fpconv](https://github.com/redis/redis/tree/8.10.1/deps/fpconv),
with its notices and Boost 1.0 license preserved in the module. This conversion
adds no C-code dependency or Cargo-dependency change.

## Indexes, TTL, quota, and AOF

A per-member index and a `(score, member)` index are updated together. Snapshot
payloads are shared through `Arc`, and changes produce a new post-image before
apply. Member lookup uses `HashMap`; ordering uses `BTreeSet`. Rank-range retrieval
walks the prefix to `start`. No performance equivalence with Redis is promised.

Quota accounts for `128 + key.len()` per entry and `member.len() + 96` per member,
logically covering both indexes and the score. This value does not measure RSS. A
batch exceeding quota is wholly rejected, including score updates for existing
members. Accepted writes preserve TTL; final removal and expiration free the entry,
expiration index, and quota.

AOF v1 uses tag `6` for the sorted-set post-image. Scores are stored as IEEE-754
bits, without decimal reconversion during replay. The decoder rejects NaN, empty
sets, and duplicate members even with a valid checksum. Earlier types retain their
tags and representations.

Responses obey RESP and output limits described in the [network guide](network.md).
If `ZRANGE` exceeds the limit, the connection closes without sending a partial
array or changing the set. `WITHSCORES` consumes two response nodes per member.

## Reproducible verification

```powershell
cargo test --locked --test sorted_sets
cargo test --locked --test tcp r06_
cargo test --locked --test collections_differential -- --ignored --exact sorted_sets_match_redis --nocapture
```

The seven native tests include 4,096 generated index changes and 28,658 combinations
of exponent, mantissa, and sign for converter round trips. They also verify quota,
TTL, arity, rejections, binary order, numeric limits, and AOF.

The differential passed with Sider on Windows and the Redis/CLI 8.10.1 image pinned
in `releases/plan.json`: 8,561 byte-for-byte responses compared. Beyond fixtures,
it uses four seeds with 512 operations each and 32 batches with up to 400 scores
derived from IEEE-754 patterns. In batches, NaN/inf patterns are filtered to keep
the batch valid; separate fixtures verify infinities and NaN rejection. Scores,
pairs, and ordering are never normalized by the comparator.

The [complete AOF-writer tests](types-persistence.md) also verify crashes,
compaction, score recovery, and evolution of the strings fixture. Evidence separates
format migration from migration between internal-baseline executables; it does not
demonstrate native Linux execution or release-package approval.
