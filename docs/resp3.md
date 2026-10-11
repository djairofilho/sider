# RESP3 protocol

Connections start in RESP2. After any required authentication, `HELLO 3`
negotiates RESP3 for that connection; `HELLO 2` restores RESP2. `HELLO`
without a version reports the current protocol.
Negotiation does not change other connections, storage, TTL, or persistence.
The response includes `server`, `version`, `proto`, `id`, `mode`, `role`, and
`modules`. These identify Sider and its current role; they do not impersonate
Redis. The connection ID is local to the running server.

The published 1.0.1 binary has no `HELLO` or RESP3 support. In the current
checkout's explicit `legacy-loopback` profile, only the version argument is
implemented:
`HELLO ... AUTH ...` and `SETNAME` return `ERR unsupported HELLO options`
without changing protocol. In the current checkout's protected profile,
`HELLO 2 AUTH username password` and `HELLO 3 AUTH username password`
authenticate and negotiate together. A protected connection must authenticate
before `HELLO` without `AUTH`. `SETNAME` remains unsupported. The
[security implementation](security-implementation.md) records the current
profile limits; [issue #93](https://github.com/djairofilho/sider/issues/93)
remains open for its full integration and release evidence.
An unsupported integer version returns `NOPROTO unsupported protocol version`;
a malformed version returns `ERR Protocol version is not an integer or out of range`.
`HELLO` inside `MULTI` is explicitly rejected, preserves the protocol, and poisons
the queue, so `EXEC` returns `EXECABORT`. Negotiate before starting a transaction.

## Command responses

| Response or command | RESP2 | RESP3 |
| --- | --- | --- |
| Missing value, conditional SET without a value, watched EXEC abort | Null bulk or null array | Null (`_`) |
| `HGETALL` | Alternating field/value array | Map (`%`) |
| `SMEMBERS` | Array | Set (`~`) |
| `ZSCORE` | Bulk decimal or null | Double (`,`) or null |
| `ZRANGE ... WITHSCORES` | Alternating member/score array | Array of member/double pairs |
| Subscription acknowledgements and messages | Arrays | Push frames (`>`) |
| `PING` while subscribed | Pub/Sub pong array | Ordinary PONG or bulk payload |
| Ordinary commands while subscribed | Restricted subset | Same commands as an unsubscribed connection |
| Simple strings, binary values, integers, application errors | Existing RESP2 framing | Same framing |

The response conversions also apply to commands queued in `MULTI`. `EXEC` with
subscriptions retains Redis's special logical command count, including push
acknowledgements interleaved in execution order. Notifications to that same
connection follow the executed commands. This special Redis stream shape must
be interpreted with command context; a general aggregate decoder alone cannot
infer its logical reply slots from the physical pushes.

## Codec and request limits

The codec represents RESP2 types plus RESP3 null, boolean, double, big number,
blob error, verbatim string, map, set, push, and attribute. Payloads remain binary,
including blob errors and verbatim content. Doubles and big numbers retain
validated ASCII bytes so spelling and equality do not depend on floating-point
NaN comparisons. A verbatim payload contains a three-byte format and colon.
Application errors continue to use simple errors; the extra codec types do not
imply new commands that emit every type.

Maps and attributes contain alternating key/value frames. Their declared pair
count is checked before multiplying it into the node budget. An attribute is
decoded as a separate metadata frame; the caller associates it with the following
data frame. Sider commands do not currently emit attributes or accept them on
requests.

Executable requests remain nonempty arrays of non-null bulk strings in both
protocols. Other root types or argument types fail safely as invalid requests.
RESP3 streamed strings and aggregates (`$?`, `*?`, chunks, and end markers) are
explicitly unsupported and close the connection after a protocol error.

The same frame size, bulk size, line size, depth, node count, input buffer,
formation deadline, response budget, and write deadline limits apply to both
protocols. Decoding preserves incomplete input and consumes exactly one complete
frame. Encoding validates the entire tree before modifying the output buffer.
RESP2 byte behavior is retained for clients that do not negotiate RESP3.

## Reproducible verification

```sh
cargo test --locked --lib resp::
cargo test --locked --lib connection::
cargo test --locked --test resp3 resp3_tcp_literal_subset
cargo test --locked --test resp3 resp3_differential_subset -- --ignored --nocapture
```

Codec tests cover every implemented type literally, every fragment boundary,
byte-at-a-time input, binary content, concatenated suffixes, malformed scalars,
unsupported streaming, and aggregate/output limits. TCP tests use a bounded
response reader independent of Sider's codec. The external differential test uses
the image, digest, Linux platform, and Redis/CLI versions pinned in
[the release manifest](../releases/plan.json), currently Redis 8.10.1.
It compares raw command bytes including transaction responses and Pub/Sub pushes.
Handshake server/version/ID metadata are deliberately checked structurally
because they identify different servers.

Protocol definitions follow the
[Redis protocol specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
and [HELLO command reference](https://redis.io/docs/latest/commands/hello/).
The declared compatible command responses are limited to the recorded cases;
this extension does not claim full Redis client compatibility.
