# Codec RESP2

The `sider::resp` module represents and encodes the five RESP2 types without
knowing commands, sockets, or storage. Representing a frame does not mean
accepting it as a request: that validation belongs to the command parser.

## Contents

- [Types and use](#types-and-use)
- [Limits](#limits)
- [Incremental decoding](#incremental-decoding)
- [Atomic encoding](#atomic-encoding)
- [Tests and scope](#tests-and-scope)

## Types and use

`Frame` distinguishes `Simple`, `Error`, `Integer`, `Bulk`, and `Array`. Payloads
use `Bytes`, including simple strings and errors; UTF-8 conversion is not required.
`Bulk(None)` and `Array(None)` are null, rather than empty values or arrays.

```rust
use bytes::BytesMut;
use sider::resp::{Decoder, Frame, RespLimits, encode};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let limits = RespLimits::default();
    let mut decoder = Decoder::new(limits)?;
    let mut input = BytesMut::from(&b"$5\r\nhello"[..]);
    assert!(decoder.decode(&mut input)?.is_none());
    assert_eq!(&input[..], b"$5\r\nhello");

    input.extend_from_slice(b"\r\n+OK\r\n");
    let frame = decoder.decode(&mut input)?.expect("complete frame");
    assert!(matches!(frame, Frame::Bulk(Some(_))));
    assert_eq!(&input[..], b"+OK\r\n");

    let mut output = BytesMut::new();
    encode(&frame, &mut output, limits)?;
    assert_eq!(&output[..], b"$5\r\nhello\r\n");
    Ok(())
}
```

Integers accept an optional `+` or `-` sign and decimal digits, without spaces.
The value must fit in `i64`. Bulk/array lengths accept unsigned digits or exactly
`-1` for null. Other negatives and a `+` sign in those lengths are invalid.
Leading zeroes can be normalized on output.

Simple strings and errors cannot contain CR or LF. Bulk strings accept every byte,
including CRLF, NUL, and invalid UTF-8 sequences. Delimiters still require exact
CRLF. The codec does not accept RESP3 prefixes or inline commands. These types and
delimiters follow the [RESP specification](https://redis.io/docs/latest/develop/reference/protocol-spec/).

## Limits

| `RespLimits` field | Default | What it counts |
| --- | --- | --- |
| `max_frame_bytes` | 4 MiB | One complete frame, including framing |
| `max_bulk_bytes` | 1 MiB | One bulk payload |
| `max_line_bytes` | 1 KiB | A complete line, including prefix and CRLF |
| `max_nodes` | 1,024 | Root, arrays, and all their elements |
| `max_depth` | 16 | Array levels; the root array counts as 1 |

Null and empty arrays also count as nodes and levels. A root scalar has depth
zero. All limits must be positive; bulk and line limits cannot exceed the frame
limit. Configurable depth is capped at 128 to also bound recursive destruction of
the `Frame` tree.

These limits do not measure RSS, capacity of all allocations, or the dataset. The
connection still needs to limit its buffer, queue, time, and request count. A
buffer with two valid frames can exceed `max_frame_bytes` in total: the decoder
limits the current frame and does not reject a valid suffix merely because it exists.

## Incremental decoding

`Decoder::new` validates limits. An instance belongs to a single stream:

- `Ok(None)`: incomplete frame; buffer bytes remain intact.
- `Ok(Some(frame))`: consumes exactly one frame; preserves the suffix and resets
  parsing state for the next frame.
- `Err(...)`: invalid format or exceeded limit; the decoder becomes unusable.
  Do not attempt to recover synchronization on the same stream.

While a frame is incomplete, the caller may only append bytes to the same buffer.
Do not remove or alter its already-supplied prefix. A detectable shortening yields
`BufferChanged`; there is no prefix hash to detect arbitrary caller changes.

The scanner retains its cursor, array stack, and metadata for elements already
read. Later reads do not reparse the entire prefix. Lengths, offsets, node count,
and depth are validated before reserving payloads. Payloads are copied into
independent `Bytes` only after the complete frame validates. A small key must not
retain a connection's entire buffer.

## Atomic encoding

`encode` first validates the whole tree and calculates its size with checked
arithmetic. Only then does it reserve space and append bytes. A content,
configuration, or limit error leaves `dst` bytes and capacity intact.

The budget is for the new frame, not the prefix already in `dst`. The network
layer must enforce its own response limit. The encoder does not create a partial
response for an array whose last element is invalid.

As elsewhere in the project, there is no promise to recover from process
out-of-memory conditions. Returned errors cover inputs and limits, not allocator
failure.

## Tests and scope

```sh
cargo test --locked --lib resp::
cargo test --locked --test resp_codec
```

Tests use literal frames of all five types, fixtures already verified against
Redis, every short fragmentation point, byte-by-byte delivery, suffixes, limits,
and invalid inputs. Properties generate valid trees and arbitrary bytes. A work
counter specific to decoder tests detects quadratic reprocessing without measuring
wall-clock time.

This validates the isolated codec, not the TCP server or command semantics.
Integration tests and publication gates are in the [testing guide](testing.md)
and [release guide](releases.md).
