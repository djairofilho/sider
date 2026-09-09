//! Small, bounded RESP2 oracle that does not import the product codec.

use std::io::{self, Read, Write};

const MAX_BYTES: usize = 4 * 1024 * 1024;
const MAX_LINE: usize = 1024;
const MAX_NODES: usize = 1024;
const MAX_DEPTH: usize = 16;

#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Response>>),
}

#[derive(Debug, PartialEq, Eq)]
pub struct Observed {
    pub value: Response,
    pub bytes: Vec<u8>,
}

/// Encodes only arrays of bulk strings as requests, independently of Sider.
pub fn request(args: &[Vec<u8>]) -> Vec<u8> {
    let mut wire = Vec::new();
    write!(&mut wire, "*{}\r\n", args.len()).unwrap();
    for arg in args {
        write!(&mut wire, "${}\r\n", arg.len()).unwrap();
        wire.extend_from_slice(arg);
        wire.extend_from_slice(b"\r\n");
    }
    wire
}

pub fn read_response(reader: &mut impl Read) -> io::Result<Observed> {
    let mut bytes = Vec::new();
    let mut nodes = 0;
    let value = read_value(reader, &mut bytes, &mut nodes, 0)?;
    Ok(Observed { value, bytes })
}

fn invalid(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

fn take(reader: &mut impl Read, wire: &mut Vec<u8>, count: usize) -> io::Result<Vec<u8>> {
    if count > MAX_BYTES - wire.len() {
        return Err(invalid("response exceeds the byte budget"));
    }
    let mut bytes = vec![0; count];
    reader.read_exact(&mut bytes)?;
    wire.extend_from_slice(&bytes);
    Ok(bytes)
}

fn line(reader: &mut impl Read, wire: &mut Vec<u8>) -> io::Result<Vec<u8>> {
    let mut result = Vec::new();
    loop {
        if result.len() >= MAX_LINE {
            return Err(invalid("RESP line exceeds the limit"));
        }
        let byte = take(reader, wire, 1)?[0];
        match byte {
            b'\r' => {
                if take(reader, wire, 1)? != b"\n" {
                    return Err(invalid("CR not followed by LF"));
                }
                return Ok(result);
            }
            b'\n' => return Err(invalid("LF without CR")),
            _ => result.push(byte),
        }
    }
}

fn integer(bytes: &[u8]) -> io::Result<i64> {
    let digits = bytes.strip_prefix(b"-").unwrap_or(bytes);
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return Err(invalid("invalid RESP integer"));
    }
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| invalid("RESP integer overflow"))
}

fn read_value(
    reader: &mut impl Read,
    wire: &mut Vec<u8>,
    nodes: &mut usize,
    depth: usize,
) -> io::Result<Response> {
    *nodes += 1;
    if *nodes > MAX_NODES {
        return Err(invalid("response exceeds the node budget"));
    }
    let marker = take(reader, wire, 1)?[0];
    let header = line(reader, wire)?;
    match marker {
        b'+' => Ok(Response::Simple(header)),
        b'-' => Ok(Response::Error(header)),
        b':' => Ok(Response::Integer(integer(&header)?)),
        b'$' | b'*' => {
            let length = integer(&header)?;
            if length == -1 {
                return Ok(if marker == b'$' {
                    Response::Bulk(None)
                } else {
                    Response::Array(None)
                });
            }
            let length = usize::try_from(length)
                .map_err(|_| invalid("negative or excessively large RESP length"))?;
            if marker == b'$' {
                if length > MAX_BYTES - wire.len() || MAX_BYTES - wire.len() - length < 2 {
                    return Err(invalid("bulk exceeds the byte budget"));
                }
                let payload = take(reader, wire, length)?;
                if take(reader, wire, 2)? != b"\r\n" {
                    return Err(invalid("invalid bulk terminator"));
                }
                Ok(Response::Bulk(Some(payload)))
            } else {
                if depth >= MAX_DEPTH || length > MAX_NODES - *nodes {
                    return Err(invalid("array exceeds the depth/node limits"));
                }
                let mut values = Vec::new();
                for _ in 0..length {
                    values.push(read_value(reader, wire, nodes, depth + 1)?);
                }
                Ok(Response::Array(Some(values)))
            }
        }
        _ => Err(invalid("unknown RESP prefix")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn request_uses_binary_lengths_and_literal_framing() {
        assert_eq!(
            request(&[b"ECHO".to_vec(), b"\0\r\n\xffA".to_vec()]),
            b"*2\r\n$4\r\nECHO\r\n$5\r\n\0\r\n\xffA\r\n"
        );
    }

    #[test]
    fn reads_all_types_and_preserves_exact_frame_boundary() {
        let bytes = b"*7\r\n+OK\r\n-ERR sample\r\n:-9223372036854775808\r\n$-1\r\n$0\r\n\r\n$3\r\n\0\xffX\r\n*-1\r\n+NEXT\r\n";
        let mut input = Cursor::new(bytes);
        let observed = read_response(&mut input).unwrap();
        assert_eq!(
            observed.value,
            Response::Array(Some(vec![
                Response::Simple(b"OK".to_vec()),
                Response::Error(b"ERR sample".to_vec()),
                Response::Integer(i64::MIN),
                Response::Bulk(None),
                Response::Bulk(Some(Vec::new())),
                Response::Bulk(Some(b"\0\xffX".to_vec())),
                Response::Array(None),
            ]))
        );
        assert_eq!(observed.bytes, bytes[..bytes.len() - 7]);
        assert_eq!(read_response(&mut input).unwrap().bytes, b"+NEXT\r\n");
    }

    #[test]
    fn rejects_bad_framing_overflow_and_declared_allocation_attacks() {
        for input in [
            &b"?x\r\n"[..],
            b"+x\n",
            b"+x\rZ",
            b":+1\r\n",
            b":\r\n",
            b":9223372036854775808\r\n",
            b"$-2\r\n",
            b"$3\r\nab",
            b"$1\r\naZZ",
            b"$9223372036854775807\r\n",
            b"*1024\r\n",
            b"*9999999999999999999999\r\n",
        ] {
            assert!(read_response(&mut Cursor::new(input)).is_err(), "{input:?}");
        }
        let mut nested = b"*1\r\n".repeat(MAX_DEPTH + 1);
        nested.extend_from_slice(b"+x\r\n");
        assert!(read_response(&mut Cursor::new(nested)).is_err());
        let mut long = vec![b'+'; MAX_LINE + 1];
        long.extend_from_slice(b"\r\n");
        assert!(read_response(&mut Cursor::new(long)).is_err());
    }

    #[test]
    fn truncations_fail_instead_of_becoming_empty_responses() {
        let frame = b"*2\r\n$3\r\nabc\r\n:42\r\n";
        for end in 0..frame.len() {
            assert!(read_response(&mut Cursor::new(&frame[..end])).is_err());
        }
        assert!(read_response(&mut Cursor::new(frame)).is_ok());
    }
}
