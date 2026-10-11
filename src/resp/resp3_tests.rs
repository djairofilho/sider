use super::*;
use bytes::{Bytes, BytesMut};

fn bytes(value: &'static [u8]) -> Bytes {
    Bytes::from_static(value)
}

#[test]
fn resp3_literal_types_roundtrip_at_every_fragment_boundary() {
    let fixtures: Vec<(&[u8], Frame)> = vec![
        (b"_\r\n", Frame::Null),
        (b"#t\r\n", Frame::Boolean(true)),
        (b"#f\r\n", Frame::Boolean(false)),
        (b",1.25\r\n", Frame::Double(bytes(b"1.25"))),
        (b",inf\r\n", Frame::Double(bytes(b"inf"))),
        (b",-inf\r\n", Frame::Double(bytes(b"-inf"))),
        (b",nan\r\n", Frame::Double(bytes(b"nan"))),
        (
            b"(123456789012345678901234567890\r\n",
            Frame::BigNumber(bytes(b"123456789012345678901234567890")),
        ),
        (
            b"!4\r\n\0\xff\r\n\r\n",
            Frame::BlobError(bytes(b"\0\xff\r\n")),
        ),
        (
            b"=6\r\ntxt:\0\xff\r\n",
            Frame::Verbatim(bytes(b"txt:\0\xff")),
        ),
        (
            b"%1\r\n$1\r\nk\r\n_\r\n",
            Frame::Map(vec![Frame::Bulk(Some(bytes(b"k"))), Frame::Null]),
        ),
        (
            b"~2\r\n:1\r\n#f\r\n",
            Frame::Set(vec![Frame::Integer(1), Frame::Boolean(false)]),
        ),
        (
            b">1\r\n+x\r\n",
            Frame::Push(vec![Frame::Simple(bytes(b"x"))]),
        ),
        (
            b"|1\r\n+k\r\n:1\r\n",
            Frame::Attribute(vec![Frame::Simple(bytes(b"k")), Frame::Integer(1)]),
        ),
        (b"%0\r\n", Frame::Map(vec![])),
        (b"~0\r\n", Frame::Set(vec![])),
        (
            b"*2\r\n%0\r\n_\r\n",
            Frame::Array(Some(vec![Frame::Map(vec![]), Frame::Null])),
        ),
    ];
    for (wire, expected) in fixtures {
        let mut output = BytesMut::new();
        encode(&expected, &mut output, RespLimits::default()).unwrap();
        assert_eq!(output.as_ref(), wire);
        for split in 0..wire.len() {
            let mut decoder = Decoder::new(RespLimits::default()).unwrap();
            let mut input = BytesMut::from(&wire[..split]);
            assert!(decoder.decode(&mut input).unwrap().is_none());
            assert_eq!(input.as_ref(), &wire[..split]);
            input.extend_from_slice(&wire[split..]);
            input.extend_from_slice(b"+next\r\n");
            assert_eq!(decoder.decode(&mut input).unwrap(), Some(expected.clone()));
            assert_eq!(input.as_ref(), b"+next\r\n");
        }
        let mut decoder = Decoder::new(RespLimits::default()).unwrap();
        let mut input = BytesMut::new();
        for (index, byte) in wire.iter().enumerate() {
            input.extend_from_slice(&[*byte]);
            let actual = decoder.decode(&mut input).unwrap();
            assert_eq!(
                actual,
                if index + 1 == wire.len() {
                    Some(expected.clone())
                } else {
                    None
                }
            );
        }
    }
}

#[test]
fn resp3_invalid_and_streamed_features_are_terminal() {
    for wire in [
        b"_x\r\n".as_slice(),
        b"#1\r\n",
        b",NaN\r\n",
        b",1e999\r\n",
        b"(\r\n",
        b"(1.2\r\n",
        b"%-1\r\n",
        b"!4\r\nx",
        b"=3\r\ntxt\r\n",
        b"$?\r\n",
        b"*?\r\n",
        b";1\r\nx\r\n",
        b".\r\n",
    ] {
        let mut decoder = Decoder::new(RespLimits::default()).unwrap();
        let mut input = BytesMut::from(wire);
        if wire == b"!4\r\nx" {
            assert!(decoder.decode(&mut input).unwrap().is_none());
            continue;
        }
        assert!(decoder.decode(&mut input).is_err(), "{wire:?}");
        assert_eq!(input.as_ref(), wire);
        assert_eq!(decoder.decode(&mut input), Err(ProtocolError::Poisoned));
    }
}

#[test]
fn resp3_aggregate_and_binary_limits_apply_before_output_mutation() {
    let limits = RespLimits {
        max_nodes: 2,
        ..RespLimits::default()
    };
    let frame = Frame::Map(vec![Frame::Null, Frame::Null]);
    let mut output = BytesMut::from(b"prefix".as_slice());
    assert!(encode(&frame, &mut output, limits).is_err());
    assert_eq!(output.as_ref(), b"prefix");
    let mut decoder = Decoder::new(limits).unwrap();
    assert!(
        decoder
            .decode(&mut BytesMut::from(b"%1\r\n".as_slice()))
            .is_err()
    );
    for frame in [
        Frame::Map(vec![Frame::Null]),
        Frame::Attribute(vec![Frame::Null]),
        Frame::Verbatim(bytes(b"bad")),
        Frame::Double(bytes(b"1e999")),
    ] {
        assert!(encode(&frame, &mut output, RespLimits::default()).is_err());
        assert_eq!(output.as_ref(), b"prefix");
    }
}
