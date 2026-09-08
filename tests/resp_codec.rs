#![forbid(unsafe_code)]

//! Contratos públicos do codec, sem sockets ou referência externa em execução.

#[path = "common/resp_fixtures.rs"]
mod resp_fixtures;

use bytes::{Bytes, BytesMut};
use proptest::prelude::*;
use sider::resp::{Decoder, EncodeError, Frame, ProtocolError, RespLimits, encode};

fn literals() -> Vec<(&'static [u8], Frame)> {
    vec![
        (b"+\r\n", Frame::Simple(Bytes::new())),
        (b"+OK\r\n", Frame::Simple(Bytes::from_static(b"OK"))),
        (
            b"+\x00\xff\x80\r\n",
            Frame::Simple(Bytes::from_static(b"\x00\xff\x80")),
        ),
        (b"-\r\n", Frame::Error(Bytes::new())),
        (
            b"-ERR example\r\n",
            Frame::Error(Bytes::from_static(b"ERR example")),
        ),
        (
            b"-\x00\xff\x80\r\n",
            Frame::Error(Bytes::from_static(b"\x00\xff\x80")),
        ),
        (b":0\r\n", Frame::Integer(0)),
        (b":-1\r\n", Frame::Integer(-1)),
        (b":9223372036854775807\r\n", Frame::Integer(i64::MAX)),
        (b":-9223372036854775808\r\n", Frame::Integer(i64::MIN)),
        (b"$-1\r\n", Frame::Bulk(None)),
        (b"$0\r\n\r\n", Frame::Bulk(Some(Bytes::new()))),
        (
            b"$5\r\n\x00\r\n\xffA\r\n",
            Frame::Bulk(Some(Bytes::from_static(b"\x00\r\n\xffA"))),
        ),
        (b"*-1\r\n", Frame::Array(None)),
        (b"*0\r\n", Frame::Array(Some(vec![]))),
        (
            b"*2\r\n$4\r\nECHO\r\n$0\r\n\r\n",
            Frame::Array(Some(vec![
                Frame::Bulk(Some(Bytes::from_static(b"ECHO"))),
                Frame::Bulk(Some(Bytes::new())),
            ])),
        ),
        (
            b"*5\r\n+OK\r\n-ERR\r\n:-1\r\n$-1\r\n*3\r\n*-1\r\n*0\r\n$0\r\n\r\n",
            Frame::Array(Some(vec![
                Frame::Simple(Bytes::from_static(b"OK")),
                Frame::Error(Bytes::from_static(b"ERR")),
                Frame::Integer(-1),
                Frame::Bulk(None),
                Frame::Array(Some(vec![
                    Frame::Array(None),
                    Frame::Array(Some(vec![])),
                    Frame::Bulk(Some(Bytes::new())),
                ])),
            ])),
        ),
    ]
}

fn decode_exact(wire: &[u8], limits: RespLimits) -> Frame {
    let mut decoder = Decoder::new(limits).expect("limites válidos");
    let mut input = BytesMut::from(wire);
    let frame = decoder
        .decode(&mut input)
        .expect("frame válido")
        .expect("frame completo");
    assert!(input.is_empty(), "bytes restantes: {input:?}");
    assert_eq!(decoder.decode(&mut input).unwrap(), None);
    frame
}

fn encoded(frame: &Frame, limits: RespLimits) -> BytesMut {
    let mut output = BytesMut::new();
    encode(frame, &mut output, limits).expect("frame válido dentro dos limites");
    output
}

fn check_every_split(wire: &[u8], expected: &Frame) {
    for split in 0..=wire.len() {
        let mut decoder = Decoder::new(RespLimits::default()).unwrap();
        let mut input = BytesMut::from(&wire[..split]);
        let before = input.clone();
        let first = decoder.decode(&mut input).unwrap();
        if split == wire.len() {
            assert_eq!(first.as_ref(), Some(expected), "corte {split}");
        } else {
            assert_eq!(first, None, "corte {split}");
            assert_eq!(input, before, "incompleto consumido no corte {split}");
            assert_eq!(decoder.decode(&mut input).unwrap(), None);
            assert_eq!(input, before, "segunda leitura consumiu incompleto");
            input.extend_from_slice(&wire[split..]);
            assert_eq!(decoder.decode(&mut input).unwrap().as_ref(), Some(expected));
        }
        assert!(input.is_empty());
    }
}

fn check_bytewise(wire: &[u8], expected: &Frame) {
    let mut decoder = Decoder::new(RespLimits::default()).unwrap();
    let mut input = BytesMut::new();
    for (index, byte) in wire.iter().enumerate() {
        input.extend_from_slice(&[*byte]);
        let before = input.clone();
        let actual = decoder.decode(&mut input).unwrap();
        if index + 1 == wire.len() {
            assert_eq!(actual.as_ref(), Some(expected));
            assert!(input.is_empty());
        } else {
            assert_eq!(actual, None, "byte {index} de {wire:?}");
            assert_eq!(input, before);
        }
    }
}

#[test]
fn five_types_match_independent_literal_bytes() {
    for (wire, expected) in literals() {
        assert_eq!(decode_exact(wire, RespLimits::default()), expected);
        let mut output = BytesMut::from(&b"existing output\x00"[..]);
        encode(&expected, &mut output, RespLimits::default()).unwrap();
        assert_eq!(&output[..16], b"existing output\x00");
        assert_eq!(&output[16..], wire);
    }
}

#[test]
fn every_literal_split_and_bytewise_delivery_preserves_incomplete_input() {
    for (wire, expected) in literals() {
        check_every_split(wire, &expected);
        check_bytewise(wire, &expected);
    }
}

#[test]
fn redis_reference_literals_round_trip_without_external_tools() {
    for case in resp_fixtures::CASES {
        for &(request, response) in case.exchanges {
            for wire in [request, response] {
                let frame = decode_exact(wire, RespLimits::default());
                assert_eq!(
                    encoded(&frame, RespLimits::default()).as_ref(),
                    wire,
                    "{}",
                    case.name
                );
                check_every_split(wire, &frame);
                check_bytewise(wire, &frame);
            }
        }
    }
}

#[test]
fn concatenation_consumes_one_frame_and_preserves_even_an_invalid_suffix() {
    let fixtures = literals();
    let mut input = BytesMut::new();
    for (wire, _) in &fixtures {
        input.extend_from_slice(wire);
    }
    input.extend_from_slice(b"not RESP\x00");
    let original = input.clone();
    let mut offset = 0;
    let mut decoder = Decoder::new(RespLimits::default()).unwrap();
    for (wire, expected) in fixtures {
        assert_eq!(decoder.decode(&mut input).unwrap(), Some(expected));
        offset += wire.len();
        assert_eq!(&input[..], &original[offset..]);
    }
    assert_eq!(&input[..], b"not RESP\x00");
    assert!(matches!(
        decoder.decode(&mut input),
        Err(ProtocolError::Malformed(_))
    ));
}

#[test]
fn frame_budget_does_not_include_following_frames() {
    let limits = RespLimits {
        max_frame_bytes: 5,
        max_bulk_bytes: 1,
        max_line_bytes: 5,
        max_nodes: 1,
        max_depth: 1,
    };
    let mut input = BytesMut::from(b"+OK\r\n".repeat(100).as_slice());
    let mut decoder = Decoder::new(limits).unwrap();
    for _ in 0..100 {
        assert_eq!(
            decoder.decode(&mut input).unwrap(),
            Some(Frame::Simple(Bytes::from_static(b"OK")))
        );
    }
    assert!(input.is_empty());
}

#[test]
fn decimal_spelling_is_normalized_without_changing_values() {
    let cases: &[(&[u8], &[u8], Frame)] = &[
        (b":+42\r\n", b":42\r\n", Frame::Integer(42)),
        (b":00042\r\n", b":42\r\n", Frame::Integer(42)),
        (b":-00042\r\n", b":-42\r\n", Frame::Integer(-42)),
        (b":-0\r\n", b":0\r\n", Frame::Integer(0)),
        (b":+0\r\n", b":0\r\n", Frame::Integer(0)),
        (
            b"$0001\r\nx\r\n",
            b"$1\r\nx\r\n",
            Frame::Bulk(Some(Bytes::from_static(b"x"))),
        ),
        (b"*00\r\n", b"*0\r\n", Frame::Array(Some(vec![]))),
    ];
    for (wire, canonical, expected) in cases {
        assert_eq!(&decode_exact(wire, RespLimits::default()), expected);
        assert_eq!(
            encoded(expected, RespLimits::default()).as_ref(),
            *canonical
        );
        check_every_split(wire, expected);
    }
}

#[test]
fn malformed_wire_is_terminal_in_whole_and_fragmented_input() {
    let cases: &[&[u8]] = &[
        b"?\r\n",
        b"PING\r\n",
        b"_\r\n",
        b"#t\r\n",
        b"+a\nb\r\n",
        b"-a\rb\r\n",
        b"+a\rx",
        b"+a\n",
        b":\r\n",
        b":+\r\n",
        b":-\r\n",
        b": 1\r\n",
        b":1 \r\n",
        b":1x\r\n",
        b":--1\r\n",
        b":+-1\r\n",
        b":\xff\r\n",
        b":9223372036854775808\r\n",
        b":-9223372036854775809\r\n",
        b"$\r\n",
        b"*\r\n",
        b"$-2\r\n",
        b"*-2\r\n",
        b"$-01\r\n",
        b"*-01\r\n",
        b"$-0\r\n",
        b"*-0\r\n",
        b"$+1\r\nx\r\n",
        b"*+0\r\n",
        b"$1x\r\nx\r\n",
        b"*1x\r\n:1\r\n",
        b"$ 1\r\nx\r\n",
        b"$1\r\nx\rx",
        b"$1\r\nx\n\n",
        b"$0\r\nx\n",
        b"*1\r\n$1\r\nx\rx",
    ];
    for wire in cases {
        for chunk_size in [1, wire.len()] {
            let mut decoder = Decoder::new(RespLimits::default()).unwrap();
            let mut input = BytesMut::new();
            let mut failed = false;
            for chunk in wire.chunks(chunk_size) {
                input.extend_from_slice(chunk);
                match decoder.decode(&mut input) {
                    Ok(None) => {}
                    Ok(Some(frame)) => panic!("entrada inválida aceita: {wire:?}, {frame:?}"),
                    Err(_) => {
                        failed = true;
                        assert_eq!(decoder.decode(&mut input), Err(ProtocolError::Poisoned));
                        break;
                    }
                }
            }
            assert!(failed, "entrada inválida aguardou mais bytes: {wire:?}");
        }
    }
}

#[test]
fn huge_advertised_lengths_fail_before_payload_or_children_arrive() {
    for wire in [
        &b"$18446744073709551616\r\n"[..],
        &b"*18446744073709551616\r\n"[..],
        &b"$999999999999999999999999999999999999999999\r\n"[..],
        &b"*999999999999999999999999999999999999999999\r\n"[..],
        &b"$1048577\r\n"[..],
        &b"*1024\r\n"[..],
    ] {
        let mut decoder = Decoder::new(RespLimits::default()).unwrap();
        let mut input = BytesMut::from(wire);
        assert!(
            decoder.decode(&mut input).is_err(),
            "declaração aceita: {wire:?}"
        );
    }
}

fn assert_limit_rejected(frame: &Frame, wire: &[u8], limits: RespLimits) {
    let mut output = BytesMut::from(&b"keep existing bytes\x00"[..]);
    let before = output.clone();
    assert!(matches!(
        encode(frame, &mut output, limits),
        Err(EncodeError::LimitExceeded(_))
    ));
    assert_eq!(output, before);
    let mut decoder = Decoder::new(limits).unwrap();
    let mut input = BytesMut::from(wire);
    assert!(matches!(
        decoder.decode(&mut input),
        Err(ProtocolError::LimitExceeded(_))
    ));
}

#[test]
fn frame_size_includes_all_framing_and_accepts_exact_boundary() {
    let frame = Frame::Bulk(Some(Bytes::from_static(b"abc")));
    let wire = b"$3\r\nabc\r\n";
    let limits = RespLimits {
        max_frame_bytes: wire.len(),
        max_bulk_bytes: 3,
        max_line_bytes: 4,
        ..RespLimits::default()
    };
    assert_eq!(decode_exact(wire, limits), frame);
    assert_eq!(encoded(&frame, limits).as_ref(), wire);
    assert_limit_rejected(
        &frame,
        wire,
        RespLimits {
            max_frame_bytes: wire.len() - 1,
            ..limits
        },
    );
}

#[test]
fn line_limit_includes_prefix_and_crlf_not_bulk_payload() {
    let limits = RespLimits {
        max_line_bytes: 5,
        ..RespLimits::default()
    };
    let frame = Frame::Simple(Bytes::from_static(b"ab"));
    assert_eq!(decode_exact(b"+ab\r\n", limits), frame);
    assert_eq!(encoded(&frame, limits).as_ref(), b"+ab\r\n");
    assert_limit_rejected(
        &frame,
        b"+ab\r\n",
        RespLimits {
            max_line_bytes: 4,
            ..limits
        },
    );
    let bulk = Frame::Bulk(Some(Bytes::from_static(b"0123456789\r\n")));
    assert_eq!(decode_exact(b"$12\r\n0123456789\r\n\r\n", limits), bulk);
    assert_eq!(
        encoded(&bulk, limits).as_ref(),
        b"$12\r\n0123456789\r\n\r\n"
    );
    let mut decoder = Decoder::new(limits).unwrap();
    let mut unterminated = BytesMut::from(&b"+abcdef"[..]);
    assert!(matches!(
        decoder.decode(&mut unterminated),
        Err(ProtocolError::LimitExceeded(_))
    ));
}

#[test]
fn bulk_limit_rejects_one_extra_byte_at_its_header() {
    let limits = RespLimits {
        max_bulk_bytes: 3,
        ..RespLimits::default()
    };
    let allowed = Frame::Bulk(Some(Bytes::from_static(b"abc")));
    assert_eq!(decode_exact(b"$3\r\nabc\r\n", limits), allowed);
    assert_eq!(encoded(&allowed, limits).as_ref(), b"$3\r\nabc\r\n");
    let rejected = Frame::Bulk(Some(Bytes::from_static(b"abcd")));
    assert_limit_rejected(&rejected, b"$4\r\n", limits);
}

#[test]
fn node_limit_counts_root_and_nested_arrays_as_well_as_scalars() {
    let limits = RespLimits {
        max_nodes: 4,
        ..RespLimits::default()
    };
    let frame = Frame::Array(Some(vec![
        Frame::Array(Some(vec![Frame::Integer(1)])),
        Frame::Bulk(None),
    ]));
    let wire = b"*2\r\n*1\r\n:1\r\n$-1\r\n";
    assert_eq!(decode_exact(wire, limits), frame);
    assert_eq!(encoded(&frame, limits).as_ref(), wire);
    assert_limit_rejected(
        &frame,
        wire,
        RespLimits {
            max_nodes: 3,
            ..limits
        },
    );
    let single = RespLimits {
        max_nodes: 1,
        ..limits
    };
    for (wire, frame) in [
        (b"*-1\r\n".as_slice(), Frame::Array(None)),
        (b"*0\r\n".as_slice(), Frame::Array(Some(vec![]))),
    ] {
        assert_eq!(decode_exact(wire, single), frame);
        assert_eq!(encoded(&frame, single).as_ref(), wire);
    }
    let mut decoder = Decoder::new(single).unwrap();
    let mut input = BytesMut::from(&b"*1\r\n"[..]);
    assert!(matches!(
        decoder.decode(&mut input),
        Err(ProtocolError::LimitExceeded(_))
    ));
}

#[test]
fn depth_counts_empty_and_null_arrays_but_not_scalars() {
    let limits = RespLimits {
        max_depth: 2,
        ..RespLimits::default()
    };
    for (wire, frame) in [
        (
            b"*1\r\n*1\r\n:1\r\n".as_slice(),
            Frame::Array(Some(vec![Frame::Array(Some(vec![Frame::Integer(1)]))])),
        ),
        (
            b"*1\r\n*-1\r\n".as_slice(),
            Frame::Array(Some(vec![Frame::Array(None)])),
        ),
        (
            b"*1\r\n*0\r\n".as_slice(),
            Frame::Array(Some(vec![Frame::Array(Some(vec![]))])),
        ),
    ] {
        assert_eq!(decode_exact(wire, limits), frame);
        assert_eq!(encoded(&frame, limits).as_ref(), wire);
        assert_limit_rejected(
            &frame,
            wire,
            RespLimits {
                max_depth: 1,
                ..limits
            },
        );
    }
    assert_eq!(
        decode_exact(
            b"*1\r\n:1\r\n",
            RespLimits {
                max_depth: 1,
                ..limits
            }
        ),
        Frame::Array(Some(vec![Frame::Integer(1)]))
    );
}

#[test]
fn invalid_limits_are_rejected_before_output_changes() {
    let default = RespLimits::default();
    let cases = [
        RespLimits {
            max_frame_bytes: 0,
            ..default
        },
        RespLimits {
            max_bulk_bytes: 0,
            ..default
        },
        RespLimits {
            max_line_bytes: 0,
            ..default
        },
        RespLimits {
            max_nodes: 0,
            ..default
        },
        RespLimits {
            max_depth: 0,
            ..default
        },
        RespLimits {
            max_bulk_bytes: default.max_frame_bytes + 1,
            ..default
        },
        RespLimits {
            max_line_bytes: default.max_frame_bytes + 1,
            ..default
        },
        RespLimits {
            max_depth: 129,
            ..default
        },
    ];
    for limits in cases {
        assert!(limits.validate().is_err());
        assert!(Decoder::new(limits).is_err());
        let mut output = BytesMut::from(&b"preserve"[..]);
        assert!(matches!(
            encode(&Frame::Integer(0), &mut output, limits),
            Err(EncodeError::InvalidLimits(_))
        ));
        assert_eq!(&output[..], b"preserve");
    }
    assert!(
        RespLimits {
            max_depth: 128,
            ..default
        }
        .validate()
        .is_ok()
    );
}

#[test]
fn invalid_nested_simple_payload_never_leaves_partial_output() {
    for bytes in [b"CR\rinside".as_slice(), b"LF\ninside".as_slice()] {
        for invalid in [
            Frame::Simple(Bytes::copy_from_slice(bytes)),
            Frame::Error(Bytes::copy_from_slice(bytes)),
        ] {
            let frame = Frame::Array(Some(vec![Frame::Integer(10), invalid]));
            let mut output = BytesMut::from(&b"untouched"[..]);
            assert!(matches!(
                encode(&frame, &mut output, RespLimits::default()),
                Err(EncodeError::InvalidFrame(_))
            ));
            assert_eq!(&output[..], b"untouched");
        }
    }
}

#[test]
fn reducing_an_incomplete_buffer_is_reported_and_poisoned() {
    let mut decoder = Decoder::new(RespLimits::default()).unwrap();
    let mut input = BytesMut::from(&b"*2\r\n$1\r\na\r\n$3\r\nb"[..]);
    assert_eq!(decoder.decode(&mut input).unwrap(), None);
    input.truncate(input.len() - 1);
    assert_eq!(
        decoder.decode(&mut input),
        Err(ProtocolError::BufferChanged)
    );
    assert_eq!(decoder.decode(&mut input), Err(ProtocolError::Poisoned));
}

#[test]
fn a_large_binary_payload_survives_reallocation_and_representative_splits() {
    let payload: Vec<u8> = (0..32_768).map(|index| (index % 256) as u8).collect();
    let frame = Frame::Bulk(Some(Bytes::copy_from_slice(&payload)));
    // Cabeçalho literal: este teste não usa o encoder para construir a entrada.
    let mut wire = b"$32768\r\n".to_vec();
    wire.extend_from_slice(&payload);
    wire.extend_from_slice(b"\r\n");
    assert_eq!(encoded(&frame, RespLimits::default()).as_ref(), &wire);
    for split in [
        0,
        1,
        7,
        8,
        9,
        255,
        4096,
        16_384,
        wire.len() - 2,
        wire.len() - 1,
    ] {
        let mut input = BytesMut::from(&wire[..split]);
        let mut decoder = Decoder::new(RespLimits::default()).unwrap();
        assert_eq!(decoder.decode(&mut input).unwrap(), None);
        assert_eq!(&input[..], &wire[..split]);
        input.reserve(wire.len() * 2);
        input.extend_from_slice(&wire[split..]);
        assert_eq!(decoder.decode(&mut input).unwrap(), Some(frame.clone()));
        assert!(input.is_empty());
    }
    let mut decoder = Decoder::new(RespLimits::default()).unwrap();
    let mut input = BytesMut::new();
    let mut offset = 0;
    let mut step = 19usize;
    while offset < wire.len() {
        step = (step * 73 + 11) % 997 + 1;
        let end = (offset + step).min(wire.len());
        input.extend_from_slice(&wire[offset..end]);
        let actual = decoder.decode(&mut input).unwrap();
        if end == wire.len() {
            assert_eq!(actual, Some(frame.clone()));
        } else {
            assert_eq!(actual, None);
            assert_eq!(&input[..], &wire[..end]);
        }
        offset = end;
    }
}

fn frame_strategy() -> impl Strategy<Value = Frame> {
    let line = prop::collection::vec(prop_oneof![0u8..=9, 11u8..=12, 14u8..=255], 0..64);
    let leaf = prop_oneof![
        line.clone()
            .prop_map(|value| Frame::Simple(Bytes::from(value))),
        line.prop_map(|value| Frame::Error(Bytes::from(value))),
        any::<i64>().prop_map(Frame::Integer),
        prop::option::of(prop::collection::vec(any::<u8>(), 0..64))
            .prop_map(|value| Frame::Bulk(value.map(Bytes::from))),
        Just(Frame::Array(None)),
        Just(Frame::Array(Some(vec![]))),
    ];
    // Três níveis recursivos e um possível array folha: profundidade máxima 4.
    leaf.prop_recursive(3, 64, 4, |inner| {
        prop::collection::vec(inner, 0..4).prop_map(|items| Frame::Array(Some(items)))
    })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    #[test]
    fn generated_frames_round_trip_and_preserve_arbitrary_suffix(
        frame in frame_strategy(),
        suffix in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let mut input = encoded(&frame, RespLimits::default());
        input.extend_from_slice(&suffix);
        let mut decoder = Decoder::new(RespLimits::default()).unwrap();
        prop_assert_eq!(decoder.decode(&mut input).unwrap(), Some(frame));
        prop_assert_eq!(&input[..], &suffix);
    }

    #[test]
    fn generated_fragment_patterns_are_equivalent_to_whole_frames(
        frame in frame_strategy(),
        pattern in prop::collection::vec(1usize..33, 1..33),
    ) {
        let wire = encoded(&frame, RespLimits::default());
        let mut input = BytesMut::new();
        let mut decoder = Decoder::new(RespLimits::default()).unwrap();
        let mut offset = 0;
        for amount in pattern.iter().cycle() {
            let end = (offset + amount).min(wire.len());
            input.extend_from_slice(&wire[offset..end]);
            let before = input.clone();
            let actual = decoder.decode(&mut input).unwrap();
            if end == wire.len() {
                prop_assert_eq!(actual, Some(frame));
                prop_assert!(input.is_empty());
                break;
            }
            prop_assert_eq!(actual, None);
            prop_assert_eq!(&input, &before);
            prop_assert_eq!(decoder.decode(&mut input).unwrap(), None);
            prop_assert_eq!(&input, &before);
            offset = end;
        }
    }

    #[test]
    fn arbitrary_bytes_under_small_limits_do_not_panic_or_consume_incompletes(
        wire in prop::collection::vec(any::<u8>(), 0..512),
        chunk_size in 1usize..65,
    ) {
        let limits = RespLimits { max_frame_bytes: 256, max_bulk_bytes: 64, max_line_bytes: 32, max_nodes: 16, max_depth: 4 };
        let mut decoder = Decoder::new(limits).unwrap();
        let mut input = BytesMut::new();
        'chunks: for chunk in wire.chunks(chunk_size) {
            input.extend_from_slice(chunk);
            loop {
                let before = input.clone();
                match decoder.decode(&mut input) {
                    Ok(None) => {
                        prop_assert_eq!(&input, &before);
                        prop_assert!(input.len() <= limits.max_frame_bytes);
                        break;
                    }
                    Ok(Some(frame)) => {
                        prop_assert!(input.len() < before.len());
                        let consumed = before.len() - input.len();
                        prop_assert_eq!(&input[..], &before[consumed..]);
                        let canonical = encoded(&frame, limits);
                        prop_assert_eq!(decode_exact(&canonical, limits), frame);
                    }
                    Err(_) => {
                        prop_assert_eq!(decoder.decode(&mut input), Err(ProtocolError::Poisoned));
                        break 'chunks;
                    }
                }
            }
        }
    }

    #[test]
    fn generated_frame_over_budget_preserves_existing_output(frame in frame_strategy()) {
        let wire = encoded(&frame, RespLimits::default());
        let frame_budget = wire.len() - 1;
        let limits = RespLimits {
            max_frame_bytes: frame_budget,
            max_bulk_bytes: frame_budget.min(RespLimits::default().max_bulk_bytes),
            max_line_bytes: frame_budget.min(RespLimits::default().max_line_bytes),
            max_nodes: 1024,
            max_depth: 16,
        };
        let mut output = BytesMut::from(&b"existing\x00output"[..]);
        let before = output.clone();
        prop_assert!(encode(&frame, &mut output, limits).is_err());
        prop_assert_eq!(output, before);
    }
}
