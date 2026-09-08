#![cfg_attr(not(test), no_main)]
#![forbid(unsafe_code)]

use bytes::{Bytes, BytesMut};
use sider::resp::{Decoder, Frame, ProtocolError, RespLimits, encode};

#[cfg(not(test))]
libfuzzer_sys::fuzz_target!(|data: &[u8]| exercise(data));

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    frames: Vec<Frame>,
    consumed: usize,
    error: Option<ProtocolError>,
}

/// Mesmo contrato executado pelo libFuzzer instrumentado e pelos seeds nativos.
pub fn exercise(data: &[u8]) {
    // O comando de fuzz limita entradas a 4096 bytes. Replay manual de um arquivo
    // maior continua limitado, sem alocar em proporção a dados fora do contrato.
    let data = &data[..data.len().min(4096)];
    let standard = RespLimits {
        max_frame_bytes: 512,
        max_bulk_bytes: 256,
        max_line_bytes: 64,
        max_nodes: 64,
        max_depth: 8,
    };
    let byte = |index| usize::from(data.get(index).copied().unwrap_or(0));
    let frame_budget = 32 + (byte(0) % 32) * 16;
    let varied = RespLimits {
        max_frame_bytes: frame_budget,
        max_bulk_bytes: (1 + byte(1)).min(frame_budget),
        max_line_bytes: (1 + byte(2) % 64).min(frame_budget),
        max_nodes: 1 + byte(3) % 64,
        max_depth: 1 + byte(4) % 8,
    };
    for limits in [standard, varied] {
        let whole = decode_stream(data, limits, data.len().max(1));
        assert_eq!(decode_stream(data, limits, 1), whole);
        assert_eq!(decode_stream(data, limits, 1 + byte(5) % 31), whole);
    }

    // Entradas aleatórias raramente formam RESP válido. Uma segunda perspectiva
    // envolve seus bytes em frames válidos para exercitar também materialização,
    // conteúdo binário e encoder, sem depender do sucesso das mutações no framing.
    let payload = Bytes::copy_from_slice(&data[..data.len().min(128)]);
    let integer = data.iter().take(8).fold(0_i64, |value, byte| {
        value.wrapping_shl(8) | i64::from(*byte)
    });
    let generated = Frame::Array(Some(vec![
        Frame::Bulk(Some(payload)),
        Frame::Integer(integer),
        Frame::Array(Some(vec![Frame::Bulk(None), Frame::Array(None)])),
        Frame::Simple(Bytes::new()),
        Frame::Error(Bytes::from_static(b"ERR fuzz")),
    ]));
    let mut wire = BytesMut::new();
    encode(&generated, &mut wire, standard).unwrap();
    let expected = Outcome {
        frames: vec![generated],
        consumed: wire.len(),
        error: None,
    };
    assert_eq!(decode_stream(&wire, standard, 1), expected);
    assert_eq!(decode_stream(&wire, standard, 1 + byte(6) % 31), expected);
}

fn decode_stream(wire: &[u8], limits: RespLimits, chunk_size: usize) -> Outcome {
    let mut decoder = Decoder::new(limits).unwrap();
    let mut input = BytesMut::new();
    let mut supplied = 0;
    let mut outcome = Outcome {
        frames: Vec::new(),
        consumed: 0,
        error: None,
    };
    loop {
        let before = input.to_vec();
        match decoder.decode(&mut input) {
            Ok(Some(frame)) => {
                let consumed = before.len().checked_sub(input.len()).unwrap();
                assert!(consumed > 0);
                assert_eq!(input.as_ref(), &before[consumed..]);
                outcome.consumed += consumed;
                assert_eq!(input.as_ref(), &wire[outcome.consumed..supplied]);
                round_trip(&frame, limits);
                outcome.frames.push(frame);
            }
            Ok(None) => {
                assert_eq!(input.as_ref(), before);
                assert_eq!(decoder.decode(&mut input), Ok(None));
                assert_eq!(input.as_ref(), before);
                if supplied == wire.len() {
                    assert_eq!(input.as_ref(), &wire[outcome.consumed..]);
                    if !input.is_empty() {
                        input.truncate(input.len() - 1);
                        let shortened = input.to_vec();
                        assert_eq!(
                            decoder.decode(&mut input),
                            Err(ProtocolError::BufferChanged)
                        );
                        assert_eq!(input.as_ref(), shortened);
                        assert_poisoned(&mut decoder, &mut input);
                    }
                    return outcome;
                }
                let end = supplied.saturating_add(chunk_size).min(wire.len());
                assert!(end > supplied);
                input.extend_from_slice(&wire[supplied..end]);
                supplied = end;
            }
            Err(error) => {
                assert_eq!(input.as_ref(), before);
                assert_poisoned(&mut decoder, &mut input);
                outcome.error = Some(error);
                return outcome;
            }
        }
    }
}

fn assert_poisoned(decoder: &mut Decoder, input: &mut BytesMut) {
    for suffix in [b"".as_slice(), b"+PONG\r\n"] {
        input.extend_from_slice(suffix);
        let before = input.to_vec();
        assert_eq!(decoder.decode(input), Err(ProtocolError::Poisoned));
        assert_eq!(input.as_ref(), before);
    }
}

fn round_trip(frame: &Frame, limits: RespLimits) {
    let mut wire = BytesMut::new();
    encode(frame, &mut wire, limits).unwrap();
    let mut decoder = Decoder::new(limits).unwrap();
    assert_eq!(decoder.decode(&mut wire), Ok(Some(frame.clone())));
    assert!(wire.is_empty());
    assert_eq!(decoder.decode(&mut wire), Ok(None));
}
