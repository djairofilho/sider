//! Two-pass RESP2/RESP3 encoder: validate the entire frame, then write it.

use bytes::BytesMut;

use super::{EncodeError, Frame, RespLimits};

/// Appends a frame to `dst`, retaining all existing output on any error.
///
/// The budget is per frame, not for prior `dst` content. The consumer must also
/// limit its output buffer and the number of queued replies.
pub fn encode(frame: &Frame, dst: &mut BytesMut, limits: RespLimits) -> Result<(), EncodeError> {
    limits.validate()?;
    let size = encoded_size(frame, limits)?;
    dst.len()
        .checked_add(size)
        .ok_or(EncodeError::LimitExceeded("output capacity"))?;
    dst.reserve(size);

    let mut pending = vec![frame];
    while let Some(frame) = pending.pop() {
        match frame {
            Frame::Simple(value) => line(b'+', value, dst),
            Frame::Error(value) => line(b'-', value, dst),
            Frame::Null => dst.extend_from_slice(b"_\r\n"),
            Frame::Boolean(value) => {
                dst.extend_from_slice(if *value { b"#t\r\n" } else { b"#f\r\n" })
            }
            Frame::Double(value) => line(b',', value, dst),
            Frame::BigNumber(value) => line(b'(', value, dst),
            Frame::BlobError(value) | Frame::Verbatim(value) => {
                line(
                    if matches!(frame, Frame::BlobError(_)) {
                        b'!'
                    } else {
                        b'='
                    },
                    value.len().to_string().as_bytes(),
                    dst,
                );
                dst.extend_from_slice(value);
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Map(values)
            | Frame::Set(values)
            | Frame::Push(values)
            | Frame::Attribute(values) => {
                let (prefix, count) = match frame {
                    Frame::Map(_) => (b'%', values.len() / 2),
                    Frame::Attribute(_) => (b'|', values.len() / 2),
                    Frame::Set(_) => (b'~', values.len()),
                    _ => (b'>', values.len()),
                };
                line(prefix, count.to_string().as_bytes(), dst);
                pending.extend(values.iter().rev());
            }
            Frame::Integer(value) => line(b':', value.to_string().as_bytes(), dst),
            Frame::Bulk(None) => dst.extend_from_slice(b"$-1\r\n"),
            Frame::Bulk(Some(value)) => {
                line(b'$', value.len().to_string().as_bytes(), dst);
                dst.extend_from_slice(value);
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Array(None) => dst.extend_from_slice(b"*-1\r\n"),
            Frame::Array(Some(values)) => {
                line(b'*', values.len().to_string().as_bytes(), dst);
                pending.extend(values.iter().rev());
            }
        }
    }
    Ok(())
}

fn line(prefix: u8, value: &[u8], dst: &mut BytesMut) {
    dst.extend_from_slice(&[prefix]);
    dst.extend_from_slice(value);
    dst.extend_from_slice(b"\r\n");
}

fn line_size(content: usize, limits: RespLimits) -> Result<usize, EncodeError> {
    let size = content
        .checked_add(3)
        .ok_or(EncodeError::LimitExceeded("max_line_bytes"))?;
    if size > limits.max_line_bytes {
        return Err(EncodeError::LimitExceeded("max_line_bytes"));
    }
    Ok(size)
}

fn encoded_size(frame: &Frame, limits: RespLimits) -> Result<usize, EncodeError> {
    let mut pending = vec![(frame, 0_usize)];
    let mut nodes = 0_usize;
    let mut size = 0_usize;
    while let Some((frame, depth)) = pending.pop() {
        nodes = nodes
            .checked_add(1)
            .filter(|nodes| *nodes <= limits.max_nodes)
            .ok_or(EncodeError::LimitExceeded("max_nodes"))?;
        let delta = match frame {
            Frame::Simple(value) | Frame::Error(value) => {
                let size = line_size(value.len(), limits)?;
                if value.iter().any(|byte| matches!(byte, b'\r' | b'\n')) {
                    return Err(EncodeError::InvalidFrame("simple/error contains CR or LF"));
                }
                size
            }
            Frame::Null => line_size(0, limits)?,
            Frame::Boolean(_) => line_size(1, limits)?,
            Frame::Double(value) | Frame::BigNumber(value) => {
                let size = line_size(value.len(), limits)?;
                let prefix = if matches!(frame, Frame::Double(_)) {
                    b','
                } else {
                    b'('
                };
                super::decoder::validate_scalar(prefix, value)
                    .map_err(|_| EncodeError::InvalidFrame("invalid RESP3 scalar"))?;
                size
            }
            Frame::BlobError(value) | Frame::Verbatim(value) => {
                if matches!(frame, Frame::Verbatim(_)) && (value.len() < 4 || value[3] != b':') {
                    return Err(EncodeError::InvalidFrame("invalid verbatim format"));
                }
                if value.len() > limits.max_bulk_bytes {
                    return Err(EncodeError::LimitExceeded("max_bulk_bytes"));
                }
                line_size(value.len().to_string().len(), limits)?
                    .checked_add(value.len())
                    .and_then(|n| n.checked_add(2))
                    .ok_or(EncodeError::LimitExceeded("max_frame_bytes"))?
            }
            Frame::Map(values)
            | Frame::Set(values)
            | Frame::Push(values)
            | Frame::Attribute(values) => {
                let paired = matches!(frame, Frame::Map(_) | Frame::Attribute(_));
                if paired && !values.len().is_multiple_of(2) {
                    return Err(EncodeError::InvalidFrame("odd map element count"));
                }
                let depth = depth
                    .checked_add(1)
                    .filter(|n| *n <= limits.max_depth)
                    .ok_or(EncodeError::LimitExceeded("max_depth"))?;
                nodes
                    .checked_add(pending.len())
                    .and_then(|n| n.checked_add(values.len()))
                    .filter(|n| *n <= limits.max_nodes)
                    .ok_or(EncodeError::LimitExceeded("max_nodes"))?;
                pending.extend(values.iter().rev().map(|f| (f, depth)));
                line_size(
                    (if paired {
                        values.len() / 2
                    } else {
                        values.len()
                    })
                    .to_string()
                    .len(),
                    limits,
                )?
            }
            Frame::Integer(value) => line_size(value.to_string().len(), limits)?,
            Frame::Bulk(None) => line_size(2, limits)?,
            Frame::Bulk(Some(value)) => {
                if value.len() > limits.max_bulk_bytes {
                    return Err(EncodeError::LimitExceeded("max_bulk_bytes"));
                }
                line_size(value.len().to_string().len(), limits)?
                    .checked_add(value.len())
                    .and_then(|size| size.checked_add(2))
                    .ok_or(EncodeError::LimitExceeded("max_frame_bytes"))?
            }
            Frame::Array(values) => {
                let depth = depth
                    .checked_add(1)
                    .filter(|depth| *depth <= limits.max_depth)
                    .ok_or(EncodeError::LimitExceeded("max_depth"))?;
                match values {
                    None => line_size(2, limits)?,
                    Some(values) => {
                        // Budget children before growing the metadata stack.
                        nodes
                            .checked_add(pending.len())
                            .and_then(|count| count.checked_add(values.len()))
                            .filter(|count| *count <= limits.max_nodes)
                            .ok_or(EncodeError::LimitExceeded("max_nodes"))?;
                        let header = line_size(values.len().to_string().len(), limits)?;
                        pending.extend(values.iter().rev().map(|frame| (frame, depth)));
                        header
                    }
                }
            }
        };
        size = size
            .checked_add(delta)
            .filter(|size| *size <= limits.max_frame_bytes)
            .ok_or(EncodeError::LimitExceeded("max_frame_bytes"))?;
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn invalid_late_child_leaves_bytes_and_capacity_unchanged() {
        let frame = Frame::Array(Some(vec![
            Frame::Bulk(Some(Bytes::from_static(b"valid"))),
            Frame::Array(Some(vec![Frame::Error(Bytes::from_static(b"bad\rline"))])),
        ]));
        let mut output = BytesMut::from(&b"existing"[..]);
        let capacity = output.capacity();
        assert!(matches!(
            encode(&frame, &mut output, RespLimits::default()),
            Err(EncodeError::InvalidFrame(_))
        ));
        assert_eq!(output, b"existing"[..]);
        assert_eq!(output.capacity(), capacity);
    }

    #[test]
    fn oversized_simple_payload_is_rejected_before_content_scan() {
        let frame = Frame::Simple(Bytes::from_static(b"oversized\n"));
        let limits = RespLimits {
            max_line_bytes: 4,
            ..RespLimits::default()
        };
        let mut output = BytesMut::from(&b"prefix"[..]);
        assert!(matches!(
            encode(&frame, &mut output, limits),
            Err(EncodeError::LimitExceeded("max_line_bytes"))
        ));
        assert_eq!(output, b"prefix"[..]);
    }

    #[test]
    fn every_budget_is_checked_without_appending_partial_output() {
        let frame = Frame::Array(Some(vec![Frame::Bulk(Some(Bytes::from_static(b"hello")))]));
        for limits in [
            RespLimits {
                max_frame_bytes: 14,
                max_bulk_bytes: 5,
                max_line_bytes: 4,
                ..RespLimits::default()
            },
            RespLimits {
                max_bulk_bytes: 4,
                ..RespLimits::default()
            },
            RespLimits {
                max_line_bytes: 3,
                ..RespLimits::default()
            },
            RespLimits {
                max_nodes: 1,
                ..RespLimits::default()
            },
            RespLimits {
                max_depth: 0,
                ..RespLimits::default()
            },
        ] {
            let mut output = BytesMut::from(&b"prefix"[..]);
            assert!(encode(&frame, &mut output, limits).is_err(), "{limits:?}");
            assert_eq!(output, b"prefix"[..]);
        }
    }

    #[test]
    fn exact_frame_limits_allow_an_existing_output_prefix() {
        let frame = Frame::Array(Some(vec![Frame::Bulk(Some(Bytes::from_static(b"hello")))]));
        let limits = RespLimits {
            max_frame_bytes: 15,
            max_bulk_bytes: 5,
            max_line_bytes: 4,
            max_nodes: 2,
            max_depth: 1,
        };
        let mut output = BytesMut::from(&b"prefix"[..]);
        encode(&frame, &mut output, limits).unwrap();
        assert_eq!(output, b"prefix*1\r\n$5\r\nhello\r\n"[..]);
    }

    #[test]
    fn null_and_empty_arrays_count_toward_depth() {
        for value in [Frame::Array(None), Frame::Array(Some(Vec::new()))] {
            let frame = Frame::Array(Some(vec![value]));
            let limits = RespLimits {
                max_depth: 1,
                ..RespLimits::default()
            };
            let mut output = BytesMut::new();
            assert!(matches!(
                encode(&frame, &mut output, limits),
                Err(EncodeError::LimitExceeded("max_depth"))
            ));
            assert!(output.is_empty());
        }
    }

    #[test]
    fn default_and_invalid_configurations_are_explicit() {
        RespLimits::default().validate().unwrap();
        for limits in [
            RespLimits {
                max_frame_bytes: 0,
                ..RespLimits::default()
            },
            RespLimits {
                max_bulk_bytes: 0,
                ..RespLimits::default()
            },
            RespLimits {
                max_line_bytes: 0,
                ..RespLimits::default()
            },
            RespLimits {
                max_nodes: 0,
                ..RespLimits::default()
            },
            RespLimits {
                max_depth: 0,
                ..RespLimits::default()
            },
            RespLimits {
                max_depth: 129,
                ..RespLimits::default()
            },
            RespLimits {
                max_bulk_bytes: usize::MAX,
                ..RespLimits::default()
            },
            RespLimits {
                max_line_bytes: usize::MAX,
                ..RespLimits::default()
            },
        ] {
            assert!(limits.validate().is_err(), "{limits:?}");
        }
    }
}
