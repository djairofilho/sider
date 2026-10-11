//! Connection control parsing without storage or protocol state mutation.

use super::{RequestError, parse_decimal};
use crate::resp::Frame;
use bytes::Bytes;

// No Debug: credential arguments must not appear in diagnostics.
pub(crate) struct HelloAuthentication<'a> {
    pub version: Option<u8>,
    pub authentication: Option<(&'a Bytes, &'a Bytes)>,
}

pub(crate) fn parse_auth(frame: &Frame) -> Result<(&Bytes, &Bytes), RequestError> {
    let parts = bulk_arguments(frame)?;
    if parts.len() != 3 {
        return Err(RequestError::WrongArity("auth"));
    }
    let (Frame::Bulk(Some(username)), Frame::Bulk(Some(password))) = (&parts[1], &parts[2]) else {
        return Err(RequestError::InvalidFormat);
    };
    Ok((username, password))
}

fn bulk_arguments(frame: &Frame) -> Result<&[Frame], RequestError> {
    match frame {
        Frame::Array(Some(parts))
            if !parts.is_empty()
                && parts
                    .iter()
                    .all(|part| matches!(part, Frame::Bulk(Some(_)))) =>
        {
            Ok(parts)
        }
        _ => Err(RequestError::InvalidFormat),
    }
}

/// Validates negotiation and complete credential grammar before scheduling verification.
pub(crate) fn parse_hello_auth(frame: &Frame) -> Result<HelloAuthentication<'_>, RequestError> {
    let parts = bulk_arguments(frame)?;
    let version = match parts.get(1) {
        None => None,
        Some(Frame::Bulk(Some(version))) => Some(match version.as_ref() {
            b"2" => 2,
            b"3" => 3,
            _ if parse_decimal(version).is_some() => {
                return Err(RequestError::UnsupportedProtocolVersion);
            }
            _ => return Err(RequestError::InvalidProtocolVersion),
        }),
        _ => return Err(RequestError::InvalidFormat),
    };
    let authentication = if parts.len() <= 2 {
        None
    } else {
        if !matches!(&parts[2], Frame::Bulk(Some(option)) if option.eq_ignore_ascii_case(b"AUTH")) {
            return Err(RequestError::UnsupportedHelloOptions);
        }
        if parts.len() != 5 {
            return Err(RequestError::Syntax);
        }
        let (Frame::Bulk(Some(username)), Frame::Bulk(Some(password))) = (&parts[3], &parts[4])
        else {
            return Err(RequestError::InvalidFormat);
        };
        Some((username, password))
    };
    Ok(HelloAuthentication {
        version,
        authentication,
    })
}

/// Validates HELLO before its caller changes the connection protocol.
pub(crate) fn parse_hello(frame: &Frame) -> Result<Option<u8>, RequestError> {
    let Frame::Array(Some(parts)) = frame else {
        return Err(RequestError::InvalidFormat);
    };
    if parts.is_empty()
        || parts
            .iter()
            .any(|part| !matches!(part, Frame::Bulk(Some(_))))
    {
        return Err(RequestError::InvalidFormat);
    }
    let Frame::Bulk(Some(name)) = &parts[0] else {
        return Err(RequestError::InvalidFormat);
    };
    if !name.eq_ignore_ascii_case(b"HELLO") {
        return Err(RequestError::UnknownCommand);
    }
    if parts.len() > 2 {
        return Err(RequestError::UnsupportedHelloOptions);
    }
    match parts.get(1) {
        None => Ok(None),
        Some(Frame::Bulk(Some(version))) => match version.as_ref() {
            b"2" => Ok(Some(2)),
            b"3" => Ok(Some(3)),
            _ if parse_decimal(version).is_some() => Err(RequestError::UnsupportedProtocolVersion),
            _ => Err(RequestError::InvalidProtocolVersion),
        },
        _ => Err(RequestError::InvalidFormat),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn request(args: &[&'static [u8]]) -> Frame {
        Frame::Array(Some(
            args.iter()
                .map(|arg| Frame::Bulk(Some(Bytes::from_static(arg))))
                .collect(),
        ))
    }

    #[test]
    fn hello_validates_format_and_version_before_negotiation() {
        assert_eq!(parse_hello(&request(&[b"HELLO"])), Ok(None));
        assert_eq!(parse_hello(&request(&[b"hElLo", b"3"])), Ok(Some(3)));
        assert_eq!(parse_hello(&request(&[b"HELLO", b"2"])), Ok(Some(2)));
        for version in [b"0".as_slice(), b"1", b"4", b"-1"] {
            assert_eq!(
                parse_hello(&request(&[b"HELLO", version])),
                Err(RequestError::UnsupportedProtocolVersion)
            );
        }
        for version in [b"".as_slice(), b"03", b"+3", b"three", b"\xff"] {
            assert_eq!(
                parse_hello(&request(&[b"HELLO", version])),
                Err(RequestError::InvalidProtocolVersion)
            );
        }
        assert_eq!(
            parse_hello(&request(&[b"HELLO", b"3", b"AUTH", b"default", b"secret"])),
            Err(RequestError::UnsupportedHelloOptions)
        );
        assert_eq!(
            parse_hello(&Frame::Array(Some(vec![
                Frame::Bulk(Some(Bytes::from_static(b"HELLO"))),
                Frame::Integer(3)
            ]))),
            Err(RequestError::InvalidFormat)
        );
    }
}
