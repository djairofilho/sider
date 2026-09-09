//! Validação completa da requisição antes de construir um comando executável.

use bytes::Bytes;
use thiserror::Error;

use super::{Command, ExpiryUnit, SetCondition, SetExpiry, SetOptions};
use crate::resp::Frame;

/// Erro de formato encerra a conexão; erros de comando permitem continuar.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RequestError {
    /// Apenas arrays não vazios de bulk strings não nulas são executáveis.
    #[error("formato de requisição inválido")]
    InvalidFormat,
    /// Mensagem simplificada, sem reproduzir dados enviados pelo cliente.
    #[error("comando desconhecido")]
    UnknownCommand,
    /// Nome canônico usado para a resposta compatível de aridade.
    #[error("aridade inválida para {0}")]
    WrongArity(&'static str),
    #[error("sintaxe inválida")]
    Syntax,
    #[error("inteiro inválido ou fora do intervalo")]
    InvalidInteger,
    #[error("prazo de SET inválido")]
    InvalidSetExpiry,
    #[error("prazo de expiração inválido")]
    InvalidExpiry(&'static str),
}

impl RequestError {
    /// Indica se o chamador precisa encerrar o fluxo após responder, se possível.
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::InvalidFormat)
    }

    /// Converte o erro em resposta controlada, sem revelar argumentos do cliente.
    pub fn into_frame(self) -> Frame {
        let message = match self {
            Self::InvalidFormat => Bytes::from_static(b"ERR invalid request format"),
            Self::UnknownCommand => Bytes::from_static(b"ERR unknown command"),
            Self::WrongArity(name) => Bytes::from(format!(
                "ERR wrong number of arguments for '{name}' command"
            )),
            Self::Syntax => Bytes::from_static(b"ERR syntax error"),
            Self::InvalidInteger => {
                Bytes::from_static(b"ERR value is not an integer or out of range")
            }
            Self::InvalidSetExpiry => {
                Bytes::from_static(b"ERR invalid expire time in 'set' command")
            }
            Self::InvalidExpiry(name) => {
                Bytes::from(format!("ERR invalid expire time in '{name}' command"))
            }
        };
        Frame::Error(message)
    }
}

/// Constrói um comando validado, movendo os payloads sem copiá-los novamente.
///
/// O formato de todos os argumentos é verificado antes da aridade ou do nome.
/// Nomes não distinguem caixa ASCII; chaves e valores preservam todos os bytes.
pub fn parse(frame: Frame) -> Result<Command, RequestError> {
    let Frame::Array(Some(frames)) = frame else {
        return Err(RequestError::InvalidFormat);
    };
    let arguments: Option<Vec<Bytes>> = frames
        .into_iter()
        .map(|frame| match frame {
            Frame::Bulk(Some(value)) => Some(value),
            _ => None,
        })
        .collect();
    let mut arguments = arguments.ok_or(RequestError::InvalidFormat)?.into_iter();
    let name = arguments.next().ok_or(RequestError::InvalidFormat)?;
    let count = arguments.len();
    if name.eq_ignore_ascii_case(b"PING") {
        if count > 1 {
            return Err(RequestError::WrongArity("ping"));
        }
        Ok(Command::Ping(arguments.next()))
    } else if name.eq_ignore_ascii_case(b"ECHO") {
        if count != 1 {
            return Err(RequestError::WrongArity("echo"));
        }
        arguments
            .next()
            .map(Command::Echo)
            .ok_or(RequestError::WrongArity("echo"))
    } else if name.eq_ignore_ascii_case(b"GET") {
        if count != 1 {
            return Err(RequestError::WrongArity("get"));
        }
        let key = arguments.next().ok_or(RequestError::WrongArity("get"))?;
        Ok(Command::Get { key })
    } else if name.eq_ignore_ascii_case(b"SET") {
        if count < 2 {
            return Err(RequestError::WrongArity("set"));
        }
        let key = arguments.next().ok_or(RequestError::WrongArity("set"))?;
        let value = arguments.next().ok_or(RequestError::WrongArity("set"))?;
        if count == 2 {
            return Ok(Command::Set { key, value });
        }
        let options = parse_set_options(arguments)?;
        Ok(Command::SetWithOptions {
            key,
            value,
            options,
        })
    } else if name.eq_ignore_ascii_case(b"DEL") {
        if count == 0 {
            return Err(RequestError::WrongArity("del"));
        }
        Ok(Command::Del {
            keys: arguments.collect(),
        })
    } else if name.eq_ignore_ascii_case(b"EXISTS") || name.eq_ignore_ascii_case(b"MGET") {
        let exists = name.eq_ignore_ascii_case(b"EXISTS");
        if count == 0 {
            return Err(RequestError::WrongArity(if exists {
                "exists"
            } else {
                "mget"
            }));
        }
        let keys = arguments.collect();
        Ok(if exists {
            Command::Exists { keys }
        } else {
            Command::MGet { keys }
        })
    } else if name.eq_ignore_ascii_case(b"INCR") || name.eq_ignore_ascii_case(b"DECR") {
        let incr = name.eq_ignore_ascii_case(b"INCR");
        let canonical = if incr { "incr" } else { "decr" };
        if count != 1 {
            return Err(RequestError::WrongArity(canonical));
        }
        let key = arguments
            .next()
            .ok_or(RequestError::WrongArity(canonical))?;
        Ok(if incr {
            Command::Incr { key }
        } else {
            Command::Decr { key }
        })
    } else if name.eq_ignore_ascii_case(b"MSET") {
        if count == 0 || !count.is_multiple_of(2) {
            return Err(RequestError::WrongArity("mset"));
        }
        let mut entries = Vec::with_capacity(count / 2);
        while let Some(key) = arguments.next() {
            let value = arguments.next().ok_or(RequestError::WrongArity("mset"))?;
            entries.push((key, value));
        }
        Ok(Command::MSet { entries })
    } else if name.eq_ignore_ascii_case(b"EXPIRE") || name.eq_ignore_ascii_case(b"PEXPIRE") {
        let seconds = name.eq_ignore_ascii_case(b"EXPIRE");
        let canonical = if seconds { "expire" } else { "pexpire" };
        if count != 2 {
            return Err(RequestError::WrongArity(canonical));
        }
        let key = arguments
            .next()
            .ok_or(RequestError::WrongArity(canonical))?;
        let argument = arguments
            .next()
            .ok_or(RequestError::WrongArity(canonical))?;
        let value = parse_integer(&argument)?;
        if seconds {
            value
                .checked_mul(1000)
                .ok_or(RequestError::InvalidExpiry(canonical))?;
        }
        Ok(Command::Expire {
            key,
            value,
            unit: if seconds {
                ExpiryUnit::Seconds
            } else {
                ExpiryUnit::Milliseconds
            },
        })
    } else if name.eq_ignore_ascii_case(b"TTL")
        || name.eq_ignore_ascii_case(b"PTTL")
        || name.eq_ignore_ascii_case(b"PERSIST")
    {
        let persist = name.eq_ignore_ascii_case(b"PERSIST");
        let milliseconds = name.eq_ignore_ascii_case(b"PTTL");
        let canonical = if persist {
            "persist"
        } else if milliseconds {
            "pttl"
        } else {
            "ttl"
        };
        if count != 1 {
            return Err(RequestError::WrongArity(canonical));
        }
        let key = arguments
            .next()
            .ok_or(RequestError::WrongArity(canonical))?;
        Ok(if persist {
            Command::Persist { key }
        } else {
            Command::Ttl { key, milliseconds }
        })
    } else {
        Err(RequestError::UnknownCommand)
    }
}

fn parse_integer(value: &[u8]) -> Result<i64, RequestError> {
    super::parse_decimal(value).ok_or(RequestError::InvalidInteger)
}

fn parse_set_options(mut args: impl Iterator<Item = Bytes>) -> Result<SetOptions, RequestError> {
    let mut options = SetOptions::default();
    let mut duration = None;
    let mut unit = None;
    while let Some(option) = args.next() {
        if option.eq_ignore_ascii_case(b"NX") && options.condition != SetCondition::Present {
            options.condition = SetCondition::Missing;
        } else if option.eq_ignore_ascii_case(b"XX") && options.condition != SetCondition::Missing {
            options.condition = SetCondition::Present;
        } else if option.eq_ignore_ascii_case(b"GET") {
            options.return_previous = true;
        } else if option.eq_ignore_ascii_case(b"KEEPTTL") && duration.is_none() {
            options.expiry = SetExpiry::Keep;
        } else if (option.eq_ignore_ascii_case(b"EX") || option.eq_ignore_ascii_case(b"PX"))
            && options.expiry != SetExpiry::Keep
        {
            let seconds = option.eq_ignore_ascii_case(b"EX");
            if unit.is_some_and(|previous| previous != seconds) {
                return Err(RequestError::Syntax);
            }
            unit = Some(seconds);
            duration = Some(args.next().ok_or(RequestError::Syntax)?);
        } else {
            return Err(RequestError::Syntax);
        }
    }
    if let Some(value) = duration {
        let value = parse_integer(&value)?;
        let millis = if unit == Some(true) {
            value
                .checked_mul(1000)
                .ok_or(RequestError::InvalidSetExpiry)?
        } else {
            value
        };
        if millis <= 0 {
            return Err(RequestError::InvalidSetExpiry);
        }
        options.expiry = SetExpiry::After(std::time::Duration::from_millis(millis as u64));
    }
    Ok(options)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(args: &[&'static [u8]]) -> Frame {
        Frame::Array(Some(
            args.iter()
                .map(|bytes| Frame::Bulk(Some(Bytes::from_static(bytes))))
                .collect(),
        ))
    }

    #[test]
    fn validates_format_before_command_and_arity() {
        for frame in [
            Frame::Array(None),
            Frame::Array(Some(vec![])),
            Frame::Bulk(Some(Bytes::from_static(b"PING"))),
            Frame::Array(Some(vec![Frame::Simple(Bytes::from_static(b"PING"))])),
            Frame::Array(Some(vec![Frame::Bulk(None)])),
            Frame::Array(Some(vec![
                Frame::Bulk(Some(Bytes::from_static(b"unknown"))),
                Frame::Integer(0),
            ])),
            Frame::Array(Some(vec![
                Frame::Bulk(Some(Bytes::from_static(b"GET"))),
                Frame::Array(Some(vec![])),
            ])),
        ] {
            assert_eq!(parse(frame), Err(RequestError::InvalidFormat));
        }
        assert!(RequestError::InvalidFormat.is_fatal());
        assert!(!RequestError::UnknownCommand.is_fatal());
        assert!(!RequestError::WrongArity("set").is_fatal());
        assert!(!RequestError::Syntax.is_fatal());
    }

    #[test]
    fn ascii_case_does_not_change_binary_arguments() {
        assert_eq!(parse(request(&[b"pInG"])), Ok(Command::Ping(None)));
        assert_eq!(
            parse(request(&[b"PiNg", b"\0\xff"])),
            Ok(Command::Ping(Some(Bytes::from_static(b"\0\xff"))))
        );
        assert_eq!(
            parse(request(&[b"eChO", b""])),
            Ok(Command::Echo(Bytes::new()))
        );
        assert_eq!(
            parse(request(&[b"gEt", b"K\r\n"])),
            Ok(Command::Get {
                key: Bytes::from_static(b"K\r\n")
            })
        );
        assert_eq!(
            parse(request(&[b"sEt", b"", b"\0\xff"])),
            Ok(Command::Set {
                key: Bytes::new(),
                value: Bytes::from_static(b"\0\xff")
            })
        );
    }

    #[test]
    fn del_preserves_duplicates_and_binary_keys() {
        assert_eq!(
            parse(request(&[b"dEl", b"a", b"a", b"\xff"])),
            Ok(Command::Del {
                keys: vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"\xff")
                ]
            })
        );
    }

    #[test]
    fn invalid_options_and_unknown_commands_have_explicit_errors() {
        for option in [b"EX".as_slice(), b"PX", b"invalid"] {
            assert_eq!(
                parse(request(&[b"SET", b"key", b"value", option])),
                Err(RequestError::Syntax)
            );
        }
        for name in [b"".as_slice(), b"\xff", b"GET\0", b"SELECT"] {
            assert_eq!(parse(request(&[name])), Err(RequestError::UnknownCommand));
        }
        assert_eq!(
            RequestError::UnknownCommand.into_frame(),
            Frame::Error(Bytes::from_static(b"ERR unknown command"))
        );
        assert_eq!(
            RequestError::Syntax.into_frame(),
            Frame::Error(Bytes::from_static(b"ERR syntax error"))
        );
    }

    #[test]
    fn parsing_moves_payload_without_a_second_copy() {
        let payload = Bytes::from(vec![b'x'; 1024]);
        let address = payload.as_ptr();
        let frame = Frame::Array(Some(vec![
            Frame::Bulk(Some(Bytes::from_static(b"ECHO"))),
            Frame::Bulk(Some(payload)),
        ]));
        let Command::Echo(value) = parse(frame).unwrap() else {
            panic!("echo esperado")
        };
        assert_eq!(value.as_ptr(), address);
    }
}
