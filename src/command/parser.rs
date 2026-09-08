//! Validação completa da requisição antes de construir um comando executável.

use bytes::Bytes;
use thiserror::Error;

use super::Command;
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
    /// A 0.1 não aceita nenhum argumento após o valor de SET.
    #[error("opções de SET não suportadas")]
    UnsupportedSetOptions,
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
            Self::UnsupportedSetOptions => Bytes::from_static(b"ERR unsupported SET options"),
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
        if count > 2 {
            return Err(RequestError::UnsupportedSetOptions);
        }
        let key = arguments.next().ok_or(RequestError::WrongArity("set"))?;
        let value = arguments.next().ok_or(RequestError::WrongArity("set"))?;
        Ok(Command::Set { key, value })
    } else if name.eq_ignore_ascii_case(b"DEL") {
        if count == 0 {
            return Err(RequestError::WrongArity("del"));
        }
        Ok(Command::Del {
            keys: arguments.collect(),
        })
    } else {
        Err(RequestError::UnknownCommand)
    }
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
        assert!(!RequestError::UnsupportedSetOptions.is_fatal());
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
    fn options_and_unknown_commands_have_explicit_divergences() {
        for option in [
            b"NX".as_slice(),
            b"XX",
            b"EX",
            b"PX",
            b"GET",
            b"KEEPTTL",
            b"invalid",
        ] {
            assert_eq!(
                parse(request(&[b"SET", b"key", b"value", option])),
                Err(RequestError::UnsupportedSetOptions)
            );
        }
        for name in [b"".as_slice(), b"\xff", b"GET\0", b"SELECT", b"INCR"] {
            assert_eq!(parse(request(&[name])), Err(RequestError::UnknownCommand));
        }
        assert_eq!(
            RequestError::UnknownCommand.into_frame(),
            Frame::Error(Bytes::from_static(b"ERR unknown command"))
        );
        assert_eq!(
            RequestError::UnsupportedSetOptions.into_frame(),
            Frame::Error(Bytes::from_static(b"ERR unsupported SET options"))
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
