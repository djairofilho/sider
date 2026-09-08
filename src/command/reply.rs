//! Resposta sem dependência de sockets ou canais.

use bytes::Bytes;

use crate::resp::Frame;

/// Resultado da execução síncrona no armazenamento.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// PING sem argumento.
    Pong,
    /// Mutação SET aceita.
    Ok,
    /// Mensagem/valor binário ou chave ausente.
    Bulk(Option<Bytes>),
    /// Quantidade de chaves removidas.
    Integer(i64),
}

impl From<Reply> for Frame {
    fn from(reply: Reply) -> Self {
        match reply {
            Reply::Pong => Self::Simple(Bytes::from_static(b"PONG")),
            Reply::Ok => Self::Simple(Bytes::from_static(b"OK")),
            Reply::Bulk(value) => Self::Bulk(value),
            Reply::Integer(value) => Self::Integer(value),
        }
    }
}
