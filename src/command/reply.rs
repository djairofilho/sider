//! Resposta sem dependência de sockets ou canais.

use bytes::Bytes;
use thiserror::Error;

use crate::resp::Frame;

/// Erros recuperáveis de execução, sem encerrar a conexão ou o worker.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ExecutionError {
    #[error("CROSSSLOT Keys in request don't hash to the same slot")]
    CrossShard,
    #[error("ERR value is not an integer or out of range")]
    InvalidInteger,
    #[error("ERR increment or decrement would overflow")]
    IntegerOverflow,
    #[error("ERR invalid expire time in 'set' command")]
    InvalidSetExpiry,
    #[error("ERR invalid expire time in '{0}' command")]
    InvalidExpiry(&'static str),
    #[error("OOM dataset memory quota exceeded")]
    OutOfMemory,
    /// Pub/Sub depende do contexto da conexão e não executa no mapa.
    #[error("ERR command requires connection context")]
    ConnectionOnly,
    #[error("ERR AOF record limit exceeded")]
    AofRecordLimit,
}

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
    /// Respostas ordenadas de comandos multichave.
    Array(Vec<Reply>),
    /// Rejeição sem efeitos no armazenamento.
    Error(ExecutionError),
}

impl From<Reply> for Frame {
    fn from(reply: Reply) -> Self {
        match reply {
            Reply::Pong => Self::Simple(Bytes::from_static(b"PONG")),
            Reply::Ok => Self::Simple(Bytes::from_static(b"OK")),
            Reply::Bulk(value) => Self::Bulk(value),
            Reply::Integer(value) => Self::Integer(value),
            Reply::Array(values) => Self::Array(Some(values.into_iter().map(Self::from).collect())),
            Reply::Error(error) => Self::Error(Bytes::from(error.to_string())),
        }
    }
}
