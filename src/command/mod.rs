//! Comandos tipados e parsing sem acesso ao armazenamento.

mod parser;
mod reply;

use bytes::Bytes;

pub use parser::{RequestError, parse};
pub use reply::{ExecutionError, Reply};

/// Comando validado, sem canais ou conhecimento do protocolo de transporte.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Responde PONG ou devolve a mensagem binária.
    Ping(Option<Bytes>),
    /// Devolve exatamente o payload.
    Echo(Bytes),
    /// Lê o valor de uma chave.
    Get { key: Bytes },
    /// Cria ou substitui um valor, sem opções na 0.1.
    Set { key: Bytes, value: Bytes },
    /// Remove chaves; duplicatas são preservadas para contar apenas efeitos reais.
    Del { keys: Vec<Bytes> },
    /// Conta cada ocorrência de uma chave existente.
    Exists { keys: Vec<Bytes> },
    /// Incrementa um inteiro decimal i64, criando zero antes da operação se ausente.
    Incr { key: Bytes },
    /// Decrementa um inteiro decimal i64.
    Decr { key: Bytes },
    /// Lê valores na ordem das chaves, preservando duplicatas.
    MGet { keys: Vec<Bytes> },
    /// Aplica um lote indivisível; o último par de uma chave prevalece.
    MSet { entries: Vec<(Bytes, Bytes)> },
}
