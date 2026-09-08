//! Os cinco tipos RESP2. Todos os payloads preservam bytes, sem exigir UTF-8.

use bytes::Bytes;

/// Um frame de protocolo, ainda sem validação como comando executável.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// Simple string; não pode conter CR ou LF.
    Simple(Bytes),
    /// Erro de protocolo/aplicação; não pode conter CR ou LF.
    Error(Bytes),
    /// Inteiro decimal assinado de 64 bits.
    Integer(i64),
    /// Bulk binário; `None` é nulo, diferente de um payload vazio.
    Bulk(Option<Bytes>),
    /// Array heterogêneo; `None` é nulo, diferente de um array vazio.
    Array(Option<Vec<Frame>>),
}
