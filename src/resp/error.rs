//! Erros de framing não carregam chaves nem valores do cliente.

use thiserror::Error;

use crate::ConfigError;

/// Falha terminal do decoder; o chamador deve descartar a conexão/decoder.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    /// Sintaxe RESP2 inválida.
    #[error("RESP inválido: {0}")]
    Malformed(&'static str),
    /// Frame excede um orçamento configurado.
    #[error("limite RESP excedido: {0}")]
    LimitExceeded(&'static str),
    /// O buffer foi reduzido enquanto um frame estava incompleto.
    #[error("buffer RESP alterado durante decodificação incompleta")]
    BufferChanged,
    /// O decoder já retornou erro e não pode recuperar sincronização.
    #[error("decoder RESP já encerrado por erro")]
    Poisoned,
}

/// O encoder valida tudo antes de alterar a saída.
#[derive(Debug, Error)]
pub enum EncodeError {
    /// Conteúdo inválido para seu tipo RESP2.
    #[error("frame RESP inválido: {0}")]
    InvalidFrame(&'static str),
    /// Frame excede um orçamento configurado.
    #[error("limite RESP excedido: {0}")]
    LimitExceeded(&'static str),
    /// Configuração precisa ser válida mesmo para um frame pequeno.
    #[error(transparent)]
    InvalidLimits(#[from] ConfigError),
}
