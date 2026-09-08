//! Erros de configuração que podem ser apresentados na inicialização.

use std::net::AddrParseError;

use thiserror::Error;

/// Falha ao interpretar uma variável de configuração do Sider.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Limites inválidos são recusados antes de criar o codec.
    #[error("limites RESP inválidos: {reason}")]
    InvalidRespLimits {
        /// Restrição de configuração violada, sem conteúdo do cliente.
        reason: &'static str,
    },

    /// O endereço contém texto que não pode ser representado em UTF-8.
    #[error("SIDER_ADDR precisa conter texto UTF-8 válido")]
    NonUnicodeAddress,

    /// O endereço não contém um IP literal e uma porta válida.
    #[error("SIDER_ADDR inválido: {value:?}; use IP e porta, como 127.0.0.1:6379")]
    InvalidAddress {
        /// Valor recebido, preservado para diagnóstico.
        value: String,
        /// Erro original do parser de endereços da biblioteca padrão.
        #[source]
        source: AddrParseError,
    },
}
