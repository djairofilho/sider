//! Erros de configuração que podem ser apresentados na inicialização.

use std::net::AddrParseError;

use thiserror::Error;

/// Falha ao interpretar uma variável de configuração do Sider.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Limites de rede, canais ou prazos incoerentes são recusados na inicialização.
    #[error("limites do servidor inválidos: {reason}")]
    InvalidServerLimits {
        /// Restrição violada, sem dados de requisições.
        reason: &'static str,
    },

    /// Uma opção textual contém dados que não podem ser interpretados em UTF-8.
    #[error("{name} precisa conter texto UTF-8 válido")]
    NonUnicodeValue {
        /// Nome da variável de configuração.
        name: &'static str,
    },

    /// Um limite ou prazo não é um inteiro decimal representável.
    #[error("{name} inválido: {value:?}; use somente dígitos decimais, sem sinal ou espaços")]
    InvalidInteger {
        /// Nome da variável de configuração.
        name: &'static str,
        /// Valor recebido, preservado para diagnóstico.
        value: String,
    },

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
