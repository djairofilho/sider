//! Configuração inicial, com leitura de ambiente separada da validação.

use std::ffi::OsString;
use std::net::SocketAddr;

use crate::ConfigError;

/// Configuração do futuro listener TCP.
///
/// Os limites do codec e do worker serão introduzidos com seus consumidores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    /// IP literal e porta; o padrão mantém o serviço em loopback.
    pub bind_addr: SocketAddr,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 6379)),
        }
    }
}

impl ServerConfig {
    /// Lê `SIDER_ADDR` do ambiente sem modificá-lo.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var_os(name))
    }

    /// Interpreta a configuração a partir de uma fonte de valores injetável.
    ///
    /// Testes podem fornecer variáveis sem alterar o ambiente global do processo.
    /// Valores vazios ou com espaços extras são inválidos; ausência usa o padrão.
    pub fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<OsString>,
    ) -> Result<Self, ConfigError> {
        let Some(value) = lookup("SIDER_ADDR") else {
            return Ok(Self::default());
        };

        let value = value
            .into_string()
            .map_err(|_| ConfigError::NonUnicodeAddress)?;
        let bind_addr = value
            .parse()
            .map_err(|source| ConfigError::InvalidAddress { value, source })?;

        Ok(Self { bind_addr })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with(value: &str) -> Result<ServerConfig, ConfigError> {
        ServerConfig::from_lookup(|name| {
            assert_eq!(name, "SIDER_ADDR");
            Some(OsString::from(value))
        })
    }

    #[test]
    fn missing_address_uses_loopback_and_redis_port() {
        let config = ServerConfig::from_lookup(|_| None).unwrap();

        assert_eq!(config.bind_addr, SocketAddr::from(([127, 0, 0, 1], 6379)));
    }

    #[test]
    fn accepts_ipv4_address() {
        let config = config_with("127.0.0.1:6380").unwrap();

        assert_eq!(config.bind_addr, SocketAddr::from(([127, 0, 0, 1], 6380)));
    }

    #[test]
    fn accepts_bracketed_ipv6_address() {
        let config = config_with("[::1]:6380").unwrap();

        assert!(config.bind_addr.is_ipv6());
        assert!(config.bind_addr.ip().is_loopback());
        assert_eq!(config.bind_addr.port(), 6380);
    }

    #[test]
    fn permits_port_zero_for_future_ephemeral_test_listeners() {
        assert_eq!(config_with("127.0.0.1:0").unwrap().bind_addr.port(), 0);
    }

    #[test]
    fn rejects_malformed_addresses_without_using_the_default() {
        for value in [
            "",
            "localhost:6379",
            "127.0.0.1",
            "127.0.0.1:65536",
            "127.0.0.1:-1",
            " 127.0.0.1:6379",
            "127.0.0.1:6379 ",
            "::1:6379",
        ] {
            match config_with(value) {
                Err(ConfigError::InvalidAddress { value: actual, .. }) => {
                    assert_eq!(actual, value);
                }
                result => panic!("expected invalid address for {value:?}, got {result:?}"),
            }
        }
    }

    #[test]
    fn invalid_address_keeps_the_original_error_source() {
        use std::error::Error;

        let error = config_with("invalid").unwrap_err();

        assert!(error.source().is_some());
        assert!(error.to_string().contains("SIDER_ADDR"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_non_unicode_address() {
        use std::os::unix::ffi::OsStringExt;

        let result = ServerConfig::from_lookup(|_| Some(OsString::from_vec(vec![0xff])));

        assert!(matches!(result, Err(ConfigError::NonUnicodeAddress)));
    }

    #[cfg(windows)]
    #[test]
    fn rejects_non_unicode_address() {
        use std::os::windows::ffi::OsStringExt;

        let result = ServerConfig::from_lookup(|_| Some(OsString::from_wide(&[0xd800])));

        assert!(matches!(result, Err(ConfigError::NonUnicodeAddress)));
    }
}
