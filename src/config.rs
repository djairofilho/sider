//! Configuração de rede, codec e worker, validada sem alterar o ambiente.

use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

use crate::ConfigError;
use crate::resp::RespLimits;

/// Configuração do servidor TCP e dos recursos limitados de cada conexão.
///
/// Campos públicos permitem configurar testes sem ambiente global. Quem constrói
/// a estrutura diretamente deve chamar [`Self::validate`] antes de usar os limites.
/// Limites de rede não representam uma quota do dataset ou do RSS do processo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    /// IP literal e porta; o padrão mantém o serviço em loopback.
    pub bind_addr: SocketAddr,
    /// Orçamento individual de cada frame recebido.
    pub resp_limits: RespLimits,
    /// Conexões simultâneas admitidas; excedentes não criam tarefas persistentes.
    pub max_connections: usize,
    /// Quantidade de comandos aceitos que podem aguardar na fila do worker.
    pub worker_queue_capacity: usize,
    /// Quantidade fixa de workers proprietários; não admite resharding online.
    pub shards: usize,
    /// Quantidade máxima de canais distintos por assinante Pub/Sub.
    pub pubsub_max_channels: usize,
    /// Notificações pendentes por assinante; lotação encerra a conexão.
    pub pubsub_queue_capacity: usize,
    /// Comandos retidos por conexão entre MULTI e EXEC/DISCARD.
    pub transaction_max_commands: usize,
    /// Soma dos bytes RESP dos comandos retidos na fila transacional.
    pub transaction_max_bytes: usize,
    /// Chaves distintas observadas por conexão.
    pub watch_max_keys: usize,
    /// Bytes não consumidos que uma conexão pode manter no buffer de entrada.
    pub max_input_buffer_bytes: usize,
    /// Bytes de uma resposta completa, incluindo framing.
    pub max_response_bytes: usize,
    /// Bytes lógicos do dataset, incluindo a taxa fixa por entrada, sem eviction.
    pub max_dataset_bytes: usize,
    /// Prazo de formação do frame, contado desde seu primeiro byte.
    pub frame_timeout: Duration,
    /// Prazo total para enviar ao worker e receber sua resposta.
    pub request_timeout: Duration,
    /// Prazo para escrever uma resposta completa.
    pub write_timeout: Duration,
    /// Prazo de drenagem após o sinal de encerramento.
    pub shutdown_timeout: Duration,
    /// Caminho nativo opcional para o registro de prontidão do binário.
    pub ready_file: Option<PathBuf>,
    /// Persistência opcional; ausência mantém o modo em memória.
    pub aof: Option<crate::persistence::AofConfig>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 6379)),
            resp_limits: RespLimits::default(),
            max_connections: 32,
            worker_queue_capacity: 32,
            shards: 1,
            pubsub_max_channels: 32,
            pubsub_queue_capacity: 32,
            transaction_max_commands: 128,
            transaction_max_bytes: 1024 * 1024,
            watch_max_keys: 128,
            max_input_buffer_bytes: 4 * 1024 * 1024,
            max_response_bytes: 4 * 1024 * 1024,
            max_dataset_bytes: crate::storage::StoreConfig::default().max_dataset_bytes,
            frame_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(5),
            ready_file: None,
            aof: None,
        }
    }
}

impl ServerConfig {
    /// Lê as variáveis `SIDER_*` do ambiente sem modificá-lo.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var_os(name))
    }

    /// Interpreta e valida opções a partir de uma fonte de valores injetável.
    ///
    /// Ausência usa o padrão. Endereço e números rejeitam espaços extras; números
    /// aceitam somente dígitos ASCII, sem sinal, e os prazos usam milissegundos.
    /// `SIDER_READY_FILE` preserva o caminho nativo, inclusive fora de UTF-8.
    pub fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<OsString>,
    ) -> Result<Self, ConfigError> {
        let mut config = Self::default();
        if let Some(value) = lookup("SIDER_ADDR") {
            let value = value
                .into_string()
                .map_err(|_| ConfigError::NonUnicodeAddress)?;
            config.bind_addr = value
                .parse()
                .map_err(|source| ConfigError::InvalidAddress { value, source })?;
        }

        macro_rules! read_size {
            ($field:expr, $name:literal) => {
                if let Some(value) = lookup($name) {
                    $field = parse_integer($name, value)?;
                }
            };
        }
        read_size!(config.max_connections, "SIDER_MAX_CONNECTIONS");
        read_size!(config.worker_queue_capacity, "SIDER_WORKER_QUEUE_CAPACITY");
        read_size!(config.shards, "SIDER_SHARDS");
        read_size!(config.pubsub_max_channels, "SIDER_PUBSUB_MAX_CHANNELS");
        read_size!(config.pubsub_queue_capacity, "SIDER_PUBSUB_QUEUE_CAPACITY");
        read_size!(
            config.transaction_max_commands,
            "SIDER_TRANSACTION_MAX_COMMANDS"
        );
        read_size!(config.transaction_max_bytes, "SIDER_TRANSACTION_MAX_BYTES");
        read_size!(config.watch_max_keys, "SIDER_WATCH_MAX_KEYS");
        read_size!(config.resp_limits.max_frame_bytes, "SIDER_MAX_FRAME_BYTES");
        read_size!(config.resp_limits.max_bulk_bytes, "SIDER_MAX_BULK_BYTES");
        read_size!(config.resp_limits.max_line_bytes, "SIDER_MAX_LINE_BYTES");
        read_size!(config.resp_limits.max_nodes, "SIDER_MAX_NODES");
        read_size!(config.resp_limits.max_depth, "SIDER_MAX_DEPTH");
        read_size!(
            config.max_input_buffer_bytes,
            "SIDER_MAX_INPUT_BUFFER_BYTES"
        );
        read_size!(config.max_response_bytes, "SIDER_MAX_RESPONSE_BYTES");
        read_size!(config.max_dataset_bytes, "SIDER_MAX_DATASET_BYTES");

        for (field, name) in [
            (&mut config.frame_timeout, "SIDER_FRAME_TIMEOUT_MS"),
            (&mut config.request_timeout, "SIDER_REQUEST_TIMEOUT_MS"),
            (&mut config.write_timeout, "SIDER_WRITE_TIMEOUT_MS"),
            (&mut config.shutdown_timeout, "SIDER_SHUTDOWN_TIMEOUT_MS"),
        ] {
            if let Some(value) = lookup(name) {
                *field = Duration::from_millis(parse_integer(name, value)?);
            }
        }
        config.ready_file = lookup("SIDER_READY_FILE").map(PathBuf::from);
        if let Some(directory) = lookup("SIDER_AOF_DIR") {
            let mut aof = crate::persistence::AofConfig::new(PathBuf::from(directory));
            crate::storage::routing::ShardRouter::new(config.shards)?;
            aof.layout.shard_count = config.shards as u32;
            if let Some(value) = lookup("SIDER_AOF_SYNC") {
                aof.sync = match value.to_str() {
                    Some("always") => crate::persistence::SyncPolicy::Always,
                    Some("everysec") => {
                        crate::persistence::SyncPolicy::Periodic(Duration::from_secs(1))
                    }
                    _ => {
                        return Err(ConfigError::InvalidServerLimits {
                            reason: "SIDER_AOF_SYNC aceita always ou everysec",
                        });
                    }
                };
            }
            read_size!(aof.queue_capacity, "SIDER_AOF_QUEUE_CAPACITY");
            read_size!(aof.limits.max_record_bytes, "SIDER_AOF_MAX_RECORD_BYTES");
            read_size!(aof.max_delta_bytes, "SIDER_AOF_MAX_DELTA_BYTES");
            read_size!(aof.compact_after_bytes, "SIDER_AOF_COMPACT_AFTER_BYTES");
            config.aof = Some(aof);
        }
        config.validate()?;
        Ok(config)
    }

    /// Recusa limites nulos, relações incoerentes e valores técnicos inseguros.
    ///
    /// A resposta deve comportar mensagens internas curtas (128 bytes) e o maior
    /// bulk configurado com seu framing. Limites de linha de entrada não limitam
    /// mensagens internas de saída. A porta zero continua válida para bind efêmero.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(aof) = &self.aof {
            aof.validate()?;
            if aof.layout.shard_count as usize != self.shards {
                return Err(ConfigError::InvalidServerLimits {
                    reason: "quantidade de shards do AOF difere do servidor",
                });
            }
        }
        self.resp_limits.validate()?;
        crate::storage::routing::ShardRouter::new(self.shards)?;
        crate::storage::StoreConfig {
            max_dataset_bytes: self.max_dataset_bytes,
        }
        .validate()?;
        let invalid = |reason| ConfigError::InvalidServerLimits { reason };
        if self.max_dataset_bytes < self.shards {
            return Err(invalid(
                "a quota total precisa reservar ao menos um byte por shard",
            ));
        }
        for (value, reason) in [
            (
                self.transaction_max_commands,
                "transaction_max_commands precisa ser maior que zero",
            ),
            (
                self.transaction_max_bytes,
                "transaction_max_bytes precisa ser maior que zero",
            ),
            (
                self.watch_max_keys,
                "watch_max_keys precisa ser maior que zero",
            ),
            (
                self.pubsub_max_channels,
                "pubsub_max_channels precisa ser maior que zero",
            ),
            (
                self.pubsub_queue_capacity,
                "pubsub_queue_capacity precisa ser maior que zero",
            ),
            (
                self.max_connections,
                "SIDER_MAX_CONNECTIONS (max_connections) precisa ser maior que zero",
            ),
            (
                self.worker_queue_capacity,
                "SIDER_WORKER_QUEUE_CAPACITY (worker_queue_capacity) precisa ser maior que zero",
            ),
            (
                self.max_input_buffer_bytes,
                "SIDER_MAX_INPUT_BUFFER_BYTES (max_input_buffer_bytes) precisa ser maior que zero",
            ),
            (
                self.max_response_bytes,
                "SIDER_MAX_RESPONSE_BYTES (max_response_bytes) precisa ser maior que zero",
            ),
        ] {
            if value == 0 {
                return Err(invalid(reason));
            }
        }
        if self.max_connections > tokio::sync::Semaphore::MAX_PERMITS
            || self.worker_queue_capacity > tokio::sync::Semaphore::MAX_PERMITS
            || self.pubsub_max_channels > tokio::sync::Semaphore::MAX_PERMITS
            || self.pubsub_queue_capacity > tokio::sync::Semaphore::MAX_PERMITS
            || self.transaction_max_commands > tokio::sync::Semaphore::MAX_PERMITS
            || self.watch_max_keys > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(invalid(
                "conexões e capacidade da fila não podem exceder Semaphore::MAX_PERMITS",
            ));
        }
        if [
            self.resp_limits.max_frame_bytes,
            self.resp_limits.max_bulk_bytes,
            self.resp_limits.max_line_bytes,
            self.max_input_buffer_bytes,
            self.max_response_bytes,
            self.transaction_max_bytes,
        ]
        .into_iter()
        .any(|bytes| bytes > isize::MAX as usize)
        {
            return Err(invalid("limites de bytes não podem exceder isize::MAX"));
        }
        if self.resp_limits.max_frame_bytes > self.max_input_buffer_bytes {
            return Err(invalid(
                "max_frame_bytes não pode exceder max_input_buffer_bytes",
            ));
        }
        let bulk = self.resp_limits.max_bulk_bytes;
        // `validate` do codec já garantiu bulk > 0. Prefixo '$', dois CRLF e
        // dígitos do comprimento são todos contabilizados antes da alocação.
        let bulk_response = bulk
            .checked_add(bulk.ilog10() as usize + 1)
            .and_then(|bytes| bytes.checked_add(5))
            .ok_or_else(|| invalid("framing da resposta bulk excede o tamanho representável"))?;
        if self.max_response_bytes < 128 {
            return Err(invalid(
                "max_response_bytes precisa comportar ao menos 128 bytes",
            ));
        }
        if self.max_response_bytes < bulk_response {
            return Err(invalid(
                "max_response_bytes precisa comportar max_bulk_bytes e seu framing",
            ));
        }
        let now = Instant::now();
        for (timeout, reason) in [
            (
                self.frame_timeout,
                "frame_timeout precisa ser positivo e representável por Instant",
            ),
            (
                self.request_timeout,
                "request_timeout precisa ser positivo e representável por Instant",
            ),
            (
                self.write_timeout,
                "write_timeout precisa ser positivo e representável por Instant",
            ),
            (
                self.shutdown_timeout,
                "shutdown_timeout precisa ser positivo e representável por Instant",
            ),
        ] {
            if timeout.is_zero() || now.checked_add(timeout).is_none() {
                return Err(invalid(reason));
            }
        }
        if self
            .ready_file
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(invalid("SIDER_READY_FILE não pode ser um caminho vazio"));
        }
        Ok(())
    }
}

fn parse_integer<T: FromStr>(name: &'static str, value: OsString) -> Result<T, ConfigError> {
    let value = value
        .into_string()
        .map_err(|_| ConfigError::NonUnicodeValue { name })?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ConfigError::InvalidInteger { name, value });
    }
    value
        .parse()
        .map_err(|_| ConfigError::InvalidInteger { name, value })
}

#[cfg(test)]
mod tests {
    use super::*;

    const NUMERIC_NAMES: [&str; 20] = [
        "SIDER_TRANSACTION_MAX_COMMANDS",
        "SIDER_TRANSACTION_MAX_BYTES",
        "SIDER_WATCH_MAX_KEYS",
        "SIDER_SHARDS",
        "SIDER_MAX_DATASET_BYTES",
        "SIDER_PUBSUB_MAX_CHANNELS",
        "SIDER_PUBSUB_QUEUE_CAPACITY",
        "SIDER_MAX_CONNECTIONS",
        "SIDER_WORKER_QUEUE_CAPACITY",
        "SIDER_MAX_FRAME_BYTES",
        "SIDER_MAX_BULK_BYTES",
        "SIDER_MAX_LINE_BYTES",
        "SIDER_MAX_NODES",
        "SIDER_MAX_DEPTH",
        "SIDER_MAX_INPUT_BUFFER_BYTES",
        "SIDER_MAX_RESPONSE_BYTES",
        "SIDER_FRAME_TIMEOUT_MS",
        "SIDER_REQUEST_TIMEOUT_MS",
        "SIDER_WRITE_TIMEOUT_MS",
        "SIDER_SHUTDOWN_TIMEOUT_MS",
    ];

    fn config_with(values: &[(&str, &str)]) -> Result<ServerConfig, ConfigError> {
        ServerConfig::from_lookup(|name| {
            values
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| OsString::from(value))
        })
    }

    #[test]
    fn defaults_match_the_documented_resource_contract() {
        let config = ServerConfig::from_lookup(|_| None).unwrap();
        assert_eq!(config, ServerConfig::default());
        assert_eq!(config.bind_addr, SocketAddr::from(([127, 0, 0, 1], 6379)));
        assert_eq!(config.resp_limits, RespLimits::default());
        assert_eq!(config.max_connections, 32);
        assert_eq!(config.worker_queue_capacity, 32);
        assert_eq!(config.shards, 1);
        assert_eq!(config.pubsub_max_channels, 32);
        assert_eq!(config.pubsub_queue_capacity, 32);
        assert_eq!(config.max_input_buffer_bytes, 4 * 1024 * 1024);
        assert_eq!(config.max_response_bytes, 4 * 1024 * 1024);
        assert_eq!(config.max_dataset_bytes, 64 * 1024 * 1024);
        assert_eq!(config.frame_timeout, Duration::from_secs(10));
        assert_eq!(config.request_timeout, Duration::from_secs(5));
        assert_eq!(config.write_timeout, Duration::from_secs(5));
        assert_eq!(config.shutdown_timeout, Duration::from_secs(5));
        assert_eq!(config.ready_file, None);
        config.validate().unwrap();
    }

    #[test]
    fn shard_count_is_bounded_and_has_a_positive_partition_budget() {
        for count in [1, 2, 3, 256] {
            let count = count.to_string();
            assert_eq!(
                config_with(&[("SIDER_SHARDS", &count)])
                    .unwrap()
                    .shards
                    .to_string(),
                count
            );
        }
        assert!(config_with(&[("SIDER_SHARDS", "257")]).is_err());
        assert!(config_with(&[("SIDER_SHARDS", "4"), ("SIDER_MAX_DATASET_BYTES", "3")]).is_err());
        assert!(config_with(&[("SIDER_SHARDS", "4"), ("SIDER_MAX_DATASET_BYTES", "4")]).is_ok());
    }

    #[test]
    fn pubsub_limits_are_read_from_injected_configuration() {
        let config = config_with(&[
            ("SIDER_PUBSUB_MAX_CHANNELS", "4"),
            ("SIDER_PUBSUB_QUEUE_CAPACITY", "7"),
        ])
        .unwrap();
        assert_eq!(config.pubsub_max_channels, 4);
        assert_eq!(config.pubsub_queue_capacity, 7);
    }

    #[test]
    fn transactions_limits_are_bounded_and_read_without_global_environment() {
        let config = config_with(&[
            ("SIDER_TRANSACTION_MAX_COMMANDS", "3"),
            ("SIDER_TRANSACTION_MAX_BYTES", "256"),
            ("SIDER_WATCH_MAX_KEYS", "5"),
        ])
        .unwrap();
        assert_eq!(config.transaction_max_commands, 3);
        assert_eq!(config.transaction_max_bytes, 256);
        assert_eq!(config.watch_max_keys, 5);
        assert_eq!(ServerConfig::default().transaction_max_commands, 128);
        assert_eq!(ServerConfig::default().transaction_max_bytes, 1_048_576);
        assert_eq!(ServerConfig::default().watch_max_keys, 128);
    }

    #[test]
    fn accepts_ipv4_ipv6_and_ephemeral_port() {
        for addr in ["127.0.0.1:6380", "[::1]:6380", "127.0.0.1:0"] {
            assert_eq!(
                config_with(&[("SIDER_ADDR", addr)]).unwrap().bind_addr,
                addr.parse().unwrap()
            );
        }
    }

    #[test]
    fn reads_every_resource_option_without_requiring_an_address_override() {
        let config = config_with(&[
            ("SIDER_MAX_CONNECTIONS", "2"),
            ("SIDER_WORKER_QUEUE_CAPACITY", "3"),
            ("SIDER_MAX_FRAME_BYTES", "768"),
            ("SIDER_MAX_BULK_BYTES", "256"),
            ("SIDER_MAX_LINE_BYTES", "128"),
            ("SIDER_MAX_NODES", "7"),
            ("SIDER_MAX_DEPTH", "3"),
            ("SIDER_MAX_INPUT_BUFFER_BYTES", "1024"),
            ("SIDER_MAX_RESPONSE_BYTES", "512"),
            ("SIDER_MAX_DATASET_BYTES", "2048"),
            ("SIDER_FRAME_TIMEOUT_MS", "1001"),
            ("SIDER_REQUEST_TIMEOUT_MS", "1002"),
            ("SIDER_WRITE_TIMEOUT_MS", "1003"),
            ("SIDER_SHUTDOWN_TIMEOUT_MS", "1004"),
            ("SIDER_READY_FILE", "target/prontidão teste.json"),
        ])
        .unwrap();
        assert_eq!(config.bind_addr, ServerConfig::default().bind_addr);
        assert_eq!(config.max_connections, 2);
        assert_eq!(config.worker_queue_capacity, 3);
        assert_eq!(
            config.resp_limits,
            RespLimits {
                max_frame_bytes: 768,
                max_bulk_bytes: 256,
                max_line_bytes: 128,
                max_nodes: 7,
                max_depth: 3
            }
        );
        assert_eq!(config.max_input_buffer_bytes, 1024);
        assert_eq!(config.max_response_bytes, 512);
        assert_eq!(config.max_dataset_bytes, 2048);
        assert_eq!(config.frame_timeout, Duration::from_millis(1001));
        assert_eq!(config.request_timeout, Duration::from_millis(1002));
        assert_eq!(config.write_timeout, Duration::from_millis(1003));
        assert_eq!(config.shutdown_timeout, Duration::from_millis(1004));
        assert_eq!(
            config.ready_file,
            Some(PathBuf::from("target/prontidão teste.json"))
        );
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
            match config_with(&[("SIDER_ADDR", value)]) {
                Err(ConfigError::InvalidAddress { value: actual, .. }) => assert_eq!(actual, value),
                result => panic!("expected invalid address for {value:?}, got {result:?}"),
            }
        }
    }

    #[test]
    fn invalid_address_keeps_the_original_error_source() {
        use std::error::Error;
        let error = config_with(&[("SIDER_ADDR", "invalid")]).unwrap_err();
        assert!(error.source().is_some());
        assert!(error.to_string().contains("SIDER_ADDR"));
    }

    #[test]
    fn numeric_options_reject_non_decimal_or_unrepresentable_values() {
        for name in NUMERIC_NAMES {
            for value in [
                "",
                " 1",
                "1 ",
                "+1",
                "-1",
                "1.0",
                "1_000",
                "0x10",
                "１",
                "1\n",
                "184467440737095516160000",
            ] {
                assert!(
                    matches!(config_with(&[(name, value)]), Err(ConfigError::InvalidInteger {
                    name: actual_name, value: actual_value }) if actual_name == name && actual_value == value),
                    "{name}={value:?}"
                );
            }
        }
    }

    #[test]
    fn every_numeric_option_rejects_zero() {
        for name in NUMERIC_NAMES {
            assert!(config_with(&[(name, "0")]).is_err(), "{name}");
        }
    }

    #[test]
    fn decimal_leading_zeroes_are_unambiguous_and_accepted() {
        let config = config_with(&[
            ("SIDER_MAX_CONNECTIONS", "0002"),
            ("SIDER_FRAME_TIMEOUT_MS", "0001"),
        ])
        .unwrap();
        assert_eq!(config.max_connections, 2);
        assert_eq!(config.frame_timeout, Duration::from_millis(1));
    }

    #[test]
    fn validates_relations_after_reading_all_overrides() {
        for values in [
            vec![("SIDER_MAX_INPUT_BUFFER_BYTES", "1024")],
            vec![("SIDER_MAX_FRAME_BYTES", "128")],
            vec![("SIDER_MAX_RESPONSE_BYTES", "1048576")],
            vec![("SIDER_MAX_DEPTH", "129")],
        ] {
            assert!(config_with(&values).is_err(), "{values:?}");
        }
    }

    #[test]
    fn response_capacity_includes_the_exact_bulk_framing() {
        for bulk in [123usize, 999, 1000, 9999, 10000] {
            let response = bulk + bulk.ilog10() as usize + 1 + 5;
            let mut config = ServerConfig {
                resp_limits: RespLimits {
                    max_bulk_bytes: bulk,
                    ..RespLimits::default()
                },
                max_response_bytes: response,
                ..ServerConfig::default()
            };
            config.validate().unwrap();
            config.max_response_bytes -= 1;
            assert!(matches!(
                config.validate(),
                Err(ConfigError::InvalidServerLimits { .. })
            ));
        }
    }

    #[test]
    fn small_input_lines_do_not_prevent_internal_error_responses() {
        let mut config = ServerConfig {
            resp_limits: RespLimits {
                max_frame_bytes: 16,
                max_bulk_bytes: 1,
                max_line_bytes: 1,
                max_nodes: 1,
                max_depth: 1,
            },
            max_input_buffer_bytes: 16,
            max_response_bytes: 128,
            ..ServerConfig::default()
        };
        config.validate().unwrap();
        config.max_response_bytes = 127;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidServerLimits { .. })
        ));
    }

    #[test]
    fn technical_channel_capacity_boundaries_are_validated_without_allocating() {
        let max = tokio::sync::Semaphore::MAX_PERMITS;
        let mut config = ServerConfig {
            max_connections: max,
            worker_queue_capacity: max,
            ..ServerConfig::default()
        };
        config.validate().unwrap();
        config.max_connections = max + 1;
        assert!(config.validate().is_err());
        config.max_connections = 1;
        config.worker_queue_capacity = max + 1;
        assert!(config.validate().is_err());
        config.worker_queue_capacity = 1;
        config.pubsub_max_channels = max + 1;
        assert!(config.validate().is_err());
        config.pubsub_max_channels = 1;
        config.pubsub_queue_capacity = max + 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn byte_limits_cannot_exceed_the_technical_allocation_boundary() {
        let excessive = isize::MAX as usize + 1;
        let input = ServerConfig {
            max_input_buffer_bytes: excessive,
            ..ServerConfig::default()
        };
        let response = ServerConfig {
            max_response_bytes: excessive,
            ..ServerConfig::default()
        };
        let frame = ServerConfig {
            resp_limits: RespLimits {
                max_frame_bytes: excessive,
                ..RespLimits::default()
            },
            max_input_buffer_bytes: excessive,
            ..ServerConfig::default()
        };
        for config in [input, response, frame] {
            assert!(matches!(
                config.validate(),
                Err(ConfigError::InvalidServerLimits { .. })
            ));
        }
    }

    #[test]
    fn direct_configuration_rejects_zero_or_unrepresentable_timeouts() {
        for timeout in [Duration::ZERO, Duration::MAX] {
            for index in 0..4 {
                let mut config = ServerConfig::default();
                match index {
                    0 => config.frame_timeout = timeout,
                    1 => config.request_timeout = timeout,
                    2 => config.write_timeout = timeout,
                    _ => config.shutdown_timeout = timeout,
                }
                assert!(matches!(
                    config.validate(),
                    Err(ConfigError::InvalidServerLimits { .. })
                ));
            }
        }
    }

    #[test]
    fn empty_ready_path_is_rejected_in_both_constructors() {
        assert!(config_with(&[("SIDER_READY_FILE", "")]).is_err());
        let config = ServerConfig {
            ready_file: Some(PathBuf::new()),
            ..ServerConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[cfg(unix)]
    fn non_unicode() -> OsString {
        use std::os::unix::ffi::OsStringExt;
        OsString::from_vec(vec![0xff])
    }

    #[cfg(windows)]
    fn non_unicode() -> OsString {
        use std::os::windows::ffi::OsStringExt;
        OsString::from_wide(&[0xd800])
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn non_unicode_options_are_rejected_except_native_file_paths() {
        let result = ServerConfig::from_lookup(|name| (name == "SIDER_ADDR").then(non_unicode));
        assert!(matches!(result, Err(ConfigError::NonUnicodeAddress)));
        for name in NUMERIC_NAMES {
            let result = ServerConfig::from_lookup(|key| (key == name).then(non_unicode));
            assert!(
                matches!(result, Err(ConfigError::NonUnicodeValue { name: actual }) if actual == name)
            );
        }
        let config =
            ServerConfig::from_lookup(|name| (name == "SIDER_READY_FILE").then(non_unicode))
                .unwrap();
        assert_eq!(config.ready_file, Some(PathBuf::from(non_unicode())));
    }
}
