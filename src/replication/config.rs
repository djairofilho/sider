//! Limits for the internal listener and the single upstream session.

use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::{ConfigError, persistence::AofConfig};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub listen: SocketAddr,
    pub upstream: Option<SocketAddr>,
    pub ready_file: Option<PathBuf>,
    pub backlog_bytes: usize,
    pub backlog_batches: usize,
    pub max_connections: usize,
    pub frame_timeout: Duration,
    pub sync_timeout: Duration,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            upstream: None,
            ready_file: None,
            backlog_bytes: 128 * 1024 * 1024,
            backlog_batches: 4096,
            max_connections: 4,
            frame_timeout: Duration::from_secs(5),
            sync_timeout: Duration::from_secs(30),
            reconnect_min: Duration::from_millis(100),
            reconnect_max: Duration::from_secs(5),
        }
    }
}

impl Config {
    pub fn from_lookup(
        lookup: &mut impl FnMut(&str) -> Option<OsString>,
    ) -> Result<Option<Self>, ConfigError> {
        let listen = lookup("SIDER_REPLICATION_ADDR");
        let upstream = lookup("SIDER_REPLICA_OF");
        if listen.is_none() && upstream.is_none() {
            return Ok(None);
        }
        let mut config = Self::default();
        let address = |name, value: OsString| -> Result<SocketAddr, ConfigError> {
            let value = value
                .into_string()
                .map_err(|_| ConfigError::NonUnicodeValue { name })?;
            value.parse().map_err(|_| ConfigError::InvalidServerLimits {
                reason: "replication address requires a literal IP address and port",
            })
        };
        if let Some(value) = listen {
            config.listen = address("SIDER_REPLICATION_ADDR", value)?;
        }
        if let Some(value) = upstream {
            config.upstream = Some(address("SIDER_REPLICA_OF", value)?);
        }
        config.ready_file = lookup("SIDER_REPLICATION_READY_FILE").map(PathBuf::from);
        for (field, name) in [
            (&mut config.backlog_bytes, "SIDER_REPLICATION_BACKLOG_BYTES"),
            (
                &mut config.backlog_batches,
                "SIDER_REPLICATION_BACKLOG_BATCHES",
            ),
            (
                &mut config.max_connections,
                "SIDER_REPLICATION_MAX_CONNECTIONS",
            ),
        ] {
            if let Some(value) = lookup(name) {
                *field = crate::config::parse_integer(name, value)?;
            }
        }
        for (field, name) in [
            (
                &mut config.frame_timeout,
                "SIDER_REPLICATION_FRAME_TIMEOUT_MS",
            ),
            (
                &mut config.sync_timeout,
                "SIDER_REPLICATION_SYNC_TIMEOUT_MS",
            ),
            (
                &mut config.reconnect_min,
                "SIDER_REPLICATION_RECONNECT_MIN_MS",
            ),
            (
                &mut config.reconnect_max,
                "SIDER_REPLICATION_RECONNECT_MAX_MS",
            ),
        ] {
            if let Some(value) = lookup(name) {
                *field = Duration::from_millis(crate::config::parse_integer(name, value)?);
            }
        }
        Ok(Some(config))
    }

    pub fn validate(&self, aof: &AofConfig) -> Result<(), ConfigError> {
        let invalid = |reason| ConfigError::InvalidServerLimits { reason };
        let frame = aof
            .limits
            .max_record_bytes
            .checked_add(super::protocol::HEADER_BYTES + 12)
            .ok_or_else(|| invalid("replication frame exceeds usize"))?;
        if self.backlog_bytes < frame
            || self.backlog_bytes > isize::MAX as usize
            || self.backlog_batches == 0
            || self.max_connections == 0
            || self.max_connections > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(invalid(
                "replication history and connections require positive limits; one complete record must fit in history",
            ));
        }
        for duration in [
            self.frame_timeout,
            self.sync_timeout,
            self.reconnect_min,
            self.reconnect_max,
        ] {
            if duration < Duration::from_millis(1)
                || std::time::Instant::now().checked_add(duration).is_none()
            {
                return Err(invalid("invalid replication timeouts"));
            }
        }
        if self.reconnect_min > self.reconnect_max
            || self.reconnect_max > Duration::from_secs(60)
            || self.sync_timeout < self.frame_timeout
            || self.upstream.is_some_and(|address| address.port() == 0)
        {
            return Err(invalid("invalid replication intervals or upstream"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(values: &[(&str, &str)]) -> Result<crate::ServerConfig, ConfigError> {
        crate::ServerConfig::from_lookup(|name| {
            values
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| OsString::from(value))
        })
    }

    #[test]
    fn replication_config_requires_aof_and_parses_injected_addresses_limits_and_paths() {
        assert!(config(&[("SIDER_REPLICATION_ADDR", "127.0.0.1:0")]).is_err());
        let actual = config(&[
            ("SIDER_AOF_DIR", "aof"),
            ("SIDER_REPLICATION_ADDR", "[::1]:0"),
            ("SIDER_REPLICA_OF", "127.0.0.1:9123"),
            ("SIDER_REPLICATION_READY_FILE", "replica.json"),
            ("SIDER_REPLICATION_MAX_CONNECTIONS", "2"),
        ])
        .unwrap()
        .replication
        .unwrap();
        assert_eq!(actual.listen, "[::1]:0".parse().unwrap());
        assert_eq!(actual.upstream, Some("127.0.0.1:9123".parse().unwrap()));
        assert_eq!(actual.ready_file, Some(PathBuf::from("replica.json")));
        assert_eq!(actual.max_connections, 2);
    }

    #[test]
    fn replication_config_rejects_unbounded_incoherent_and_malformed_values() {
        for (key, value) in [
            ("SIDER_REPLICA_OF", "127.0.0.1:0"),
            ("SIDER_REPLICA_OF", "localhost:1234"),
            ("SIDER_REPLICATION_MAX_CONNECTIONS", "0"),
            ("SIDER_REPLICATION_MAX_CONNECTIONS", "18446744073709551615"),
            ("SIDER_REPLICATION_BACKLOG_BYTES", "100"),
            ("SIDER_REPLICATION_BACKLOG_BATCHES", "0"),
            ("SIDER_REPLICATION_FRAME_TIMEOUT_MS", "0"),
            ("SIDER_REPLICATION_FRAME_TIMEOUT_MS", "30001"),
            ("SIDER_REPLICATION_SYNC_TIMEOUT_MS", "1"),
            ("SIDER_REPLICATION_RECONNECT_MIN_MS", "5001"),
            ("SIDER_REPLICATION_RECONNECT_MAX_MS", "60001"),
            ("SIDER_REPLICATION_RECONNECT_MAX_MS", " 10"),
        ] {
            assert!(
                config(&[
                    ("SIDER_AOF_DIR", "aof"),
                    ("SIDER_REPLICATION_ADDR", "127.0.0.1:0"),
                    (key, value)
                ])
                .is_err(),
                "{key}={value}"
            );
        }
    }
}
