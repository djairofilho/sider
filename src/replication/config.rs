//! Limites do listener interno e da única sessão upstream.

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
                reason: "endereço de replicação exige IP literal e porta",
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
            .ok_or_else(|| invalid("frame de replicação excede usize"))?;
        if self.backlog_bytes < frame
            || self.backlog_bytes > isize::MAX as usize
            || self.backlog_batches == 0
            || self.max_connections == 0
            || self.max_connections > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(invalid(
                "histórico e conexões de replicação precisam de limites positivos; um registro completo deve caber no histórico",
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
                return Err(invalid("prazos de replicação inválidos"));
            }
        }
        if self.reconnect_min > self.reconnect_max
            || self.reconnect_max > Duration::from_secs(60)
            || self.sync_timeout < self.frame_timeout
            || self.upstream.is_some_and(|address| address.port() == 0)
        {
            return Err(invalid("intervalos ou upstream da replicação inválidos"));
        }
        Ok(())
    }
}
