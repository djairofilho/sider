//! Durable partitioning identity; changes require offline migration.

use crate::ConfigError;
use crate::storage::routing::ShardRouter;

/// FNV-1a64 over the first non-empty hash tag; `storage::routing` rules.
pub const ROUTING_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurableLayout {
    pub shard_count: u32,
    pub routing_version: u32,
}

impl Default for DurableLayout {
    fn default() -> Self {
        Self {
            shard_count: 1,
            routing_version: ROUTING_VERSION,
        }
    }
}

impl DurableLayout {
    pub fn validate(self) -> Result<(), ConfigError> {
        ShardRouter::new(self.shard_count as usize)?;
        if self.routing_version != ROUTING_VERSION {
            return Err(ConfigError::InvalidServerLimits {
                reason: "unsupported AOF routing version",
            });
        }
        Ok(())
    }

    pub fn shard_for(self, key: &[u8]) -> Result<usize, ConfigError> {
        self.validate()?;
        Ok(ShardRouter::new(self.shard_count as usize)?.shard_for(key))
    }

    pub fn quota(self, total_bytes: usize, shard: usize) -> Result<usize, ConfigError> {
        self.validate()?;
        let count = self.shard_count as usize;
        if total_bytes < count || shard >= count {
            return Err(ConfigError::InvalidServerLimits {
                reason: "total quota must reserve at least one byte per shard",
            });
        }
        Ok(total_bytes / count + usize::from(shard < total_bytes % count))
    }
}
