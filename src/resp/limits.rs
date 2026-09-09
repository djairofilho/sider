//! Budget for one frame; it is not the dataset quota or process RSS.

use crate::ConfigError;

/// Framing limits applied before materializing payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RespLimits {
    /// Bytes in a complete frame, including prefixes, headers, and CRLF.
    pub max_frame_bytes: usize,
    /// Bytes in a single bulk payload.
    pub max_bulk_bytes: usize,
    /// Bytes in a whole line, including its prefix and CRLF but not bulk payload.
    pub max_line_bytes: usize,
    /// Total nodes, including root, arrays, and all their elements.
    pub max_nodes: usize,
    /// Array levels; the root array counts as 1. Safety ceiling: 128.
    pub max_depth: usize,
}

impl Default for RespLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 4 * 1024 * 1024,
            max_bulk_bytes: 1024 * 1024,
            max_line_bytes: 1024,
            max_nodes: 1024,
            max_depth: 16,
        }
    }
}

impl RespLimits {
    /// Rejects zero limits, inconsistent relationships, and excessive depth.
    ///
    /// The depth ceiling also limits recursive `Frame` destruction.
    pub fn validate(self) -> Result<(), ConfigError> {
        let reason = if self.max_frame_bytes == 0
            || self.max_bulk_bytes == 0
            || self.max_line_bytes == 0
            || self.max_nodes == 0
            || self.max_depth == 0
        {
            Some("all limits must be greater than zero")
        } else if self.max_bulk_bytes > self.max_frame_bytes {
            Some("max_bulk_bytes cannot exceed max_frame_bytes")
        } else if self.max_line_bytes > self.max_frame_bytes {
            Some("max_line_bytes cannot exceed max_frame_bytes")
        } else if self.max_depth > 128 {
            Some("max_depth cannot exceed 128")
        } else {
            None
        };
        match reason {
            Some(reason) => Err(ConfigError::InvalidRespLimits { reason }),
            None => Ok(()),
        }
    }
}
