//! Orçamento de um frame; não equivale à quota do dataset ou ao RSS do processo.

use crate::ConfigError;

/// Limites de framing aplicados antes de materializar os payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RespLimits {
    /// Bytes de um frame completo, incluindo prefixos, cabeçalhos e CRLF.
    pub max_frame_bytes: usize,
    /// Bytes de um único payload bulk.
    pub max_bulk_bytes: usize,
    /// Bytes de uma linha inteira, incluindo prefixo e CRLF, mas não payload bulk.
    pub max_line_bytes: usize,
    /// Nós totais, incluindo raiz, arrays e todos os seus elementos.
    pub max_nodes: usize,
    /// Níveis de arrays; raiz array conta como 1. Teto de segurança: 128.
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
    /// Recusa limites nulos, relações incoerentes e profundidade excessiva.
    ///
    /// O teto de profundidade também limita a destruição recursiva de `Frame`.
    pub fn validate(self) -> Result<(), ConfigError> {
        let reason = if self.max_frame_bytes == 0
            || self.max_bulk_bytes == 0
            || self.max_line_bytes == 0
            || self.max_nodes == 0
            || self.max_depth == 0
        {
            Some("todos os limites precisam ser maiores que zero")
        } else if self.max_bulk_bytes > self.max_frame_bytes {
            Some("max_bulk_bytes não pode exceder max_frame_bytes")
        } else if self.max_line_bytes > self.max_frame_bytes {
            Some("max_line_bytes não pode exceder max_frame_bytes")
        } else if self.max_depth > 128 {
            Some("max_depth não pode exceder 128")
        } else {
            None
        };
        match reason {
            Some(reason) => Err(ConfigError::InvalidRespLimits { reason }),
            None => Ok(()),
        }
    }
}
