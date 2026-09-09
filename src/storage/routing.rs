//! Roteamento binário estável, independente do hasher aleatório do dataset.

use crate::ConfigError;
use crate::command::{Command, ExecutionError};

/// Limita workers, filas e metadados de coordenação criados na inicialização.
pub const MAX_SHARDS: usize = 256;

/// Extrai o primeiro par de chaves não vazio, sem interpretar UTF-8.
///
/// Um primeiro par vazio ou incompleto mantém a chave inteira, inclusive quando
/// existir outro par depois. A abertura aninhada pertence aos bytes da tag.
pub fn hash_tag(key: &[u8]) -> &[u8] {
    let Some(open) = key.iter().position(|byte| *byte == b'{') else {
        return key;
    };
    let suffix = &key[open + 1..];
    match suffix.iter().position(|byte| *byte == b'}') {
        Some(close) if close > 0 => &suffix[..close],
        _ => key,
    }
}

/// FNV-1a de 64 bits, com multiplicação módulo 2^64 em cada byte.
pub fn stable_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

#[derive(Clone, Copy, Debug)]
pub struct ShardRouter {
    count: usize,
}

impl ShardRouter {
    pub fn new(count: usize) -> Result<Self, ConfigError> {
        if !(1..=MAX_SHARDS).contains(&count) {
            return Err(ConfigError::InvalidServerLimits {
                reason: "SIDER_SHARDS precisa estar entre 1 e 256",
            });
        }
        Ok(Self { count })
    }

    pub fn shard_for(&self, key: &[u8]) -> usize {
        (stable_hash(hash_tag(key)) % self.count as u64) as usize
    }

    /// Verifica todas as chaves antes de selecionar a fila. Comandos sem chave
    /// usam o worker zero; duplicatas continuam intactas no comando original.
    pub fn route(&self, command: &Command) -> Result<usize, ExecutionError> {
        let mut selected = None;
        self.select_command(command, &mut selected)?;
        Ok(selected.unwrap_or(0))
    }

    pub fn select_key(
        &self,
        key: &[u8],
        selected: &mut Option<usize>,
    ) -> Result<(), ExecutionError> {
        let shard = self.shard_for(key);
        if selected.is_some_and(|previous| previous != shard) {
            return Err(ExecutionError::CrossShard);
        }
        *selected = Some(shard);
        Ok(())
    }

    pub fn select_command(
        &self,
        command: &Command,
        selected: &mut Option<usize>,
    ) -> Result<(), ExecutionError> {
        let mut crossed = false;
        command.visit_keys(|key| {
            crossed |= self.select_key(key, selected).is_err();
        });
        if crossed {
            Err(ExecutionError::CrossShard)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_hash_vectors_and_binary_tags() {
        assert_eq!(stable_hash(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(stable_hash(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(stable_hash(b"foobar"), 0x8594_4171_f739_67e8);
        for (key, tag) in [
            (b"a{tag}b".as_slice(), b"tag".as_slice()),
            (b"{}later{tag}", b"{}later{tag}"),
            (b"a{missing", b"a{missing"),
            (b"a{{tag}}", b"{tag"),
            (b"{\0\xff}", b"\0\xff"),
            (b"", b""),
        ] {
            assert_eq!(hash_tag(key), tag);
        }
        for count in [1, 2, 3, 16, 256] {
            let router = ShardRouter::new(count).unwrap();
            assert_eq!(router.shard_for(b"foo{\xff}a"), router.shard_for(b"{\xff}"));
        }
        assert!(ShardRouter::new(0).is_err());
        assert!(ShardRouter::new(257).is_err());
    }
}
