//! Valores tipados com snapshots compartilhados e consumo lógico por tipo.

use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Payload persistível; a entrada contém TTL e geração comuns a todos os tipos.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    String(Bytes),
    Hash(Arc<BTreeMap<Bytes, Bytes>>),
}

impl From<Bytes> for Value {
    fn from(value: Bytes) -> Self {
        Self::String(value)
    }
}

impl Value {
    pub fn as_string(&self) -> Option<&Bytes> {
        match self {
            Self::String(value) => Some(value),
            Self::Hash(_) => None,
        }
    }

    /// Bytes lógicos do payload; a taxa por chave fica no Store.
    pub fn logical_bytes(&self) -> Option<usize> {
        match self {
            Self::String(value) => Some(value.len()),
            Self::Hash(fields) if !fields.is_empty() => {
                fields.iter().try_fold(0usize, |total, (field, value)| {
                    total
                        .checked_add(field.len())?
                        .checked_add(value.len())?
                        .checked_add(64)
                })
            }
            Self::Hash(_) => None,
        }
    }
}
