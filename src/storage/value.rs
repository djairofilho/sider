//! Typed values with shared snapshots and logical consumption by type.

use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

/// Persistable payload; the entry holds TTL and generation shared by all types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    String(Bytes),
    Hash(Arc<BTreeMap<Bytes, Bytes>>),
    List(Arc<VecDeque<Bytes>>),
    Set(Arc<BTreeSet<Bytes>>),
    SortedSet(Arc<super::SortedSet>),
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
            Self::Hash(_) | Self::List(_) | Self::Set(_) | Self::SortedSet(_) => None,
        }
    }

    /// Logical payload bytes; the per-key charge belongs to the Store.
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
            Self::List(values) if !values.is_empty() => {
                values.iter().try_fold(0usize, |total, value| {
                    total.checked_add(value.len())?.checked_add(32)
                })
            }
            Self::List(_) => None,
            Self::Set(members) if !members.is_empty() => {
                members.iter().try_fold(0usize, |total, member| {
                    total.checked_add(member.len())?.checked_add(64)
                })
            }
            Self::Set(_) => None,
            Self::SortedSet(members) if !members.is_empty() => {
                members.iter().try_fold(0usize, |total, (_, member)| {
                    total.checked_add(member.len())?.checked_add(96)
                })
            }
            Self::SortedSet(_) => None,
        }
    }
}
