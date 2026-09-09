//! Execução tipada no proprietário do mapa, com substituição após pré-validar quota.

use super::*;
use crate::command::{HashCommand, ListCommand};
use std::collections::{BTreeMap, VecDeque};

impl Store {
    pub(super) fn list(&mut self, key: Bytes, operation: ListCommand, now: Instant) -> Reply {
        self.expire_key(&key, now);
        let entry = self.values.get(&key);
        let mut values = match entry.map(|entry| &entry.value) {
            None => Arc::new(VecDeque::new()),
            Some(Value::List(values)) => values.clone(),
            Some(_) => return Reply::Error(ExecutionError::WrongType),
        };
        let expiry = entry.and_then(Self::entry_expiry);
        match operation {
            ListCommand::Len => Reply::Integer(values.len() as i64),
            ListCommand::Range { start, stop } => Reply::Array(
                range(values.len(), start, stop)
                    .map(|(start, end)| {
                        values
                            .range(start..end)
                            .map(|value| Reply::Bulk(Some(value.clone())))
                            .collect()
                    })
                    .unwrap_or_default(),
            ),
            ListCommand::Push {
                left,
                values: incoming,
            } => {
                if incoming.is_empty() {
                    return Reply::Integer(values.len() as i64);
                }
                for value in incoming {
                    if left {
                        Arc::make_mut(&mut values).push_front(value);
                    } else {
                        Arc::make_mut(&mut values).push_back(value);
                    }
                }
                let count = values.len() as i64;
                let value = Value::List(values);
                if !self.can_replace(&key, &value) {
                    return Reply::Error(ExecutionError::OutOfMemory);
                }
                self.insert(key, value, expiry);
                Reply::Integer(count)
            }
            ListCommand::Pop { left } => {
                let result = if left {
                    Arc::make_mut(&mut values).pop_front()
                } else {
                    Arc::make_mut(&mut values).pop_back()
                };
                if result.is_some() {
                    if values.is_empty() {
                        self.remove(&key);
                    } else {
                        self.insert(key, Value::List(values), expiry);
                    }
                }
                Reply::Bulk(result)
            }
        }
    }

    pub(super) fn hash(&mut self, key: Bytes, operation: HashCommand, now: Instant) -> Reply {
        self.expire_key(&key, now);
        let entry = self.values.get(&key);
        let mut values = match entry.map(|entry| &entry.value) {
            None => Arc::new(BTreeMap::new()),
            Some(Value::Hash(values)) => values.clone(),
            Some(_) => return Reply::Error(ExecutionError::WrongType),
        };
        let expiry = entry.and_then(Self::entry_expiry);
        match operation {
            HashCommand::Get { field } => Reply::Bulk(values.get(&field).cloned()),
            HashCommand::Exists { field } => Reply::Integer(i64::from(values.contains_key(&field))),
            HashCommand::Len => Reply::Integer(values.len() as i64),
            HashCommand::GetAll => Reply::Array(
                values
                    .iter()
                    .flat_map(|(field, value)| {
                        [
                            Reply::Bulk(Some(field.clone())),
                            Reply::Bulk(Some(value.clone())),
                        ]
                    })
                    .collect(),
            ),
            HashCommand::Set { entries } => {
                if entries.is_empty() {
                    return Reply::Integer(0);
                }
                let mut added = 0;
                for (field, value) in entries {
                    if Arc::make_mut(&mut values).insert(field, value).is_none() {
                        added += 1;
                    }
                }
                let value = Value::Hash(values);
                if !self.can_replace(&key, &value) {
                    return Reply::Error(ExecutionError::OutOfMemory);
                }
                self.insert(key, value, expiry);
                Reply::Integer(added)
            }
            HashCommand::Delete { fields } => {
                let mut removed = 0;
                for field in fields {
                    if Arc::make_mut(&mut values).remove(&field).is_some() {
                        removed += 1;
                    }
                }
                if removed > 0 {
                    if values.is_empty() {
                        self.remove(&key);
                    } else {
                        self.insert(key, Value::Hash(values), expiry);
                    }
                }
                Reply::Integer(removed)
            }
        }
    }
}

/// Converte índices inclusivos assinados sem overflow nos extremos de i64.
pub(super) fn range(length: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    let length = length as i128;
    let mut start = i128::from(start);
    let mut stop = i128::from(stop);
    if start < 0 {
        start += length;
    }
    if stop < 0 {
        stop += length;
    }
    start = start.max(0);
    stop = stop.min(length - 1);
    if start > stop || start >= length {
        None
    } else {
        Some((start as usize, (stop + 1) as usize))
    }
}
