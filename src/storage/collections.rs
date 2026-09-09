//! Execução tipada no proprietário do mapa, com substituição após pré-validar quota.

use super::*;
use crate::command::HashCommand;
use std::collections::BTreeMap;

impl Store {
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
