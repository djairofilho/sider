//! Índices de membros e ordem por score/binários, atualizados como uma unidade.

use super::*;
use crate::command::{Score, SortedSetCommand};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SortedSet {
    members: HashMap<Bytes, Score>,
    ordered: BTreeSet<(Score, Bytes)>,
}

impl SortedSet {
    pub fn len(&self) -> usize {
        self.members.len()
    }
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
    pub fn get(&self, member: &[u8]) -> Option<Score> {
        self.members.get(member).copied()
    }
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &(Score, Bytes)> {
        self.ordered.iter()
    }

    /// Retorna se o membro foi criado e se houve alteração de estado.
    pub fn insert(&mut self, member: Bytes, score: Score) -> (bool, bool) {
        let previous = self.members.get(&member).copied();
        if previous == Some(score) {
            return (false, false);
        }
        if let Some(previous) = previous {
            self.ordered.remove(&(previous, member.clone()));
        }
        self.ordered.insert((score, member.clone()));
        self.members.insert(member, score);
        (previous.is_none(), true)
    }

    pub fn remove(&mut self, member: &Bytes) -> bool {
        let Some(score) = self.members.remove(member) else {
            return false;
        };
        self.ordered.remove(&(score, member.clone()));
        true
    }
}

impl Store {
    pub(super) fn sorted_set(
        &mut self,
        key: Bytes,
        operation: SortedSetCommand,
        now: Instant,
    ) -> Reply {
        self.expire_key(&key, now);
        let entry = self.values.get(&key);
        let mut members = match entry.map(|entry| &entry.value) {
            None => Arc::new(SortedSet::default()),
            Some(Value::SortedSet(members)) => members.clone(),
            Some(_) => return Reply::Error(ExecutionError::WrongType),
        };
        let expiry = entry.and_then(Self::entry_expiry);
        match operation {
            SortedSetCommand::Card => Reply::Integer(members.len() as i64),
            SortedSetCommand::Score { member } => {
                Reply::Bulk(members.get(&member).map(Score::to_bytes))
            }
            SortedSetCommand::Range {
                start,
                stop,
                with_scores,
            } => Reply::Array(
                super::collections::range(members.len(), start, stop)
                    .map(|(start, end)| {
                        let mut result =
                            Vec::with_capacity((end - start) * if with_scores { 2 } else { 1 });
                        for (score, member) in members.iter().skip(start).take(end - start) {
                            result.push(Reply::Bulk(Some(member.clone())));
                            if with_scores {
                                result.push(Reply::Bulk(Some(score.to_bytes())));
                            }
                        }
                        result
                    })
                    .unwrap_or_default(),
            ),
            SortedSetCommand::Add { entries } => {
                let mut added = 0;
                let mut changed = false;
                for (score, member) in entries {
                    let (new, update) = Arc::make_mut(&mut members).insert(member, score);
                    added += i64::from(new);
                    changed |= update;
                }
                if changed {
                    let value = Value::SortedSet(members);
                    if !self.can_replace(&key, &value) {
                        return Reply::Error(ExecutionError::OutOfMemory);
                    }
                    self.insert(key, value, expiry);
                }
                Reply::Integer(added)
            }
            SortedSetCommand::Remove { members: incoming } => {
                let mut removed = 0;
                for member in incoming {
                    removed += i64::from(Arc::make_mut(&mut members).remove(&member));
                }
                if removed > 0 {
                    if members.is_empty() {
                        self.remove(&key);
                    } else {
                        self.insert(key, Value::SortedSet(members), expiry);
                    }
                }
                Reply::Integer(removed)
            }
        }
    }
}
