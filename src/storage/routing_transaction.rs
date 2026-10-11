//! Pure command planning for a future supervised multi-owner EXEC coordinator.
//!
//! This does not admit or execute a transaction. The public database path still
//! rejects mixed-owner batches before enqueueing any worker request.

use std::collections::BTreeMap;

use bytes::Bytes;
use thiserror::Error;

use crate::command::{Command, ExecutionError, Reply};

use super::ShardRouter;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fragment {
    pub owner: usize,
    pub command: Command,
}

/// Complete read-only preflight of a queued transaction and its watched keys.
/// Constructing this plan does not reserve owners or admit an EXEC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionPlan {
    participants: Vec<usize>,
    commands: Vec<CommandPlan>,
}

impl TransactionPlan {
    /// Sorted and unique owners, including owners touched only by WATCH.
    pub fn participants(&self) -> &[usize] {
        &self.participants
    }

    /// Command plans in their original queue order.
    pub fn commands(&self) -> &[CommandPlan] {
        &self.commands
    }

    pub fn into_parts(self) -> (Vec<usize>, Vec<CommandPlan>) {
        (self.participants, self.commands)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandPlan {
    fragments: Vec<Fragment>,
    reply: ReplyPlan,
    joint_commit: bool,
}

impl CommandPlan {
    /// The caller must prepare every fragment of a mutating command before
    /// accepting any of its candidates or publishing its one durable decision.
    pub fn requires_joint_commit(&self) -> bool {
        self.joint_commit
    }

    pub fn into_parts(self) -> (Vec<Fragment>, ReplyPlan) {
        (self.fragments, self.reply)
    }

    fn direct(owner: usize, command: Command) -> Self {
        Self {
            fragments: vec![Fragment { owner, command }],
            reply: ReplyPlan::Direct,
            joint_commit: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplyPlan {
    Direct,
    Sum {
        fragments: usize,
    },
    OrderedBulk {
        positions: Vec<(usize, usize)>,
        lengths: Vec<usize>,
    },
    AllOk {
        fragments: usize,
    },
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PlanError {
    #[error("fragment returned an invalid reply")]
    InvalidReply,
    #[error("combined integer reply overflowed")]
    Overflow,
}

impl ReplyPlan {
    /// Reconstructs the original command's reply after all fragment results are
    /// known. This does not accept candidates or make a durable decision.
    pub fn assemble(self, replies: Vec<Reply>) -> Result<Reply, PlanError> {
        match self {
            Self::Direct if replies.len() == 1 => Ok(replies.into_iter().next().unwrap()),
            Self::Direct => Err(PlanError::InvalidReply),
            Self::Sum { fragments } if replies.len() == fragments => {
                let mut total = 0i64;
                for reply in replies {
                    match reply {
                        Reply::Integer(value) if value >= 0 => {
                            total = total.checked_add(value).ok_or(PlanError::Overflow)?;
                        }
                        Reply::Error(error) => return Ok(Reply::Error(error)),
                        _ => return Err(PlanError::InvalidReply),
                    }
                }
                Ok(Reply::Integer(total))
            }
            Self::AllOk { fragments } if replies.len() == fragments => {
                for reply in replies {
                    match reply {
                        Reply::Ok => {}
                        Reply::Error(error) => return Ok(Reply::Error(error)),
                        _ => return Err(PlanError::InvalidReply),
                    }
                }
                Ok(Reply::Ok)
            }
            Self::OrderedBulk { positions, lengths } if replies.len() == lengths.len() => {
                let mut rows = Vec::with_capacity(lengths.len());
                for (reply, expected) in replies.into_iter().zip(lengths) {
                    match reply {
                        Reply::Array(values)
                            if values.len() == expected
                                && values.iter().all(|value| matches!(value, Reply::Bulk(_))) =>
                        {
                            rows.push(values.into_iter().map(Some).collect::<Vec<_>>());
                        }
                        Reply::Error(error) => return Ok(Reply::Error(error)),
                        _ => return Err(PlanError::InvalidReply),
                    }
                }
                let mut ordered = Vec::with_capacity(positions.len());
                for (fragment, local) in positions {
                    ordered.push(
                        rows.get_mut(fragment)
                            .and_then(|row| row.get_mut(local))
                            .and_then(Option::take)
                            .ok_or(PlanError::InvalidReply)?,
                    );
                }
                if rows.iter().any(|row| row.iter().any(Option::is_some)) {
                    return Err(PlanError::InvalidReply);
                }
                Ok(Reply::Array(ordered))
            }
            _ => Err(PlanError::InvalidReply),
        }
    }
}

impl ShardRouter {
    /// Validates every command before any coordinator sends owner controls.
    /// Watched keys may belong to owners with no queued command fragment.
    pub fn plan_transaction(
        &self,
        commands: Vec<Command>,
        watched: &[Bytes],
    ) -> Result<TransactionPlan, ExecutionError> {
        let participants =
            self.transaction_participants(&commands, watched.iter().map(Bytes::as_ref));
        let commands = commands
            .into_iter()
            .map(|command| self.plan_transaction_command(command))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TransactionPlan {
            participants,
            commands,
        })
    }

    /// Splits only the four multi-key commands whose per-owner replies can be
    /// recombined exactly. Unknown mixed-owner commands remain rejected.
    pub fn plan_transaction_command(
        &self,
        command: Command,
    ) -> Result<CommandPlan, ExecutionError> {
        let participants = self.transaction_participants(std::slice::from_ref(&command), []);
        if let [owner] = participants.as_slice() {
            return Ok(CommandPlan::direct(*owner, command));
        }
        match command {
            Command::MGet { keys } => {
                Ok(self.split_keys(keys, |keys| Command::MGet { keys }, false, true))
            }
            Command::Exists { keys } => {
                Ok(self.split_keys(keys, |keys| Command::Exists { keys }, false, false))
            }
            Command::Del { keys } => {
                Ok(self.split_keys(keys, |keys| Command::Del { keys }, true, false))
            }
            Command::MSet { entries } => Ok(self.split_mset(entries)),
            _ => Err(ExecutionError::CrossShard),
        }
    }

    fn split_keys(
        &self,
        keys: Vec<Bytes>,
        command: fn(Vec<Bytes>) -> Command,
        joint_commit: bool,
        ordered: bool,
    ) -> CommandPlan {
        let mut groups: BTreeMap<usize, Vec<Bytes>> = BTreeMap::new();
        let mut owner_positions = Vec::with_capacity(keys.len());
        for key in keys {
            let owner = self.shard_for(&key);
            let group = groups.entry(owner).or_default();
            owner_positions.push((owner, group.len()));
            group.push(key);
        }
        let owners: Vec<_> = groups.keys().copied().collect();
        let positions = owner_positions
            .into_iter()
            .map(|(owner, local)| (owners.binary_search(&owner).unwrap(), local))
            .collect();
        let lengths = groups.values().map(Vec::len).collect();
        let fragments = groups
            .into_iter()
            .map(|(owner, keys)| Fragment {
                owner,
                command: command(keys),
            })
            .collect();
        CommandPlan {
            fragments,
            reply: if ordered {
                ReplyPlan::OrderedBulk { positions, lengths }
            } else {
                ReplyPlan::Sum {
                    fragments: owners.len(),
                }
            },
            joint_commit,
        }
    }

    fn split_mset(&self, entries: Vec<(Bytes, Bytes)>) -> CommandPlan {
        let mut groups: BTreeMap<usize, Vec<(Bytes, Bytes)>> = BTreeMap::new();
        for (key, value) in entries {
            groups
                .entry(self.shard_for(&key))
                .or_default()
                .push((key, value));
        }
        let fragments = groups
            .into_iter()
            .map(|(owner, entries)| Fragment {
                owner,
                command: Command::MSet { entries },
            })
            .collect::<Vec<_>>();
        CommandPlan {
            reply: ReplyPlan::AllOk {
                fragments: fragments.len(),
            },
            fragments,
            joint_commit: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(values: &[&[u8]]) -> Vec<Bytes> {
        values
            .iter()
            .map(|key| Bytes::copy_from_slice(key))
            .collect()
    }

    #[test]
    fn whole_queue_preflight_includes_watched_only_owners_and_keeps_command_order() {
        let router = ShardRouter::new(4).unwrap();
        let mut owners = BTreeMap::new();
        for byte in 0u8..=255 {
            let key = Bytes::from(vec![byte]);
            owners.entry(router.shard_for(&key)).or_insert(key);
        }
        let keys: Vec<_> = owners.into_values().take(3).collect();
        assert_eq!(keys.len(), 3);
        let commands = vec![
            Command::Get {
                key: keys[0].clone(),
            },
            Command::MGet {
                keys: vec![keys[1].clone(), keys[0].clone(), keys[1].clone()],
            },
            Command::Set {
                key: keys[1].clone(),
                value: Bytes::from_static(b"value"),
            },
        ];
        let original = commands.clone();
        let plan = router
            .plan_transaction(commands, &[keys[2].clone()])
            .unwrap();
        assert_eq!(plan.participants(), &[0, 1, 2]);
        assert_eq!(plan.commands().len(), original.len());
        let (_, planned) = plan.into_parts();
        assert_eq!(planned[0].clone().into_parts().0[0].command, original[0]);
        assert_eq!(planned[1].clone().into_parts().0.len(), 2);
        assert_eq!(planned[2].clone().into_parts().0[0].command, original[2]);
        let empty = router.plan_transaction(vec![], &[keys[2].clone()]).unwrap();
        assert_eq!(empty.participants(), &[2]);
        assert!(empty.commands().is_empty());
    }

    #[test]
    fn late_unsupported_mixed_command_rejects_the_complete_plan() {
        let router = ShardRouter::new(2).unwrap();
        let queued = vec![
            Command::Set {
                key: Bytes::from_static(b"a"),
                value: Bytes::from_static(b"v"),
            },
            Command::MSet {
                entries: vec![
                    (Bytes::from_static(b"a"), Bytes::from_static(b"x")),
                    (Bytes::from_static(b"b"), Bytes::from_static(b"y")),
                ],
            },
            Command::Watch {
                keys: keys(&[b"a", b"b"]),
            },
        ];
        assert_eq!(
            router.plan_transaction(queued, &[]),
            Err(ExecutionError::CrossShard)
        );
    }

    #[test]
    fn mixed_mget_preserves_binary_order_and_duplicates() {
        for count in [2, 4, 256] {
            let router = ShardRouter::new(count).unwrap();
            let left = Bytes::from_static(b"a");
            let right = (0u8..=255)
                .map(|byte| Bytes::from(vec![byte]))
                .find(|key| router.shard_for(key) != router.shard_for(&left))
                .unwrap();
            let original = vec![
                right.clone(),
                left.clone(),
                right.clone(),
                Bytes::new(),
                left.clone(),
            ];
            let plan = router
                .plan_transaction_command(Command::MGet {
                    keys: original.clone(),
                })
                .unwrap();
            assert!(!plan.requires_joint_commit());
            let (fragments, reply_plan) = plan.into_parts();
            assert!(
                fragments
                    .windows(2)
                    .all(|pair| pair[0].owner < pair[1].owner)
            );
            let replies = fragments
                .into_iter()
                .map(|fragment| {
                    let Command::MGet { keys } = fragment.command else {
                        panic!()
                    };
                    assert!(
                        keys.iter()
                            .all(|key| router.shard_for(key) == fragment.owner)
                    );
                    Reply::Array(keys.into_iter().map(|key| Reply::Bulk(Some(key))).collect())
                })
                .collect();
            assert_eq!(
                reply_plan.assemble(replies).unwrap(),
                Reply::Array(
                    original
                        .into_iter()
                        .map(|key| Reply::Bulk(Some(key)))
                        .collect()
                )
            );
        }
    }

    #[test]
    fn split_counts_and_mset_keep_owner_order_and_command_boundaries() {
        let router = ShardRouter::new(2).unwrap();
        let input = keys(&[b"b", b"a", b"b"]);
        for command in [
            Command::Del {
                keys: input.clone(),
            },
            Command::Exists {
                keys: input.clone(),
            },
        ] {
            let plan = router.plan_transaction_command(command.clone()).unwrap();
            assert_eq!(
                plan.requires_joint_commit(),
                matches!(command, Command::Del { .. })
            );
            let (fragments, reply_plan) = plan.into_parts();
            assert_eq!(fragments.len(), 2);
            assert_eq!(
                reply_plan
                    .assemble(vec![Reply::Integer(1), Reply::Integer(2)])
                    .unwrap(),
                Reply::Integer(3)
            );
        }
        let entries = vec![
            (Bytes::from_static(b"b"), Bytes::from_static(b"first")),
            (Bytes::from_static(b"a"), Bytes::from_static(b"other")),
            (Bytes::from_static(b"b"), Bytes::from_static(b"last")),
        ];
        let plan = router
            .plan_transaction_command(Command::MSet {
                entries: entries.clone(),
            })
            .unwrap();
        assert!(plan.requires_joint_commit());
        let (fragments, reply_plan) = plan.into_parts();
        assert_eq!(fragments.len(), 2);
        let retained: Vec<_> = fragments
            .into_iter()
            .flat_map(|fragment| match fragment.command {
                Command::MSet { entries } => entries,
                _ => panic!(),
            })
            .collect();
        assert_eq!(
            retained
                .iter()
                .filter(|(key, _)| key.as_ref() == b"b")
                .count(),
            2
        );
        assert_eq!(
            retained
                .iter()
                .filter(|(key, _)| key.as_ref() == b"b")
                .map(|(_, value)| value.as_ref())
                .collect::<Vec<_>>(),
            vec![b"first".as_slice(), b"last".as_slice()]
        );
        assert_eq!(
            reply_plan.assemble(vec![Reply::Ok, Reply::Ok]).unwrap(),
            Reply::Ok
        );
    }

    #[test]
    fn malformed_fragment_replies_and_unsupported_mixed_commands_fail_closed() {
        let router = ShardRouter::new(2).unwrap();
        let plan = router
            .plan_transaction_command(Command::MGet {
                keys: keys(&[b"a", b"b"]),
            })
            .unwrap();
        let (_, reply_plan) = plan.into_parts();
        assert_eq!(
            reply_plan.assemble(vec![
                Reply::Array(vec![]),
                Reply::Array(vec![Reply::Bulk(None)])
            ]),
            Err(PlanError::InvalidReply)
        );
        assert_eq!(
            ReplyPlan::Sum { fragments: 2 }
                .assemble(vec![Reply::Integer(i64::MAX), Reply::Integer(1)]),
            Err(PlanError::Overflow)
        );
        assert_eq!(
            ReplyPlan::AllOk { fragments: 2 }
                .assemble(vec![Reply::Ok, Reply::Error(ExecutionError::OutOfMemory)])
                .unwrap(),
            Reply::Error(ExecutionError::OutOfMemory)
        );
        assert_eq!(
            router.plan_transaction_command(Command::Watch {
                keys: keys(&[b"a", b"b"])
            }),
            Err(ExecutionError::CrossShard)
        );
        let direct = router
            .plan_transaction_command(Command::MGet {
                keys: keys(&[b"{same}a", b"{same}b"]),
            })
            .unwrap();
        assert!(!direct.requires_joint_commit());
        assert_eq!(direct.into_parts().0.len(), 1);
    }
}
