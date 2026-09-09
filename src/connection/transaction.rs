//! Queue and observations belong to the connection; no queued command touches the database.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::ServerConfig;
use crate::command::Command;
use crate::resp::Frame;
use crate::storage::WatchToken;
use crate::storage::worker::{DbError, DbHandle};

pub(super) enum Action {
    Execute(Command),
    Reply(Frame),
    Exec {
        commands: Vec<Command>,
        watched: Vec<WatchToken>,
    },
}

#[derive(Default)]
pub(super) struct Transaction {
    queue: Option<Queue>,
    watched: BTreeMap<Bytes, WatchToken>,
    watch_shard: Option<usize>,
}

#[derive(Default)]
struct Queue {
    commands: Vec<Command>,
    bytes: usize,
    shard: Option<usize>,
    dirty: bool,
}

fn error(message: &'static [u8]) -> Action {
    Action::Reply(Frame::Error(Bytes::from_static(message)))
}
fn ok() -> Action {
    Action::Reply(Frame::Simple(Bytes::from_static(b"OK")))
}

impl Transaction {
    pub(super) fn poison(&mut self) {
        if let Some(queue) = &mut self.queue {
            queue.dirty = true;
            queue.commands.clear();
            queue.bytes = 0;
        }
    }

    pub(super) async fn handle(
        &mut self,
        command: Command,
        bytes: usize,
        config: &ServerConfig,
        database: &DbHandle,
        subscribed: bool,
    ) -> Result<Action, DbError> {
        if subscribed {
            return Ok(Action::Execute(command));
        }
        if database.readonly() && command.writes_dataset() {
            self.poison();
            return Ok(Action::Reply(
                crate::command::Reply::Error(crate::command::ExecutionError::ReadOnly).into(),
            ));
        }
        let result = match command {
            Command::Multi => {
                if self.queue.is_some() {
                    error(b"ERR MULTI calls can not be nested")
                } else {
                    self.queue = Some(Queue {
                        shard: self.watch_shard,
                        ..Queue::default()
                    });
                    ok()
                }
            }
            Command::Discard => {
                if self.queue.take().is_none() {
                    error(b"ERR DISCARD without MULTI")
                } else {
                    self.watched.clear();
                    self.watch_shard = None;
                    ok()
                }
            }
            Command::Exec => {
                let Some(queue) = self.queue.take() else {
                    return Ok(error(b"ERR EXEC without MULTI"));
                };
                let watched = std::mem::take(&mut self.watched).into_values().collect();
                self.watch_shard = None;
                if queue.dirty {
                    drop(watched);
                    error(b"EXECABORT Transaction discarded because of previous errors.")
                } else {
                    Action::Exec {
                        commands: queue.commands,
                        watched,
                    }
                }
            }
            Command::Watch { keys } => {
                if self.queue.is_some() {
                    error(b"ERR WATCH inside MULTI is not allowed")
                } else {
                    let keys: BTreeSet<_> = keys
                        .into_iter()
                        .filter(|key| !self.watched.contains_key(key))
                        .collect();
                    if keys.len().saturating_add(self.watched.len()) > config.watch_max_keys {
                        return Ok(error(b"ERR WATCH key limit exceeded"));
                    }
                    let mut shard = self.watch_shard;
                    for key in &keys {
                        if let Err(error) = database.router().select_key(key, &mut shard) {
                            return Ok(Action::Reply(Frame::Error(Bytes::from(error.to_string()))));
                        }
                    }
                    if !keys.is_empty() {
                        let (reply, tokens) = database.watch(keys.into_iter().collect()).await?;
                        if matches!(reply, crate::command::Reply::Error(_)) {
                            return Ok(Action::Reply(reply.into()));
                        }
                        for token in tokens {
                            self.watched.insert(token.key().clone(), token);
                        }
                    }
                    self.watch_shard = shard;
                    ok()
                }
            }
            Command::Unwatch if self.queue.is_none() => {
                self.watched.clear();
                self.watch_shard = None;
                ok()
            }
            command => {
                let Some(queue) = &mut self.queue else {
                    return Ok(Action::Execute(command));
                };
                if queue.dirty {
                    return Ok(Action::Reply(Frame::Simple(Bytes::from_static(b"QUEUED"))));
                }
                let mut shard = queue.shard;
                if let Err(error) = database.router().select_command(&command, &mut shard) {
                    self.poison();
                    return Ok(Action::Reply(Frame::Error(Bytes::from(error.to_string()))));
                }
                if queue.commands.len() >= config.transaction_max_commands
                    || queue
                        .bytes
                        .checked_add(bytes)
                        .is_none_or(|size| size > config.transaction_max_bytes)
                {
                    self.poison();
                    return Ok(error(b"ERR transaction queue limit exceeded"));
                }
                queue.shard = shard;
                queue.bytes += bytes;
                queue.commands.push(command);
                Action::Reply(Frame::Simple(Bytes::from_static(b"QUEUED")))
            }
        };
        Ok(result)
    }
}

/// Exact RESP size for the accepted request format (an array of bulks).
pub(super) fn request_bytes(frame: &Frame) -> usize {
    fn digits(value: usize) -> usize {
        value.checked_ilog10().unwrap_or(0) as usize + 1
    }
    match frame {
        Frame::Array(Some(arguments)) => arguments.iter().fold(
            digits(arguments.len()) + 3,
            |size, argument| match argument {
                Frame::Bulk(Some(value)) => size
                    .saturating_add(value.len())
                    .saturating_add(digits(value.len()) + 5),
                _ => size,
            },
        ),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Store, worker};

    fn reply(action: Action) -> Frame {
        match action {
            Action::Reply(frame) => frame,
            _ => panic!("expected immediate reply"),
        }
    }

    #[tokio::test]
    async fn transactions_watch_limit_and_shard_rejection_preserve_existing_observation() {
        let config = ServerConfig {
            watch_max_keys: 1,
            ..ServerConfig::default()
        };
        let (stop, shutdown) = tokio::sync::watch::channel(false);
        let (db, owners) = worker::channel_with_stores(
            2,
            config.request_timeout,
            shutdown,
            vec![Store::new(), Store::new()],
        )
        .unwrap();
        let tasks: Vec<_> = owners
            .into_iter()
            .map(|owner| tokio::spawn(owner.run()))
            .collect();
        let mut tx = Transaction::default();
        let watch = |key| Command::Watch {
            keys: vec![Bytes::from_static(key)],
        };
        assert_eq!(
            reply(
                tx.handle(watch(b"a"), 1, &config, &db, false)
                    .await
                    .unwrap()
            ),
            Frame::Simple(Bytes::from_static(b"OK"))
        );
        assert_eq!(
            reply(
                tx.handle(watch(b"a"), 1, &config, &db, false)
                    .await
                    .unwrap()
            ),
            Frame::Simple(Bytes::from_static(b"OK"))
        );
        assert_eq!(
            reply(
                tx.handle(watch(b"b"), 1, &config, &db, false)
                    .await
                    .unwrap()
            ),
            Frame::Error(Bytes::from_static(b"ERR WATCH key limit exceeded"))
        );
        let config = ServerConfig {
            watch_max_keys: 2,
            ..config
        };
        assert!(
            matches!(reply(tx.handle(watch(b"b"), 1, &config, &db, false).await.unwrap()), Frame::Error(error) if error.starts_with(b"CROSSSLOT"))
        );
        tx.handle(Command::Multi, 1, &config, &db, false)
            .await
            .unwrap();
        assert!(
            matches!(reply(tx.handle(Command::Get { key: Bytes::from_static(b"b") }, 1, &config, &db, false).await.unwrap()), Frame::Error(error) if error.starts_with(b"CROSSSLOT"))
        );
        assert!(
            matches!(reply(tx.handle(Command::Exec, 1, &config, &db, false).await.unwrap()), Frame::Error(error) if error.starts_with(b"EXECABORT"))
        );
        // EXECABORT removes tokens and the pinned shard; WATCH then accepts the other shard.
        assert_eq!(
            reply(
                tx.handle(watch(b"b"), 1, &config, &db, false)
                    .await
                    .unwrap()
            ),
            Frame::Simple(Bytes::from_static(b"OK"))
        );
        drop(tx);
        stop.send_replace(true);
        for task in tasks {
            task.await.unwrap();
        }
    }
}
