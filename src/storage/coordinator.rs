//! Worker-owned tentative execution under exclusive admission, without persistence.
//!
//! The explicit local composed AOF profile uses this owner-control protocol for
//! mixed-owner EXEC. Default EXEC remains single-owner; replica roles refuse it.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use tokio::sync::{OwnedRwLockWriteGuard, mpsc, oneshot};
use tokio::time::{Instant, sleep_until};

use crate::command::{Command, ExecutionError, Reply};

use super::routing::TransactionPlan;
use super::transaction::TransactionTime;
use super::worker::{DbError, DbHandle, Worker};
use super::{Clock, MutationOrigin, Prepared, ResolvedBatch, WatchToken};

#[cfg(test)]
#[path = "coordinator_crash_tests.rs"]
mod crash_tests;
#[cfg(test)]
#[path = "coordinator_durable_tests.rs"]
mod durable_tests;

#[derive(Clone)]
pub(super) enum Mode {
    InMemory,
    Composed(crate::persistence::AofHandle),
}

pub(super) enum Control {
    Probe {
        mode: Mode,
        reply: oneshot::Sender<bool>,
    },
    PrepareWatch {
        mode: Mode,
        keys: Vec<Bytes>,
        reply: oneshot::Sender<Result<Prepared, DbError>>,
    },
    ApplyWatch {
        mode: Mode,
        prepared: Prepared,
        keys: Vec<Bytes>,
        reply: oneshot::Sender<Result<Vec<WatchToken>, DbError>>,
    },
    Clock(oneshot::Sender<TransactionTime>),
    CheckWatches {
        tokens: Vec<WatchToken>,
        time: TransactionTime,
        reply: oneshot::Sender<bool>,
    },
    Begin {
        mode: Mode,
        keys: BTreeSet<Bytes>,
        time: TransactionTime,
        reply: oneshot::Sender<Result<(), DbError>>,
    },
    Candidate {
        command: Command,
        reply: oneshot::Sender<Result<Prepared, DbError>>,
    },
    Accept {
        candidate: Prepared,
        reply: oneshot::Sender<Result<Reply, DbError>>,
    },
    Seal(oneshot::Sender<Result<Prepared, DbError>>),
    Apply {
        mode: Mode,
        prepared: Prepared,
        reply: oneshot::Sender<Result<(), DbError>>,
    },
    Abort(oneshot::Sender<()>),
}

impl Worker {
    fn supports_coordinated_mode(&self, mode: &Mode) -> bool {
        match mode {
            Mode::InMemory => self.replication.is_none() && self.aof.is_none(),
            Mode::Composed(expected) => {
                !self
                    .replication
                    .as_ref()
                    .is_some_and(|runtime| runtime.readonly())
                    && self.aof.as_ref().is_some_and(|actual| {
                        actual.is_composed_legacy_mixed() && actual.same_writer(expected)
                    })
            }
        }
    }

    pub(super) fn coordination_control(&mut self, control: Control) {
        match control {
            Control::Probe { mode, reply } => {
                let _ =
                    reply.send(self.supports_coordinated_mode(&mode) && self.coordinated.is_none());
            }
            Control::PrepareWatch { mode, keys, reply } => {
                let result = if self.supports_coordinated_mode(&mode) && self.coordinated.is_none()
                {
                    Ok(self.store.prepare(Command::Exists { keys }))
                } else {
                    Err(DbError::Unavailable)
                };
                let _ = reply.send(result);
            }
            Control::ApplyWatch {
                mode,
                prepared,
                keys,
                reply,
            } => {
                let result = if self.supports_coordinated_mode(&mode)
                    && self.coordinated.is_none()
                    && !matches!(&prepared.reply, Reply::Error(_))
                {
                    self.apply_prepared(prepared);
                    Ok(self.store.watch(keys))
                } else {
                    Err(DbError::Unavailable)
                };
                let _ = reply.send(result);
            }
            Control::Clock(reply) => {
                let _ = reply.send(self.store.transaction_time());
            }
            Control::CheckWatches {
                tokens,
                time,
                reply,
            } => {
                let _ = reply.send(self.store.watches_valid_at(&tokens, time.now()));
            }
            Control::Begin {
                mode,
                keys,
                time,
                reply,
            } => {
                let result = if !self.supports_coordinated_mode(&mode) || self.coordinated.is_some()
                {
                    Err(DbError::Unavailable)
                } else {
                    self.coordinated = Some(self.store.begin_owner_transaction(keys, time));
                    Ok(())
                };
                let _ = reply.send(result);
            }
            Control::Candidate { command, reply } => {
                let result = self
                    .coordinated
                    .as_ref()
                    .map(|owner| owner.prepare_candidate(command))
                    .ok_or(DbError::Unavailable);
                let _ = reply.send(result);
            }
            Control::Accept { candidate, reply } => {
                let result = self
                    .coordinated
                    .as_mut()
                    .map(|owner| owner.accept_candidate(candidate))
                    .ok_or(DbError::Unavailable);
                let _ = reply.send(result);
            }
            Control::Seal(reply) => {
                let result = self
                    .coordinated
                    .take()
                    .map(|owner| owner.seal(&self.store, Vec::new()))
                    .ok_or(DbError::Unavailable);
                let _ = reply.send(result);
            }
            Control::Apply {
                mode,
                prepared,
                reply,
            } => {
                let result = if !self.supports_coordinated_mode(&mode) || self.coordinated.is_some()
                {
                    Err(DbError::Unavailable)
                } else {
                    self.apply_prepared(prepared);
                    Ok(())
                };
                let _ = reply.send(result);
            }
            Control::Abort(reply) => {
                self.coordinated = None;
                let _ = reply.send(());
            }
        }
    }
}

async fn control<T>(
    sender: &mpsc::Sender<Control>,
    deadline: Instant,
    make: impl FnOnce(oneshot::Sender<T>) -> Control,
) -> Result<T, DbError> {
    let (reply, response) = oneshot::channel();
    tokio::time::timeout_at(deadline, sender.send(make(reply)))
        .await
        .map_err(|_| DbError::Timeout)?
        .map_err(|_| DbError::Unavailable)?;
    tokio::time::timeout_at(deadline, response)
        .await
        .map_err(|_| DbError::Timeout)?
        .map_err(|_| DbError::Unavailable)
}

impl DbHandle {
    /// Registers keys from several owners at one excluded durable cut.
    pub(super) async fn watch_coordinated(
        &self,
        keys: Vec<Bytes>,
    ) -> Result<(Reply, Vec<WatchToken>), DbError> {
        if self
            .replication
            .as_ref()
            .is_some_and(|runtime| runtime.readonly())
            || self.composed_aof.is_none()
        {
            return Err(DbError::Unavailable);
        }
        let deadline = Instant::now()
            .checked_add(self.request_timeout)
            .ok_or(DbError::Timeout)?;
        self.admission.check()?;
        let mut shutdown = self.shutdown.clone();
        let exclusive = tokio::select! {
            biased;
            () = sleep_until(deadline) => return Err(DbError::Timeout),
            () = super::worker::stopping(&mut shutdown) => return Err(DbError::ShuttingDown),
            guard = self.barrier.clone().write_owned() => guard,
        };
        self.admission.check()?;
        if *shutdown.borrow() {
            return Err(DbError::ShuttingDown);
        }
        if self
            .security_lease
            .as_ref()
            .is_some_and(|lease| !lease.admitted_now())
        {
            return Err(DbError::SecurityExpired);
        }
        let handle = self.clone();
        let (reply, response) = oneshot::channel();
        tokio::spawn(async move {
            let result = run_watch(handle, keys, deadline, exclusive).await;
            let _ = reply.send(result);
        });
        tokio::select! {
            () = sleep_until(deadline) => Err(DbError::Timeout),
            result = response => result.map_err(|_| DbError::Unavailable)?,
        }
    }

    /// Executes a dataset-only queue across owners under exclusive admission.
    /// The explicit local composed profile appends one record before application;
    /// RESP EXEC can select it on a writable primary.
    /// After acceptance, caller cancellation does not stop owner application.
    pub(crate) async fn execute_coordinated(
        &self,
        commands: Vec<Command>,
        watched: Vec<WatchToken>,
    ) -> Result<Reply, DbError> {
        if commands
            .iter()
            .any(|command| matches!(command, Command::Memory { .. }))
        {
            return Ok(Reply::Error(ExecutionError::Memory(
                "ERR memory commands are not supported in MULTI",
            )));
        }
        if commands.iter().any(|command| {
            matches!(
                command,
                Command::Info(_)
                    | Command::Subscribe { .. }
                    | Command::Unsubscribe { .. }
                    | Command::Publish { .. }
                    | Command::Multi
                    | Command::Exec
                    | Command::Discard
                    | Command::Watch { .. }
            )
        }) {
            return Ok(Reply::Error(ExecutionError::ConnectionOnly));
        }
        if self
            .replication
            .as_ref()
            .is_some_and(|runtime| runtime.readonly())
        {
            return Err(DbError::Unavailable);
        }
        let watch_keys = watched
            .iter()
            .map(|token| token.key().clone())
            .collect::<Vec<_>>();
        let plan = match self.router.plan_transaction(commands, &watch_keys) {
            Ok(plan) => plan,
            Err(error) => return Ok(Reply::Error(error)),
        };
        let deadline = Instant::now()
            .checked_add(self.request_timeout)
            .ok_or(DbError::Timeout)?;
        self.admission.check()?;
        let mut shutdown = self.shutdown.clone();
        let exclusive = tokio::select! {
            biased;
            () = sleep_until(deadline) => return Err(DbError::Timeout),
            () = super::worker::stopping(&mut shutdown) => return Err(DbError::ShuttingDown),
            guard = self.barrier.clone().write_owned() => guard,
        };
        self.admission.check()?;
        if *shutdown.borrow() {
            return Err(DbError::ShuttingDown);
        }
        if self
            .security_lease
            .as_ref()
            .is_some_and(|lease| !lease.admitted_now())
        {
            return Err(DbError::SecurityExpired);
        }
        let handle = self.clone();
        let (reply, response) = oneshot::channel();
        tokio::spawn(async move {
            let result = run(handle, plan, watched, deadline, exclusive).await;
            let _ = reply.send(result);
        });
        tokio::select! {
            () = sleep_until(deadline) => Err(DbError::Timeout),
            result = response => result.map_err(|_| DbError::Unavailable)?,
        }
    }
}

async fn run_watch(
    handle: DbHandle,
    keys: Vec<Bytes>,
    deadline: Instant,
    _exclusive: OwnedRwLockWriteGuard<()>,
) -> Result<(Reply, Vec<WatchToken>), DbError> {
    let aof = handle.composed_aof.as_ref().ok_or(DbError::Unavailable)?;
    let mode = Mode::Composed(aof.clone());
    let mut by_owner = BTreeMap::<usize, Vec<Bytes>>::new();
    for key in keys {
        by_owner
            .entry(handle.router.shard_for(&key))
            .or_default()
            .push(key);
    }
    let mut prepared = Vec::with_capacity(by_owner.len());
    for (owner, keys) in by_owner {
        let candidate = control(&handle.coordination[owner], deadline, |reply| {
            Control::PrepareWatch {
                mode: mode.clone(),
                keys: keys.clone(),
                reply,
            }
        })
        .await??;
        if matches!(&candidate.reply, Reply::Error(_)) {
            return Ok((candidate.reply, Vec::new()));
        }
        prepared.push((owner, keys, candidate));
    }
    let mut batch = ResolvedBatch {
        origin: MutationOrigin::Expiration,
        mutations: prepared
            .iter()
            .flat_map(|(_, _, candidate)| candidate.batch.mutations.iter().cloned())
            .collect(),
    };
    batch
        .mutations
        .sort_unstable_by(|left, right| left.key().cmp(right.key()));
    if batch
        .mutations
        .windows(2)
        .any(|pair| pair[0].key() == pair[1].key())
    {
        return Err(DbError::Unavailable);
    }
    let mut publication = if batch.mutations.is_empty() {
        None
    } else {
        Some(handle.admission.publication())
    };
    if publication.is_some() {
        match aof.append(batch).await {
            Ok(_) => {}
            Err(crate::persistence::AofError::Format(
                crate::persistence::format::FormatError::Limit,
            )) => {
                publication
                    .take()
                    .expect("decision guard created")
                    .complete();
                return Ok((Reply::Error(ExecutionError::AofRecordLimit), Vec::new()));
            }
            Err(error) => {
                tracing::error!(%error, "multi-owner WATCH expiration decision failed");
                return Err(DbError::Unavailable);
            }
        }
    }
    let apply_deadline = Instant::now()
        .checked_add(handle.request_timeout)
        .ok_or(DbError::Timeout)?;
    let mut tokens = Vec::new();
    for (owner, keys, candidate) in prepared {
        let mut acquired = control(&handle.coordination[owner], apply_deadline, |reply| {
            Control::ApplyWatch {
                mode: mode.clone(),
                prepared: candidate,
                keys,
                reply,
            }
        })
        .await??;
        tokens.append(&mut acquired);
    }
    if let Some(publication) = publication {
        publication.complete();
    }
    Ok((Reply::Ok, tokens))
}

async fn run(
    handle: DbHandle,
    plan: TransactionPlan,
    watched: Vec<WatchToken>,
    deadline: Instant,
    _exclusive: OwnedRwLockWriteGuard<()>,
) -> Result<Reply, DbError> {
    let participants = plan.participants().to_vec();
    let mode = match &handle.composed_aof {
        Some(aof) if aof.is_composed_legacy_mixed() => Mode::Composed(aof.clone()),
        Some(_) => return Err(DbError::Unavailable),
        None => Mode::InMemory,
    };
    for sender in &handle.coordination {
        if !control(sender, deadline, |reply| Control::Probe {
            mode: mode.clone(),
            reply,
        })
        .await?
        {
            return Err(DbError::Unavailable);
        }
    }
    let time = control(
        &handle.coordination[participants[0]],
        deadline,
        Control::Clock,
    )
    .await?;
    let mut watches = (0..handle.coordination.len())
        .map(|_| Vec::new())
        .collect::<Vec<_>>();
    for token in watched {
        watches[handle.router.shard_for(token.key())].push(token);
    }
    for &owner in &participants {
        if !control(&handle.coordination[owner], deadline, |reply| {
            Control::CheckWatches {
                tokens: std::mem::take(&mut watches[owner]),
                time,
                reply,
            }
        })
        .await?
        {
            return Ok(Reply::NullArray);
        }
    }

    let publication = handle.admission.publication();
    let prepared = prepare(&handle, plan, &mode, time, deadline).await;
    let (replies, owners) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            let cleanup_deadline = Instant::now()
                .checked_add(handle.request_timeout)
                .unwrap_or(deadline);
            let cleaned = abort_all(&handle, &participants, cleanup_deadline)
                .await
                .is_ok();
            if cleaned {
                publication.complete();
            }
            return Err(error);
        }
    };
    if let Mode::Composed(aof) = &mode {
        aof.lifecycle_fault("mixed_before_append")
            .map_err(|_| DbError::Unavailable)?;
        let mut batch = ResolvedBatch {
            origin: MutationOrigin::Client,
            mutations: owners
                .iter()
                .flat_map(|(_, prepared)| prepared.batch.mutations.iter().cloned())
                .collect(),
        };
        batch
            .mutations
            .sort_unstable_by(|left, right| left.key().cmp(right.key()));
        if batch
            .mutations
            .windows(2)
            .any(|pair| pair[0].key() == pair[1].key())
        {
            return Err(DbError::Unavailable);
        }
        if !batch.mutations.is_empty() {
            match aof.append(batch).await {
                Ok(_) => {}
                Err(crate::persistence::AofError::Format(
                    crate::persistence::format::FormatError::Limit,
                )) => {
                    publication.complete();
                    return Ok(Reply::Error(ExecutionError::AofRecordLimit));
                }
                Err(error) => {
                    tracing::error!(%error, "mixed-owner AOF decision failed; closing admission");
                    return Err(DbError::Unavailable);
                }
            }
        }
    }
    // The commit decision exists. Client timeout no longer cancels application.
    // Any failed owner acknowledgment closes admission before exclusion drops.
    let apply_deadline = Instant::now()
        .checked_add(handle.request_timeout)
        .ok_or(DbError::Timeout)?;
    if let Mode::Composed(aof) = &mode {
        aof.lifecycle_fault("mixed_before_first_apply")
            .map_err(|_| DbError::Unavailable)?;
    }
    for (index, (owner, prepared)) in owners.into_iter().enumerate() {
        control(&handle.coordination[owner], apply_deadline, |reply| {
            Control::Apply {
                mode: mode.clone(),
                prepared,
                reply,
            }
        })
        .await??;
        if index == 0
            && let Mode::Composed(aof) = &mode
        {
            aof.lifecycle_fault("mixed_after_first_apply")
                .map_err(|_| DbError::Unavailable)?;
        }
    }
    if let Mode::Composed(aof) = &mode {
        aof.lifecycle_fault("mixed_after_all_apply")
            .map_err(|_| DbError::Unavailable)?;
    }
    publication.complete();
    Ok(Reply::Array(replies))
}

async fn abort_all(
    handle: &DbHandle,
    participants: &[usize],
    deadline: Instant,
) -> Result<(), DbError> {
    for &owner in participants {
        control(&handle.coordination[owner], deadline, Control::Abort).await?;
    }
    Ok(())
}

async fn prepare(
    handle: &DbHandle,
    plan: TransactionPlan,
    mode: &Mode,
    time: TransactionTime,
    deadline: Instant,
) -> Result<(Vec<Reply>, Vec<(usize, Prepared)>), DbError> {
    let (participants, commands) = plan.into_parts();
    let mut keys = (0..handle.coordination.len())
        .map(|_| BTreeSet::new())
        .collect::<Vec<_>>();
    for command in &commands {
        for fragment in command.clone().into_parts().0 {
            fragment.command.visit_keys(|key| {
                keys[fragment.owner].insert(key.clone());
            });
        }
    }
    for &owner in &participants {
        control(&handle.coordination[owner], deadline, |reply| {
            Control::Begin {
                mode: mode.clone(),
                keys: std::mem::take(&mut keys[owner]),
                time,
                reply,
            }
        })
        .await??;
    }
    let mut replies = Vec::with_capacity(commands.len());
    for planned in commands {
        let (fragments, reply_plan) = planned.into_parts();
        if fragments.len() == 1 && matches!(fragments[0].command, Command::Unwatch) {
            replies.push(Reply::Ok);
            continue;
        }
        let mut candidates = Vec::with_capacity(fragments.len());
        for fragment in fragments {
            let candidate = control(&handle.coordination[fragment.owner], deadline, |reply| {
                Control::Candidate {
                    command: fragment.command,
                    reply,
                }
            })
            .await??;
            candidates.push((fragment.owner, candidate));
        }
        let fragment_replies = candidates
            .iter()
            .map(|(_, candidate)| candidate.reply.clone())
            .collect();
        let assembled = reply_plan
            .assemble(fragment_replies)
            .map_err(|_| DbError::Unavailable)?;
        let reject_joint = candidates.len() > 1 && matches!(assembled, Reply::Error(_));
        if !reject_joint {
            for (owner, candidate) in candidates {
                let expected = candidate.reply.clone();
                let accepted = control(&handle.coordination[owner], deadline, |reply| {
                    Control::Accept { candidate, reply }
                })
                .await??;
                if accepted != expected {
                    return Err(DbError::Unavailable);
                }
            }
        }
        replies.push(assembled);
    }
    let mut owners = Vec::with_capacity(participants.len());
    for owner in participants {
        let prepared = control(&handle.coordination[owner], deadline, Control::Seal).await??;
        owners.push((owner, prepared));
    }
    Ok((replies, owners))
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;

    use tokio::sync::watch;

    use super::*;
    use crate::storage::{Store, StoreConfig, SystemClock, worker};

    fn set(key: &'static [u8], value: &'static [u8]) -> Command {
        Command::Set {
            key: Bytes::from_static(key),
            value: Bytes::from_static(value),
        }
    }

    fn get(key: &'static [u8]) -> Command {
        Command::Get {
            key: Bytes::from_static(key),
        }
    }

    async fn start(
        stores: Vec<Store>,
    ) -> (
        watch::Sender<bool>,
        DbHandle,
        Vec<tokio::task::JoinHandle<()>>,
    ) {
        let (stop, shutdown) = watch::channel(false);
        let (handle, owners) =
            worker::channel_with_stores(2, Duration::from_secs(5), shutdown, stores).unwrap();
        let tasks = owners
            .into_iter()
            .map(|owner| tokio::spawn(owner.run()))
            .collect();
        (stop, handle.with_unpersisted_multi_owner(), tasks)
    }

    async fn stop(
        stop: watch::Sender<bool>,
        handle: DbHandle,
        tasks: Vec<tokio::task::JoinHandle<()>>,
    ) {
        drop(handle);
        stop.send_replace(true);
        for task in tasks {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn mixed_owner_exec_sees_prior_writes_and_publishes_complete_result() {
        let (stop_signal, handle, tasks) = start(vec![Store::new(), Store::new()]).await;
        let reply = handle
            .execute_batch(
                vec![
                    set(b"a", b"left"),
                    set(b"b", b"right"),
                    Command::MGet {
                        keys: vec![
                            Bytes::from_static(b"b"),
                            Bytes::from_static(b"a"),
                            Bytes::from_static(b"b"),
                        ],
                    },
                    Command::Del {
                        keys: vec![
                            Bytes::from_static(b"a"),
                            Bytes::from_static(b"b"),
                            Bytes::from_static(b"a"),
                        ],
                    },
                    get(b"a"),
                ],
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            reply,
            Reply::Array(vec![
                Reply::Ok,
                Reply::Ok,
                Reply::Array(vec![
                    Reply::Bulk(Some(Bytes::from_static(b"right"))),
                    Reply::Bulk(Some(Bytes::from_static(b"left"))),
                    Reply::Bulk(Some(Bytes::from_static(b"right"))),
                ]),
                Reply::Integer(2),
                Reply::Bulk(None),
            ])
        );
        assert_eq!(handle.execute(get(b"a")).await.unwrap(), Reply::Bulk(None));
        assert_eq!(handle.execute(get(b"b")).await.unwrap(), Reply::Bulk(None));
        stop(stop_signal, handle, tasks).await;
    }

    #[tokio::test]
    async fn failed_mset_on_last_owner_discards_whole_candidate_but_retains_other_commands() {
        let first = Store::with_config(
            StoreConfig {
                max_dataset_bytes: 200,
            },
            Arc::new(SystemClock),
        )
        .unwrap();
        let second = Store::with_config(
            StoreConfig {
                max_dataset_bytes: 130,
            },
            Arc::new(SystemClock),
        )
        .unwrap();
        let (stop_signal, handle, tasks) = start(vec![first, second]).await;
        let reply = handle
            .execute_batch(
                vec![
                    set(b"a", b"1"),
                    Command::MSet {
                        entries: vec![
                            (Bytes::from_static(b"a"), Bytes::from_static(b"changed")),
                            (Bytes::from_static(b"b"), Bytes::from_static(b"too-large")),
                        ],
                    },
                    set(b"b", b"2"),
                    get(b"a"),
                ],
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            reply,
            Reply::Array(vec![
                Reply::Ok,
                Reply::Error(ExecutionError::OutOfMemory),
                Reply::Ok,
                Reply::Bulk(Some(Bytes::from_static(b"1"))),
            ])
        );
        assert_eq!(
            handle.execute(get(b"a")).await.unwrap(),
            Reply::Bulk(Some(Bytes::from_static(b"1")))
        );
        assert_eq!(
            handle.execute(get(b"b")).await.unwrap(),
            Reply::Bulk(Some(Bytes::from_static(b"2")))
        );
        stop(stop_signal, handle, tasks).await;
    }

    #[tokio::test]
    async fn watched_owner_conflict_aborts_before_any_other_owner_changes() {
        let (stop_signal, handle, tasks) = start(vec![Store::new(), Store::new()]).await;
        let (_, watched) = handle.watch(vec![Bytes::from_static(b"a")]).await.unwrap();
        assert_eq!(
            handle.execute(set(b"a", b"changed")).await.unwrap(),
            Reply::Ok
        );
        let reply = handle
            .execute_batch(vec![set(b"b", b"forbidden")], watched)
            .await
            .unwrap();
        assert_eq!(reply, Reply::NullArray);
        assert_eq!(handle.execute(get(b"b")).await.unwrap(), Reply::Bulk(None));
        stop(stop_signal, handle, tasks).await;
    }

    #[tokio::test]
    async fn unsupported_context_command_rejects_before_earlier_write() {
        let (stop_signal, handle, tasks) = start(vec![Store::new(), Store::new()]).await;
        let reply = handle
            .execute_batch(
                vec![
                    set(b"a", b"not-applied"),
                    set(b"b", b"not-applied"),
                    Command::Exec,
                ],
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(reply, Reply::Error(ExecutionError::ConnectionOnly));
        assert_eq!(handle.execute(get(b"a")).await.unwrap(), Reply::Bulk(None));
        assert_eq!(handle.execute(get(b"b")).await.unwrap(), Reply::Bulk(None));
        stop(stop_signal, handle, tasks).await;
    }

    #[tokio::test]
    async fn concurrent_opposite_owner_orders_publish_complete_pairs() {
        let (stop_signal, handle, tasks) = start(vec![Store::new(), Store::new()]).await;
        let gate = Arc::new(tokio::sync::Barrier::new(3));
        let mut submissions = Vec::new();
        for (value, reverse) in [(b"first".as_slice(), false), (b"second".as_slice(), true)] {
            let handle = handle.clone();
            let gate = gate.clone();
            let value = Bytes::copy_from_slice(value);
            submissions.push(tokio::spawn(async move {
                let mut entries = vec![
                    (Bytes::from_static(b"a"), value.clone()),
                    (Bytes::from_static(b"b"), value),
                ];
                if reverse {
                    entries.reverse();
                }
                gate.wait().await;
                handle
                    .execute_batch(vec![Command::MSet { entries }], Vec::new())
                    .await
            }));
        }
        gate.wait().await;
        for submission in submissions {
            assert_eq!(
                submission.await.unwrap().unwrap(),
                Reply::Array(vec![Reply::Ok])
            );
        }
        let a = handle.execute(get(b"a")).await.unwrap();
        let b = handle.execute(get(b"b")).await.unwrap();
        assert_eq!(a, b);
        assert!(
            matches!(a, Reply::Bulk(Some(value)) if value == b"first".as_slice() || value == b"second".as_slice())
        );
        stop(stop_signal, handle, tasks).await;
    }

    #[tokio::test]
    async fn cancelled_client_cannot_expose_first_owner_before_second_apply() {
        let (stop_signal, shutdown) = watch::channel(false);
        let (handle, mut owners) = worker::channel_with_stores(
            2,
            Duration::from_secs(5),
            shutdown,
            vec![Store::new(), Store::new()],
        )
        .unwrap();
        let first = tokio::spawn(owners.remove(0).run());
        let (reached, at_second_apply) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        let second = tokio::spawn(async move {
            let mut worker = owners.remove(0);
            loop {
                let control = worker.coordination.recv().await.unwrap();
                if matches!(control, Control::Apply { .. }) {
                    reached.send(()).unwrap();
                    resumed.await.unwrap();
                    worker.coordination_control(control);
                    worker.run().await;
                    break;
                }
                worker.coordination_control(control);
            }
        });
        let client = {
            let handle = handle.clone().with_unpersisted_multi_owner();
            tokio::spawn(async move {
                handle
                    .execute_batch(vec![set(b"a", b"left"), set(b"b", b"right")], Vec::new())
                    .await
            })
        };
        at_second_apply.await.unwrap();
        client.abort();
        assert!(client.await.unwrap_err().is_cancelled());

        let mut read = Box::pin(handle.execute(get(b"a")));
        let mut snapshot = Box::pin(handle.snapshot(None));
        poll_fn(|cx| {
            assert!(read.as_mut().poll(cx).is_pending());
            assert!(snapshot.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        resume.send(()).unwrap();
        assert_eq!(
            read.await.unwrap(),
            Reply::Bulk(Some(Bytes::from_static(b"left")))
        );
        let snapshot = snapshot.await.unwrap();
        assert_eq!(snapshot.mutations.len(), 2);
        assert_eq!(
            handle.execute(get(b"b")).await.unwrap(),
            Reply::Bulk(Some(Bytes::from_static(b"right")))
        );
        stop(stop_signal, handle, vec![first, second]).await;
    }

    #[tokio::test]
    async fn lost_second_owner_after_first_apply_closes_all_admission() {
        let (stop_signal, shutdown) = watch::channel(false);
        let (handle, mut owners) = worker::channel_with_stores(
            2,
            Duration::from_secs(5),
            shutdown,
            vec![Store::new(), Store::new()],
        )
        .unwrap();
        let first = tokio::spawn(owners.remove(0).run());
        let second = tokio::spawn(async move {
            let mut worker = owners.remove(0);
            loop {
                let control = worker.coordination.recv().await.unwrap();
                if matches!(control, Control::Apply { .. }) {
                    // The channel and tentative state disappear without an ACK.
                    drop(control);
                    return;
                }
                worker.coordination_control(control);
            }
        });
        assert_eq!(
            handle
                .clone()
                .with_unpersisted_multi_owner()
                .execute_batch(vec![set(b"a", b"left"), set(b"b", b"right")], Vec::new())
                .await,
            Err(DbError::Unavailable)
        );
        second.await.unwrap();
        assert_eq!(handle.execute(get(b"a")).await, Err(DbError::Unavailable));
        assert!(matches!(
            handle.snapshot(None).await,
            Err(crate::storage::snapshot::SnapshotError::Database(
                DbError::Unavailable
            ))
        ));
        stop(stop_signal, handle, vec![first]).await;
    }

    #[tokio::test]
    async fn failed_preparation_aborts_overlays_and_keeps_admission_open() {
        let (stop_signal, shutdown) = watch::channel(false);
        let (handle, mut owners) = worker::channel_with_stores(
            2,
            Duration::from_secs(5),
            shutdown,
            vec![Store::new(), Store::new()],
        )
        .unwrap();
        let first = tokio::spawn(owners.remove(0).run());
        let second = tokio::spawn(async move {
            let mut worker = owners.remove(0);
            let mut rejected = false;
            loop {
                let control = worker.coordination.recv().await.unwrap();
                match control {
                    Control::Candidate { reply, .. } if !rejected => {
                        rejected = true;
                        let _ = reply.send(Err(DbError::Unavailable));
                    }
                    control => worker.coordination_control(control),
                }
                if rejected && worker.coordinated.is_none() {
                    worker.run().await;
                    break;
                }
            }
        });
        assert_eq!(
            handle
                .clone()
                .with_unpersisted_multi_owner()
                .execute_batch(vec![set(b"a", b"left"), set(b"b", b"right")], Vec::new())
                .await,
            Err(DbError::Unavailable)
        );
        assert_eq!(handle.execute(get(b"a")).await.unwrap(), Reply::Bulk(None));
        assert_eq!(handle.execute(get(b"b")).await.unwrap(), Reply::Bulk(None));
        stop(stop_signal, handle, vec![first, second]).await;
    }

    #[tokio::test(start_paused = true)]
    async fn owner_preparations_share_the_first_participants_frozen_ttl_clock() {
        use crate::command::{SetExpiry, SetOptions};
        use crate::storage::Mutation;

        #[derive(Clone)]
        struct FixedClock {
            now: Instant,
            unix_ms: i64,
        }
        impl Clock for FixedClock {
            fn now(&self) -> Instant {
                self.now
            }
            fn unix_millis(&self) -> i64 {
                self.unix_ms
            }
        }
        let now = Instant::now();
        let first = Store::with_clock(Arc::new(FixedClock {
            now,
            unix_ms: 1_000,
        }));
        let second = Store::with_clock(Arc::new(FixedClock {
            now: now + Duration::from_secs(10),
            unix_ms: 11_000,
        }));
        let (stop_signal, handle, tasks) = start(vec![first, second]).await;
        let set_ttl = |key| Command::SetWithOptions {
            key: Bytes::from_static(key),
            value: Bytes::from_static(b"v"),
            options: SetOptions {
                expiry: SetExpiry::After(Duration::from_millis(20)),
                ..SetOptions::default()
            },
        };
        assert_eq!(
            handle
                .execute_batch(vec![set_ttl(b"a"), set_ttl(b"b")], Vec::new())
                .await
                .unwrap(),
            Reply::Array(vec![Reply::Ok, Reply::Ok])
        );
        let snapshot = handle.snapshot(None).await.unwrap();
        assert_eq!(snapshot.mutations.len(), 2);
        assert!(snapshot.mutations.iter().all(|mutation| matches!(
            mutation,
            Mutation::Put {
                expires_at_unix_ms: Some(1_020),
                ..
            }
        )));
        stop(stop_signal, handle, tasks).await;
    }

    #[tokio::test]
    async fn any_replication_role_refuses_before_other_owner_mutates() {
        let router = super::super::routing::ShardRouter::new(4).unwrap();
        assert_eq!((router.shard_for(b"a"), router.shard_for(b"b")), (0, 1));
        for (role, guarded_owner) in [
            (crate::persistence::Role::Primary, 1),
            (crate::persistence::Role::Primary, 3),
            (crate::persistence::Role::Replica, 1),
            (crate::persistence::Role::Replica, 3),
        ] {
            let (stop_signal, shutdown) = watch::channel(false);
            let (handle, owners) = worker::channel_with_stores(
                2,
                Duration::from_secs(5),
                shutdown,
                vec![Store::new(), Store::new(), Store::new(), Store::new()],
            )
            .unwrap();
            let runtime = crate::replication::state::Runtime::new(
                role,
                crate::replication::Cursor {
                    epoch: [1; 16],
                    sequence: 0,
                },
            );
            let tasks = owners
                .into_iter()
                .enumerate()
                .map(|(owner, worker)| {
                    tokio::spawn(
                        if owner == guarded_owner {
                            worker.with_replication(runtime.clone())
                        } else {
                            worker
                        }
                        .run(),
                    )
                })
                .collect();
            assert_eq!(
                handle
                    .clone()
                    .with_unpersisted_multi_owner()
                    .execute_batch(vec![set(b"a", b"left"), set(b"b", b"right")], Vec::new())
                    .await,
                Err(DbError::Unavailable)
            );
            assert_eq!(handle.execute(get(b"a")).await.unwrap(), Reply::Bulk(None));
            stop(stop_signal, handle, tasks).await;
        }
    }

    #[tokio::test]
    async fn aof_owner_refuses_before_any_in_memory_candidate_is_applied() {
        let directory = std::env::temp_dir().join(format!(
            "sider-coordinator-aof-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir(&directory).unwrap();
        let mut config = crate::persistence::AofConfig::new(directory.clone());
        config.layout.shard_count = 2;
        let (_empty, aof, writer) =
            crate::persistence::recover(config, StoreConfig::default(), Arc::new(SystemClock))
                .unwrap()
                .start();
        let (stop_signal, shutdown) = watch::channel(false);
        let (handle, owners) = worker::channel_with_stores(
            2,
            Duration::from_secs(5),
            shutdown,
            vec![Store::new(), Store::new()],
        )
        .unwrap();
        let tasks = owners
            .into_iter()
            .enumerate()
            .map(|(owner, worker)| {
                tokio::spawn(
                    if owner == 1 {
                        worker.with_aof(aof.clone(), 0)
                    } else {
                        worker
                    }
                    .run(),
                )
            })
            .collect();
        assert_eq!(
            handle
                .clone()
                .with_unpersisted_multi_owner()
                .execute_batch(vec![set(b"a", b"left"), set(b"b", b"right")], Vec::new())
                .await,
            Err(DbError::Unavailable)
        );
        assert_eq!(handle.execute(get(b"a")).await.unwrap(), Reply::Bulk(None));
        stop(stop_signal, handle, tasks).await;
        drop(aof);
        writer.await.unwrap().unwrap();
        let parent = std::env::temp_dir().canonicalize().unwrap();
        let target = directory.canonicalize().unwrap();
        assert_eq!(target.parent(), Some(parent.as_path()));
        assert!(
            target
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("sider-coordinator-aof-")
        );
        std::fs::remove_dir_all(target).unwrap();
    }
}
