//! Whole-record replica application for the internal composed record-1 profile.

use tokio::sync::oneshot;

use super::*;

impl DbHandle {
    pub(super) async fn apply_composed_replica(
        &self,
        context: Context,
        generation: u64,
        cursor: Cursor,
        batch: ResolvedBatch,
    ) -> Result<(), Error> {
        let database = self.clone();
        tokio::spawn(async move {
            let _guard = database.internal_barrier().await?;
            database.admission.check()?;
            database.check_internal_admission()?;
            if *database.shutdown.borrow() {
                return Err(DbError::ShuttingDown.into());
            }
            if !database.readonly()
                || !context.aof.is_composed_legacy_mixed()
                || context.layout != context.aof.layout()
                || database.controls.len() != context.layout.shard_count as usize
                || !context.runtime.accepts(generation)
            {
                return Err(Error::Sequence);
            }
            let position = context.runtime.status().applied;
            if position.epoch != cursor.epoch
                || position.sequence.checked_add(1) != Some(cursor.sequence)
            {
                return Err(Error::Sequence);
            }
            let mut fragments = vec![Vec::new(); database.controls.len()];
            let mut previous = None;
            for mutation in &batch.mutations {
                if previous.is_some_and(|key: &bytes::Bytes| key >= mutation.key()) {
                    return Err(Error::Sequence);
                }
                previous = Some(mutation.key());
                let owner = context.layout.shard_for(mutation.key())?;
                fragments[owner].push(mutation.clone());
            }
            if batch.mutations.is_empty() {
                return Err(Error::Sequence);
            }
            let mut prepared = Vec::new();
            for (owner, mutations) in fragments.into_iter().enumerate() {
                if mutations.is_empty() {
                    continue;
                }
                let (reply, response) = oneshot::channel();
                tokio::time::timeout(
                    database.request_timeout,
                    database.controls[owner].send(Control::PrepareComposed {
                        expected: context.aof.clone(),
                        mutations,
                        origin: batch.origin,
                        reply,
                        _security: database.internal_lease.clone(),
                    }),
                )
                .await
                .map_err(|_| DbError::Timeout)?
                .map_err(|_| DbError::Unavailable)?;
                let candidate = tokio::time::timeout(database.request_timeout, response)
                    .await
                    .map_err(|_| DbError::Timeout)?
                    .map_err(|_| DbError::Unavailable)??;
                prepared.push((owner, candidate));
            }
            database.admission.check()?;
            database.check_internal_admission()?;
            let mut permits =
                tokio::time::timeout(database.request_timeout, reserve_controls(&database))
                    .await
                    .map_err(|_| DbError::Timeout)??
                    .into_iter()
                    .map(Some)
                    .collect::<Vec<_>>();
            database.check_internal_admission()?;
            let publication = database.admission.publication();
            if let Err(error) = context.aof.append_expected(cursor.sequence, batch).await {
                if matches!(
                    error,
                    crate::persistence::AofError::Sequence
                        | crate::persistence::AofError::Format(
                            crate::persistence::format::FormatError::Limit
                        )
                ) {
                    publication.complete();
                }
                return Err(error.into());
            }
            context
                .aof
                .lifecycle_fault("replica_mixed_before_first_apply")?;
            for (index, (owner, candidate)) in prepared.into_iter().enumerate() {
                let (reply, response) = oneshot::channel();
                permits[owner]
                    .take()
                    .ok_or(Error::Sequence)?
                    .send(Control::ApplyComposed {
                        expected: context.aof.clone(),
                        prepared: candidate,
                        reply,
                        _security: database.internal_lease.clone(),
                    });
                tokio::time::timeout(database.request_timeout, response)
                    .await
                    .map_err(|_| DbError::Timeout)?
                    .map_err(|_| DbError::Unavailable)??;
                if index == 0 {
                    context
                        .aof
                        .lifecycle_fault("replica_mixed_after_first_apply")?;
                }
            }
            context
                .aof
                .lifecycle_fault("replica_mixed_after_all_apply")?;
            if context.aof.flush().await? != cursor.sequence {
                return Err(Error::Sequence);
            }
            context.runtime.applied(cursor);
            publication.complete();
            Ok(())
        })
        .await?
    }
}
