//! Publicação global de snapshots e lotes recebidos com workers proprietários.

use std::sync::Arc;

use tokio::sync::oneshot;

use super::{
    Clock, Mutation, ResolvedBatch, Store, StoreConfig,
    worker::{DbError, DbHandle},
};
use crate::persistence::{AofHandle, DurableLayout, ReplicationMetadata, Role};
use crate::replication::{
    Cursor, Error,
    journal::{Journal, Limits, Subscriber},
    state::Runtime,
};

#[derive(Clone)]
pub struct Context {
    pub aof: AofHandle,
    pub runtime: Runtime,
    pub layout: DurableLayout,
    pub store_config: StoreConfig,
    pub clock: Arc<dyn Clock>,
    pub journal_limits: Limits,
    pub aof_limits: crate::persistence::format::Limits,
}

pub(super) enum Control {
    Replace {
        store: Store,
        reply: oneshot::Sender<()>,
    },
    Apply {
        batch: ResolvedBatch,
        sequence: u64,
        reply: oneshot::Sender<Result<(), Error>>,
    },
}

pub struct Snapshot {
    pub cursor: Cursor,
    pub mutations: Vec<Mutation>,
    pub subscription: Option<Subscriber>,
}

impl DbHandle {
    /// Captura e assina ainda sob a mesma barreira; serialização ocorre fora dela.
    pub async fn replication_snapshot(
        &self,
        context: &Context,
        follow: bool,
    ) -> Result<Snapshot, Error> {
        let (_guard, mutations) = self.freeze().await?;
        if context.runtime.readonly() {
            return Err(Error::Stale);
        }
        let sequence = context.aof.flush().await?;
        let journal = context.runtime.journal().ok_or(Error::Stale)?;
        let cursor = journal.status()?.head;
        if cursor.sequence != sequence {
            return Err(Error::Sequence);
        }
        let subscription = if follow {
            Some(journal.subscribe(cursor)?)
        } else {
            None
        };
        Ok(Snapshot {
            cursor,
            mutations,
            subscription,
        })
    }

    /// A barreira espera também uma aplicação aceita cuja sessão foi desconectada.
    pub async fn replication_position(&self, runtime: &Runtime) -> Result<Cursor, Error> {
        let _guard = self.barrier.clone().write_owned().await;
        if *self.shutdown.borrow() {
            return Err(DbError::ShuttingDown.into());
        }
        Ok(runtime.status().applied)
    }

    /// A tarefa possui a barreira até o último worker trocar o mapa, mesmo se o
    /// consumidor da resposta abandonar a future após a publicação no disco.
    pub async fn install_replica(
        &self,
        context: Context,
        generation: u64,
        cursor: Cursor,
        mutations: Vec<Mutation>,
    ) -> Result<(), Error> {
        let database = self.clone();
        tokio::spawn(async move {
            let mut stores = (0..context.layout.shard_count)
                .map(|shard| {
                    Store::with_config(
                        StoreConfig {
                            max_dataset_bytes: context
                                .layout
                                .quota(context.store_config.max_dataset_bytes, shard as usize)?,
                        },
                        context.clock.clone(),
                    )
                })
                .collect::<Result<Vec<_>, crate::ConfigError>>()?;
            let mut previous = None;
            for mutation in &mutations {
                if !matches!(mutation, Mutation::Put { .. })
                    || previous.as_ref().is_some_and(|key| key >= mutation.key())
                {
                    return Err(Error::Sequence);
                }
                previous = Some(mutation.key().clone());
                stores[context.layout.shard_for(mutation.key())?]
                    .replay(std::slice::from_ref(mutation))?;
            }
            let _guard = database.barrier.clone().write_owned().await;
            if *database.shutdown.borrow() {
                return Err(DbError::ShuttingDown.into());
            }
            if !context.runtime.accepts(generation) {
                return Err(Error::Stale);
            }
            if database.controls.len() != stores.len() {
                return Err(Error::Sequence);
            }
            context
                .aof
                .install_snapshot(
                    mutations,
                    cursor.sequence,
                    ReplicationMetadata {
                        role: Role::Replica,
                        epoch: cursor.epoch,
                    },
                )
                .await?;
            // Após o rename, qualquer falha obriga a parar: o disco já seleciona o snapshot novo.
            for (control, store) in database.controls.iter().zip(stores) {
                let (reply, response) = oneshot::channel();
                control
                    .send(Control::Replace { store, reply })
                    .await
                    .map_err(|_| DbError::Unavailable)?;
                response.await.map_err(|_| DbError::Unavailable)?;
            }
            context.runtime.applied(cursor);
            Ok(())
        })
        .await?
    }

    pub async fn apply_replica(
        &self,
        context: Context,
        generation: u64,
        cursor: Cursor,
        batch: ResolvedBatch,
    ) -> Result<(), Error> {
        let database = self.clone();
        tokio::spawn(async move {
            let _guard = database.barrier.clone().write_owned().await;
            if *database.shutdown.borrow() {
                return Err(DbError::ShuttingDown.into());
            }
            if !context.runtime.accepts(generation) {
                return Err(Error::Stale);
            }
            let position = context.runtime.status().applied;
            if position.epoch != cursor.epoch
                || position.sequence.checked_add(1) != Some(cursor.sequence)
            {
                return Err(Error::Sequence);
            }
            let mut shard = None;
            for mutation in &batch.mutations {
                let next = context.layout.shard_for(mutation.key())?;
                if shard.is_some_and(|value| value != next) {
                    return Err(Error::Sequence);
                }
                shard = Some(next);
            }
            let shard = shard.ok_or(Error::Sequence)?;
            let (reply, response) = oneshot::channel();
            database.controls[shard]
                .send(Control::Apply {
                    batch,
                    sequence: cursor.sequence,
                    reply,
                })
                .await
                .map_err(|_| DbError::Unavailable)?;
            response.await.map_err(|_| DbError::Unavailable)??;
            context.runtime.applied(cursor);
            Ok(())
        })
        .await?
    }

    /// Também inicializa a época de um primário recuperado antes da admissão de clientes.
    pub async fn promote_replica(
        &self,
        context: Context,
        epoch: [u8; 16],
    ) -> Result<Cursor, Error> {
        let database = self.clone();
        tokio::spawn(async move {
            let (_guard, snapshot) = database.freeze().await?;
            let sequence = context.aof.flush().await?;
            let cursor = Cursor { epoch, sequence };
            let journal = Journal::new(cursor, context.journal_limits)?;
            context
                .aof
                .install_snapshot(
                    snapshot,
                    sequence,
                    ReplicationMetadata {
                        role: Role::Primary,
                        epoch,
                    },
                )
                .await?;
            context.aof.attach_journal(journal.clone()).await?;
            context.runtime.primary(cursor, journal);
            Ok(cursor)
        })
        .await?
    }
}
