//! Native worker lifecycle staging; MEM wire grammar and durable activation are pending.

use bytes::Bytes;
use sider::command::{Command, ExecutionError, Reply, SetCondition};
use sider::storage::{
    Store, StoreConfig, SystemClock,
    memory::{Descriptor, Member, Operation, Ttl},
    worker,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

struct RecoveryClock;
impl sider::storage::Clock for RecoveryClock {
    fn now(&self) -> tokio::time::Instant {
        tokio::time::Instant::now()
    }
    fn unix_millis(&self) -> i64 {
        2000
    }
}

#[test]
fn memory_native_replay_retains_elapsed_collection_for_historical_deltas() {
    use sider::storage::memory::{Collection, Edit};
    use sider::storage::{Mutation, Value};
    let mut historical = member();
    historical.deadline = Some(1001);
    let image = Collection::new(descriptor())
        .unwrap()
        .apply_edits(&descriptor(), &[Edit::Upsert(historical)], b"c")
        .unwrap();
    let mut store = Store::with_clock(Arc::new(RecoveryClock));
    store
        .replay(&[Mutation::Put {
            key: Bytes::from_static(b"c"),
            value: Value::Memory(Arc::new(image)),
            expires_at_unix_ms: Some(1500),
        }])
        .unwrap();
    assert_eq!(
        store.len(),
        1,
        "an elapsed memory image must remain available to later guarded history"
    );
    assert_eq!(store.used_bytes(), 389 + member().logical_bytes());
}

#[tokio::test(start_paused = true)]
async fn memory_native_cleanup_is_bounded_and_watch_conflicts_at_member_deadline() {
    let mut store = Store::new();
    execute(&mut store, Operation::Create(descriptor()));
    for number in 0..66 {
        let mut record = member();
        record.id = Bytes::from(format!("{number:03}"));
        execute(
            &mut store,
            put(record, Ttl::After(10), SetCondition::Always),
        );
    }
    let watched = store.watch(vec![Bytes::from_static(b"c")]);
    tokio::time::advance(Duration::from_millis(10)).await;
    assert!(!store.watches_valid(&watched));
    let new_watch = store.watch(vec![Bytes::from_static(b"c")]);
    assert!(
        store.watches_valid(&new_watch),
        "members expired before registration are excluded from the deadline guard"
    );
    let prepared = store.prepare_expiration(1000);
    let resolved = store.resolved_memory_mutations(&prepared);
    assert!(
        matches!(&resolved[0], sider::persistence::memory_format::Mutation::Delta { edits, .. } if edits.len() == 64)
    );
    assert_eq!(store.apply(prepared), Reply::Integer(64));
    let snapshot = store.snapshot();
    let sider::storage::Mutation::Put {
        value: sider::storage::Value::Memory(collection),
        ..
    } = &snapshot[0]
    else {
        panic!("memory image");
    };
    assert_eq!(collection.retained_count(), 2);
    let prepared = store.prepare_expiration(64);
    assert_eq!(store.apply(prepared), Reply::Integer(2));
    assert_eq!(store.used_bytes(), 389);
    assert_eq!(store.len(), 1);
}

#[test]
fn memory_native_guarded_store_replay_is_atomic_and_historical_persist_restores_visibility() {
    use sider::persistence::memory_format::Mutation as DurableMutation;
    use sider::storage::memory::{Collection, Edit};
    use sider::storage::{Mutation, Value};
    let mut historical = member();
    historical.deadline = Some(1001);
    let image = Collection::new(descriptor())
        .unwrap()
        .apply_edits(&descriptor(), &[Edit::Upsert(historical)], b"c")
        .unwrap();
    let mut store = Store::with_clock(Arc::new(RecoveryClock));
    store
        .replay_memory(&[DurableMutation::Image {
            key: Bytes::from_static(b"c"),
            collection: image,
            key_deadline: Some(1500),
        }])
        .unwrap();
    let delta = DurableMutation::Delta {
        key: Bytes::from_static(b"c"),
        descriptor: descriptor(),
        edits: vec![Edit::Upsert(member())],
        key_deadline: None,
    };
    store.replay_memory(&[delta]).unwrap();
    assert!(matches!(
        execute(&mut store, Operation::Get { id: member().id }),
        Reply::Array(_)
    ));
    let before = store.snapshot();
    assert!(
        store
            .replay_memory(&[
                DurableMutation::Legacy(Mutation::Delete {
                    key: Bytes::from_static(b"c")
                }),
                DurableMutation::Delta {
                    key: Bytes::from_static(b"absent"),
                    descriptor: descriptor(),
                    edits: vec![],
                    key_deadline: None
                },
            ])
            .is_err()
    );
    assert_eq!(store.snapshot(), before);
    assert!(
        store
            .replay_memory(&[DurableMutation::Delta {
                key: Bytes::from_static(b"c"),
                descriptor: descriptor(),
                edits: vec![Edit::Remove(Bytes::from_static(b"absent"))],
                key_deadline: None
            }])
            .is_err()
    );
    assert_eq!(store.snapshot(), before);
    let snapshot = store.snapshot_memory();
    let mut destination = Store::new();
    destination.replay_memory(&snapshot).unwrap();
    assert_eq!(destination.snapshot(), before);
    assert!(matches!(
        &before[0],
        Mutation::Put {
            value: Value::Memory(_),
            ..
        }
    ));
    assert!(
        sider::persistence::format::encode(
            &sider::persistence::format::Record::Snapshot(before[0].clone()),
            sider::persistence::format::Limits::default()
        )
        .is_err(),
        "record1 must never serialize memory"
    );
}

#[test]
fn memory_native_snapshot_limit_prevents_acknowledging_an_unsnapshotable_delta() {
    let mut store = Store::new();
    execute(&mut store, Operation::Create(descriptor()));
    let mut existing = member();
    existing.content = Bytes::from(vec![0; 1024]);
    execute(
        &mut store,
        put(existing, Ttl::Persistent, SetCondition::Always),
    );
    let mut incoming = member();
    incoming.id = Bytes::from_static(b"other");
    let prepared = store.prepare_memory(
        Bytes::from_static(b"c"),
        put(incoming.clone(), Ttl::Persistent, SetCondition::Always),
    );
    let delta_limit = prepared.resolved().unwrap().encoded_bytes().unwrap() + 14;
    let before = store.snapshot();
    let prepared = store.prepare_memory_with_limit(
        Bytes::from_static(b"c"),
        put(incoming, Ttl::Persistent, SetCondition::Always),
        delta_limit,
    );
    assert_eq!(
        store.apply_memory(prepared),
        Reply::Error(ExecutionError::AofRecordLimit)
    );
    assert_eq!(store.snapshot(), before);
}

fn descriptor() -> Descriptor {
    Descriptor {
        dimension: 1,
        provider: Bytes::from_static(b"p"),
        model: Bytes::from_static(b"m"),
        revision: Bytes::from_static(b"r"),
        transform: Bytes::from_static(b"t"),
    }
}
fn member() -> Member {
    Member {
        id: Bytes::from_static(b"\xff{other}\0"),
        namespace: Bytes::new(),
        content: Bytes::from_static(b"\0\r\n\xff"),
        vector: Bytes::from_static(b"\0\0\0\x80"),
        deadline: None,
        metadata: BTreeMap::new(),
        provenance: BTreeMap::new(),
    }
}
fn put(member: Member, ttl: Ttl, condition: SetCondition) -> Operation {
    Operation::Put {
        descriptor: descriptor(),
        member: Box::new(member),
        ttl,
        condition,
    }
}
fn execute(store: &mut Store, operation: Operation) -> Reply {
    let prepared = store.prepare_memory(Bytes::from_static(b"c"), operation);
    store.apply_memory(prepared)
}

#[test]
fn memory_native_preparation_binary_replacement_conditions_and_generic_types() {
    let mut store = Store::new();
    assert_eq!(
        execute(&mut store, Operation::Create(descriptor())),
        Reply::Ok
    );
    let prepared = store.prepare_memory(
        Bytes::from_static(b"c"),
        put(member(), Ttl::Persistent, SetCondition::Always),
    );
    assert_eq!(store.used_bytes(), 389);
    store.apply_memory(prepared);
    assert_eq!(store.used_bytes(), 389 + member().logical_bytes());
    assert_eq!(
        execute(
            &mut store,
            put(member(), Ttl::Persistent, SetCondition::Missing)
        ),
        Reply::Integer(0)
    );
    let mut invalid = member();
    invalid.vector = Bytes::from(f32::NAN.to_le_bytes().to_vec());
    assert!(matches!(
        execute(
            &mut store,
            put(invalid, Ttl::Persistent, SetCondition::Missing)
        ),
        Reply::Error(_)
    ));
    assert_eq!(
        store.execute(Command::Get {
            key: Bytes::from_static(b"c")
        }),
        Reply::Error(ExecutionError::WrongType)
    );
    assert_eq!(
        store.execute(Command::MGet {
            keys: vec![Bytes::from_static(b"c")]
        }),
        Reply::Array(vec![Reply::Bulk(None)])
    );
    assert_eq!(
        execute(&mut store, Operation::Create(descriptor())),
        Reply::Ok
    );
    assert_eq!(
        execute(&mut store, Operation::Delete { id: member().id }),
        Reply::Integer(1)
    );
    assert_eq!(store.len(), 1);
    assert_eq!(store.used_bytes(), 389);
    assert_eq!(
        store.execute(Command::Set {
            key: Bytes::from_static(b"c"),
            value: Bytes::new()
        }),
        Reply::Ok
    );
    assert_eq!(store.used_bytes(), 129);
}

#[test]
fn memory_native_quota_failure_preserves_old_record_and_watch() {
    let mut store = Store::with_config(
        StoreConfig {
            max_dataset_bytes: 800,
        },
        Arc::new(SystemClock),
    )
    .unwrap();
    execute(&mut store, Operation::Create(descriptor()));
    execute(
        &mut store,
        put(member(), Ttl::Persistent, SetCondition::Always),
    );
    let watched = store.watch(vec![Bytes::from_static(b"c")]);
    let before = store.used_bytes();
    let mut large = member();
    large.content = Bytes::from(vec![0; 1000]);
    assert_eq!(
        execute(
            &mut store,
            put(large, Ttl::Persistent, SetCondition::Always)
        ),
        Reply::Error(ExecutionError::OutOfMemory)
    );
    assert_eq!(store.used_bytes(), before);
    assert!(store.watches_valid(&watched));
    execute(
        &mut store,
        put(member(), Ttl::Persistent, SetCondition::Always),
    );
    assert!(!store.watches_valid(&watched));
}

#[tokio::test(start_paused = true)]
async fn memory_native_owner_routes_collection_key_and_hides_exact_ttl_boundary() {
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let (handle, owners) = worker::channel_with_stores(
        4,
        Duration::from_secs(5),
        receiver,
        vec![Store::new(), Store::new()],
    )
    .unwrap();
    let owners: Vec<_> = owners
        .into_iter()
        .map(|owner| tokio::spawn(owner.run()))
        .collect();
    let key = Bytes::from_static(b"memory:{agent}:a");
    assert_eq!(
        handle
            .execute_memory(key.clone(), Operation::Create(descriptor()))
            .await
            .unwrap(),
        Reply::Ok
    );
    assert_eq!(
        handle
            .execute_memory(
                key.clone(),
                put(member(), Ttl::After(10), SetCondition::Always)
            )
            .await
            .unwrap(),
        Reply::Integer(1)
    );
    assert_eq!(
        handle
            .execute_memory(key.clone(), Operation::Pttl { id: member().id })
            .await
            .unwrap(),
        Reply::Integer(10)
    );
    tokio::time::advance(Duration::from_millis(10)).await;
    assert_eq!(
        handle
            .execute_memory(key.clone(), Operation::Get { id: member().id })
            .await
            .unwrap(),
        Reply::Bulk(None)
    );
    assert_eq!(
        handle
            .execute_memory(key.clone(), Operation::Pttl { id: member().id })
            .await
            .unwrap(),
        Reply::Integer(-2)
    );
    assert_eq!(
        handle
            .execute(Command::Exists { keys: vec![key] })
            .await
            .unwrap(),
        Reply::Integer(1)
    );
    shutdown.send(true).unwrap();
    for owner in owners {
        owner.await.unwrap();
    }
}
