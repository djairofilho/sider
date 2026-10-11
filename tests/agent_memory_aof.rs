//! Isolated record-2 AOF writer/recovery/compaction integration.
use bytes::Bytes;
use sider::command::{Reply, SetCondition};
use sider::persistence::{self, AofConfig};
use sider::storage::{
    StoreConfig, SystemClock,
    memory::{Descriptor, Member, Operation, Ttl},
    worker,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

static NEXT_DIRECTORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sider-memory-aof-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_DIRECTORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn config(&self) -> AofConfig {
        AofConfig::new(self.0.clone()).with_memory_profile()
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        assert_eq!(self.0.parent(), Some(std::env::temp_dir().as_path()));
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn descriptor() -> Descriptor {
    Descriptor {
        dimension: 2,
        provider: Bytes::from_static(b"p"),
        model: Bytes::from_static(b"m"),
        revision: Bytes::from_static(b"r"),
        transform: Bytes::from_static(b"t"),
    }
}
fn member() -> Member {
    Member {
        id: Bytes::from_static(b"\xff\0"),
        namespace: Bytes::new(),
        content: Bytes::from_static(b"\0\r\n\xff"),
        vector: Bytes::from([(-0.0f32).to_le_bytes(), f32::from_bits(1).to_le_bytes()].concat()),
        deadline: None,
        metadata: BTreeMap::new(),
        provenance: BTreeMap::new(),
    }
}
fn put(record: Member, ttl: Ttl) -> Operation {
    Operation::Put {
        descriptor: descriptor(),
        member: Box::new(record),
        ttl,
        condition: SetCondition::Always,
    }
}
fn recover(directory: &Directory) -> persistence::Recovered {
    persistence::recover(
        directory.config(),
        StoreConfig::default(),
        Arc::new(SystemClock),
    )
    .unwrap()
}

#[test]
fn memory_aof_empty_profile_advertises_record2_before_first_memory_write() {
    let directory = Directory::new();
    let recovered = recover(&directory);
    assert_eq!(recovered.metadata.format_version, 4);
    drop(recovered);
    let bytes =
        std::fs::read(directory.path().join("generation-00000000000000000000.aof")).unwrap();
    let header = persistence::memory_format::read_header(bytes.as_slice()).unwrap();
    assert_eq!(header.sequence, 0);
}

#[tokio::test]
async fn memory_aof_owner_appends_guarded_delta_and_roundtrips_compaction() {
    let directory = Directory::new();
    let (store, aof, writer_task) = recover(&directory).start();
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let (handle, owner) =
        worker::channel_with_store(4, Duration::from_secs(5), receiver, store).unwrap();
    let owner_task = tokio::spawn(owner.with_aof(aof.clone(), 0).run());
    let key = Bytes::from_static(b"c");
    assert_eq!(
        handle
            .execute_memory(key.clone(), Operation::Create(descriptor()))
            .await
            .unwrap(),
        Reply::Ok
    );
    assert_eq!(
        handle
            .execute_memory(key.clone(), put(member(), Ttl::Persistent))
            .await
            .unwrap(),
        Reply::Integer(1)
    );
    assert_eq!(
        handle
            .execute_memory(key.clone(), Operation::Create(descriptor()))
            .await
            .unwrap(),
        Reply::Ok
    );
    assert_eq!(
        aof.flush().await.unwrap(),
        2,
        "identical CREATE is not a durable write"
    );
    for operation in [
        put(member(), Ttl::Persistent),
        Operation::Expire {
            id: member().id,
            milliseconds: 100_000,
        },
        Operation::Persist { id: member().id },
        Operation::Delete { id: member().id },
        put(member(), Ttl::Persistent),
    ] {
        assert_eq!(
            handle.execute_memory(key.clone(), operation).await.unwrap(),
            Reply::Integer(1)
        );
    }
    assert_eq!(aof.flush().await.unwrap(), 7);
    let bytes =
        std::fs::read(directory.path().join("generation-00000000000000000000.aof")).unwrap();
    let mut records = &bytes[persistence::memory_format::HEADER_BYTES..];
    assert!(
        matches!(persistence::memory_format::read_record(&mut records,persistence::format::Limits::default()).unwrap(),persistence::memory_format::Next::Record(record) if matches!(*record,persistence::memory_format::Record::Seal{..}))
    );
    for sequence in 1..=7 {
        let persistence::memory_format::Next::Record(record) =
            persistence::memory_format::read_record(
                &mut records,
                persistence::format::Limits::default(),
            )
            .unwrap()
        else {
            panic!("batch")
        };
        let persistence::memory_format::Record::Batch {
            sequence: actual,
            mutations,
            ..
        } = *record
        else {
            panic!("batch")
        };
        assert_eq!(actual, sequence);
        assert_eq!(mutations.len(), 1);
        if sequence == 1 {
            assert!(matches!(
                mutations[0],
                persistence::memory_format::Mutation::Image { .. }
            ));
        } else {
            assert!(
                matches!(&mutations[0],persistence::memory_format::Mutation::Delta {edits,..} if edits.len()==1),
                "routine member operation must retain its exact guarded edit"
            );
        }
    }
    let expected = handle.snapshot(Some(&aof)).await.unwrap().mutations;
    aof.compact(expected.clone()).await.unwrap();
    shutdown.send(true).unwrap();
    owner_task.await.unwrap();
    drop(handle);
    drop(aof);
    writer_task.await.unwrap().unwrap();
    let recovered = recover(&directory);
    assert_eq!(recovered.metadata.sequence, 7);
    assert_eq!(recovered.metadata.format_version, 4);
    assert_eq!(recovered.store.snapshot(), expected);
}

#[tokio::test]
async fn memory_aof_record_limit_rejects_before_append_and_keeps_worker_usable() {
    let directory = Directory::new();
    let mut config = directory.config();
    config.limits.max_record_bytes = 300;
    let recovered =
        persistence::recover(config, StoreConfig::default(), Arc::new(SystemClock)).unwrap();
    let (store, aof, writer) = recovered.start();
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let (handle, owner) =
        worker::channel_with_store(4, Duration::from_secs(5), receiver, store).unwrap();
    let task = tokio::spawn(owner.with_aof(aof.clone(), 0).run());
    assert_eq!(
        handle
            .execute_memory(Bytes::from_static(b"c"), Operation::Create(descriptor()))
            .await
            .unwrap(),
        Reply::Ok
    );
    let before = handle.snapshot(Some(&aof)).await.unwrap().mutations;
    let mut large = member();
    large.content = Bytes::from(vec![0xff; 400]);
    assert_eq!(
        handle
            .execute_memory(Bytes::from_static(b"c"), put(large, Ttl::Persistent))
            .await
            .unwrap(),
        Reply::Error(sider::command::ExecutionError::AofRecordLimit)
    );
    assert_eq!(aof.flush().await.unwrap(), 1);
    assert_eq!(handle.snapshot(Some(&aof)).await.unwrap().mutations, before);
    assert_eq!(
        handle
            .execute_memory(Bytes::from_static(b"c"), put(member(), Ttl::Persistent))
            .await
            .unwrap(),
        Reply::Integer(1)
    );
    shutdown.send(true).unwrap();
    task.await.unwrap();
    drop(handle);
    drop(aof);
    writer.await.unwrap().unwrap();
    assert_eq!(recover(&directory).metadata.sequence, 2);
}

fn initial(replication: Option<persistence::ReplicationMetadata>) -> Vec<u8> {
    use persistence::memory_format::{Header, Record};
    let mut bytes = persistence::memory_format::encode_header(Header {
        sequence: 0,
        layout: persistence::DurableLayout::default(),
        replication,
    })
    .unwrap();
    bytes.extend(
        persistence::memory_format::encode(
            &Record::Seal {
                sequence: 0,
                entries: 0,
                digest: 0,
            },
            persistence::format::Limits::default(),
        )
        .unwrap(),
    );
    bytes
}
fn image() -> persistence::memory_format::Mutation {
    persistence::memory_format::Mutation::Image {
        key: Bytes::from_static(b"c"),
        collection: sider::storage::memory::Collection::new(descriptor()).unwrap(),
        key_deadline: None,
    }
}

#[tokio::test]
async fn memory_aof_replica_install_publishes_complete_header5_generation() {
    use sider::storage::{Mutation, Store, Value, memory::Edit};

    let directory = Directory::new();
    let (initial, aof, writer) = recover(&directory).start();
    drop(initial);
    let metadata = persistence::ReplicationMetadata {
        role: persistence::Role::Replica,
        epoch: [0x51; 16],
    };
    assert!(matches!(
        aof.install_memory_snapshot(
            vec![delta(vec![Edit::Upsert(member())], None)],
            40,
            metadata
        )
        .await,
        Err(persistence::AofError::Sequence)
    ));
    assert_eq!(aof.flush().await.unwrap(), 0);
    assert!(
        !directory
            .path()
            .join("generation-00000000000000000001.aof")
            .exists()
    );
    let collection = sider::storage::memory::Collection::new(descriptor())
        .unwrap()
        .apply_edits(&descriptor(), &[Edit::Upsert(member())], b"c")
        .unwrap();
    let snapshot = vec![
        persistence::memory_format::Mutation::Image {
            key: Bytes::from_static(b"c"),
            collection,
            key_deadline: None,
        },
        persistence::memory_format::Mutation::Legacy(Mutation::Put {
            key: Bytes::from_static(b"z"),
            value: Value::from(Bytes::from_static(b"binary\0\xff")),
            expires_at_unix_ms: None,
        }),
    ];
    let mut expected = Store::with_config(StoreConfig::default(), Arc::new(SystemClock)).unwrap();
    for mutation in &snapshot {
        expected
            .replay_memory(std::slice::from_ref(mutation))
            .unwrap();
    }
    aof.install_memory_snapshot(snapshot, 40, metadata)
        .await
        .unwrap();
    assert_eq!(aof.flush().await.unwrap(), 40);
    drop(aof);
    writer.await.unwrap().unwrap();
    let recovered = recover(&directory);
    assert_eq!(recovered.metadata.format_version, 5);
    assert_eq!(recovered.metadata.replication, Some(metadata));
    assert_eq!(recovered.metadata.sequence, 40);
    assert_eq!(recovered.store.snapshot(), expected.snapshot());
}

#[tokio::test]
async fn memory_replica_install_and_guarded_delta_recover_exact_sequence() {
    use sider::replication::{Cursor, journal, state::Runtime};
    use sider::storage::{MutationOrigin, memory::Edit, replication::Context};

    let directory = Directory::new();
    let config = directory.config();
    let (store, aof, writer) = recover(&directory).start();
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let (database, owner) =
        worker::channel_with_store(4, Duration::from_secs(5), shutdown, store).unwrap();
    let runtime = Runtime::new(
        persistence::Role::Replica,
        Cursor {
            epoch: [0; 16],
            sequence: 0,
        },
    );
    let database = database.with_replication(runtime.clone());
    let owner_task = tokio::spawn(
        owner
            .with_aof(aof.clone(), 0)
            .with_replication(runtime.clone())
            .run(),
    );
    let context = Context {
        aof: aof.clone(),
        runtime: runtime.clone(),
        layout: config.layout,
        store_config: StoreConfig::default(),
        clock: Arc::new(SystemClock),
        journal_limits: journal::Limits {
            max_bytes: 4096,
            max_batches: 8,
            max_frame_bytes: 4096,
        },
        aof_limits: config.limits,
    };
    let collection = sider::storage::memory::Collection::new(descriptor())
        .unwrap()
        .apply_edits(&descriptor(), &[Edit::Upsert(member())], b"c")
        .unwrap();
    let cursor = Cursor {
        epoch: [0x61; 16],
        sequence: 17,
    };
    let generation = runtime.begin_session();
    database
        .install_memory_replica(
            context.clone(),
            generation,
            cursor,
            vec![persistence::memory_format::Mutation::Image {
                key: Bytes::from_static(b"c"),
                collection,
                key_deadline: None,
            }],
        )
        .await
        .unwrap();
    let image = database.snapshot(Some(&aof)).await.unwrap();
    assert_eq!(image.sequence, Some(17));
    assert_eq!(runtime.status().applied, cursor);
    assert_eq!(image.mutations.len(), 1);
    let mut changed = member();
    changed.content = Bytes::from_static(b"updated\0\xff");
    let next = Cursor {
        sequence: 18,
        ..cursor
    };
    let change = delta(vec![Edit::Upsert(changed.clone())], None);
    assert!(
        database
            .apply_memory_replica(
                context.clone(),
                generation,
                Cursor {
                    sequence: 19,
                    ..cursor
                },
                MutationOrigin::Client,
                vec![change.clone()],
            )
            .await
            .is_err()
    );
    let mut wrong = descriptor();
    wrong.model = Bytes::from_static(b"other");
    assert!(
        database
            .apply_memory_replica(
                context.clone(),
                generation,
                next,
                MutationOrigin::Client,
                vec![persistence::memory_format::Mutation::Delta {
                    key: Bytes::from_static(b"c"),
                    descriptor: wrong,
                    edits: vec![Edit::Upsert(changed)],
                    key_deadline: None,
                }],
            )
            .await
            .is_err()
    );
    assert_eq!(aof.flush().await.unwrap(), 17);
    database
        .apply_memory_replica(
            context.clone(),
            generation,
            next,
            MutationOrigin::Client,
            vec![change],
        )
        .await
        .unwrap();
    assert_eq!(runtime.status().applied, next);
    let image = database.snapshot(Some(&aof)).await.unwrap();
    assert_eq!(image.sequence, Some(18));
    assert!(
        database
            .apply_memory_replica(
                context.clone(),
                generation,
                next,
                MutationOrigin::Client,
                vec![delta(vec![Edit::Upsert(member())], None)],
            )
            .await
            .is_err()
    );
    assert_eq!(aof.flush().await.unwrap(), 18);
    stop.send_replace(true);
    owner_task.await.unwrap();
    drop(database);
    drop(context);
    drop(aof);
    writer.await.unwrap().unwrap();
    let recovered = recover(&directory);
    assert_eq!(recovered.metadata.sequence, 18);
    assert_eq!(recovered.metadata.format_version, 5);
    assert_eq!(
        recovered.metadata.replication.unwrap().role,
        persistence::Role::Replica
    );
    assert_eq!(recovered.store.snapshot(), image.mutations);
}

fn delta(
    edits: Vec<sider::storage::memory::Edit>,
    key_deadline: Option<i64>,
) -> persistence::memory_format::Mutation {
    persistence::memory_format::Mutation::Delta {
        key: Bytes::from_static(b"c"),
        descriptor: descriptor(),
        edits,
        key_deadline,
    }
}
fn batch(sequence: u64, mutation: persistence::memory_format::Mutation) -> Vec<u8> {
    persistence::memory_format::encode(
        &persistence::memory_format::Record::Batch {
            sequence,
            origin: sider::storage::MutationOrigin::Client,
            mutations: vec![mutation],
        },
        persistence::format::Limits::default(),
    )
    .unwrap()
}
fn seed(directory: &Directory, bytes: &[u8]) {
    std::fs::write(
        directory.path().join("generation-00000000000000000000.aof"),
        bytes,
    )
    .unwrap();
}

#[test]
fn memory_aof_repairs_only_incomplete_supported_batch_after_valid_seal() {
    let baseline = [initial(None), batch(1, image())].concat();
    let change = batch(
        2,
        delta(vec![sider::storage::memory::Edit::Upsert(member())], None),
    );
    for prefix in 0..=change.len() {
        let directory = Directory::new();
        seed(
            &directory,
            &[baseline.as_slice(), &change[..prefix]].concat(),
        );
        let recovered = recover(&directory);
        assert_eq!(
            recovered.metadata.sequence,
            if prefix == change.len() { 2 } else { 1 }
        );
        assert_eq!(
            recovered.metadata.valid_bytes as usize,
            baseline.len()
                + if prefix == change.len() {
                    change.len()
                } else {
                    0
                }
        );
        let snapshot = recovered.store.snapshot();
        let sider::storage::Value::Memory(collection) = (match &snapshot[0] {
            sider::storage::Mutation::Put { value, .. } => value,
            _ => unreachable!(),
        }) else {
            panic!("memory image required")
        };
        assert_eq!(
            collection.retained_count(),
            usize::from(prefix == change.len())
        );
        drop(recovered);
        assert_eq!(
            std::fs::metadata(directory.path().join("generation-00000000000000000000.aof"))
                .unwrap()
                .len() as usize,
            baseline.len()
                + if prefix == change.len() {
                    change.len()
                } else {
                    0
                }
        );
    }
}

#[test]
fn memory_aof_guard_and_sequence_corruption_never_trigger_repair_or_cleanup() {
    use sider::storage::memory::Edit;
    let mut wrong = descriptor();
    wrong.model = Bytes::from_static(b"other");
    let invalid = [
        batch(2, delta(vec![Edit::Remove(member().id)], None)),
        batch(
            2,
            persistence::memory_format::Mutation::Delta {
                key: Bytes::from_static(b"c"),
                descriptor: wrong,
                edits: vec![],
                key_deadline: None,
            },
        ),
        batch(3, delta(vec![Edit::Upsert(member())], None)),
    ];
    for tail in invalid {
        let directory = Directory::new();
        let bytes = [initial(None), batch(1, image()), tail, vec![0, 1]].concat();
        seed(&directory, &bytes);
        let residue = directory.path().join("replication-install-evidence.tmp");
        std::fs::write(&residue, b"evidence").unwrap();
        assert!(
            persistence::recover(
                directory.config(),
                StoreConfig::default(),
                Arc::new(SystemClock)
            )
            .is_err()
        );
        assert_eq!(
            std::fs::read(directory.path().join("generation-00000000000000000000.aof")).unwrap(),
            bytes
        );
        assert_eq!(std::fs::read(residue).unwrap(), b"evidence");
        assert!(!std::fs::read_dir(directory.path()).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("tail-")
        }));
    }
}

#[test]
fn memory_aof_profiles_refuse_incompatible_header_before_repair() {
    for memory_selected in [false, true] {
        let directory = Directory::new();
        let bytes = if memory_selected {
            let legacy = persistence::recover(
                AofConfig::new(directory.0.clone()),
                StoreConfig::default(),
                Arc::new(SystemClock),
            )
            .unwrap();
            drop(legacy);
            let mut bytes =
                std::fs::read(directory.path().join("generation-00000000000000000000.aof"))
                    .unwrap();
            bytes.extend([0, 1]);
            bytes
        } else {
            [initial(None), vec![0, 1]].concat()
        };
        seed(&directory, &bytes);
        let residue = directory.path().join("replication-install-evidence.tmp");
        std::fs::write(&residue, b"evidence").unwrap();
        let config = if memory_selected {
            directory.config()
        } else {
            AofConfig::new(directory.0.clone())
        };
        let error = persistence::recover(config, StoreConfig::default(), Arc::new(SystemClock))
            .err()
            .unwrap();
        if memory_selected {
            assert!(matches!(error, persistence::AofError::Migration(_)));
        } else {
            assert!(matches!(
                error,
                persistence::AofError::Format(persistence::format::FormatError::Version(4))
            ));
        }
        assert_eq!(
            std::fs::read(directory.path().join("generation-00000000000000000000.aof")).unwrap(),
            bytes
        );
        assert_eq!(std::fs::read(residue).unwrap(), b"evidence");
    }
}

#[test]
fn memory_aof_replay_keeps_elapsed_intermediate_member_and_key_for_later_persist() {
    use sider::storage::memory::Edit;
    let directory = Directory::new();
    let mut expired = member();
    expired.deadline = Some(1);
    seed(
        &directory,
        &[
            initial(None),
            batch(1, image()),
            batch(2, delta(vec![Edit::Upsert(expired)], Some(1))),
            batch(3, delta(vec![Edit::Upsert(member())], None)),
        ]
        .concat(),
    );
    let recovered = recover(&directory);
    assert_eq!(recovered.metadata.sequence, 3);
    let snapshot = recovered.store.snapshot();
    let sider::storage::Mutation::Put {
        value: sider::storage::Value::Memory(collection),
        expires_at_unix_ms,
        ..
    } = &snapshot[0]
    else {
        panic!("memory")
    };
    assert_eq!(*expires_at_unix_ms, None);
    assert_eq!(collection.members().get(&member().id), Some(&member()));
}

struct FailOnce {
    point: &'static str,
    armed: std::sync::atomic::AtomicBool,
}
impl persistence::FaultInjector for FailOnce {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if point == self.point && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            Err(std::io::Error::other("injected memory lifecycle fault"))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn memory_replica_install_faults_preserve_old_or_recover_published_generation() {
    for (point, published) in [
        ("replication_before_publish", false),
        ("replication_after_publish", true),
    ] {
        let directory = Directory::new();
        let faults = Arc::new(FailOnce {
            point,
            armed: std::sync::atomic::AtomicBool::new(true),
        });
        let recovered = persistence::recover_with_faults(
            directory.config(),
            StoreConfig::default(),
            Arc::new(SystemClock),
            faults,
        )
        .unwrap();
        let (initial, aof, writer) = recovered.start();
        drop(initial);
        let metadata = persistence::ReplicationMetadata {
            role: persistence::Role::Replica,
            epoch: [0x72; 16],
        };
        let result = aof
            .install_memory_snapshot(vec![image()], 7, metadata)
            .await;
        assert!(result.is_err(), "{point}");
        drop(aof);
        let _ = writer.await.unwrap();
        let recovered = recover(&directory);
        assert_eq!(
            recovered.metadata.sequence,
            if published { 7 } else { 0 },
            "{point}"
        );
        assert_eq!(
            recovered.metadata.replication,
            published.then_some(metadata),
            "{point}"
        );
        assert_eq!(recovered.store.snapshot().len(), usize::from(published));
    }
}

#[tokio::test]
async fn memory_aof_fault_boundaries_fail_closed_and_replay_only_complete_delta() {
    for point in [
        "before_append",
        "after_append",
        "before_sync",
        "after_sync",
        "before_reply",
        "memory_before_apply",
        "memory_after_apply",
        "memory_before_response",
        "memory_after_response",
    ] {
        let directory = Directory::new();
        let faults = Arc::new(FailOnce {
            point,
            armed: std::sync::atomic::AtomicBool::new(false),
        });
        let recovered = persistence::recover_with_faults(
            directory.config(),
            StoreConfig::default(),
            Arc::new(SystemClock),
            faults.clone(),
        )
        .unwrap();
        let (store, aof, writer) = recovered.start();
        let (_shutdown, receiver) = tokio::sync::watch::channel(false);
        let (handle, owner) =
            worker::channel_with_store(4, Duration::from_secs(5), receiver, store).unwrap();
        let task = tokio::spawn(owner.with_aof(aof.clone(), 0).run());
        assert_eq!(
            handle
                .execute_memory(Bytes::from_static(b"c"), Operation::Create(descriptor()))
                .await
                .unwrap(),
            Reply::Ok
        );
        faults
            .armed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let result = handle
            .execute_memory(Bytes::from_static(b"c"), put(member(), Ttl::Persistent))
            .await;
        if point == "memory_after_response" {
            assert_eq!(result.unwrap(), Reply::Integer(1));
        } else {
            assert!(result.is_err(), "{point}");
        }
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        assert!(
            handle
                .execute_memory(Bytes::from_static(b"c"), Operation::Info)
                .await
                .is_err()
        );
        drop(handle);
        drop(aof);
        let _ = writer.await.unwrap();
        let recovered = recover(&directory);
        assert_eq!(
            recovered.metadata.sequence,
            if point == "before_append" { 1 } else { 2 },
            "{point}"
        );
        let snapshot = recovered.store.snapshot();
        let sider::storage::Mutation::Put {
            value: sider::storage::Value::Memory(collection),
            ..
        } = &snapshot[0]
        else {
            panic!("memory")
        };
        assert_eq!(
            collection.retained_count(),
            usize::from(point != "before_append"),
            "{point}"
        );
    }
}

#[tokio::test]
async fn memory_aof_compaction_preserves_header5_role_epoch_and_record2() {
    let directory = Directory::new();
    let metadata = persistence::ReplicationMetadata {
        role: persistence::Role::Primary,
        epoch: [0x31; 16],
    };
    seed(
        &directory,
        &[
            initial(Some(metadata)),
            batch(1, image()),
            batch(
                2,
                delta(vec![sider::storage::memory::Edit::Upsert(member())], None),
            ),
        ]
        .concat(),
    );
    let recovered = recover(&directory);
    assert_eq!(recovered.metadata.format_version, 5);
    let (store, aof, writer) = recovered.start();
    aof.compact(store.snapshot()).await.unwrap();
    drop(aof);
    writer.await.unwrap().unwrap();
    let recovered = recover(&directory);
    assert_eq!(recovered.metadata.format_version, 5);
    assert_eq!(recovered.metadata.replication, Some(metadata));
    assert_eq!(recovered.metadata.sequence, 2);
    assert_eq!(recovered.store.snapshot(), store.snapshot());
}

struct PauseSnapshot {
    entered: tokio::sync::Semaphore,
    release: (std::sync::Mutex<bool>, std::sync::Condvar),
}
impl persistence::FaultInjector for PauseSnapshot {
    fn hit(&self, point: &'static str) -> std::io::Result<()> {
        if point == "compact_before_snapshot" {
            self.entered.add_permits(1);
            let (lock, ready) = &self.release;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn memory_aof_compaction_captures_concurrent_guarded_delta_and_aborts_at_bound() {
    for abort in [false, true] {
        let directory = Directory::new();
        let faults = Arc::new(PauseSnapshot {
            entered: tokio::sync::Semaphore::new(0),
            release: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        });
        let mut config = directory.config();
        if abort {
            config.max_delta_bytes = 1;
        }
        let recovered = persistence::recover_with_faults(
            config,
            StoreConfig::default(),
            Arc::new(SystemClock),
            faults.clone(),
        )
        .unwrap();
        let (mut store, aof, writer) = recovered.start();
        aof.append_memory(sider::storage::MutationOrigin::Client, vec![image()])
            .await
            .unwrap();
        store.replay_memory(&[image()]).unwrap();
        let complete = aof.begin_compaction(store.snapshot()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), faults.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let change = delta(vec![sider::storage::memory::Edit::Upsert(member())], None);
        assert_eq!(
            aof.append_memory(sider::storage::MutationOrigin::Client, vec![change.clone()])
                .await
                .unwrap(),
            2
        );
        store.replay_memory(&[change]).unwrap();
        *faults.release.0.lock().unwrap() = true;
        faults.release.1.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(5), complete)
            .await
            .unwrap()
            .unwrap();
        if abort {
            assert!(matches!(result, Err(persistence::AofError::DeltaLimit)));
        } else {
            result.unwrap();
        }
        drop(aof);
        writer.await.unwrap().unwrap();
        let recovered = recover(&directory);
        assert_eq!(recovered.metadata.sequence, 2);
        assert_eq!(recovered.store.snapshot(), store.snapshot());
        assert_eq!(
            directory
                .path()
                .join("generation-00000000000000000001.aof")
                .exists(),
            !abort
        );
    }
}
