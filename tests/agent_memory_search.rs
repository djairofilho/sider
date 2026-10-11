//! Independent exact-search oracle for the frozen memory contract.
#![forbid(unsafe_code)]

use bytes::Bytes;
use proptest::prelude::*;
use sider::storage::memory::{Collection, Descriptor, Edit, Member, Scalar};
use sider::storage::{Clock, Store};
use std::collections::BTreeMap;
use std::sync::Arc;

fn descriptor() -> Descriptor {
    Descriptor {
        dimension: 2,
        provider: Bytes::from_static(b"p"),
        model: Bytes::from_static(b"m"),
        revision: Bytes::from_static(b"r"),
        transform: Bytes::from_static(b"t"),
    }
}

fn vector(x: f32, y: f32) -> Bytes {
    Bytes::from([x.to_le_bytes(), y.to_le_bytes()].concat())
}

fn member(id: &[u8], namespace: &[u8], x: f32, y: f32) -> Member {
    Member {
        id: Bytes::copy_from_slice(id),
        namespace: Bytes::copy_from_slice(namespace),
        content: Bytes::new(),
        vector: vector(x, y),
        deadline: None,
        metadata: BTreeMap::new(),
        provenance: BTreeMap::new(),
    }
}

fn oracle(
    collection: &Collection,
    query: &[u8],
    k: usize,
    namespace: &[u8],
    filter: &BTreeMap<Bytes, Scalar>,
    unix_ms: i64,
) -> Vec<(Bytes, f64)> {
    let query: Vec<f64> = query
        .chunks_exact(4)
        .map(|coordinate| f64::from(f32::from_le_bytes(coordinate.try_into().unwrap())))
        .collect();
    let mut scored = Vec::new();
    for record in collection.members().values() {
        if record.namespace.as_ref() != namespace
            || record.deadline.is_some_and(|deadline| deadline <= unix_ms)
            || filter
                .iter()
                .any(|(field, wanted)| record.metadata.get(field) != Some(wanted))
        {
            continue;
        }
        let coordinates = record
            .vector
            .chunks_exact(4)
            .map(|coordinate| f64::from(f32::from_le_bytes(coordinate.try_into().unwrap())));
        let distance =
            query
                .iter()
                .copied()
                .zip(coordinates)
                .fold(0.0_f64, |sum, (query, stored)| {
                    let squared = (query - stored).powi(2);
                    sum + squared
                });
        scored.push((record.id.clone(), distance));
    }
    scored.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    scored.truncate(k);
    scored
}

#[test]
fn exact_search_matches_independent_oracle_with_ties_binary_ids_filters_and_expiry() {
    let mut collection = Collection::new(descriptor()).unwrap();
    let mut records = vec![
        member(b"a\0", b"n", 1.0, 0.0),
        member(b"a", b"n", -1.0, 0.0),
        member(b"\xff", b"n", 0.0, 1.0),
        member(b"z", b"other", 0.0, 0.0),
        member(b"elapsed", b"n", 0.0, 0.0),
        member(b"wrong-type", b"n", 0.0, 0.0),
    ];
    records[4].deadline = Some(1000);
    for record in &mut records[0..3] {
        record
            .metadata
            .insert(Bytes::from_static(b"rank"), Scalar::Integer(-7));
    }
    records[5].metadata.insert(
        Bytes::from_static(b"rank"),
        Scalar::Bytes(Bytes::from_static(b"-7")),
    );
    for record in records.into_iter().rev() {
        collection = collection
            .apply_edits(&descriptor(), &[Edit::Upsert(record)], b"c")
            .unwrap();
    }
    let mut filter = BTreeMap::new();
    filter.insert(Bytes::from_static(b"rank"), Scalar::Integer(-7));
    let query = vector(0.0, 0.0);
    for k in [1, 2, 3, 6, 128] {
        let actual = collection.search(&query, k, b"n", &filter, 1000).unwrap();
        let expected = oracle(&collection, &query, k, b"n", &filter, 1000);
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert_eq!(&actual.0, &expected.0);
            assert_eq!(actual.1.to_bits(), expected.1.to_bits());
        }
    }
    assert!(
        collection
            .search(&query, 3, b"missing", &filter, 1000)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        collection.search(&query, 3, b"n", &filter, 1000).unwrap()[0]
            .0
            .as_ref(),
        b"a".as_slice()
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn exact_search_matches_oracle_across_insertion_orders(
        rows in proptest::collection::vec((-100i16..100, -100i16..100, any::<bool>(), any::<bool>()), 0..64),
        k in 1usize..=128,
    ) {
        let make = |reverse: bool| {
            let mut collection = Collection::new(descriptor()).unwrap();
            let indices: Vec<_> = if reverse {
                (0..rows.len()).rev().collect()
            } else {
                (0..rows.len()).collect()
            };
            for index in indices {
                let (x, y, eligible, elapsed) = rows[index];
                let id = (index as u16).to_be_bytes();
                let mut record = member(&id, if eligible { b"n".as_slice() } else { b"other".as_slice() }, f32::from(x), f32::from(y));
                if elapsed {
                    record.deadline = Some(1000);
                }
                record.metadata.insert(Bytes::from_static(b"rank"), Scalar::Integer(if eligible { 7 } else { 8 }));
                collection = collection.apply_edits(&descriptor(), &[Edit::Upsert(record)], b"c").unwrap();
            }
            collection
        };
        let filter = BTreeMap::from([(Bytes::from_static(b"rank"), Scalar::Integer(7))]);
        let query = vector(3.0, -2.0);
        let forward = make(false);
        let reverse = make(true);
        let expected = oracle(&forward, &query, k, b"n", &filter, 1000);
        let actual = forward.search(&query, k, b"n", &filter, 1000).unwrap();
        let reordered = reverse.search(&query, k, b"n", &filter, 1000).unwrap();
        prop_assert_eq!(&actual, &expected);
        prop_assert_eq!(&reordered, &expected);
        for ((actual_id, actual_distance), (expected_id, expected_distance)) in actual.iter().zip(expected.iter()) {
            prop_assert_eq!(actual_id, expected_id);
            prop_assert_eq!(actual_distance.to_bits(), expected_distance.to_bits());
        }
    }
}

#[test]
fn exact_search_bounds_retained_scan_and_top_k() {
    let mut collection = Collection::new(descriptor()).unwrap();
    for number in (0..1024u16).rev() {
        let id = Bytes::from(number.to_be_bytes().to_vec());
        let mut record = member(&id, b"n", f32::from(number), 0.0);
        record.id = id;
        collection = collection
            .apply_edits(&descriptor(), &[Edit::Upsert(record)], b"c")
            .unwrap();
    }
    let query = vector(3.0, 0.0);
    let hits = collection
        .search(&query, 128, b"n", &BTreeMap::new(), 0)
        .unwrap();
    let expected = oracle(&collection, &query, 128, b"n", &BTreeMap::new(), 0);
    assert_eq!(hits.len(), 128);
    assert_eq!(hits, expected);
    assert_eq!(hits[0].0.as_ref(), 3u16.to_be_bytes().as_slice());
    assert_eq!(hits[0].1.to_bits(), 0.0_f64.to_bits());
    assert!(
        collection
            .search(&query, 0, b"n", &BTreeMap::new(), 0)
            .is_err()
    );
    assert!(
        collection
            .search(&query, 129, b"n", &BTreeMap::new(), 0)
            .is_err()
    );
    assert!(
        collection
            .search(&[0; 4], 1, b"n", &BTreeMap::new(), 0)
            .is_err()
    );
    assert!(
        collection
            .search(
                &[f32::INFINITY.to_le_bytes(), 0.0_f32.to_le_bytes()].concat(),
                1,
                b"n",
                &BTreeMap::new(),
                0,
            )
            .is_err()
    );
}

struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> tokio::time::Instant {
        tokio::time::Instant::now()
    }
    fn unix_millis(&self) -> i64 {
        2000
    }
}

fn apply(store: &mut Store, operation: sider::storage::memory::Operation) -> sider::command::Reply {
    let prepared = store.prepare_memory(Bytes::from_static(b"c"), operation);
    store.apply_memory(prepared)
}

fn search(store: &mut Store) -> sider::command::Reply {
    apply(
        store,
        sider::storage::memory::Operation::Search {
            descriptor: descriptor(),
            vector: vector(0.0, 0.0),
            k: 2,
            namespace: Bytes::from_static(b"n"),
            metadata: BTreeMap::new(),
        },
    )
}

#[test]
fn exact_search_tracks_replacement_delete_and_replayed_image() {
    use sider::command::{Reply, SetCondition};
    use sider::storage::memory::{Operation, Ttl};
    let mut store = Store::with_clock(Arc::new(FixedClock));
    assert_eq!(
        apply(&mut store, Operation::Create(descriptor())),
        Reply::Ok
    );
    let record = member(b"a", b"n", 2.0, 0.0);
    assert_eq!(
        apply(
            &mut store,
            Operation::Put {
                descriptor: descriptor(),
                member: Box::new(record),
                ttl: Ttl::Persistent,
                condition: SetCondition::Always,
            },
        ),
        Reply::Integer(1)
    );
    let first = search(&mut store);
    assert_eq!(
        first,
        Reply::Array(vec![Reply::Array(vec![
            Reply::Bulk(Some(Bytes::from_static(b"a"))),
            Reply::Bulk(Some(Bytes::copy_from_slice(&4.0_f64.to_le_bytes()))),
        ])])
    );
    let replacement = member(b"a", b"n", 1.0, 0.0);
    assert_eq!(
        apply(
            &mut store,
            Operation::Put {
                descriptor: descriptor(),
                member: Box::new(replacement),
                ttl: Ttl::After(1000),
                condition: SetCondition::Always,
            },
        ),
        Reply::Integer(1)
    );
    let changed = search(&mut store);
    assert_ne!(changed, first);
    let image = store.snapshot_memory();
    let mut replayed = Store::with_clock(Arc::new(FixedClock));
    replayed.replay_memory(&image).unwrap();
    assert_eq!(search(&mut replayed), changed);
    assert_eq!(
        apply(
            &mut store,
            Operation::Delete {
                id: Bytes::from_static(b"a"),
            }
        ),
        Reply::Integer(1)
    );
    assert_eq!(search(&mut store), Reply::Array(vec![]));
}

#[tokio::test(start_paused = true)]
async fn exact_search_does_not_resurrect_elapsed_member_when_wall_clock_stalls() {
    use sider::command::{Reply, SetCondition};
    use sider::storage::memory::{Operation, Ttl};
    let mut store = Store::with_clock(Arc::new(FixedClock));
    assert_eq!(
        apply(&mut store, Operation::Create(descriptor())),
        Reply::Ok
    );
    assert_eq!(
        apply(
            &mut store,
            Operation::Put {
                descriptor: descriptor(),
                member: Box::new(member(b"a", b"n", 0.0, 0.0)),
                ttl: Ttl::After(10),
                condition: SetCondition::Always,
            }
        ),
        Reply::Integer(1)
    );
    assert!(matches!(search(&mut store), Reply::Array(rows) if rows.len() == 1));
    tokio::time::advance(std::time::Duration::from_millis(10)).await;
    assert_eq!(search(&mut store), Reply::Array(vec![]));
}
