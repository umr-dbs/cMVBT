//! The GC must never recycle a node that a registered (or pinned) reader still needs.
//!
//! The snapshot versions come from the versions the operations committed at, and readers are pinned at exactly those:
//! the global commit watermark is shared by all trees of a process, so other tests would otherwise skew it.

use std::collections::HashSet;

use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_root::index_root::RootIndexType;
use crate::mv_tree::mvbt::{MVBTSt, FAN_OUT, NUM_RECORDS};
use crate::mv_utils::interval::Interval;

type Tree = MVBTSt<FAN_OUT, NUM_RECORDS, u64, u64>;

fn insert(tree: &Tree, key: u64) -> u64 {
    match tree.dispatch_crud(CRUDOperation::Insert(key, key)) {
        CRUDOperationResult::Inserted(version) => version,
        _ => panic!("insert {key} failed"),
    }
}

fn scan(tree: &Tree, version: u64) -> Vec<(u64, u64)> {
    match tree.dispatch_crud(CRUDOperation::Range(Interval::new(0, u64::MAX), version)) {
        CRUDOperationResult::MatchedRecords(found) => found.iter().map(|r| (r.key, r.payload)).collect(),
        _ => panic!("range query failed"),
    }
}

/// Sliding-window churn: `rounds` insertions of fresh keys, each followed by the deletion of the key `window` older.
fn churn(tree: &Tree, from: u64, rounds: u64, window: u64) {
    for k in from..from + rounds {
        assert!(matches!(tree.dispatch_crud(CRUDOperation::Insert(k, k)), CRUDOperationResult::Inserted(..)));
        assert!(matches!(tree.dispatch_crud(CRUDOperation::Delete(k - window)), CRUDOperationResult::Deleted(..)));
    }
}

#[test]
fn a_pinned_reader_keeps_its_snapshot_intact_while_the_gc_recycles_everything_else() {
    let _serial = crate::mv_sync::TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    const N: u64 = 6000;
    let tree = Tree::make_standard(RootIndexType::FrugalList);
    tree.enable_gc(false);
    let version = (0..N).map(|k| insert(&tree, k)).last().unwrap();
    let tracker = tree.tracker().unwrap();
    let pin = tracker.pin_reader(version);
    let before = scan(&tree, version);
    assert_eq!(before.len(), N as usize);

    // The sliding window replaces the whole data set several times: every node of the pinned snapshot dies.
    churn(&tree, N, 40_000, N);
    let (alloc_pinned, reuse_pinned) = tree.block_manager.alloc_reuse_counts();

    let mut after = scan(&tree, version);
    after.sort();
    let mut before = before;
    before.sort();
    assert_eq!(after, before, "the pinned snapshot changed: GC recycled a node it still needed");
    assert_eq!(after.iter().map(|(k, _)| *k).collect::<HashSet<_>>(), (0..N).collect::<HashSet<_>>());

    // Once the reader is gone, the same churn is served from the graveyard.
    tracker.unpin_reader(&pin);
    tree.block_manager.reset_alloc_reuse_counts();
    churn(&tree, N + 40_000, 40_000, N);
    let (alloc_free, reuse_free) = tree.block_manager.alloc_reuse_counts();
    assert!(reuse_free > 20 * alloc_free.max(1),
            "without readers nodes must be recycled: {alloc_free} allocated, {reuse_free} reused");
    assert!(alloc_pinned > alloc_free,
            "the pin must have held nodes back ({alloc_pinned} allocated while pinned vs {alloc_free} afterwards; \
             reused {reuse_pinned} meanwhile)");

    let newest = insert(&tree, 1_000_000);
    assert_eq!(scan(&tree, newest).len(), N as usize + 1, "the live window keeps its size");
}

#[test]
fn registered_readers_of_many_versions_all_see_their_own_snapshot() {
    let _serial = crate::mv_sync::TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let tree = Tree::make_standard(RootIndexType::FrugalList);
    tree.enable_gc(false);
    let mut version = (0..2000u64).map(|k| insert(&tree, k)).last().unwrap();
    let tracker = tree.tracker().unwrap();

    let mut readers = vec![];
    for round in 0..6u64 {
        let pin = tracker.pin_reader(version);
        let mut keys: Vec<u64> = scan(&tree, version).into_iter().map(|(k, _)| k).collect();
        keys.sort();
        readers.push((version, pin, keys));
        churn(&tree, 2000 + round * 5000, 5000, 2000);
        version = insert(&tree, 900_000 + round); // the version after this round's churn
        // (extra key outside of the window; it only shifts the expected sets of later readers by itself)
    }

    for (version, pin, keys) in &readers {
        let mut now: Vec<u64> = scan(&tree, *version).into_iter().map(|(k, _)| k).collect();
        now.sort();
        assert_eq!(&now, keys, "snapshot {version} changed");
        tracker.unpin_reader(pin);
    }
}

#[test]
fn lazy_historical_scan_is_exact_while_writers_reorganize_with_gc() {
    let _serial = crate::mv_sync::TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    const N: u64 = 6000;
    const WRITERS: u64 = 4;
    let tree = Tree::make_standard(RootIndexType::FrugalList);
    tree.enable_gc(false);
    let version = (0..N).map(|k| insert(&tree, k)).last().unwrap();

    let mut iter = match tree.dispatch_crud(CRUDOperation::RangeIter(Interval::new(0, u64::MAX), version)) {
        CRUDOperationResult::MatchedRecordIter(iter) => iter,
        _ => panic!("range iterator was not created"),
    };
    let mut seen: Vec<(u64, u64)> = iter.by_ref().take(17).map(|r| (r.key, r.payload)).collect();

    std::thread::scope(|scope| {
        for writer in 0..WRITERS {
            let tree = &tree;
            scope.spawn(move || {
                for key in (writer..N).step_by(WRITERS as usize) {
                    assert!(matches!(tree.dispatch_crud(CRUDOperation::Delete(key)), CRUDOperationResult::Deleted(..)));
                    let fresh = N + key;
                    assert!(matches!(tree.dispatch_crud(CRUDOperation::Insert(fresh, fresh)), CRUDOperationResult::Inserted(..)));
                }
            });
        }
    });

    seen.extend(iter.map(|r| (r.key, r.payload)));
    seen.sort_unstable();
    assert_eq!(seen, (0..N).map(|k| (k, k)).collect::<Vec<_>>(),
               "the historical iterator mixed versions, lost keys, duplicated keys, or returned a wrong payload");
    assert_eq!(scan(&tree, tree.current_version()).len(), N as usize, "the live window changed size");
}

#[test]
fn dropping_a_partial_lazy_scan_releases_its_snapshot() {
    let _serial = crate::mv_sync::TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let tree = Tree::make_standard(RootIndexType::FrugalList);
    tree.enable_gc(false);
    let version = (0..1000u64).map(|k| insert(&tree, k)).last().unwrap();
    let tracker = tree.tracker().unwrap();

    {
        let mut iter = match tree.dispatch_crud(CRUDOperation::RangeIter(Interval::new(0, u64::MAX), version)) {
            CRUDOperationResult::MatchedRecordIter(iter) => iter,
            _ => panic!("range iterator was not created"),
        };
        assert_eq!(tracker.newest_live_si(), Some(version));
        assert!(iter.next().is_some());
    }

    assert_eq!(tracker.newest_live_si(), None, "dropping an unfinished iterator leaked its reader registration");
}
