//! Regression test for snapshot publication (EDBT'27 review, R3-D1): writer A takes version `v`
//! for a delete and pauses before modifying its leaf; writer B completes `v + 1` on another leaf.
//! A reader must not obtain a snapshot `>= v` until A has taken effect, otherwise A's delete would
//! change the result of a scan at that snapshot.

use std::sync::Barrier;

use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_root::index_root::RootIndexType;
use crate::mv_tree::mvbt::{MVBTSt, FAN_OUT, NUM_RECORDS};
use crate::mv_utils::interval::Interval;

type Tree = MVBTSt<FAN_OUT, NUM_RECORDS, u64, u64>;
const N: u64 = 2000;

fn scan(tree: &Tree, version: u64) -> usize {
    match tree.dispatch_crud(CRUDOperation::Range(Interval::new(0, u64::MAX), version)) {
        CRUDOperationResult::MatchedRecords(found) => found.len(),
        _ => panic!("range query failed"),
    }
}

#[test]
fn in_flight_write_is_excluded_from_reader_snapshots() {
    let tree = Tree::make_standard(RootIndexType::FrugalList);
    tree.enable_gc(false);

    // Bulk load in a thread that has ended before the experiment starts.
    std::thread::scope(|s| { s.spawn(|| {
        for k in 0..N {
            assert!(matches!(tree.dispatch_crud(CRUDOperation::Insert(k, k)), CRUDOperationResult::Inserted(..)));
        }
    }); });

    let taken = Barrier::new(2);
    let resume = Barrier::new(2);
    let (v_a, v_b, snap, before, after) = std::thread::scope(|s| {
        // Writer A: the CRUDOperation::Delete path of dispatch.rs, paused between the version
        // allocation and the leaf modification.
        let a = s.spawn(|| {
            let key = 0;
            let guard = tree.traversal_write_olc(key);
            let leaf = guard.deref_mut().as_leaf_page();
            let commit = tree.start_tx_commit();
            let version = commit.version();
            taken.wait();
            resume.wait(); // paused while B commits and the reader scans
            assert!(matches!(leaf.delete(key, version), Ok(Some(..))));
            leaf.commit_delta(-1, 1);
            commit.publish();
            version
        });
        taken.wait();

        // Writer B: an ordinary update on a different leaf, in a fresh thread.
        let v_b = s.spawn(|| match tree.dispatch_crud(CRUDOperation::Update(N - 1, 42)) {
            CRUDOperationResult::Updated(v) => v,
            _ => panic!("update failed"),
        }).join().unwrap();

        // Reader: obtain a snapshot and scan, let A finish, scan again at the same snapshot.
        let snap = tree.current_version_for_reader();
        let before = scan(&tree, snap);
        resume.wait();
        let v_a = a.join().unwrap();
        let after = scan(&tree, snap);
        (v_a, v_b, snap, before, after)
    });

    assert!(v_b > v_a);
    assert!(snap < v_a, "reader snapshot {snap} includes the in-flight version {v_a}");
    assert_eq!(before, after, "scan at snapshot {snap} is not repeatable");
    assert_eq!(before, N as usize);

    // Once A has taken effect, new readers see both writes.
    let fresh = tree.current_version_for_reader();
    assert!(fresh >= v_b);
    assert_eq!(scan(&tree, fresh), N as usize - 1);
}
