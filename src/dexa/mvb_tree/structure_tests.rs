//! Single-threaded model tests of the tree structure with a tiny fan-out, so that a few thousand
//! keys build trees of height >= 3 and the internal-node merge paths (combine, key-split, root
//! collapse) run. Deletions only shrink leaves with GC on (splits filter reclaimable records),
//! so these tests run with GC.

use std::collections::BTreeSet;

use crate::dexa::mvb_crud_model::crud_api::CRUDDispatcher;
use crate::dexa::mvb_crud_model::crud_operation::CRUDOperation;
use crate::dexa::mvb_crud_model::crud_operation_result::CRUDOperationResult;
use crate::dexa::mvb_locking::locking_strategy::LockingStrategy;
use crate::dexa::mvb_page_model::node::Node;
use crate::dexa::mvb_record_model::v_record_point::{VersionIndexType, VersionedRecordPoint};
use crate::dexa::mvb_tree::bplus_tree::MVBPlusTree;
use crate::dexa::mvb_utils::interval::Interval;
use crate::dexa::types::{dec_key, inc_key};

type Small = MVBPlusTree<12, 32, u64, u64>;

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }
}

fn visible_keys(tree: &Small) -> Vec<u64> {
    let version = tree.current_version_for_reader();
    match tree.dispatch(CRUDOperation::Range(Interval::new(u64::MIN, u64::MAX), version)).1 {
        CRUDOperationResult::MatchedRecords(records) => {
            let mut keys: Vec<u64> = records.iter().map(|r| r.key).collect();
            keys.sort_unstable();
            keys
        }
        _ => panic!("range scan failed"),
    }
}

/// Depth of the tree along its leftmost path (single-threaded, so no latches needed).
fn depth(tree: &Small) -> usize {
    let mut depth = 1;
    let mut node = tree.root.block.unsafe_borrow();
    while let Node::Index(page) = node.as_ref() {
        node = page.children()[0].unsafe_borrow();
        depth += 1;
    }
    depth
}

fn check(tree: &Small, model: &BTreeSet<u64>, removed: &[u64], round: usize) {
    let scanned = visible_keys(tree);
    let expected: Vec<u64> = model.iter().copied().collect();
    if scanned != expected {
        let missing: Vec<_> = model.iter().filter(|k| scanned.binary_search(k).is_err()).take(10).collect();
        let extra: Vec<_> = scanned.iter().filter(|k| !model.contains(k)).take(10).collect();
        panic!("round {round}: scan has {} keys, model {} (missing {missing:?}, unexpected {extra:?})",
               scanned.len(), model.len());
    }
    // Lookups by the latest version (no snapshot involved).
    for &k in model {
        assert!(matches!(tree.dispatch(CRUDOperation::Update(k, k)).1, CRUDOperationResult::Updated(..)),
                "round {round}: present key {k} not found by update");
    }
    for &k in removed.iter().rev().take(200) {
        assert!(!matches!(tree.dispatch(CRUDOperation::Update(k, k)).1, CRUDOperationResult::Updated(..)),
                "round {round}: deleted key {k} still updatable");
    }
}

/// Grows and shrinks the tree in waves, checking every key after each wave.
#[test]
fn internal_merges_keep_all_keys() {
    for index in [VersionIndexType::VANILLA, VersionIndexType::FrugalSkipList, VersionIndexType::SkipList] {
        for seed in 1..=6u64 {
            let tree = Small::new_with(LockingStrategy::OLC, u64::MIN, u64::MAX, inc_key, dec_key, index, true);
            let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ seed);
            let mut model = BTreeSet::new();
            let mut removed = vec![];
            let mut ever = std::collections::HashSet::new();
            let mut max_height = 0;

            for round in 0..60 {
                // Grow first, then shrink slowly: leaves must keep splitting while less than 40%
                // of their records are reclaimable, or the GC never filters (and never shrinks) them.
                let (inserts, deletes) = if round < 4 { (2000, 0) } else { (model.len() / 25, model.len() / 12) };
                for _ in 0..inserts {
                    // Random fresh keys spread the inserts (and so the splits, which filter
                    // reclaimable records with GC) over the leaves holding deleted keys.
                    let k = loop {
                        let k = rng.below(1 << 16);
                        if ever.insert(k) {
                            break k;
                        }
                    };
                    assert!(matches!(tree.dispatch(CRUDOperation::Insert(k, k)).1, CRUDOperationResult::Inserted(..)));
                    model.insert(k);
                }
                for _ in 0..deletes.min(model.len()) {
                    let nth = rng.below(model.len() as u64) as usize;
                    let k = *model.iter().nth(nth).unwrap();
                    assert!(matches!(tree.dispatch(CRUDOperation::Delete(k)).1, CRUDOperationResult::Deleted(..)),
                            "round {round}: delete of present key {k} failed");
                    model.remove(&k);
                    removed.push(k);
                }
                max_height = max_height.max(depth(&tree));
                check(&tree, &model, &removed, round);
                assert_eq!(tree.height() as usize, depth(&tree), "round {round}: stored root height");
            }
            assert!(max_height >= 3, "the test must build trees of height >= 3 (got {max_height})");
        }
    }
}

// Hand-built trees: root -> two internal nodes -> leaves of 16 records. With FAN_OUT = 12 an
// internal node underflows below 4 keys, overflows at 10, and a pair collapses into the root
// only if the union has fewer than 10 keys.

const LEAF_SPAN: u64 = 100;
const LEAF_RECORDS: u64 = 16;

fn leaf_cell(tree: &Small, leaf: u64, kind: VersionIndexType) -> crate::dexa::mvb_page_model::BlockRef<12, 32, u64, u64> {
    let block = tree.block_manager.new_empty_leaf();
    {
        let records = block.records_mut();
        for k in leaf * LEAF_SPAN..leaf * LEAF_SPAN + LEAF_RECORDS {
            records.push(VersionedRecordPoint::new(k, 0, k, kind));
        }
    }
    block.into_cell(tree.locking_strategy.latch_type())
}

/// An index block over leaves `first..first + n`; its separators are the leaves' first keys.
fn index_over_leaves(tree: &Small, first: u64, n: u64, kind: VersionIndexType) -> crate::dexa::mvb_block::block::Block<12, 32, u64, u64> {
    let block = tree.block_manager.new_empty_index_block();
    unsafe {
        for (i, leaf) in (first..first + n).enumerate() {
            block.children_mut().ptr.add(i).write(leaf_cell(tree, leaf, kind));
            if i > 0 {
                block.keys_mut().ptr.add(i - 1).write(leaf * LEAF_SPAN);
            }
        }
    }
    block.set_key_len(n as usize - 1);
    block
}

/// Builds root[A, B] with `a` leaves under A and `b` under B, inserts a key into the leaf
/// `target` (which makes the traversal fix the underflowing internal node first), then checks
/// every key and keeps operating on the tree.
fn run_shape(a: u64, b: u64, target: u64, kind: VersionIndexType) {
    let tree = Small::new_with(LockingStrategy::OLC, u64::MIN, u64::MAX, inc_key, dec_key, kind, true);
    let latch = tree.locking_strategy.latch_type();

    let node_a = index_over_leaves(&tree, 0, a, kind).into_cell(latch);
    let node_b = index_over_leaves(&tree, a, b, kind).into_cell(latch);
    let root = tree.block_manager.new_empty_index_block();
    unsafe {
        root.children_mut().ptr.write(node_a);
        root.children_mut().ptr.add(1).write(node_b);
        root.keys_mut().ptr.write(a * LEAF_SPAN);
    }
    root.set_key_len(1);
    tree.set_new_root(root, 3);

    let mut model: BTreeSet<u64> = (0..a + b)
        .flat_map(|leaf| leaf * LEAF_SPAN..leaf * LEAF_SPAN + LEAF_RECORDS)
        .collect();
    let shape = format!("a={a} b={b} target={target} kind={}", kind as u8);
    check(&tree, &model, &[], 0);

    let k = target * LEAF_SPAN + LEAF_SPAN / 2;
    assert!(matches!(tree.dispatch(CRUDOperation::Insert(k, k)).1, CRUDOperationResult::Inserted(..)), "{shape}");
    model.insert(k);
    check(&tree, &model, &[], 1);
    assert_eq!(tree.height() as usize, depth(&tree), "{shape}: stored root height");

    // Keep going on the restructured tree: fill every gap, then delete half of the keys.
    let mut removed = vec![];
    for leaf in 0..a + b {
        for k in leaf * LEAF_SPAN + LEAF_RECORDS..leaf * LEAF_SPAN + LEAF_SPAN / 2 {
            assert!(matches!(tree.dispatch(CRUDOperation::Insert(k, k)).1, CRUDOperationResult::Inserted(..)), "{shape}");
            model.insert(k);
        }
    }
    check(&tree, &model, &removed, 2);
    for k in model.iter().copied().step_by(2).collect::<Vec<_>>() {
        assert!(matches!(tree.dispatch(CRUDOperation::Delete(k)).1, CRUDOperationResult::Deleted(..)), "{shape}");
        model.remove(&k);
        removed.push(k);
    }
    check(&tree, &model, &removed, 3);
}

/// The pair does not fit into one node, so it is redistributed (internal key-split). The joined
/// keys need the separator between the two nodes, and the new separator belongs between them.
#[test]
fn internal_key_split_keeps_all_keys() {
    for kind in [VersionIndexType::VANILLA, VersionIndexType::SkipList] {
        run_shape(2, 10, 0, kind);  // underflowing left node
        run_shape(10, 2, 11, kind); // underflowing right node
        run_shape(9, 2, 10, kind);  // union of 10 keys: too large for a root collapse
    }
}

/// The pair fits into one node: the root collapses into it and the tree loses a level. The
/// separator between the two nodes has to be kept whichever of them underflowed.
#[test]
fn root_collapse_keeps_all_keys() {
    for kind in [VersionIndexType::VANILLA, VersionIndexType::SkipList] {
        run_shape(2, 5, 0, kind); // underflowing left node
        run_shape(5, 2, 6, kind); // underflowing right node
    }
}
