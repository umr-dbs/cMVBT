use std::fmt::{Display, Formatter};
use cc_bplustree::crud_model::crud_api::{CRUDDispatcher, NodeVisits};
use cc_bplustree::crud_model::crud_operation::CRUDOperation;
use cc_bplustree::locking::locking_strategy::LockingStrategy;
use cc_bplustree::tree::bplus_tree::BPlusTree;
use crate::record_model::AtomicVersion;
use crate::record_model::version_info::Version;
use crate::tx_model::transaction::SnapShot;
use crate::utils::safe_cell::SafeCell;

type Key = SnapShot;
type Value = ();

const FAN_OUT: usize = 250;
const NUM_RECORDS: usize = 499;
const MIN_KEY: Version = Version::MIN;
const MAX_KEY: Version = Version::MAX;
const LOCKING_PROTOCOL: LockingStrategy = LockingStrategy::OLC;

fn inc_key(k: Key) -> Key {
    k.checked_add(1).unwrap_or(MAX_KEY)
}
fn dec_key(k: Key) -> Key {
    k.checked_sub(1).unwrap_or(MIN_KEY)
}

#[derive(Clone, Default)]
struct DummyValue;

impl Display for DummyValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "()")
    }
}
pub struct ActiveQueryIndex {
    tree: SafeCell<Box<BPlusTree<FAN_OUT, NUM_RECORDS, SnapShot, DummyValue>>>,
    min_snapshot: AtomicVersion
}

impl ActiveQueryIndex {
    pub fn new() -> Self {
        Self {
            tree: SafeCell::new(Box::new(BPlusTree::new_with(
                LOCKING_PROTOCOL,
                MIN_KEY,
                MAX_KEY,
                inc_key,
                dec_key))),
            min_snapshot: AtomicVersion::new(SnapShot::MIN),
        }
    }

    pub fn enqueue(&self, snapshot: SnapShot) {
        let _ = self.tree.as_ref().dispatch(CRUDOperation::Insert(snapshot, DummyValue));
    }

    pub fn dequeue(&self, snapshot: SnapShot) {
        let _ = self.tree.as_ref().dispatch(CRUDOperation::Delete(snapshot));
    }
}