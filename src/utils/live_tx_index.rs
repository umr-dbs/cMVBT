use std::fmt::{Display, Formatter};
use cc_bplustree::crud_model::crud_api::CRUDDispatcher;
use cc_bplustree::crud_model::crud_operation::CRUDOperation;
use cc_bplustree::locking::locking_strategy::LockingStrategy::OLC;
use cc_bplustree::tree::bplus_tree::BPlusTree;
use crate::test::{dec_key, inc_key};
use crate::tx_model::transaction::SnapShot;
use crate::utils::safe_cell::SafeCell;

const AUX_ATX_FAN_OUT: usize = 250;
const AUX_ATX_LEAF_SIZE: usize = 499;

#[derive(Default, Clone)]
struct NullValue;

impl Display for crate::tx_model::tx_manager::NullValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "()")
    }
}

pub struct LiveTxIndex(SafeCell<BPlusTree<AUX_ATX_FAN_OUT, AUX_ATX_LEAF_SIZE, SnapShot, NullValue>>);

impl LiveTxIndex {
    pub fn new() -> Self {
        Self(SafeCell::new(BPlusTree::new_with(
            OLC,
            SnapShot::MIN,
            SnapShot::MAX,
            inc_key,
            dec_key,
        )))
    }

    pub fn register_tx(&self, snapshot: SnapShot) {
        self.0.as_ref().dispatch(CRUDOperation::Insert(snapshot, NullValue));
    }
}