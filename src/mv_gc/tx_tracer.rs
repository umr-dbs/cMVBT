use std::fmt::{Display, Formatter};
use std::ops::Deref;
use CCBPlusTree::crud_model::crud_api::CRUDDispatcher;
use CCBPlusTree::crud_model::crud_operation::CRUDOperation;
use CCBPlusTree::crud_model::crud_operation_result::CRUDOperationResult;
use CCBPlusTree::tree::bplus_tree::BPlusTree;

use crate::mv_gc::db_tracker::AUX_PROTOCOL;
use crate::mv_test::{dec_key, inc_key};
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_tx_model::transaction_result::SnapShot;

const AUX_ATX_FAN_OUT: usize = 250;
const AUX_ATX_LEAF_SIZE: usize = 499;

type TxLiveKey = SnapShot;
type TxLiveValue = NullValue;

#[derive(Default, Clone)]
pub struct NullValue;

impl Display for NullValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "()")
    }
}

pub(crate) struct TxTrace
(SafeCell<BPlusTree<AUX_ATX_FAN_OUT, AUX_ATX_LEAF_SIZE, TxLiveKey, TxLiveValue>>);

impl Deref for TxTrace {
    type Target = BPlusTree<AUX_ATX_FAN_OUT, AUX_ATX_LEAF_SIZE, TxLiveKey, TxLiveValue>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl TxTrace {
    pub(crate) fn new() -> Self {
        Self(SafeCell::new(BPlusTree::new_with(
            AUX_PROTOCOL,
            SnapShot::MIN,
            SnapShot::MAX,
            inc_key,
            dec_key,
        )))
    }

    #[inline(always)]
    pub(crate) fn peek_min(&self) -> SnapShot {
        match self.dispatch(CRUDOperation::PeekMin) {
            (_, CRUDOperationResult::MatchedRecord(Some(r))) => r.key(),
            _ => SnapShot::MAX
        }
    }

    #[inline(always)]
    pub(crate) fn peek_max(&self) -> SnapShot {
        match self.dispatch(CRUDOperation::PeekMax) {
            (_, CRUDOperationResult::MatchedRecord(Some(r))) => r.key(),
            _ => SnapShot::MIN
        }
    }

    #[inline(always)]
    pub(crate) fn on_tx_start(&self, snapshot: SnapShot) -> bool {
        match self.dispatch(CRUDOperation::Insert(snapshot, NullValue)) {
            (.., CRUDOperationResult::Inserted(..)) => true,
            _ => false
        }
    }

    #[inline(always)]
    pub(crate) fn on_tx_completed(&self, snap_shot: SnapShot) {
        let _ = self.dispatch(CRUDOperation::Delete(snap_shot));
    }
}