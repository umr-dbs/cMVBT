use std::hash::Hash;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::tree::bplus_tree::BPlusTree;
use crate::tx_model::dispatch::TransactionResult;
use crate::tx_model::transaction::{SnapShot, Transaction};

pub trait TransactionDispatcher<Key: Ord + Copy + Hash + Default> {
    fn dispatch_loop(
        &self,
        tx: Transaction<Key>
    ) -> TransactionResult<Key>;
}

pub struct IsolatedSnapShot<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Copy + Ord>
(
    SnapShot,
    &'a BPlusTree<FAN_OUT, NUM_RECORDS, Key>,
);

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Copy + Ord> IsolatedSnapShot<'a, FAN_OUT, NUM_RECORDS, Key>
{
    #[inline(always)]
    pub const fn snapshot(&self) -> SnapShot {
        self.0
    }

    #[inline(always)]
    pub const fn mv_tree(&self) -> &BPlusTree<FAN_OUT, NUM_RECORDS, Key> {
        self.1
    }
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Copy + Ord + 'static> CRUDDispatcher<Key> for IsolatedSnapShot<'a, FAN_OUT, NUM_RECORDS, Key>
{
    #[inline]
    fn dispatch(&self, operation: CRUDOperation<Key>) -> CRUDOperationResult<Key> {
        match operation {
            CRUDOperation::PointSi(key)=> self.1
                .dispatch(CRUDOperation::Point(key, self.0)),
            CRUDOperation::RangeSi(range) => self.1
                .dispatch(CRUDOperation::Range(range, self.0)),
            _ => self.1.dispatch(operation)
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Copy + Ord> BPlusTree<FAN_OUT, NUM_RECORDS, Key>
{
    pub fn snapshot(&self, snap_shot: SnapShot) -> IsolatedSnapShot<FAN_OUT, NUM_RECORDS, Key> {
        IsolatedSnapShot(snap_shot, self)
    }
}
