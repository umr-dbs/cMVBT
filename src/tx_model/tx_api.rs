use std::fmt::Display;
use std::hash::Hash;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::tree::mvbplus_tree::MVBPlusTree;
use crate::tx_model::dispatch::TransactionResult;
use crate::tx_model::transaction::{SnapShot, Transaction};

pub trait TransactionDispatcher<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Ord + Copy + Hash + Default + Display> {
    fn dispatch_loop(
        &'a self,
        tx: Transaction<Key>
    ) -> TransactionResult<FAN_OUT, NUM_RECORDS, Key>;
}

pub struct IsolatedSnapShot<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Copy + Ord + Display>
(
    pub SnapShot,
    pub &'a MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>,
);

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Copy + Ord + Display>
IsolatedSnapShot<'a, FAN_OUT, NUM_RECORDS, Key>
{
    #[inline(always)]
    pub const fn snapshot(&self) -> SnapShot {
        self.0
    }

    #[inline(always)]
    pub const fn mv_tree(&self) -> &MVBPlusTree<FAN_OUT, NUM_RECORDS, Key> {
        self.1
    }
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Copy + Ord + 'static + Display>
CRUDDispatcher<'a, FAN_OUT, NUM_RECORDS, Key> for IsolatedSnapShot<'a, FAN_OUT, NUM_RECORDS, Key>
{
    #[inline]
    fn dispatch(&'a self, operation: CRUDOperation<Key>) -> CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key> {
        match operation {
            CRUDOperation::PointSi(key)=> self
                .mv_tree()
                .dispatch(CRUDOperation::Point(key, self.snapshot())),
            CRUDOperation::RangeSi(range) => self
                .mv_tree()
                .dispatch(CRUDOperation::Range(range, self.snapshot())),
            CRUDOperation::RangeIterSi(key) => self
                .mv_tree()
                .dispatch(CRUDOperation::RangeIter(key, self.snapshot())),
            _ => self
                .mv_tree()
                .dispatch(operation)
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Copy + Ord + Display> MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>
{
    pub fn snapshot(&self, snap_shot: SnapShot) -> IsolatedSnapShot<FAN_OUT, NUM_RECORDS, Key> {
        IsolatedSnapShot(snap_shot, self)
    }
}
