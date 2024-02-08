use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::tree::mvbplus_tree::MVBPlusTree;
use crate::tx_model::transaction::{AtomicTransaction, SnapShot, Transaction};
use crate::tx_model::tx_api::TransactionDispatcher;

pub type TransactionResult<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize, Key>
= Result<(SnapShot, Vec<CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key>>),
    (Transaction<Key>, Vec<CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key>>)>;

pub type AtomicTransactionResult<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize, Key>
= Result<(SnapShot, CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key>), SnapShot>;

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static + Display
> TransactionDispatcher<'a, FAN_OUT, NUM_RECORDS, Key> for MVBPlusTree<FAN_OUT, NUM_RECORDS, Key> {
    #[inline]
    fn dispatch_transaction(&'a self, mut tx: Transaction<Key>) -> TransactionResult<'a, FAN_OUT, NUM_RECORDS, Key> {
        let snapshot
            = self.snapshot_for(tx.snapshot());

        let mut result = Vec::with_capacity(tx.crud.len());
        while let Some(crud) = tx.crud.pop_front() {
            match unsafe { mem::transmute(snapshot.dispatch_crud(crud)) } {
                CRUDOperationResult::Error => {
                    result.push(CRUDOperationResult::Error);

                    return Err((tx, result));
                }
                res => result.push(res),
            }
        }

        Ok((tx.snapshot(), result))
    }

    #[inline(always)]
    fn dispatch_atomic_transaction(&'a self, tx: AtomicTransaction<Key>)
        -> AtomicTransactionResult<'a, FAN_OUT, NUM_RECORDS, Key>
    {
        let snapshot = tx.snapshot();
        match unsafe { mem::transmute(self.snapshot_for(snapshot).dispatch_crud(tx.crud)) } {
            CRUDOperationResult::Error =>
                Err(snapshot),
            crud_result =>
                Ok((snapshot, crud_result))
        }
    }
}