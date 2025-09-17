use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_tree::mvbplus_tree::MVBPlusTree;
use crate::mv_tx_model::transaction::{AtomicTransaction, Transaction};
use crate::mv_tx_model::transaction_result::{AtomicTransactionResult, TransactionResult};
use crate::mv_tx_query::tx_api::TransactionDispatcher;

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Send + Sync + 'static
> TransactionDispatcher<'a, FAN_OUT, NUM_RECORDS, Key, Payload> for MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline]
    fn dispatch_transaction(&'a self, mut tx: Transaction<Key, Payload>) -> TransactionResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
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
    fn dispatch_atomic_transaction(&'a self, tx: AtomicTransaction<Key, Payload>)
        -> AtomicTransactionResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let snapshot = tx
            .snapshot()
            .map(|si| self.snapshot_for(si))
            .unwrap_or(self.snapshot_current());

        match unsafe { mem::transmute(snapshot.dispatch_crud(tx.crud)) } {
            CRUDOperationResult::Error =>
                Err(snapshot.snapshot()),
            crud_result =>
                Ok((snapshot.snapshot(), crud_result))
        }
    }
}