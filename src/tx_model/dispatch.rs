use std::hash::Hash;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::tree::bplus_tree::BPlusTree;
use crate::tx_model::transaction::{SnapShot, Transaction};
use crate::tx_model::tx_api::TransactionDispatcher;

pub type TransactionResult<Key>
= Result<(SnapShot, Vec<CRUDOperationResult<Key>>), (Transaction<Key>, Vec<CRUDOperationResult<Key>>)>;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static
> TransactionDispatcher<Key> for BPlusTree<FAN_OUT, NUM_RECORDS, Key> {
    fn dispatch_loop(&self, mut tx: Transaction<Key>) -> TransactionResult<Key> {
        let snapshot
            = self.snapshot(tx.snapshot());

        let mut result = Vec::with_capacity(tx.crud.len());
        while let Some(crud) = tx.crud.pop_front() {
            match snapshot.dispatch(crud) {
                CRUDOperationResult::Error => {
                    result.push(CRUDOperationResult::Error);
                    return Err((tx, result));
                }
                res => result.push(res),
            }
        }

        Ok((tx.snapshot(), result))
    }
}