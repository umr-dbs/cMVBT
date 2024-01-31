use std::hash::Hash;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::tx_model::dispatch::TransactionResult;
use crate::tx_model::transaction::Transaction;

pub trait TransactionDispatcher<Key: Ord + Copy + Hash + Default> {
    fn dispatch_loop(
        &self,
        tx: Transaction<Key>
    ) -> TransactionResult<Key>;
}