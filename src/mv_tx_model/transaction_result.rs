use std::fmt::Display;
use std::hash::Hash;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_record_model::version_info::Version;
use crate::mv_tx_model::transaction::Transaction;

pub type SnapShot = Version;

pub type TransactionResult<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key,
    Payload
> = Result<(SnapShot, Vec<CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload>>),
    (Transaction<Key, Payload>, Vec<CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload>>)>;

pub type AtomicTransactionResult<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key,
    Payload>
= Result<(SnapShot, CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload>), SnapShot>;

#[inline(always)]
pub const fn snapshot_from_atomic_tx_result<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
>(
    atomic_transaction_result: &AtomicTransactionResult<FAN_OUT, NUM_RECORDS, Key, Payload>) -> SnapShot
{
    match atomic_transaction_result {
        Ok((snapshot, ..)) |
        Err(snapshot) => *snapshot,
    }
}

#[inline(always)]
pub fn snapshot_from_tx_result<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
>(
    transaction_result: &TransactionResult<FAN_OUT, NUM_RECORDS, Key, Payload>) -> SnapShot
{
    match transaction_result {
        Ok((snapshot, ..)) => *snapshot,
        Err((tx, ..)) => tx.snapshot()
    }
}