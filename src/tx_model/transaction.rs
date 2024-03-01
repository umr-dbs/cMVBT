use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use crossbeam_channel::at;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::record_model::version_info::Version;

pub type SnapShot = Version;

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

#[inline(always)]
pub const fn snapshot_from_atomic_tx_result<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display>(
    atomic_transaction_result: &AtomicTransactionResult<FAN_OUT, NUM_RECORDS, Key>) -> SnapShot
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
    Key: Default + Ord + Copy + Hash + Display>(
    transaction_result: &TransactionResult<FAN_OUT, NUM_RECORDS, Key>) -> SnapShot
{
    match transaction_result {
        Ok((snapshot, ..)) => *snapshot,
        Err((tx, ..)) => tx.snapshot()
    }
}

#[derive(Clone)]
pub struct Transaction<Key: Ord + Copy + Hash + Default + Display> {
    pub(crate) snapshot: Option<SnapShot>,
    pub(crate) crud: VecDeque<CRUDOperation<Key>>,
}

#[derive(Clone)]
pub struct AtomicTransaction<Key: Ord + Copy + Hash + Default + Display> {
    pub(crate) snapshot: Option<SnapShot>,
    pub(crate) crud: CRUDOperation<Key>,
}

impl<Key: Ord + Copy + Hash + Default + Display> Into<Transaction<Key>> for AtomicTransaction<Key> {
    fn into(self) -> Transaction<Key> {
        self.into_transaction()
    }
}

impl<Key: Ord + Copy + Hash + Default + Display> AtomicTransaction<Key> {
    #[inline(always)]
    pub const fn new(snapshot: Option<SnapShot>, crud: CRUDOperation<Key>) -> Self {
        Self {
            snapshot,
            crud
        }
    }

    #[inline(always)]
    pub const fn new_latest_si(crud: CRUDOperation<Key>) -> Self {
        Self {
            snapshot: None,
            crud
        }
    }

    #[inline(always)]
    pub fn into_transaction(self) -> Transaction<Key> {
        Transaction::new(self.snapshot, VecDeque::from([self.crud]))
    }

    #[inline(always)]
    pub const fn from_crud(crud: CRUDOperation<Key>) -> Self {
        Self::new_latest_si(crud)
    }

    #[inline(always)]
    pub const fn snapshot(&self) -> Option<Version> {
        self.snapshot
    }
}

impl<Key: Ord + Copy + Hash + Default + Display> Into<AtomicTransaction<Key>> for CRUDOperation<Key> {
    fn into(self) -> AtomicTransaction<Key> {
        AtomicTransaction::from_crud(self)
    }
}

impl<Key: Ord + Copy + Hash + Default + Display> Transaction<Key> {
    #[inline(always)]
    pub const fn new(
        snapshot: Option<Version>,
        crud: VecDeque<CRUDOperation<Key>>)
        -> Self
    {
        Self {
            snapshot,
            crud,
        }
    }

    #[inline(always)]
    pub fn try_into_atomic_transaction(mut self) -> Result<AtomicTransaction<Key>, Self> {
        if self.crud.len() == 1 {
            Ok(AtomicTransaction::new(self.snapshot, self.crud.pop_front().unwrap()))
        }
        else {
            Err(self)
        }
    }

    #[inline(always)]
    pub fn snapshot(&self) -> Version {
        self.snapshot.unwrap_or(Version::MAX)
    }

    #[inline(always)]
    pub const fn crud(&self) -> &VecDeque<CRUDOperation<Key>> {
        &self.crud
    }
}