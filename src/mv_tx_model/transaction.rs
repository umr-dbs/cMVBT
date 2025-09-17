use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_record_model::version_info::Version;
use crate::mv_tx_model::transaction_result::SnapShot;

#[derive(Clone)]
pub struct Transaction<Key: Ord + Copy + Hash + Default + Display, Payload: Clone> {
    pub snapshot: Option<SnapShot>,
    pub crud: VecDeque<CRUDOperation<Key, Payload>>,
}

#[derive(Clone)]
pub struct AtomicTransaction<Key: Ord + Copy + Hash + Default + Display, Payload: Clone> {
    pub snapshot: Option<SnapShot>,
    pub crud: CRUDOperation<Key, Payload>,
}

impl<Key: Ord + Copy + Hash + Default + Display, Payload: Clone> Into<Transaction<Key, Payload>> for AtomicTransaction<Key, Payload> {
    fn into(self) -> Transaction<Key, Payload> {
        self.into_transaction()
    }
}

impl<Key: Ord + Copy + Hash + Default + Display, Payload: Clone> AtomicTransaction<Key, Payload> {
    #[inline(always)]
    pub const fn new(snapshot: Option<SnapShot>, crud: CRUDOperation<Key, Payload>) -> Self {
        Self {
            snapshot,
            crud
        }
    }

    #[inline(always)]
    pub const fn new_latest_si(crud: CRUDOperation<Key, Payload>) -> Self {
        Self {
            snapshot: None,
            crud
        }
    }

    #[inline(always)]
    pub fn into_transaction(self) -> Transaction<Key, Payload> {
        Transaction::new(self.snapshot, VecDeque::from([self.crud]))
    }

    #[inline(always)]
    pub const fn from_crud(crud: CRUDOperation<Key, Payload>) -> Self {
        Self::new_latest_si(crud)
    }

    #[inline(always)]
    pub const fn snapshot(&self) -> Option<Version> {
        self.snapshot
    }
}

impl<Key: Ord + Copy + Hash + Default + Display, Payload: Clone>
Into<AtomicTransaction<Key, Payload>> for CRUDOperation<Key, Payload> {
    fn into(self) -> AtomicTransaction<Key, Payload> {
        AtomicTransaction::from_crud(self)
    }
}

impl<Key: Ord + Copy + Hash + Default + Display, Payload: Clone> Transaction<Key, Payload> {
    #[inline(always)]
    pub const fn new(
        snapshot: Option<Version>,
        crud: VecDeque<CRUDOperation<Key, Payload>>)
        -> Self
    {
        Self {
            snapshot,
            crud,
        }
    }

    #[inline(always)]
    pub fn try_into_atomic_transaction(mut self) -> Result<AtomicTransaction<Key, Payload>, Self> {
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
    pub const fn crud(&self) -> &VecDeque<CRUDOperation<Key, Payload>> {
        &self.crud
    }
}