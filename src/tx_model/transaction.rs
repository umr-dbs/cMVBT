use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::record_model::version_info::Version;

pub type SnapShot = Version;

pub struct Transaction<Key: Ord + Copy + Hash + Default + Display> {
    pub(crate) snapshot: Option<SnapShot>,
    pub(crate) crud: VecDeque<CRUDOperation<Key>>,
}

pub struct AtomicTransaction<Key: Ord + Copy + Hash + Default + Display> {
    pub(crate) snapshot: Option<SnapShot>,
    pub(crate) crud: CRUDOperation<Key>,
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
    pub fn snapshot(&self) -> Version {
        self.snapshot.unwrap_or(Version::MAX)
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
    pub fn snapshot(&self) -> Version {
        self.snapshot.unwrap_or(Version::MAX)
    }

    #[inline(always)]
    pub const fn crud(&self) -> &VecDeque<CRUDOperation<Key>> {
        &self.crud
    }
}