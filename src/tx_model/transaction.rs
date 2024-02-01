use std::collections::VecDeque;
use std::hash::Hash;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::record_model::version_info::Version;

pub type SnapShot = Version;

pub struct Transaction<Key: Ord + Copy + Hash + Default> {
    pub(crate) snapshot: SnapShot,
    pub(crate) crud: VecDeque<CRUDOperation<Key>>,
}

impl<Key: Ord + Copy + Hash + Default> Transaction<Key> {
    pub fn new(
        snapshot: Version,
        crud: VecDeque<CRUDOperation<Key>>)
        -> Self
    {
        Self {
            snapshot,
            crud,
        }
    }

    pub const fn snapshot(&self) -> Version {
        self.snapshot
    }

    pub fn crud(&self) -> &VecDeque<CRUDOperation<Key>> {
        &self.crud
    }
}