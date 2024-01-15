use std::collections::VecDeque;
use std::hash::Hash;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::record_model::version_info::Version;

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum TXState {
    Waiting = 0,
    Running = 1,
    Completed = 2,
    Error = 3,
}

pub struct Transaction<Key: Ord + Copy + Hash + Default> {
    pub(crate) state: TXState,
    pub(crate) snapshot: Version,
    pub(crate) crud: VecDeque<CRUDOperation<Key>>,
    pub(crate) result: Vec<CRUDOperationResult<Key>>,
}

impl<Key: Ord + Copy + Hash + Default> Transaction<Key> {
    pub fn new(
        snapshot: Version,
        crud: VecDeque<CRUDOperation<Key>>)
        -> Self
    {
        Self {
            state: TXState::Waiting,
            result: Vec::with_capacity(crud.len()),
            snapshot,
            crud,
        }
    }

    pub const fn state(&self) -> TXState {
        self.state
    }

    pub const fn snapshot(&self) -> Version {
        self.snapshot
    }

    pub fn result(&self) -> &[CRUDOperationResult<Key>] {
        self.result.as_slice()
    }

    pub fn crud(&self) -> &VecDeque<CRUDOperation<Key>> {
        &self.crud
    }
}