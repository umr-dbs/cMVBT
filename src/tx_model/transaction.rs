use std::hash::Hash;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::record_model::version_info::Version;
use crate::tree::bplus_tree::BPlusTree;

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum TXState {
    Waiting = 0,
    Running = 1,
    Completed = 2,
    Error = 3,
}

pub struct Transaction<Key: Ord + Copy + Hash + Default> {
    state: TXState,
    snapshot: Version,
    crud: Box<[CRUDOperation<Key>]>,
    result: Vec<CRUDOperationResult<Key>>,
}

impl<Key: Ord + Copy + Hash + Default> Transaction<Key> {
    pub fn new(
        snapshot: Version,
        crud: Box<[CRUDOperation<Key>]>)
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

    pub fn crud(&self) -> &[CRUDOperation<Key>] {
        self.crud.as_ref()
    }

    pub fn execute<
        const FAN_OUT: usize,
        const NUM_RECORDS: usize>(
        &mut self,
        index: BPlusTree<FAN_OUT, NUM_RECORDS, Key>
    ) {
        if let TXState::Waiting = self.state {
            self.state = TXState::Running;

            for crud in self.crud.iter() {
                match index.dispatch(crud) {
                    CRUDOperationResult::Error => {
                        self.result.push(CRUDOperationResult::Error);
                        self.state = TXState::Error;
                        return
                    }
                    res => self.result.push(res),
                }
            }

            self.state = TXState::Completed
        }
    }
}