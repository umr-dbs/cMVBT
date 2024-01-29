use std::hash::Hash;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::tree::bplus_tree::BPlusTree;
use crate::tx_model::transaction::{Transaction, TXState};

impl<Key: Ord + Copy + Hash + Default> Transaction<Key> {
    pub fn dispatch_loop(
        &mut self,
        index: &impl CRUDDispatcher<Key>
    ) {
        if let TXState::Waiting = self.state {
            self.state = TXState::Running;

            while let Some(crud) = self.crud.pop_front() {
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

    // NEXT: Dispatch with prev information for transaction context awareness
}