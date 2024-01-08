use std::hash::Hash;
use std::fmt::Display;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::tree::bplus_tree::BPlusTree;

const WRITE: bool = true;
const READ: bool = !WRITE;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static,
> CRUDDispatcher<Key> for BPlusTree<FAN_OUT, NUM_RECORDS, Key>
{
    #[inline]
    fn dispatch(&self, crud: &CRUDOperation<Key>) -> CRUDOperationResult<Key> {
        match crud.is_write() {
            WRITE => {
                let lookup_root
                    = self.retrieve_root_latest();

                unimplemented!()
            }
            READ => match crud {
                CRUDOperation::Range(range, version) =>
                    Self::key_range_read_from_root(
                        self.retrieve_root_for(*version),
                        range,
                        *version),
                CRUDOperation::Point(key, version) =>
                    Self::key_point_read_from_root(
                        self.retrieve_root_for(*version),
                        *key,
                        *version),
                _ => CRUDOperationResult::Error
            }
        }
    }
}