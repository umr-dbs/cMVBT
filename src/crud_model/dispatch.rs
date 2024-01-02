use std::hash::Hash;
use std::fmt::Display;
use crate::crud_model::crud_api::{CRUDDispatcher, NodeVisits};
use crate::page_model::node::Node;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::tree::bplus_tree::BPlusTree;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display,
> CRUDDispatcher<Key> for BPlusTree<FAN_OUT, NUM_RECORDS, Key>
{
    #[inline]
    fn dispatch(&self, crud_operation: CRUDOperation<Key>)
                -> (NodeVisits, CRUDOperationResult<Key>) {

        unimplemented!()
    }
}