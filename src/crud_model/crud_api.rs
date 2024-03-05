use std::fmt::Display;
use std::hash::Hash;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;

pub type NodeVisits = usize;
pub trait CRUDDispatcher<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    fn dispatch_crud(&'a self,
                     operation: CRUDOperation<Key, Payload>
    ) -> CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload>;
}