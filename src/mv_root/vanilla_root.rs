use std::collections::LinkedList;
use crate::mv_root::root::Root;

pub(crate) type VanillaRootSt<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key, Payload> = LinkedList<Root<FAN_OUT, NUM_RECORDS, Key, Payload>>;