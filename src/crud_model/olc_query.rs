use std::fmt::Display;
use std::hash::Hash;
use crate::record_model::unsafe_clone::UnsafeClone;
use crate::tree::bplus_tree::BPlusTree;


impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display
> BPlusTree<FAN_OUT, NUM_RECORDS, Key>
{

}