use std::fmt::{Display, Formatter};
use std::hash::Hash;
use crate::mv_block::block::Block;
use crate::mv_sync::smart_cell::SmartCell;

pub mod internal_page;
pub mod leaf_page;
pub mod node;
pub mod time_matcher;

pub type ObjectCount = u16;
pub type BlockID = u64;
// pub type AtomicBlockID = AtomicU32;
// pub type Level = u16;
pub type Height =  u16;
pub type Attempts = u32;

pub type BlockRef<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key,
    Payload
> = SmartCell<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Display for BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IsLeaf: {}, Len: {}", self.unsafe_borrow().is_leaf(), self.unsafe_borrow().len())
    }
}