use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::atomic::AtomicU32;
use crate::mv_block::block::Block;
use crate::mv_sync::smart_cell::SmartCell;

pub mod internal_page;
pub mod leaf_page;
pub mod node;

pub type ObjectCount = u16;
pub type BlockID = u32;
pub type AtomicBlockID = AtomicU32;
pub type Level = u16;
pub type Height = Level;
pub type Attempts = u32;

pub type BlockRef<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key,
    Payload
> = SmartCell<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static
> Display for BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IsLeaf: {}, Len: {}", self.is_leaf(), self.len())
    }
}