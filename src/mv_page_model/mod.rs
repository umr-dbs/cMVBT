use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use parking_lot::lock_api::Mutex;
use crate::mv_block::block::Block;
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::smart_cell::{OptCell, SmartCell, SmartFlavor};

pub mod internal_page;
pub mod leaf_page;
pub mod node;

pub type ObjectCount = u16;
pub type BlockID = u32;
// pub type AtomicBlockID = AtomicU32;
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
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Display for BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IsLeaf: {}, Len: {}", self.unsafe_borrow().is_leaf(), self.unsafe_borrow().len())
    }
}