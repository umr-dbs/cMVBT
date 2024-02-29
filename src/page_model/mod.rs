use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use parking_lot::lock_api::{Mutex, RwLock};
use crate::block::block::Block;
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::{OptCell, SmartCell, SmartFlavor};

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
    Key
> = SmartCell<Block<FAN_OUT, NUM_RECORDS, Key>>;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    E: Default + Ord + Copy + Hash + Display
> Display for BlockRef<FAN_OUT, NUM_RECORDS, E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IsLeaf: {}, Len: {}", self.unsafe_borrow().is_leaf(), self.unsafe_borrow().len())
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Hash + Copy + Display
> Block<FAN_OUT, NUM_RECORDS, Key> {
    #[inline(always)]
    pub fn into_rw(self) -> SmartCell<Block<FAN_OUT, NUM_RECORDS, Key>> {
        SmartCell(Arc::new(SmartFlavor::ReadersWriterCell(
            Mutex::new(()),
            SafeCell::new(self))))
    }

    #[inline(always)]
    pub fn into_free(self) -> SmartCell<Block<FAN_OUT, NUM_RECORDS, Key>> {
        SmartCell(Arc::new(SmartFlavor::FreeCell(
            SafeCell::new(self))))
    }

    #[inline(always)]
    pub fn into_olc(self) -> SmartCell<Block<FAN_OUT, NUM_RECORDS, Key>> {
        SmartCell(Arc::new(SmartFlavor::OLCCell(
            OptCell::new(self))))
    }

    #[inline(always)]
    pub fn into_lightweight_hybrid(self) -> SmartCell<Block<FAN_OUT, NUM_RECORDS, Key>> {
        SmartCell(Arc::new(SmartFlavor::LightWeightHybridCell(
            OptCell::new(self))))
    }

    #[inline(always)]
    pub fn into_exclusive(self) -> SmartCell<Block<FAN_OUT, NUM_RECORDS, Key>> {
        SmartCell(Arc::new(SmartFlavor::ExclusiveCell(
            Mutex::new(()),
            SafeCell::new(self))))
    }

    #[inline(always)]
    pub fn into_hybrid(self) -> SmartCell<Block<FAN_OUT, NUM_RECORDS, Key>> {
        SmartCell(Arc::new(SmartFlavor::HybridCell(
            OptCell::new(self),
            RwLock::new(()))))
    }
}