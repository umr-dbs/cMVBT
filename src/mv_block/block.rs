use std::fmt::Display;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::ptr::{addr_of, addr_of_mut};
use crate::mv_block::block_manager::BlockManager;

use crate::mv_page_model::BlockRef;
use crate::mv_page_model::node::{Node, PageType};
use crate::mv_utils::interval::Interval;
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::smart_cell::{LatchType, SmartGuard};

#[repr(u8)]
pub enum BlockUnsafeDegree {
    Ok,
    Overflow,
    ActiveUnderflow
}

// impl BlockUnsafeDegree {
//     pub const fn is_ok(&self) -> bool {
//         match self {
//             Self::Ok => true,
//             _ => false
//         }
//     }
// 
//     pub const fn is_length_overflow(&self) -> bool {
//         match self {
//             Self::Overflow => true,
//             _ => false
//         }
//     }
// 
//     pub const fn is_active_underflow(&self) -> bool {
//         match self {
//             Self::ActiveUnderflow => true,
//             _ => false
//         }
//     }
// }

pub(crate) enum BlockSplit<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    ByKey(Interval<Key>,
          BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
          Interval<Key>,
          BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>),
    ByVersion(BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default> BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload
> {

}

// #[repr(align(4096))]
// #[repr(packed)]
// #[repr(align(4096))]
// #[derive(Clone)]
pub struct Block<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    // pub block_id: BlockID,
    pub node_data: SafeCell<Node<FAN_OUT, NUM_RECORDS, Key, Payload>>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Clone for Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    fn clone(&self) -> Self {
        Self {
            node_data: SafeCell::new(self.node_data.as_ref().clone())
        }
    }
}
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Default for Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    fn default() -> Self {
        Block {
            // block_id: 0,
            node_data: SafeCell::new(Node::new_leaf()),
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static
> Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{ // #[inline(always)]
    // pub const fn block_id(&self) -> BlockID {
    //     0
    // }

    #[inline(always)]
    pub fn unsafe_degree(&self) -> BlockUnsafeDegree {
        let (active, dead)
            = self.active_dead_count();

        let (active, dead)
            = (active as usize,  dead as usize);

        let one_d
            = self.filling_20_percent();

        if active <= one_d {
            BlockUnsafeDegree::ActiveUnderflow
        }
        else {
            let overflow_units_count
                = self.overflow_units_count();

            let is_overflow
                = active + dead >= overflow_units_count;

            if is_overflow && active <= one_d * 2 {
                BlockUnsafeDegree::ActiveUnderflow
            } else if is_overflow {
                BlockUnsafeDegree::Overflow
            } else {
                BlockUnsafeDegree::Ok
            }
        }
    }

    #[inline(always)]
    pub fn unsafe_degree_root(&self) -> BlockUnsafeDegree {
        let (active, dead)
            = self.active_dead_count();

        let (active, dead)
            = (active as usize,  dead as usize);

        let is_leaf
            = self.is_leaf();

        if active == 1 && !is_leaf { // single child
            BlockUnsafeDegree::ActiveUnderflow
        }
        else if active + dead >= self.overflow_units_count() {
            BlockUnsafeDegree::Overflow
        }
        else {
            BlockUnsafeDegree::Ok
        }
    }

    // #[inline(always)]
    // pub fn min_active_units(&self) -> usize { // 20%
        // match self.is_leaf() {
        //     true => BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::min_active_records(),
        //     false => BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::min_active_keys()
        // }
    // }

    // #[inline(always)]
    // pub fn max_active_units(&self) -> usize { // 80%
    //     match self.is_leaf() {
    //         true => BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::min_active_records() * 2,
    //         false => BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::min_active_keys() * 2
    //     }
    // }

    #[inline(always)]
    pub fn max_units(&self) -> usize { // absolute units
        match self.is_leaf() {
            true => BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::max_records(),
            false => BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::max_keys()
        }
    }

    #[inline(always)]
    pub fn filling_40_percent(&self) -> usize { // 40%
        self.filling_20_percent() * 2
    }

    #[inline(always)]
    pub fn filling_80_percent(&self) -> usize { // 80%
        self.filling_40_percent() * 2
    }

    #[inline(always)]
    pub fn filling_20_percent(&self) -> usize { // 20%
        let max_units = self.max_units();
        (max_units as f32 / 5_f32).ceil() as usize
    }

    #[inline(always)]
    pub fn overflow_units_count(&self) -> usize { // trigger for overflow
        match self.is_leaf() {
            true => BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::overflow_records_count(),
            false => BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::overflow_keys_count()
        }
    }

    #[inline(always)]
    pub(crate) fn active_dead_count(&self) -> (u32, u32) {
        match self.as_page_ref() {
            PageType::IndexRef(internal_page) => internal_page.active_dead_count(),
            PageType::LeafRef(leaf_page) => leaf_page.active_dead_count(),
            _ => unreachable!()
        }
    }

    // #[inline(always)]
    // pub(crate) fn active_dead(&self) -> (usize, usize) {
    //     match self.as_ref() {
    //         Node::Index(internal_page) =>
    //             internal_page.active_dead(),
    //         Node::Leaf(leaf_page) =>
    //             leaf_page.active_dead()
    //     }
    // }

    #[inline(always)]
    pub fn into_cell(self, latch: LatchType) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match latch {
            LatchType::Optimistic => self.into_olc(),
            LatchType::None => self.into_free(),
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Deref for Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
    type Target = Node<FAN_OUT, NUM_RECORDS, Key, Payload>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe {
            &*addr_of!(self.node_data) as &Self::Target
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> DerefMut for Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            &mut *addr_of_mut!(self.node_data) as &mut Self::Target
        }
        // &mut self.node_data
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> AsRef<Node<FAN_OUT, NUM_RECORDS, Key, Payload>> for Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    fn as_ref(&self) -> &Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
        unsafe {
            &*addr_of!(self.node_data) as _
        }
        // &self.node_data
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> AsMut<Node<FAN_OUT, NUM_RECORDS, Key, Payload>> for Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    fn as_mut(&mut self) -> &mut Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
        unsafe {
            &mut *addr_of_mut!(self.node_data) as _
        }
    }
}

pub type BlockGuard<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key,
    Payload: Clone
> = SmartGuard<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>;

impl<    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {

}
