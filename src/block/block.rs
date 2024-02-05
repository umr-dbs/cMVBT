use std::fmt::Display;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::ptr::{addr_of, addr_of_mut};
use crate::block::block_manager::BlockManager;

use crate::page_model::BlockRef;
use crate::page_model::leaf_page::LeafPage;
use crate::page_model::node::Node;
use crate::utils::interval::Interval;
use crate::utils::smart_cell::{LatchType, SmartGuard};

#[repr(u8)]
pub enum BlockUnsafeDegree {
    Ok,
    Overflow,
    ActiveUnderflow
}

impl BlockUnsafeDegree {
    pub const fn is_ok(&self) -> bool {
        match self {
            Self::Ok => true,
            _ => false
        }
    }

    pub const fn is_length_overflow(&self) -> bool {
        match self {
            Self::Overflow => true,
            _ => false
        }
    }

    pub const fn is_active_underflow(&self) -> bool {
        match self {
            Self::ActiveUnderflow => true,
            _ => false
        }
    }
}

pub(crate) enum BlockSplit<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display>
{
    ByKey(Interval<Key>, BlockRef<FAN_OUT, NUM_RECORDS, Key>, Interval<Key>, BlockRef<FAN_OUT, NUM_RECORDS, Key>),
    ByVersion(BlockRef<FAN_OUT, NUM_RECORDS, Key>)
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display> BlockSplit<FAN_OUT, NUM_RECORDS, Key>
{

}

// #[repr(align(4096))]
// #[repr(packed)]
#[repr(align(4096))]
#[derive(Clone)]
pub struct Block<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
> {
    // pub block_id: BlockID,
    pub node_data: Node<FAN_OUT, NUM_RECORDS, Key>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
> Default for Block<FAN_OUT, NUM_RECORDS, Key>
{
    fn default() -> Self {
        Block {
            // block_id: 0,
            node_data: Node::Leaf(LeafPage::new()),
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display
> Block<FAN_OUT, NUM_RECORDS, Key>
{ // #[inline(always)]
    // pub const fn block_id(&self) -> BlockID {
    //     0
    // }

    #[inline(always)]
    pub fn unsafe_degree(&self) -> BlockUnsafeDegree {
        let active
            = self.active_count();

        if active > self.max_active_units() || self.len() >= self.max_units_safe() {
            BlockUnsafeDegree::Overflow
        }
        else if active < self.min_active_units() {
            BlockUnsafeDegree::ActiveUnderflow
        } else {
            BlockUnsafeDegree::Ok
        }
    }

    #[inline(always)]
    pub fn min_active_units(&self) -> usize {
        match self.is_leaf() {
            true => BlockManager::<FAN_OUT, NUM_RECORDS, Key>::min_active_records(),
            false => BlockManager::<FAN_OUT, NUM_RECORDS, Key>::min_active_keys()
        }
    }

    #[inline(always)]
    pub fn max_active_units(&self) -> usize {
        match self.is_leaf() {
            true => BlockManager::<FAN_OUT, NUM_RECORDS, Key>::min_active_records() * 4,
            false => BlockManager::<FAN_OUT, NUM_RECORDS, Key>::min_active_keys() * 4
        }
    }

    #[inline(always)]
    pub fn max_units(&self) -> usize {
        match self.is_leaf() {
            true => BlockManager::<FAN_OUT, NUM_RECORDS, Key>::max_records(),
            false => BlockManager::<FAN_OUT, NUM_RECORDS, Key>::max_keys()
        }
    }

    #[inline(always)]
    pub fn max_units_safe(&self) -> usize {
        match self.is_leaf() {
            true => BlockManager::<FAN_OUT, NUM_RECORDS, Key>::max_records_safe(),
            false => BlockManager::<FAN_OUT, NUM_RECORDS, Key>::max_keys_safe()
        }
    }

    #[inline(always)]
    pub(crate) fn active_count(&self) -> usize {
        match self.as_ref() {
            Node::Index(internal_page) =>
                internal_page.active_count(),
            Node::Leaf(leaf_page) =>
                leaf_page.active_count()
        }
    }

    #[inline(always)]
    pub(crate) fn active_dead(&self) -> (usize, usize) {
        match self.as_ref() {
            Node::Index(internal_page) =>
                internal_page.active_dead(),
            Node::Leaf(leaf_page) =>
                leaf_page.active_dead()
        }
    }

    #[inline(always)]
    pub fn into_cell(self, latch: LatchType) -> BlockRef<FAN_OUT, NUM_RECORDS, Key> {
        match latch {
            LatchType::Exclusive => self.into_exclusive(),
            LatchType::ReadersWriter => self.into_rw(),
            LatchType::Optimistic => self.into_olc(),
            LatchType::Hybrid => self.into_hybrid(),
            LatchType::None => self.into_free(),
            LatchType::LightWeightHybrid => self.into_lightweight_hybrid()
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display
> Deref for Block<FAN_OUT, NUM_RECORDS, Key> {
    type Target = Node<FAN_OUT, NUM_RECORDS, Key>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe {
            &*addr_of!(self.node_data) as &Self::Target
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display
> DerefMut for Block<FAN_OUT, NUM_RECORDS, Key> {
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
> AsRef<Node<FAN_OUT, NUM_RECORDS, Key>> for Block<FAN_OUT, NUM_RECORDS, Key> {
    #[inline(always)]
    fn as_ref(&self) -> &Node<FAN_OUT, NUM_RECORDS, Key> {
        unsafe {
            &*addr_of!(self.node_data) as _
        }
        // &self.node_data
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display
> AsMut<Node<FAN_OUT, NUM_RECORDS, Key>> for Block<FAN_OUT, NUM_RECORDS, Key> {
    #[inline(always)]
    fn as_mut(&mut self) -> &mut Node<FAN_OUT, NUM_RECORDS, Key> {
        unsafe {
            &mut *addr_of_mut!(self.node_data) as _
        }
    }
}

pub type BlockGuard<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key
> = SmartGuard<'a, Block<FAN_OUT, NUM_RECORDS, Key>>;

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
> BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key> {

}
