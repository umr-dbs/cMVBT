use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::ptr::{addr_of, addr_of_mut};

use crate::page_model::BlockRef;
use crate::page_model::leaf_page::LeafPage;
use crate::page_model::node::Node;
use crate::utils::smart_cell::{LatchType, SmartGuard};

// #[repr(align(4096))]
// #[repr(packed)]
#[repr(align(4096))]
pub struct Block<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
> {
    // pub block_id: BlockID,
    pub node_data: Node<FAN_OUT, NUM_RECORDS, Key>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
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
    Key: Default + Ord + Copy + Hash
> Block<FAN_OUT, NUM_RECORDS, Key>
{
    // #[inline(always)]
    // pub const fn block_id(&self) -> BlockID {
    //     0
    // }

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
    Key: Default + Ord + Copy + Hash
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
    Key: Default + Ord + Copy + Hash
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
    Key: Default + Ord + Copy + Hash,
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
    Key: Default + Ord + Copy + Hash
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
    Key: Default + Ord + Copy + Hash,
> BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key> {
}
