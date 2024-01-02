use std::hash::Hash;
use crate::page_model::node::{Node, NodeUnsafeDegree};
use crate::tree::bplus_tree::{BPlusTree, INIT_TREE_HEIGHT, LockLevel, MAX_TREE_HEIGHT};

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> BPlusTree<FAN_OUT, NUM_RECORDS, Key>
{
    #[inline(always)]
    pub(crate) fn has_overflow(&self, node: &Node<FAN_OUT, NUM_RECORDS, Key>) -> bool {
        match node.is_leaf() {
            true => node.is_overflow(self.block_manager.allocation_leaf()),
            false => node.is_overflow(self.block_manager.allocation_directory())
        }
    }

    fn has_underflow(&self, node: &Node<FAN_OUT, NUM_RECORDS, Key>) -> bool {
        match node.is_leaf() {
            true => node.is_underflow(self.block_manager.allocation_leaf()),
            false => node.is_underflow(self.block_manager.allocation_directory())
        }
    }

    fn unsafe_degree_of(&self, node: &Node<FAN_OUT, NUM_RECORDS, Key>) -> NodeUnsafeDegree {
        match node.is_leaf() {
            true => node.unsafe_degree(self.block_manager.allocation_leaf()),
            false => node.unsafe_degree(self.block_manager.allocation_directory()),
        }
    }

}