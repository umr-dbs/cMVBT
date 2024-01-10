use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use itertools::Itertools;
use parking_lot::lock_api::RwLock;
use parking_lot::Mutex;
use crate::block::block::{Block, BlockGuard, BlockSplit};
use crate::block::block_manager::BlockManager;
use crate::tree::root::Root;
use crate::page_model::{Attempts, BlockRef, Height, Level, ObjectCount};
use crate::page_model::node::Node;
use crate::tree::locking_strategy::LockingStrategy;
use crate::tree::version_manager::VersionManager;
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::{LatchType, OptCell, SmartCell, SmartFlavor, SmartGuard};

pub type LockLevel = ObjectCount;

pub const INIT_TREE_HEIGHT: Height = 1;
pub const MAX_TREE_HEIGHT: Height = Height::MAX;

pub enum ClockType {
    FREE,
    OPTIMISTIC,
    SYNCED,
}

#[derive(Default)]
pub(crate) struct RootItem<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> {
    pub(crate) root: Root<FAN_OUT, NUM_RECORDS, Key>,
    pub(crate) prev: Option<SmartCell<RootItem<FAN_OUT, NUM_RECORDS, Key>>>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> Deref for RootItem<FAN_OUT, NUM_RECORDS, Key> {
    type Target = Root<FAN_OUT, NUM_RECORDS, Key>;

    fn deref(&self) -> &Self::Target {
        &self.root
    }
}

pub(crate) type SmartRoot<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key
> = SmartCell<RootItem<FAN_OUT, NUM_RECORDS, Key>>;


pub(crate) type SmartRootGuard<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key
> = SmartGuard<'a, RootItem<FAN_OUT, NUM_RECORDS, Key>>;

pub struct BPlusTree<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> {
    pub(crate) root: SmartRoot<FAN_OUT, NUM_RECORDS, Key>,
    pub(crate) locking_strategy: LockingStrategy,
    pub(crate) block_manager: BlockManager<FAN_OUT, NUM_RECORDS, Key>,
    pub(crate) version_manager: VersionManager,
}

unsafe impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
> Sync for BPlusTree<FAN_OUT, NUM_RECORDS, Key> {}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> Send for BPlusTree<FAN_OUT, NUM_RECORDS, Key> {}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize
> Default for BPlusTree<FAN_OUT, NUM_RECORDS, u64> {
    fn default() -> Self {
        BPlusTree::new()
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
> BPlusTree<FAN_OUT, NUM_RECORDS, Key>
{
    pub(crate) fn split(&self, block: &Block<FAN_OUT, NUM_RECORDS, Key>)
                        -> BlockSplit<FAN_OUT, NUM_RECORDS, Key>
    {
        let active_count
            = block.active_count();

        if active_count >= Block::<FAN_OUT, NUM_RECORDS, Key>::min_active() {
            // KEY_SPLIT
            match block.is_leaf() {
                true => { // LeafPage
                    let (mut left, mut right) = (self.block_manager
                         .new_empty_leaf()
                         .into_cell(self.locking_strategy.latch_type()),
                     self.block_manager
                         .new_empty_leaf()
                         .into_cell(self.locking_strategy.latch_type()));

                    let sorted_block = block
                        .as_records()
                        .iter()
                        .sorted_by_key(|r| r.key())
                        .cloned()
                        .collect_vec();

                    let (first, second) = sorted_block
                        .as_slice()
                        .split_at(block.len() / 2);

                    if let Node::Leaf(leaf_page) = left.unsafe_borrow_mut().as_mut() {
                        leaf_page.bulk_push(first, 0);
                    }

                    if let Node::Leaf(leaf_page) = right.unsafe_borrow_mut().as_mut() {
                        leaf_page.bulk_push(second, 0);
                    }
                },
                false => {


                    let (left, right) = (self.block_manager
                         .new_empty_index_block()
                         .into_cell(self.locking_strategy.latch_type()),
                     self.block_manager
                         .new_empty_index_block()
                         .into_cell(self.locking_strategy.latch_type()));
                }
            };


        } else {
            // VERSION SPLIT
        }


        unimplemented!()
    }

    #[inline]
    fn make(locking_strategy: LockingStrategy, clock_type: ClockType) -> Self {
        let block_manager
            = BlockManager::new();

        let version_manager = match clock_type {
            ClockType::FREE => VersionManager::new_free(),
            ClockType::OPTIMISTIC => VersionManager::new_optimistic(),
            ClockType::SYNCED => VersionManager::new_locked()
        };

        let empty_node
            = block_manager.new_empty_leaf();

        let root_item = RootItem {
            root: Root::new(
                empty_node.into_cell(locking_strategy.latch_type()),
                VersionManager::START_VERSION,
                INIT_TREE_HEIGHT,
            ),
            prev: None,
        };

        let root = SmartCell(Arc::new(match locking_strategy.latch_type() {
            LatchType::Exclusive => SmartFlavor::ExclusiveCell(
                Mutex::new(()),
                SafeCell::new(root_item), ),
            LatchType::ReadersWriter => SmartFlavor::ReadersWriterCell(
                RwLock::new(()),
                SafeCell::new(root_item)),
            LatchType::Optimistic => SmartFlavor::OLCCell(
                OptCell::new(root_item)),
            LatchType::Hybrid => SmartFlavor::HybridCell(
                OptCell::new(root_item),
                RwLock::new(())),
            LatchType::LightWeightHybrid => SmartFlavor::LightWeightHybridCell(
                OptCell::new(root_item)),
            LatchType::None => SmartFlavor::FreeCell(
                SafeCell::new(root_item))
        }));

        Self {
            root,
            locking_strategy,
            block_manager,
            version_manager,
        }
    }

    #[inline]
    pub(crate) fn apply_for_ref(
        &self,
        curr: &BlockRef<FAN_OUT, NUM_RECORDS, Key>,
        height: Height,
        curr_level: Level,
        attempts: Attempts,
        max_level: Level,
    ) -> SmartGuard<'static, Block<{ FAN_OUT }, { NUM_RECORDS }, Key>>
    {
        match self.locking_strategy() {
            LockingStrategy::ORWC {
                write_level,
                write_attempt
            } if *write_level <= 1f32 &&
                (height <= curr_level || curr_level >= max_level || curr_level as f32 * write_level >= height as f32 || attempts > *write_attempt) =>
                curr.borrow_mut(),
            LockingStrategy::LightweightHybridLock {
                write_level,
                write_attempt,
                ..
            } if *write_level <= 1f32 &&
                (height <= curr_level || curr_level >= max_level || curr_level as f32 * write_level >= height as f32 || attempts > *write_attempt) =>
                curr.borrow_pin(),
            LockingStrategy::MonoWriter =>
                curr.borrow_free(),
            LockingStrategy::LockCoupling =>
                curr.borrow_mut(),
            LockingStrategy::OLC => curr.borrow_read(),
            LockingStrategy::ORWC { .. } => curr.borrow_read(),
            LockingStrategy::LightweightHybridLock { .. } => curr.borrow_read(),
            LockingStrategy::HybridLocking { .. } => curr.borrow_mut()
        }
    }
    #[inline(always)]
    pub fn new_with(locking_strategy: LockingStrategy) -> Self {
        Self::make(locking_strategy, ClockType::SYNCED)
    }

    #[inline(always)]
    pub fn new() -> Self {
        Self::make(LockingStrategy::default(), ClockType::SYNCED)
    }

    #[inline(always)]
    pub const fn locking_strategy(&self) -> &LockingStrategy {
        &self.locking_strategy
    }
}