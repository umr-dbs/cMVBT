use std::hash::Hash;
use std::sync::Arc;
use parking_lot::lock_api::RwLock;
use parking_lot::Mutex;
use crate::block::block_manager::BlockManager;
use crate::tree::root::Root;
use crate::page_model::{Height, ObjectCount};
use crate::tree::locking_strategy::LockingStrategy;
use crate::tree::version_manager::VersionManager;
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::{LatchType, OptCell, SmartCell, SmartFlavor};

pub type LockLevel = ObjectCount;

pub const INIT_TREE_HEIGHT: Height = 1;
pub const MAX_TREE_HEIGHT: Height = Height::MAX;

#[derive(Default)]
pub(crate) struct RootItem<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> {
    root: Root<FAN_OUT, NUM_RECORDS, Key>,
    prev: Option<SmartCell<RootItem<FAN_OUT, NUM_RECORDS, Key>>>,
}

pub struct BPlusTree<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> {
    pub(crate) root: SmartCell<RootItem<FAN_OUT, NUM_RECORDS, Key>>,
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
    #[inline]
    fn make(locking_strategy: LockingStrategy) -> Self
    {
        let block_manager
            = BlockManager::new();

        let version_manager
            = VersionManager::new();

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

    #[inline(always)]
    pub fn new_with(locking_strategy: LockingStrategy) -> Self {
        Self::make(locking_strategy)
    }

    #[inline(always)]
    pub fn new() -> Self {
        Self::make(LockingStrategy::default())
    }

    #[inline(always)]
    pub const fn locking_strategy(&self) -> &LockingStrategy {
        &self.locking_strategy
    }
}