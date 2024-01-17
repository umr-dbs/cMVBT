use std::cmp::Ordering;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use itertools::Itertools;
use parking_lot::lock_api::RwLock;
use parking_lot::Mutex;
use crate::block::block::{Block, BlockSplit};
use crate::block::block_manager::BlockManager;
use crate::tree::root::Root;
use crate::page_model::{Attempts, BlockRef, Height, Level, ObjectCount};
use crate::page_model::internal_page::InternalPage;
use crate::page_model::node::Node;
use crate::record_model::version_info::Version;
use crate::test::{dec_key, inc_key, Key};
use crate::tree::locking_strategy::LockingStrategy;
use crate::tree::version_manager::VersionManager;
use crate::utils::interval::Interval;
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::{LatchType, OptCell, SmartCell, SmartFlavor, SmartGuard};
use crate::utils::un_cell::UnCell;

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


pub(crate) type RootItemGuard<
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
    pub(crate) root: UnCell<SmartRoot<FAN_OUT, NUM_RECORDS, Key>>,
    pub(crate) locking_strategy: LockingStrategy,
    pub(crate) block_manager: BlockManager<FAN_OUT, NUM_RECORDS, Key>,
    pub(crate) version_manager: VersionManager,
    pub(crate) inc_key: fn(Key) -> Key,
    pub(crate) dec_key: fn(Key) -> Key,
    pub(crate) min_key: Key,
    pub(crate) max_key: Key,
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
        fn inc_key(k: Key) -> Key {
            k.checked_add(1).unwrap_or(Key::MAX)
        }
        fn dec_key(k: Key) -> Key {
            k.checked_sub(1).unwrap_or(Key::MIN)
        }

        BPlusTree::new(inc_key, dec_key, u64::MIN, u64::MAX)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
> BPlusTree<FAN_OUT, NUM_RECORDS, u64>
{
    pub fn standard() -> Self {
        Self::make(LockingStrategy::MonoWriter, ClockType::FREE, inc_key, dec_key, u64::MIN, u64::MAX)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
> BPlusTree<FAN_OUT, NUM_RECORDS, Key>
{
    pub(crate) fn split(
        &self,
        block: &Block<FAN_OUT, NUM_RECORDS, Key>,
        fence: &Interval<Key>,
    ) -> BlockSplit<FAN_OUT, NUM_RECORDS, Key>
    {
        let active_count
            = block.active_count();

        let is_leaf
            = block.is_leaf();

        if active_count >= Block::<FAN_OUT, NUM_RECORDS, Key>::min_active() {
            // KEY_SPLIT
            match is_leaf {
                true => unsafe { // LeafPage
                    let (left, right) =
                        (self.block_manager
                             .new_empty_leaf()
                             .into_cell(self.locking_strategy.latch_type()),
                         self.block_manager
                             .new_empty_leaf()
                             .into_cell(self.locking_strategy.latch_type()));

                    let mut sorted_block = block
                        .as_records()
                        .iter()
                        .sorted_by_key(|r| r.key())
                        .collect_vec();

                    let middle = block.len() / 2;
                    let (first, second) = sorted_block
                        .split_at_mut(middle);

                    let fence_left = Interval::new(
                        fence.lower,
                        (self.dec_key)(second.get_unchecked(0).key));

                    if let Node::Leaf(leaf_page) = left.unsafe_borrow_mut().as_mut() {
                        first.sort_by_key(|r| r.version().insertion_version());
                        leaf_page.bulk_push_from_slice(first);
                    }

                    let fence_right = Interval::new(
                        second.get_unchecked(0).key,
                        fence.upper);

                    if let Node::Leaf(leaf_page) = right.unsafe_borrow_mut().as_mut() {
                        second.sort_by_key(|r| r.version().insertion_version());
                        leaf_page.bulk_push_from_slice(second)
                    }

                    BlockSplit::ByKey(fence_left, left, fence_right, right)
                }
                false => unsafe { // KEY_SPLIT InternalPage
                    let (left, right) =
                        (self.block_manager
                             .new_empty_index_block()
                             .into_cell(self.locking_strategy.latch_type()),
                         self.block_manager
                             .new_empty_index_block()
                             .into_cell(self.locking_strategy.latch_type()));

                    let (key_intervals, versions, pointers) = block
                        .keys_versions_pointers();

                    let mut filtered = key_intervals
                        .iter()
                        .zip(versions.iter())
                        .zip(pointers.iter())
                        // .filter(|((.., v), ..)|
                        //     InternalPage::<FAN_OUT, NUM_RECORDS, Key>::is_active(**v))
                        .sorted_by(|((k0, ..), ..), ((k1, ..), ..)|
                            match k0.lower.cmp(&k1.lower) {
                                Ordering::Equal => k0.upper.cmp(&k1.upper),
                                order => order
                            })
                        .collect_vec();

                    let mut first = vec![];
                    let mut second = vec![];
                    filtered.into_iter().for_each(|((i, v), p)| {
                        if first.is_empty() {
                            first.push(((i, v), p));
                        } else if first.get_unchecked(first.len() - 1).0.0.is_disjoint(i) {
                            first.push(((i, v), p));
                        } else {
                            second.push(((i, v), p));
                        }
                    });

                    debug_assert!(!first.is_empty() && !second.is_empty());
                    // let middle = filtered.len() / 2;
                    // let (first, second)
                    //     = filtered.split_at_mut(middle);

                    let fence_left = Interval::new(
                        fence.lower,
                        (self.dec_key)(second.get_unchecked(0).0.0.lower));

                    if let Node::Index(internal_page) = left.unsafe_borrow_mut().as_mut() {
                        first.sort_by_key(|((.., v), ..)| **v);
                        internal_page.bulk_push(first)
                    }

                    let fence_right = Interval::new(
                        second.get_unchecked(0).0.0.lower,
                        fence.upper);

                    if let Node::Index(internal_page) = right.unsafe_borrow_mut().as_mut() {
                        second.sort_by_key(|((.., v), ..)| **v);
                        internal_page.bulk_push(second)
                    }

                    BlockSplit::ByKey(fence_left, left, fence_right, right)
                }
            }
        } else {
            // VERSION SPLIT
            match is_leaf {
                true => { // LeafPage
                    let new_leaf = self.block_manager
                        .new_empty_leaf()
                        .into_cell(self.locking_strategy.latch_type());

                    let active_records = block
                        .as_records()
                        .iter()
                        .filter(|record| !record.version().is_deleted())
                        .collect_vec();

                    debug_assert!(!active_records.is_empty());
                    if let Node::Leaf(leaf_page) = new_leaf.unsafe_borrow_mut().as_mut() {
                        leaf_page.bulk_push(active_records);
                    }

                    BlockSplit::ByVersion(new_leaf)
                }
                false => { // VERSION SPLIT InternalPage
                    let new_internal_page = self.block_manager
                        .new_empty_index_block()
                        .into_cell(self.locking_strategy.latch_type());

                    let (key_intervals, versions, pointers) = block
                        .keys_versions_pointers();

                    let active_entries = key_intervals
                        .iter()
                        .zip(versions.iter())
                        .zip(pointers.iter())
                        .filter(|((.., v), ..)| InternalPage::<FAN_OUT, NUM_RECORDS, Key>::is_active(**v))
                        .collect_vec();

                    debug_assert!(!active_entries.is_empty());
                    if let Node::Index(internal_page) = new_internal_page.unsafe_borrow_mut().as_mut() {
                        internal_page.bulk_push(active_entries)
                    }

                    BlockSplit::ByVersion(new_internal_page)
                }
            }
        }
    }

    pub(crate) fn from(locking_strategy: &LockingStrategy,
                       block: BlockRef<FAN_OUT, NUM_RECORDS, Key>,
                       version: Version,
                       height: Height,
                       prev: Option<SmartRoot<FAN_OUT, NUM_RECORDS, Key>>,
    ) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key>
    {
        let root_item = RootItem {
            root: Root::new(
                block,
                version,
                height,
            ),
            prev,
        };

        SmartCell(Arc::new(match locking_strategy.latch_type() {
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
        }))
    }

    pub(crate) fn make_root_item(locking_strategy: &LockingStrategy,
                                 block_manager: &BlockManager<FAN_OUT, NUM_RECORDS, Key>,
                                 version: Version,
                                 prev: Option<SmartRoot<FAN_OUT, NUM_RECORDS, Key>>,
    ) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key>
    {
        let root_item = RootItem {
            root: Root::new(
                block_manager.new_empty_leaf().into_cell(locking_strategy.latch_type()),
                version,
                INIT_TREE_HEIGHT,
            ),
            prev,
        };

        SmartCell(Arc::new(match locking_strategy.latch_type() {
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
        }))
    }

    #[inline]
    fn make(locking_strategy: LockingStrategy,
            clock_type: ClockType,
            inc_key: fn(Key) -> Key,
            dec_key: fn(Key) -> Key,
            min_key: Key,
            max_key: Key,
    ) -> Self {
        let block_manager
            = BlockManager::new();

        let version_manager = match clock_type {
            ClockType::FREE => VersionManager::new_free(),
            ClockType::OPTIMISTIC => VersionManager::new_optimistic(),
            ClockType::SYNCED => VersionManager::new_locked()
        };


        Self {
            root: UnCell::new(
                Self::make_root_item(&locking_strategy, &block_manager, VersionManager::START_VERSION, None)),
            locking_strategy,
            block_manager,
            version_manager,
            inc_key,
            dec_key,
            min_key,
            max_key,
        }
    }


    #[inline]
    pub(crate) fn apply_for_root(
        &self,
        curr: &BlockRef<FAN_OUT, NUM_RECORDS, Key>,
        attempts: Attempts,
        height: Height,
    ) -> SmartGuard<'static, Block<{ FAN_OUT }, { NUM_RECORDS }, Key>>
    {
        self.apply_for_ref(
            curr,
            height,
            INIT_TREE_HEIGHT,
            attempts,
            Level::MAX)
    }

    #[inline]
    pub(crate) fn is_lock(&self, attempts: Attempts, height: Height) -> bool {
        match self.locking_strategy() {
            LockingStrategy::ORWC {
                write_level,
                write_attempt
            } if *write_level <= 1f32 &&
                (height <= INIT_TREE_HEIGHT || INIT_TREE_HEIGHT as f32 * write_level >= height as f32 || attempts > *write_attempt) =>
                true,
            LockingStrategy::LightweightHybridLock {
                write_level,
                write_attempt,
                ..
            } if *write_level <= 1f32 &&
                (height <= INIT_TREE_HEIGHT || INIT_TREE_HEIGHT as f32 * write_level >= height as f32 || attempts > *write_attempt) =>
                true,
            LockingStrategy::MonoWriter => false,
            LockingStrategy::LockCoupling => true,
            LockingStrategy::OLC => false,
            LockingStrategy::ORWC { .. } => false,
            LockingStrategy::LightweightHybridLock { .. } => false,
            LockingStrategy::HybridLocking { .. } => false
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
            } if curr.unsafe_borrow().as_ref().is_leaf() || *write_level <= 1f32 &&
                (height <= curr_level || curr_level >= max_level || curr_level as f32 * write_level >= height as f32 || attempts > *write_attempt) =>
                curr.borrow_mut(),
            LockingStrategy::LightweightHybridLock {
                write_level,
                write_attempt,
                ..
            } if curr.unsafe_borrow().as_ref().is_leaf() || *write_level <= 1f32 &&
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
    pub fn new_with(locking_strategy: LockingStrategy,
                    inc_key: fn(Key) -> Key,
                    dec_key: fn(Key) -> Key,
                    min_key: Key,
                    max_key: Key,
    ) -> Self {
        Self::make(locking_strategy, ClockType::SYNCED, inc_key, dec_key, min_key, max_key)
    }

    #[inline(always)]
    pub fn new(inc_key: fn(Key) -> Key,
               dec_key: fn(Key) -> Key,
               min_key: Key,
               max_key: Key) -> Self {
        Self::make(LockingStrategy::default(), ClockType::FREE, inc_key, dec_key, min_key, max_key)
    }

    #[inline(always)]
    pub const fn locking_strategy(&self) -> &LockingStrategy {
        &self.locking_strategy
    }
}