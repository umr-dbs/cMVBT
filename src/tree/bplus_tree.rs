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
use crate::page_model::internal_page::InternalPage;
use crate::page_model::leaf_page::LeafPage;
use crate::page_model::node::Node;
use crate::record_model::version_info::Version;
use crate::test::{dec_key, inc_key, Key};
use crate::tree::locking_strategy::{LockingStrategy, orwc};
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

#[derive(Default, Clone)]
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

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> RootItem<FAN_OUT, NUM_RECORDS, Key> {
    pub(crate) fn deep_clone(&self, latch_type: LatchType) -> Self {
        Self {
            root: Root {
                block: self.root.block.unsafe_borrow().clone().into_cell(latch_type),
                version: self.version,
                height: self.height,
            },
            prev: self.prev.clone(),
        }
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
        Self::standard()
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
> BPlusTree<FAN_OUT, NUM_RECORDS, u64>
{
    #[inline]
    fn make_standard(locking_strategy: LockingStrategy, clock_type: ClockType) -> Self {
        fn inc_key(k: u64) -> u64 {
            k.checked_add(1).unwrap_or(u64::MAX)
        }

        fn dec_key(k: u64) -> u64 {
            k.checked_sub(1).unwrap_or(u64::MIN)
        }

        Self::make(locking_strategy, clock_type, inc_key, dec_key, u64::MIN, u64::MAX)
    }

    pub fn standard() -> Self {
        Self::make_standard(LockingStrategy::MonoWriter, ClockType::FREE)
    }

    pub fn orwc() -> Self {
        Self::make_standard(orwc(), ClockType::SYNCED)
    }

    pub fn orwc_optimistic_clock() -> Self {
        Self::make_standard(orwc(), ClockType::OPTIMISTIC)
    }

    pub fn lc() -> Self {
        Self::make_standard(LockingStrategy::LockCoupling, ClockType::SYNCED)
    }

    pub fn lc_optimistic_clock() -> Self {
        Self::make_standard(LockingStrategy::LockCoupling, ClockType::OPTIMISTIC)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static,
> BPlusTree<FAN_OUT, NUM_RECORDS, Key>
{
    pub(crate) fn merge(
        &self,
        mufasa: &Block<FAN_OUT, NUM_RECORDS, Key>,
        simba: &Block<FAN_OUT, NUM_RECORDS, Key>,
        simba_index: usize,
    ) -> Result<(usize, Interval<Key>, BlockRef<FAN_OUT, NUM_RECORDS, Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key>), ()>
    {
        let mufasa_internal_page
            = mufasa.as_internal_page_ref();

        let is_simba_leaf
            = simba.is_leaf();

        let simba_fence
            = mufasa_internal_page.get_key(simba_index);

        let max_len
            = simba.max_units_safe();

        let simba_active_count
            = simba.active_count();

        let mut all_merge_candidates = mufasa_internal_page
            .children()
            .iter()
            .enumerate()
            .zip(mufasa_internal_page.versions())
            .zip(mufasa_internal_page.keys())
            .filter(|(((index, ..), ..), ..)|
                *index != simba_index)
            .filter(|((.., version), ..)|
                InternalPage::<FAN_OUT, NUM_RECORDS, Key>::is_active(**version))
            .sorted_by_key(|(.., fence)| fence.lower())
            .map(|(((index, bro), ..), fence)|
                (index, bro.borrow_mut(), bro, bro.unsafe_borrow().active_count(), fence))
            .collect_vec();

        let mut merge_candidates = all_merge_candidates
            .iter()
            .enumerate()
            .filter(|(vec_index, (.., active_count, _))| *active_count + simba_active_count <= max_len)
            .map(|(vec_index, (.., fence))| (vec_index, *fence))
            .collect_vec();

        let mut candidate = move || match merge_candidates
            .binary_search_by_key(&simba_fence.lower, |(.., fence)| fence.lower)
        {
            Ok(found) => all_merge_candidates.remove(merge_candidates.get(found).unwrap().0),
            Err(next_closest) if next_closest < merge_candidates.len() =>
                all_merge_candidates.remove(merge_candidates.get(next_closest).unwrap().0),
            _ => unreachable!()
        };

        let (candidate_index,
            candidate_guard,
            candidate_block,
            candidate_active_count,
            candidate_fence) = candidate();

        let combined_block = match is_simba_leaf {
            false => {
                let mut combined_block = self.block_manager
                    .new_empty_index_block()
                    .into_cell(self.locking_strategy.latch_type());

                let (keys, versions, pointers)
                    = simba.as_internal_page_ref().keys_versions_pointers();

                let (c_keys, c_versions, c_pointers) = candidate_guard
                    .deref()
                    .unwrap()
                    .as_internal_page_ref()
                    .keys_versions_pointers();

                let shadow_copy = keys
                    .iter()
                    .zip(versions.iter())
                    .zip(pointers.iter())
                    .filter(|((.., version), ..)|
                        InternalPage::<FAN_OUT, NUM_RECORDS, Key>::is_active(**version))
                    .merge_by(
                        c_keys.iter()
                            .zip(c_versions.iter())
                            .zip(c_pointers.iter()),
                        |(((.., v0), ..)), (((.., v1), ..))| v0 <= v1)
                    .into_iter()
                    .collect_vec();

                combined_block
                    .unsafe_borrow_mut()
                    .as_internal_page()
                    .bulk_push(shadow_copy);

                combined_block
            }
            true => {
                let mut combined_block = self.block_manager
                    .new_empty_leaf()
                    .into_cell(self.locking_strategy.latch_type());

                combined_block
                    .unsafe_borrow_mut()
                    .as_leaf_page()
                    .bulk_push(simba
                        .as_records()
                        .iter()
                        .filter(|r| !r.version().is_deleted())
                        .merge_by(candidate_guard
                                      .deref()
                                      .unwrap()
                                      .as_records()
                                      .iter()
                                      .filter(|r| !r.version().is_deleted()),
                                  |f, s|
                                      f.version().insert_version <= s.version().insert_version)
                        .collect_vec());

                combined_block
            }
        };

        Ok((candidate_index, candidate_fence.clone(), combined_block, candidate_guard))
    }

    pub(crate) fn split(
        &self,
        block: &Block<FAN_OUT, NUM_RECORDS, Key>,
        fence: &Interval<Key>,
    ) -> BlockSplit<FAN_OUT, NUM_RECORDS, Key>
    {
        let is_leaf
            = block.is_leaf();

        if block.active_count() >= block.max_units_safe() {
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
                        .filter(|r| !r.version().is_deleted())
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
                        leaf_page.bulk_push_from_slice_ref(first);
                    }

                    let fence_right = Interval::new(
                        second.get_unchecked(0).key,
                        fence.upper);

                    if let Node::Leaf(leaf_page) = right.unsafe_borrow_mut().as_mut() {
                        second.sort_by_key(|r| r.version().insertion_version());
                        leaf_page.bulk_push_from_slice_ref(second)
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
                        .filter(|((.., v), ..)|
                            InternalPage::<FAN_OUT, NUM_RECORDS, Key>::is_active(**v))
                        .sorted_by_key(|((i, ..), ..)| *i)
                        .collect_vec();

                    let middle = filtered.len() / 2;
                    let (first, second)
                        = filtered.split_at_mut(middle);

                    debug_assert!(!first.is_empty() && !second.is_empty());

                    let fence_left = Interval::new(
                        fence.lower,
                        (self.dec_key)(second.get_unchecked(0).0.0.lower));

                    if let Node::Index(internal_page) = left.unsafe_borrow_mut().as_mut() {
                        first.sort_by_key(|((.., v), ..)| **v);
                        internal_page.bulk_push_from_slice(first)
                    }

                    let fence_right = Interval::new(
                        second.get_unchecked(0).0.0.lower,
                        fence.upper);

                    if let Node::Index(internal_page) = right.unsafe_borrow_mut().as_mut() {
                        second.sort_by_key(|((.., v), ..)| **v);
                        internal_page.bulk_push_from_slice(second)
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

        Self::make_smart_root(locking_strategy.latch_type(), root_item)
    }

    pub(crate) fn make_smart_root(latch_type: LatchType, root_item: RootItem<FAN_OUT, NUM_RECORDS, Key>) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key> {
        SmartCell(Arc::new(match latch_type {
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
                                 height: Height,
                                 prev: Option<SmartRoot<FAN_OUT, NUM_RECORDS, Key>>,
    ) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key>
    {
        let root_item = RootItem {
            root: Root::new(
                block_manager.new_empty_leaf().into_cell(locking_strategy.latch_type()),
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
            root: UnCell::new(Self::make_root_item(
                &locking_strategy,
                &block_manager,
                VersionManager::START_VERSION,
                INIT_TREE_HEIGHT,
                None)),
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