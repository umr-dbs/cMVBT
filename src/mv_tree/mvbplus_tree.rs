use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use itertools::Itertools;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use crate::mv_block::block::{Block, BlockGuard, BlockSplit};
use crate::mv_block::block_manager::BlockManager;
use crate::mv_tree::root::Root;
use crate::mv_page_model::{Attempts, BlockRef, Height, Level, ObjectCount};
use crate::mv_page_model::internal_page::TimeMatcher;
use crate::mv_page_model::node::PageType;
use crate::mv_record_model::version_info::Version;
use crate::mv_tree::global_clock::GlobalClock;
use crate::mv_tree::locking_strategy::{LockingStrategy, OLC, orwc};
use crate::mv_tree::version_manager::VersionManager;
use crate::mv_utils::interval::Interval;
use crate::mv_utils::safe_cell::SafeCell;
use crate::mv_utils::smart_cell::{LatchType, OptCell, SmartCell, SmartFlavor, SmartGuard};
use crate::mv_utils::un_cell::UnCell;

pub type LockLevel = ObjectCount;

pub const INIT_TREE_HEIGHT: Height = 1;
pub const MAX_TREE_HEIGHT: Height = Height::MAX;

#[derive(Clone, Serialize, Deserialize)]
pub enum ClockType {
    FREE,
    OPTIMISTIC,
    SYNCED,
}

impl Display for ClockType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClockType::FREE => write!(f, "FREE"),
            ClockType::OPTIMISTIC => write!(f, "OPTIMISTIC"),
            ClockType::SYNCED => write!(f, "SYNCED"),
        }
    }
}

#[derive(Default, Clone)]
pub(crate) struct RootItem<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    pub(crate) root: Root<FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub(crate) prev: Option<SmartCell<RootItem<FAN_OUT, NUM_RECORDS, Key, Payload>>>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Deref for RootItem<FAN_OUT, NUM_RECORDS, Key, Payload> {
    type Target = Root<FAN_OUT, NUM_RECORDS, Key, Payload>;

    fn deref(&self) -> &Self::Target {
        &self.root
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static
> RootItem<FAN_OUT, NUM_RECORDS, Key, Payload> {
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
    Key,
    Payload
> = SmartCell<RootItem<FAN_OUT, NUM_RECORDS, Key, Payload>>;

pub(crate) type RootItemGuard<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key,
    Payload
> = SmartGuard<'a, RootItem<FAN_OUT, NUM_RECORDS, Key, Payload>>;

pub struct MVBPlusTree<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static
> {
    pub(crate) root: UnCell<SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    pub(crate) locking_strategy: LockingStrategy,
    pub(crate) block_manager: BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub(crate) version_manager: VersionManager,
    pub(crate) inc_key: fn(Key) -> Key,
    pub(crate) dec_key: fn(Key) -> Key,
    pub(crate) min_key: Key,
    pub(crate) max_key: Key,
}

// impl<const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Ord + Copy + Hash + Display
// > Drop for MVBPlusTree<FAN_OUT, NUM_RECORDS, Key> {
//     fn drop(&mut self) {
//         self.block_manager = self.block_manager.clone();
//         let y = self.root.unsafe_borrow_mut().prev.take();
//         let x = &self.root.unsafe_borrow_mut().root.mv_block;
//     }
// }

unsafe impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Sync for MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Send for MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Payload: Clone + Default + 'static
> Default for MVBPlusTree<FAN_OUT, NUM_RECORDS, u64, Payload> {
    fn default() -> Self {
        Self::standard()
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Payload: Clone + Default + 'static
> MVBPlusTree<FAN_OUT, NUM_RECORDS, u64, Payload>
{
    #[inline]
    pub fn make_standard(locking_strategy: LockingStrategy, clock_type: ClockType) -> Self {
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

    pub fn olc_optimistic_clock() -> Self {
        Self::make_standard(OLC(), ClockType::OPTIMISTIC)
    }

    pub fn olc() -> Self {
        Self::make_standard(OLC(), ClockType::SYNCED)
    }
}

pub(crate) enum MergeResult<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    Merged(usize,
           Interval<Key>,
           BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
           BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>),
    KeySplit(usize,
             BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload>,
             BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>),
    Error,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static + Display,
    Payload: Clone + Default + 'static
> MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub(crate) fn merge(
        &self,
        mufasa: &Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: &Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba_index: usize,
    ) -> MergeResult<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let mufasa_internal_page
            = mufasa.as_internal_page_ref();

        let is_simba_leaf
            = simba.is_leaf();

        let simba_fence
            = mufasa_internal_page.get_key(simba_index);

        let max_active_units
            = simba.max_active_units();

        let simba_active_count
            = simba.active_count();

        let mut all_candidates = mufasa_internal_page
            .children()
            .iter()
            .enumerate()
            .zip(mufasa_internal_page.versions())
            .zip(mufasa_internal_page.keys())
            .filter(|(((index, ..), ..), ..)|
                *index != simba_index)
            // .filter(|((.., version), ..)| version.is_active())
            .sorted_by_key(|(.., fence)| fence.lower())
            .map(|(((index, bro), ..), fence)|
                (index, bro, fence))
            .collect_vec();

        let mut compute_candidate = ||
            match all_candidates.binary_search_by_key(&simba_fence.lower, |(.., f)| f.lower) {
                Ok(index) => Ok(all_candidates.remove(index)),
                Err(index) => if index >= 0 && index < all_candidates.len() {
                    Ok(all_candidates.remove(index))
                } else if !all_candidates.is_empty() {
                    return Ok(all_candidates.pop().unwrap());
                } else {
                    return Err(());
                },
            };

        let (candidate_index,
            // candidate_guard,
            candidate_block,
            // candidate_active_count,
            candidate_fence
        ) = match compute_candidate() {
            Ok((index,
                   // mut candidate_guard,
                   block,
                   // cac,
                   cf)
            ) => (index, block, cf),
            _ => return MergeResult::Error
        };

        all_candidates.clear();

        let candidate_guard = candidate_block
            .borrow_mut();

        if !candidate_guard.is_valid() {
            return MergeResult::Error
        }

        let candidate_active_count = candidate_block
            .unsafe_borrow()
            .active_count();

        if candidate_active_count + simba_active_count <= max_active_units { // <= 4d
            let combined_block = match is_simba_leaf {
                false => {
                    let mut combined_block = self.block_manager
                        .new_empty_index_block(self.locking_strategy.latch_type());

                    let (keys, versions, pointers)
                        = simba.as_internal_page_ref().keys_versions_pointers();

                    let (c_keys, c_versions, c_pointers) = candidate_guard
                        .deref()
                        .unwrap()
                        .as_internal_page_ref()
                        .keys_versions_pointers();

                    let shadow_copy = keys
                        .iter()
                        .zip(versions)
                        .zip(pointers)
                        .filter(|((.., version), ..)| version.is_active())
                        .merge_by(c_keys.iter()
                                      .zip(c_versions)
                                      .zip(c_pointers)
                                      .filter(|((.., version), ..)| version.is_active()),
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
                        .new_empty_leaf(self.locking_strategy.latch_type());

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

            MergeResult::Merged(candidate_index, candidate_fence.clone(), combined_block, candidate_guard)
        } else {
            match is_simba_leaf {
                true => unsafe {
                    let mut joined = candidate_guard
                        .deref()
                        .unwrap()
                        .as_records()
                        .iter()
                        .filter(|r| !r.version().is_deleted())
                        .merge_by(simba.as_records()
                                      .iter()
                                      .filter(|r| !r.version().is_deleted()),
                                  |f, s| f.key() < s.key())
                        .collect_vec();

                    let joined_len = joined.len();
                    let (first, second)
                        = joined.split_at_mut(joined_len / 2);

                    let left_interval = Interval::new(
                        candidate_fence.lower.min(simba_fence.lower),
                        (self.dec_key)(second.get_unchecked(0).key()));

                    let right_interval = Interval::new(
                        second.get_unchecked(0).key(),
                        candidate_fence.upper.max(simba_fence.upper));

                    first.sort_by_key(|r|
                        r.version().insert_version);

                    second.sort_by_key(|r|
                        r.version().insert_version);

                    let mut combined_block_0 = self.block_manager
                        .new_empty_leaf(self.locking_strategy.latch_type());

                    let mut combined_block_1 = self.block_manager
                        .new_empty_leaf(self.locking_strategy.latch_type());

                    combined_block_0
                        .unsafe_borrow_mut()
                        .as_leaf_page()
                        .bulk_push_from_slice_ref(first);

                    combined_block_1
                        .unsafe_borrow_mut()
                        .as_leaf_page()
                        .bulk_push_from_slice_ref(second);

                    MergeResult::KeySplit(
                        candidate_index,
                        BlockSplit::ByKey(
                            left_interval,
                            combined_block_0,
                            right_interval,
                            combined_block_1),
                        candidate_guard)
                }
                false => unsafe {
                    let candidate_internal_page = candidate_guard
                        .deref()
                        .unwrap()
                        .as_internal_page_ref();

                    let (c_keys, c_versions, c_children)
                        = candidate_internal_page.keys_versions_pointers();

                    let (s_keys, s_version, s_children)
                        = simba.keys_versions_pointers();

                    let mut joined = c_keys
                        .iter()
                        .zip(c_versions)
                        .zip(c_children)
                        .filter(|((.., v), ..)| v.is_active())
                        .merge_by(s_keys.iter()
                                      .zip(s_version)
                                      .zip(s_children)
                                      .filter(|((.., v), ..)| v.is_active()),
                                  |((f, ..), ..), ((s, ..), ..)|
                                      f.lower < s.lower)
                        .collect_vec();

                    let joined_len = joined.len();
                    let (first, second)
                        = joined.split_at_mut(joined_len / 2);

                    let left_fence = Interval::new(
                        candidate_fence.lower.min(simba_fence.lower),
                        (self.dec_key)(second.get_unchecked(0).0.0.lower));

                    let right_fence = Interval::new(
                        second.get_unchecked(0).0.0.lower,
                        candidate_fence.upper.max(simba_fence.upper));

                    first.sort_by_key(|((.., v), ..)| **v);
                    second.sort_by_key(|((.., v), ..)| **v);

                    let mut combined_block_0 = self.block_manager
                        .new_empty_index_block(self.locking_strategy.latch_type());

                    let mut combined_block_1 = self.block_manager
                        .new_empty_index_block(self.locking_strategy.latch_type());

                    combined_block_0
                        .unsafe_borrow_mut()
                        .as_internal_page()
                        .bulk_push_from_slice(first);

                    combined_block_1
                        .unsafe_borrow_mut()
                        .as_internal_page()
                        .bulk_push_from_slice(second);

                    MergeResult::KeySplit(
                        candidate_index,
                        BlockSplit::ByKey(
                            left_fence,
                            combined_block_0,
                            right_fence,
                            combined_block_1),
                        candidate_guard)
                }
            }
        }
    }

    pub(crate) fn split(
        &self,
        block: &Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
        fence: &Interval<Key>,
    ) -> BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let is_leaf
            = block.is_leaf();

        if block.active_count() > block.max_active_units() {
            // KEY_SPLIT
            match is_leaf {
                true => unsafe { // LeafPage
                    let (left, right) =
                        (self.block_manager
                             .new_empty_leaf(self.locking_strategy.latch_type()),
                         self.block_manager
                             .new_empty_leaf(self.locking_strategy.latch_type()));

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

                    if let PageType::LeafMut(leaf_page) = left.unsafe_borrow_mut().as_page_mut() {
                        first.sort_by_key(|r| r.version().insertion_version());
                        leaf_page.bulk_push_from_slice_ref(first);
                    }

                    let fence_right = Interval::new(
                        second.get_unchecked(0).key,
                        fence.upper);

                    if let PageType::LeafMut(leaf_page) = right.unsafe_borrow_mut().as_page_mut() {
                        second.sort_by_key(|r| r.version().insertion_version());
                        leaf_page.bulk_push_from_slice_ref(second)
                    }

                    BlockSplit::ByKey(fence_left, left, fence_right, right)
                }
                false => unsafe { // KEY_SPLIT InternalPage
                    let (left, right) =
                        (self.block_manager
                             .new_empty_index_block(self.locking_strategy.latch_type()),
                         self.block_manager
                             .new_empty_index_block(self.locking_strategy.latch_type()));

                    let (key_intervals, versions, pointers) = block
                        .keys_versions_pointers();

                    let mut filtered = key_intervals
                        .iter()
                        .zip(versions.iter())
                        .zip(pointers.iter())
                        .filter(|((.., v), ..)| v.is_active())
                        .sorted_by_key(|((i, ..), ..)| i.lower)
                        .collect_vec();

                    let middle = filtered.len() / 2;
                    let (first, second)
                        = filtered.split_at_mut(middle);

                    debug_assert!(!first.is_empty() && !second.is_empty());

                    let fence_left = Interval::new(
                        fence.lower,
                        (self.dec_key)(second.get_unchecked(0).0.0.lower));

                    if let PageType::IndexMut(internal_page) = left.unsafe_borrow_mut().as_page_mut() {
                        first.sort_by_key(|((.., v), ..)| **v);
                        internal_page.bulk_push_from_slice(first)
                    }

                    let fence_right = Interval::new(
                        second.get_unchecked(0).0.0.lower,
                        fence.upper);

                    if let PageType::IndexMut(internal_page) = right.unsafe_borrow_mut().as_page_mut() {
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
                        .new_empty_leaf(self.locking_strategy.latch_type());

                    let active_records = block
                        .as_records()
                        .iter()
                        .filter(|record| !record.version().is_deleted())
                        .collect_vec();

                    debug_assert!(!active_records.is_empty());
                    if let PageType::LeafMut(leaf_page) = new_leaf.unsafe_borrow_mut().as_page_mut() {
                        leaf_page.bulk_push(active_records);
                    }

                    BlockSplit::ByVersion(new_leaf)
                }
                false => { // VERSION SPLIT InternalPage
                    let new_internal_page = self.block_manager
                        .new_empty_index_block(self.locking_strategy.latch_type());

                    let (key_intervals, versions, pointers) = block
                        .keys_versions_pointers();

                    let active_entries = key_intervals
                        .iter()
                        .zip(versions.iter())
                        .zip(pointers.iter())
                        .filter(|((.., v), ..)| v.is_active())
                        .collect_vec();

                    debug_assert!(!active_entries.is_empty());
                    if let PageType::IndexMut(internal_page) = new_internal_page.unsafe_borrow_mut().as_page_mut() {
                        internal_page.bulk_push(active_entries)
                    }

                    BlockSplit::ByVersion(new_internal_page)
                }
            }
        }
    }

    pub(crate) fn from(locking_strategy: &LockingStrategy,
                       block: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
                       version: Version,
                       height: Height,
                       prev: Option<SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    ) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload>
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

    pub(crate) fn clock_type(&self) -> ClockType {
        match self.version_manager.committed_version {
            GlobalClock::Locked(_) => ClockType::SYNCED,
            GlobalClock::Atomic(_) => ClockType::OPTIMISTIC,
            GlobalClock::Free(_) => ClockType::FREE
        }
    }

    pub(crate) fn make_smart_root(latch_type: LatchType, root_item: RootItem<FAN_OUT, NUM_RECORDS, Key, Payload>) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload> {
        SmartCell(Arc::new(match latch_type {
            LatchType::ReadersWriter => SmartFlavor::ReadersWriterCell(
                Mutex::new(()),
                SafeCell::new(root_item)),
            LatchType::Optimistic => SmartFlavor::OLCCell(
                OptCell::new(root_item)),
            LatchType::None => SmartFlavor::FreeCell(
                SafeCell::new(root_item))
        }))
    }

    pub(crate) fn make_root_item(locking_strategy: &LockingStrategy,
                                 block_manager: &BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload>,
                                 version: Version,
                                 height: Height,
                                 prev: Option<SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    ) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let root_item = RootItem {
            root: Root::new(
                block_manager.new_empty_leaf(locking_strategy.latch_type()),
                version,
                height,
            ),
            prev,
        };

        SmartCell(Arc::new(match locking_strategy.latch_type() {
            LatchType::ReadersWriter => SmartFlavor::ReadersWriterCell(
                Mutex::new(()),
                SafeCell::new(root_item)),
            LatchType::Optimistic => SmartFlavor::OLCCell(
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


    // #[inline]
    // pub(crate) fn apply_for_root(
    //     &self,
    //     curr: &BlockRef<FAN_OUT, NUM_RECORDS, Key>,
    //     attempts: Attempts,
    //     height: Height,
    // ) -> SmartGuard<'static, Block<{ FAN_OUT }, { NUM_RECORDS }, Key>>
    // {
    //     self.apply_for_ref(
    //         curr,
    //         height,
    //         INIT_TREE_HEIGHT,
    //         attempts,
    //         Level::MAX)
    // }

    #[inline]
    pub(crate) fn is_lock(&self, attempts: Attempts, height: Height) -> bool {
        match self.locking_strategy() {
            LockingStrategy::ORWC {
                write_level,
                write_attempt
            } if *write_level <= 1f32 &&
                (height <= INIT_TREE_HEIGHT || INIT_TREE_HEIGHT as f32 * write_level >= height as f32 || attempts > *write_attempt) =>
                true,
            LockingStrategy::MonoWriter => false,
            LockingStrategy::OLC => false,
            LockingStrategy::ORWC { .. } => false,
        }
    }

    #[inline]
    pub(crate) fn apply_for_ref(
        &self,
        curr: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        height: Height,
        curr_level: Level,
        attempts: Attempts,
        max_level: Level,
    ) -> BlockGuard<'static, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        match self.locking_strategy() {
            LockingStrategy::ORWC {
                write_level,
                write_attempt
            } if curr.unsafe_borrow().as_ref().is_leaf() || *write_level <= 1f32 &&
                (height <= curr_level || curr_level >= max_level || curr_level as f32 * write_level >= height as f32 || attempts > *write_attempt) =>
                curr.borrow_mut(),
            LockingStrategy::MonoWriter =>
                curr.borrow_free(),
            LockingStrategy::ORWC { .. } |
            LockingStrategy::OLC=> curr.borrow_read(),
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

    pub fn make_empty_copy(&self) -> Self {
        Self {
            root: UnCell::new(Self::make_root_item(
                self.locking_strategy(),
                &self.block_manager,
                VersionManager::START_VERSION,
                INIT_TREE_HEIGHT,
                None)),
            locking_strategy: self.locking_strategy.clone(),
            block_manager: self.block_manager.clone(),
            version_manager: self.version_manager.clone(),
            inc_key: self.inc_key,
            dec_key: self.dec_key,
            min_key: self.min_key,
            max_key: self.max_key,
        }
    }
}