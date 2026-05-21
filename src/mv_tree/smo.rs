use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use itertools::Itertools;
use crate::mv_block::block::{Block, BlockGuard};
use crate::mv_block::block_handle::BlockAllocManager;
use crate::mv_page_model::{BlockRef, Height};
use crate::mv_page_model::internal_page::TimeMatcher;
use crate::mv_page_model::node::PageType;
use crate::mv_root::index_root::RootIndexGuard;
use crate::mv_root::root::Root;
use crate::mv_sync::latch_protocol::LatchProtocol;
use crate::mv_sync::smart_cell::sched_yield;
use crate::mv_test::VERBOSE;
use crate::mv_tree::mvtree::MVTreeSt;
use crate::mv_utils::interval::Interval;

#[repr(u8)]
pub enum BlockUnsafeDegree {
    Ok,
    Overflow,
    ActiveUnderflow
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
            true => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::max_records(),
            false => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::max_keys()
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
            true => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::overflow_records_count(),
            false => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::overflow_keys_count()
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
}

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
> { }

pub(crate) enum MergeResult<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    Merged(usize,
           Interval<Key>,
           BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
           BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>),
    KeySplit(usize,
             BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload>,
             BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>),
    Error,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + 'static + Display,
    Payload: Display + Clone + Default + Sync + 'static
> MVTreeSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub(crate) fn on_overflow_node<'a>(
        &self,
        mufasa: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        child_index: usize) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let mufasa_deref_mut
            = mufasa.deref_mut().unwrap();

        let internal_page
            = mufasa_deref_mut.as_internal_page();

        let fence = internal_page
            .get_key(child_index)
            .clone();

        let current_len
            = internal_page.sum_len();

        match self.split(simba.deref(), &fence) {
            BlockSplit::ByKey(left_fence,
                              left,
                              right_fence,
                              right
            ) => {
                let version
                    = self.start_tx_commit();

                internal_page.push_uncommitted(
                    left_fence,
                    version,
                    left,
                    current_len);

                internal_page.push_uncommitted(
                    right_fence,
                    version,
                    right,
                    current_len + 1);

                internal_page.commit_delta(1, 1);
                internal_page.mark_version_obsolete(child_index);
                self.end_tx_commit(version);
            }
            BlockSplit::ByVersion(fresh) => {
                let version
                    = self.start_tx_commit();

                internal_page.push_uncommitted(
                    fence,
                    version,
                    fresh,
                    current_len);

                internal_page.commit_delta(0, 1);
                internal_page.mark_version_obsolete(child_index);
                self.end_tx_commit(version);
            }
        }

        self.block_manager.register_dead(
            internal_page.get_version(child_index),
            internal_page.get_pointer(child_index).clone());

        mufasa
    }

    pub(crate) fn on_underflow_node(
        &self,
        mufasa: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        index_simba: usize)
        -> Result<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
        if VERBOSE {
            println!("on_underflow_node");
        }
        let mufasa_deref_mut
            = mufasa.deref_mut().unwrap();

        match self.merge(mufasa_deref_mut, simba.deref(), index_simba) {
            MergeResult::Merged(
                index_sibling,
                fence_sibling,
                merged_block,
                _candidate_guard
            ) => {
                if VERBOSE {

                    println!("MergeResult::Merged: Simba-fence: {} - Sibling-fence: {}",
                             mufasa_deref_mut.as_internal_page_ref().get_key(index_simba),
                             fence_sibling);
                }
                let mufasa_internal_page = mufasa_deref_mut
                    .as_internal_page();

                let mufasa_len
                    = mufasa_internal_page.sum_len();

                let mut merged_fence = mufasa_internal_page
                    .get_key(index_simba)
                    .clone();

                merged_fence.merged(&fence_sibling);

                let version
                    = self.start_tx_commit();

                mufasa_internal_page.push_uncommitted(
                    merged_fence,
                    version,
                    merged_block,
                    mufasa_len);

                mufasa_internal_page
                    .commit_delta(-1, 2);

                mufasa_internal_page
                    .mark_version_obsolete(index_sibling);

                mufasa_internal_page
                    .mark_version_obsolete(index_simba);

                self.block_manager.register_dead_col([
                    (mufasa_internal_page.get_version(index_simba),
                     mufasa_internal_page.get_pointer(index_simba).clone()),
                    (mufasa_internal_page.get_version(index_sibling),
                     mufasa_internal_page.get_pointer(index_sibling).clone())
                ]);

                Ok(mufasa)
            }
            MergeResult::KeySplit(
                index_sibling,
                BlockSplit::ByKey(left_interval,
                                  left,
                                  right_interval,
                                  right),
                _candidate_guard
            ) => {
                if VERBOSE {
                    unsafe {
                        println!("MergeResult::KeySplit: \
                       \tleft-fence: {}, \
                       \tright-fence: {}.\
                        \n\tSimba-fence: {} - Sibling-fence: {}\n\
                        \tsimba:\n{}",
                                 left_interval,
                                 right_interval,
                                 mufasa_deref_mut.keys().get_unchecked(index_simba),
                                 mufasa_deref_mut.keys().get_unchecked(index_sibling),
                                 simba.deref().node_data.as_ref()
                        );
                    }
                }
                let mufasa_internal_page = mufasa_deref_mut
                    .as_internal_page();

                let mufasa_len
                    = mufasa_internal_page.sum_len();

                let version
                    = self.start_tx_commit();

                mufasa_internal_page.push_uncommitted(
                    left_interval,
                    version,
                    left,
                    mufasa_len);

                mufasa_internal_page.push_uncommitted(
                    right_interval,
                    version,
                    right,
                    mufasa_len + 1);

                mufasa_internal_page
                    .commit_delta(0, 2);

                mufasa_internal_page
                    .mark_version_obsolete(index_sibling);

                mufasa_internal_page
                    .mark_version_obsolete(index_simba);

                self.block_manager.register_dead_col([
                    (mufasa_internal_page.get_version(index_simba),
                     mufasa_internal_page.get_pointer(index_simba).clone()),
                    (mufasa_internal_page.get_version(index_sibling),
                     mufasa_internal_page.get_pointer(index_sibling).clone())
                ]);

                Ok(mufasa)
            }
            _ => Err(()),
        }
    }

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

        let simba_max_units
            = simba.max_units();

        let (simba_active_count, _simba_dead_count)
            = simba.active_dead_count();

        let (simba_active_count, _simba_dead_count)
            = (simba_active_count as usize, _simba_dead_count as usize);

        let mut all_candidates = mufasa_internal_page
            .children()
            .iter()
            .enumerate()
            .zip(mufasa_internal_page.versions())
            .zip(mufasa_internal_page.keys())
            .filter(|(((index, ..), ..), ..)|
                *index != simba_index)
            .filter(|((.., version), ..)| version.is_active())
            .sorted_by_key(|(.., fence)| fence.lower())
            .map(|(((index, bro), ..), fence)|
                (index, bro, fence))
            .collect_vec();

        let mut compute_candidate = ||
            match all_candidates.binary_search_by_key(&simba_fence.lower, |(.., f)| f.lower) {
                Ok(index) => Ok(all_candidates.remove(index)),
                Err(index) => if index < all_candidates.len() {
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

        let mut candidate_guard = candidate_block
            .borrow_read();

        if !candidate_guard.upgrade_write_lock() {
            return MergeResult::Error
        }

        let (candidate_active_count, _candidate_dead_count) = candidate_block
            .unsafe_borrow()
            .active_dead_count();

        let candidate_active_count
            = candidate_active_count as usize;

        if candidate_active_count + simba_active_count <= ((4 * simba_max_units) / 5) { // <= 80% ok merge
            let combined_block = match is_simba_leaf {
                false => {
                    let combined_block = self.block_manager
                        .new_empty_index_block(self.locking_strategy.latch_type());

                    let (keys, versions, pointers)
                        = simba.as_internal_page_ref().keys_versions_pointers();

                    let (c_keys, c_versions, c_pointers) = candidate_guard
                        .deref()
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
                                  |((.., v0), ..), ((.., v1), ..)| v0 <= v1)
                        .collect_vec();

                    combined_block
                        .unsafe_borrow_mut()
                        .as_internal_page()
                        .bulk_push(shadow_copy);

                    combined_block
                }
                true => {
                    let combined_block = self.block_manager
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
        } else { // Keysplit when merged: > 80% active entries ---> redistribute the keys
            match is_simba_leaf {
                true => unsafe {
                    let mut joined = candidate_guard
                        .deref()
                        .as_records()
                        .iter()
                        .filter(|r| !r.version().is_deleted())
                        .sorted_by_key(|r| r.key)
                        .merge_by(simba.as_records()
                                      .iter()
                                      .filter(|r| !r.version().is_deleted())
                                      .sorted_by_key(|r| r.key),
                                  |f, s|
                                      f.key() <= s.key())
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

                    let combined_block_0 = self.block_manager
                        .new_empty_leaf(self.locking_strategy.latch_type());

                    let combined_block_1 = self.block_manager
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
                        .sorted_by_key(|((k, ..), ..)| k.lower)
                        .merge_by(s_keys.iter()
                                      .zip(s_version)
                                      .zip(s_children)
                                      .filter(|((.., v), ..)| v.is_active())
                                      .sorted_by_key(|((k, ..), ..)| k.lower),
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

                    let combined_block_0 = self.block_manager
                        .new_empty_index_block(self.locking_strategy.latch_type());

                    let combined_block_1 = self.block_manager
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

        let (active_block, _dead_block)
            = block.active_dead_count();

        if active_block as usize >= block.filling_80_percent() {
            // KEY_SPLIT
            match is_leaf {
                true => unsafe { // LeafPage
                    if VERBOSE {
                        println!("Key Split: Leaf\n{}", block.as_records().iter().join("\n\t"));

                    }
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
                    if VERBOSE {
                        println!("Key Split: Internal");
                    }
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
        } else { // < max_units_safe. meaning: active >= 40% and active < 80%
            // VERSION SPLIT
            match is_leaf {
                true => { // LeafPage
                    if VERBOSE {
                        println!("Version Split: Leaf");
                    }
                    let new_leaf = self.block_manager
                        .new_empty_leaf(self.locking_strategy.latch_type());

                    let active_records = block
                        .as_records()
                        .iter()
                        .filter(|record| !record.version().is_deleted())
                        .collect_vec();

                    debug_assert!(active_records.len() >= block.filling_40_percent(),
                                  "Active records = {}, required >= {}", active_records.len(), block.filling_40_percent());

                    // if active_records.len() <=
                    //     BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::min_active_records()
                    // {
                    //     let s = "asds".to_string();
                    //     let hase = "asdfasdasdaoshufiusdjbf".to_string();
                    //     exit(1);
                    // }

                    // debug_assert!(active_records.len() <=
                    //     BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::min_active_records());

                    if let PageType::LeafMut(leaf_page) = new_leaf.unsafe_borrow_mut().as_page_mut() {
                        leaf_page.bulk_push(active_records);
                    }

                    BlockSplit::ByVersion(new_leaf)
                }
                false => { // VERSION SPLIT InternalPage
                    if VERBOSE {
                        println!("Version Split: Internal");
                    }
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

                    if VERBOSE {
                        let key_intervals = active_entries
                            .iter()
                            .map(|((k, ..), ..)| (k.lower, k.upper))
                            .sorted_by_key(|i| i.0)
                            .collect_vec();

                        if !key_intervals.iter().zip(key_intervals.iter().skip(1))
                            .all(|((k0, k1), (k2, k3))|
                                (self.dec_key)(*k2) == *k1) {
                            let s = "sdasdasdasdasln".to_string();
                        }
                    }

                    // RootSplit calls this too! Root may run under conditioned 2d
                    // debug_assert!(active_entries.len() >= block.two_d_filling(),
                    //               "Active entries = {}, required >= {}", active_entries.len(), block.two_d_filling());
                    if let PageType::IndexMut(internal_page) = new_internal_page.unsafe_borrow_mut().as_page_mut() {
                        internal_page.bulk_push(active_entries)
                    }

                    BlockSplit::ByVersion(new_internal_page)
                }
            }
        }
    }

    #[inline]
    pub(crate) fn merge_root(
        &self,
        master_guard: RootIndexGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        root_guard: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        height: Height,
    ) -> Result<
        (BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>), ()>
    {
        if VERBOSE {
            println!("merge root");
        }
        let child_ptr = root_guard
            .deref()
            .as_internal_page_ref()
            .last_child();

        let mut child_guard = child_ptr
            .borrow_read();

        if !child_guard.upgrade_write_lock() {
            return Err(())
        }

        if VERBOSE {
            println!("Old root height = {}, new height = {}", height, height - 1);
        }

        let guard
            = self.split_root(master_guard, child_guard, height - 1);

        if VERBOSE {
            let guard_deref
                = guard.deref_mut_unsafe();

            let (active, dead)
                = guard_deref.active_dead_count();

            println!("active dead count: ({} / {})", active, dead);
        }

        Ok(guard)
    }

    pub(crate) fn split_root(
        &self,
        _master_guard: RootIndexGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        root_guard: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        height: Height,
    ) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let root_guard_deref_mut
            = root_guard.deref_mut_unsafe();

        match self.split(root_guard_deref_mut, &Interval::new(self.min_key, self.max_key)) {
            BlockSplit::ByKey(left_fence,
                              left,
                              right_fence,
                              right
            ) => {
                let new_root_block = self
                    .block_manager
                    .new_empty_index_block(self.locking_strategy.latch_type());

                let root_internal_page = new_root_block
                    .unsafe_borrow_mut()
                    .as_mut()
                    .as_internal_page();

                let version
                    = self.start_tx_commit();

                root_internal_page
                    .push_uncommitted(left_fence, version, left, 0);

                root_internal_page
                    .push_uncommitted(right_fence, version, right, 1);

                root_internal_page.commit_delta(2, 0);

                let new_root_latch
                    = new_root_block.borrow_read();

                let old_v = _master_guard.version();
                self.root.append_root(
                    Root::new(new_root_block.clone(), version, height + 1));

                self.end_tx_commit(version);

                self.block_manager.register_dead(
                    old_v, root_guard.inner_cell());

                new_root_latch
            }
            BlockSplit::ByVersion(new_root_block) => {
                let version
                    = self.start_tx_commit();

                let new_root_latch
                    = new_root_block.borrow_read();

                let old_v = _master_guard.version();
                self.root.append_root(
                    Root::new(new_root_block.clone(), version, height));

                self.end_tx_commit(version);

                self.block_manager.register_dead(
                    old_v, root_guard.inner_cell());

                new_root_latch
            }
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

    // #[inline]
    // pub(crate) fn is_lock(&self, attempts: Attempts, height: Height) -> bool {
    //     match self.locking_strategy() {
    //         LockingStrategy::MonoWriter => false,
    //         LockingStrategy::OLC => false,
    //     }
    // }
    //
    // #[inline]
    // pub(crate) fn apply_for_ref(
    //     &self,
    //     curr: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>
    // ) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>
    // {
    //     match self.locking_strategy() {
    //         LockingStrategy::MonoWriter => curr.borrow_free(),
    //         LockingStrategy::OLC => curr.borrow_read(),
    //     }
    // }
    //
    // #[inline(always)]
    // pub fn new_with(locking_strategy: LockingStrategy,
    //                 inc_key: fn(Key) -> Key,
    //                 dec_key: fn(Key) -> Key,
    //                 min_key: Key,
    //                 max_key: Key,
    // ) -> Self {
    //     Self::make(locking_strategy, ClockType::SYNC, inc_key, dec_key, min_key, max_key)
    // }
    //
    // #[inline(always)]
    // pub fn new(inc_key: fn(Key) -> Key,
    //            dec_key: fn(Key) -> Key,
    //            min_key: Key,
    //            max_key: Key) -> Self {
    //     Self::make(LockingStrategy::default(), ClockType::FREE, inc_key, dec_key, min_key, max_key)
    // }
    //
    #[inline(always)]
    pub const fn locking_strategy(&self) -> &LatchProtocol {
        &self.locking_strategy
    }
    //
    // pub fn make_empty_copy(&self) -> Self {
    //     Self {
    //         root: UnCell::new(Self::make_root_item(
    //             self.locking_strategy(),
    //             &self.block_manager,
    //             VersionManager::START_VERSION,
    //             INIT_TREE_HEIGHT,
    //             None)),
    //         locking_strategy: self.locking_strategy.clone(),
    //         block_manager: self.block_manager.clone(),
    //         version_manager: self.version_manager.clone(),
    //         inc_key: self.inc_key,
    //         dec_key: self.dec_key,
    //         min_key: self.min_key,
    //         max_key: self.max_key,
    //     }
    // }
}