use crate::mv_block::block::{Block, BlockGuard};
use crate::mv_block::block_handle::BlockAllocManager;
use crate::mv_page_model::{BlockRef, Height};
use crate::mv_query::time_matcher::TimeMatcher;
use crate::mv_root::index_root::RootIndexGuard;
use crate::mv_root::root::Root;
use crate::mv_test::VERBOSE;
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_utils::interval::Interval;
use itertools::Itertools;
use std::fmt::Display;
use std::hash::Hash;

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
{
    #[inline(always)]
    pub fn max_units(&self, is_leaf: bool) -> usize { // absolute units
        match is_leaf {
            true => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::max_records(),
            false => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::max_keys()
        }
    }

    #[inline(always)]
    pub fn filling_40_percent(&self, is_leaf: bool) -> usize { // 40%
        (2 * self.max_units(is_leaf) + 4) / 5
    }

    #[inline(always)]
    pub fn filling_80_percent(&self, is_leaf: bool) -> usize { // 80%
        (4 * self.max_units(is_leaf) + 4) / 5
    }

    #[inline(always)]
    pub fn filling_20_percent(&self, is_leaf: bool) -> usize { // 20%
        (self.max_units(is_leaf) + 4) / 5
    }
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
    Key: Default + Ord + Copy + Hash + Sync + 'static + Display,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub(crate) fn on_overflow_node<'a>(
        &self,
        mufasa: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        child_index: usize) -> BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let (current_len, internal_page)
            = mufasa.as_internal_page();

        let fence = *internal_page
            .get_key(child_index);

        match self.split(simba, &fence) {
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

                mufasa.commit_delta(1, 1);

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

                mufasa.commit_delta(0, 1);
                internal_page.mark_version_obsolete(child_index);
                self.end_tx_commit(version);
            }
        }

        self.block_manager.register_dead(
            internal_page.get_version(child_index),
            internal_page.get_pointer(child_index).clone());

        mufasa
    }

    pub(crate) fn on_underflow_node<'a>(
        &self,
        mufasa: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        index_simba: usize)
        -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
        match self.merge(&mufasa, simba, index_simba) {
            MergeResult::Merged(
                index_sibling,
                fence_sibling,
                merged_block,
                _candidate_guard
            ) => {
                let (mufasa_len, mufasa_internal_page) = mufasa
                    .as_internal_page();

                let mut merged_fence = *mufasa_internal_page
                    .get_key(index_simba);

                merged_fence.merged(&fence_sibling);

                let version
                    = self.start_tx_commit();

                mufasa_internal_page.push_uncommitted(
                    merged_fence,
                    version,
                    merged_block,
                    mufasa_len);

                mufasa
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
                ])
            }
            MergeResult::KeySplit(
                index_sibling,
                BlockSplit::ByKey(left_interval,
                                  left,
                                  right_interval,
                                  right),
                _candidate_guard
            ) => {
                let (mufasa_len, mufasa_internal_page) = mufasa
                    .as_internal_page();

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

                mufasa
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
                ])
            }
            _ => return Err(()),
        }

        Ok(mufasa)
    }

    pub(crate) fn merge<'a>(
        &self,
        mufasa: &BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba_index: usize,
    ) -> MergeResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let (mufasa_keys, mufasa_versions, mufasa_pointers)
            = mufasa.keys_versions_pointers();

        let (is_simba_leaf, _, simba_active_count, _)
            = simba.meta_block();

        let simba_fence
            = unsafe { *mufasa_keys.get_unchecked(simba_index) };

        let simba_active_count
            = simba_active_count as usize;

        let mut all_candidates = mufasa_pointers
            .iter()
            .enumerate()
            .zip(mufasa_versions)
            .zip(mufasa_keys)
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

        let (candidate_active_count, _candidate_dead_count) = candidate_guard
            .active_dead();

        let candidate_active_count
            = candidate_active_count as usize;

        if candidate_active_count + simba_active_count <= simba.filling_80_percent(is_simba_leaf) { // <= 80% ok merge
            let combined_block = match is_simba_leaf {
                false => {
                    let combined_block = self.block_manager
                        .new_empty_index_block();

                    let (keys, versions, pointers)
                        = simba.keys_versions_pointers();

                    let (c_keys, c_versions, c_pointers)
                        = candidate_guard.keys_versions_pointers();

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
                    // let (_init_len, combined_internal_page) = combined_block
                    //     .as_internal_page();
                    //
                    // let len = combined_internal_page
                    //     .bulk_push(shadow_copy);
                    let len = {
                        let (_init_len, p)
                            = combined_block.as_internal_page();

                        p.bulk_push(shadow_copy)
                    };
                    combined_block.commit_init(len, false);
                    combined_block
                }
                true => {
                    let combined_block = self.block_manager
                        .new_empty_leaf();

                    // let (_init_len, combined_leaf_page) = combined_block
                    //     .as_leaf_page();
                    // let len = combined_leaf_page
                    //     .bulk_push(simba
                    //         .as_records()
                    //         .iter()
                    //         .filter(|r| !r.version().is_deleted())
                    //         .merge_by(candidate_block
                    //                       .as_records()
                    //                       .iter()
                    //                       .filter(|r| !r.version().is_deleted()),
                    //                   |f, s|
                    //                       f.version().insert_version <= s.version().insert_version)
                    //         .collect_vec());
                    let len = {
                        let (_init_len, p)
                            = combined_block.as_leaf_page();

                        p.bulk_push(simba
                            .as_records()
                            .iter()
                            .filter(|r| !r.version().is_deleted())
                            .merge_by(candidate_block
                                          .as_records()
                                          .iter()
                                          .filter(|r| !r.version().is_deleted()),
                                      |f, s|
                                          f.version().insert_version <= s.version().insert_version)
                            .collect_vec())
                    };

                    combined_block.commit_init(len, true);
                    combined_block
                }
            };

            MergeResult::Merged(candidate_index, candidate_fence.clone(), combined_block, candidate_guard)
        } else { // Keysplit when merged: > 80% active entries ---> redistribute the keys
            match is_simba_leaf {
                true => unsafe {
                    let mut joined = candidate_block
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
                        .new_empty_leaf();

                    let combined_block_1 = self.block_manager
                        .new_empty_leaf();

                    // let (_init_0_len, combined_leaf_page_0)
                    //     = combined_block_0.as_leaf_page();
                    //
                    // let len = combined_leaf_page_0
                    //     .bulk_push_from_slice_ref(first);
                    let len = {
                        let (_init_len, p) = combined_block_0
                            .as_leaf_page();

                        p.bulk_push_from_slice_ref(first)
                    };

                    combined_block_0.commit_init(len, true);
                    // let (_init_1_len, combined_leaf_page_1)
                    //     = combined_block_1.as_leaf_page();
                    //
                    // let len = combined_leaf_page_1
                    //     .bulk_push_from_slice_ref(second);
                    let len = {
                        let (_init_len, p) = combined_block_1
                            .as_leaf_page();

                        p.bulk_push_from_slice_ref(second)
                    };

                    combined_block_1.commit_init(len, true);

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
                    let (c_keys, c_versions, c_children)
                        = candidate_guard.keys_versions_pointers();

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
                        .new_empty_index_block();

                    let combined_block_1 = self.block_manager
                        .new_empty_index_block();

                    // let (_init_0_len, combined_internal_page_0)
                    //     = combined_block_0.as_internal_page();
                    //
                    // let len = combined_internal_page_0
                    //     .bulk_push_from_slice(first);
                    let len = {
                        let (_init_len, p) = combined_block_0
                            .as_internal_page();

                        p.bulk_push_from_slice(first)
                    };

                    combined_block_0.commit_init(len, false);
                    // let (_init_1_len, combined_internal_page_1)
                    //     = combined_block_1.as_internal_page();
                    //
                    // let len = combined_internal_page_1
                    //     .bulk_push_from_slice(second);
                    let len = {
                        let (_init_len, p) = combined_block_1
                            .as_internal_page();

                        p.bulk_push_from_slice(second)
                    };

                    combined_block_1.commit_init(len, false);

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
        block: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        fence: &Interval<Key>,
    ) -> BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let (active_block, _dead_block, is_leaf)
            = block.active_dead_is_leaf();

        if active_block as usize >= block.filling_80_percent(is_leaf) {
            // KEY_SPLIT
            match is_leaf {
                true => unsafe { // LeafPage
                    if VERBOSE {
                        println!("Key Split: Leaf\n{}", block.as_records().iter().join("\n\t"));

                    }
                    let (left, right) =
                        (self.block_manager
                             .new_empty_leaf(),
                         self.block_manager
                             .new_empty_leaf());

                    let mut sorted_block = block
                        .as_records()
                        .iter()
                        .filter(|r| !r.version().is_deleted())
                        .sorted_by_key(|r| r.key())
                        .collect_vec();

                    let middle = sorted_block.len() / 2;
                    let (first, second) = sorted_block
                        .split_at_mut(middle);

                    let fence_left = Interval::new(
                        fence.lower,
                        (self.dec_key)(second.get_unchecked(0).key));

                    first.sort_by_key(|r|
                        r.version().insertion_version());
                    // let (_init_len, leaf_page)
                    //     = left.as_leaf_page();
                    // let len
                    //     = leaf_page.bulk_push_from_slice_ref(first);
                    let len = {
                        let (_init_len, p) = left
                            .as_leaf_page();

                        p.bulk_push_from_slice_ref(first)
                    };

                    left.commit_init(len, true);

                    let fence_right = Interval::new(
                        second.get_unchecked(0).key,
                        fence.upper);

                    second.sort_by_key(|r| r.version().insertion_version());
                    // let (_init_len, leaf_page)
                    //     = right.as_leaf_page();
                    // let len
                    //     = leaf_page.bulk_push_from_slice_ref(second);
                    let len = {
                        let (_init_len, p) = right
                            .as_leaf_page();

                        p.bulk_push_from_slice_ref(second)
                    };

                    right.commit_init(len, true);

                    BlockSplit::ByKey(fence_left, left, fence_right, right)
                }
                false => unsafe { // KEY_SPLIT InternalPage
                    if VERBOSE {
                        println!("Key Split: Internal");
                    }
                    let (left, right) =
                        (self.block_manager
                             .new_empty_index_block(),
                         self.block_manager
                             .new_empty_index_block());

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

                    first.sort_by_key(|((.., v), ..)| **v);
                    // let (_init_len, internal_page)
                    //     = left.as_internal_page();
                    // let len
                    //     = internal_page.bulk_push_from_slice(first);
                    let len = {
                        let (_init_len, p) = left
                            .as_internal_page();

                        p.bulk_push_from_slice(first)
                    };

                    left.commit_init(len, false);

                    let fence_right = Interval::new(
                        second.get_unchecked(0).0.0.lower,
                        fence.upper);

                    second.sort_by_key(|((.., v), ..)| **v);
                    // let (_init_len, internal_page)
                    //     = right.as_internal_page();
                    // let len
                    //     = internal_page.bulk_push_from_slice(second);
                    let len = {
                        let (_init_len, p) = right
                            .as_internal_page();

                        p.bulk_push_from_slice(second)
                    };

                    right.commit_init(len, false);

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
                        .new_empty_leaf();

                    let active_records = block
                        .as_records()
                        .iter()
                        .filter(|record| !record.version().is_deleted())
                        .collect_vec();

                    debug_assert!(active_records.len() >= block.filling_40_percent(is_leaf),
                                  "Active records = {}, required >= {}",
                                  active_records.len(),
                                  block.filling_40_percent(is_leaf));
                    // let (_init_len, leaf_page)
                    //     = new_leaf.as_leaf_page();
                    // let len
                    //     = leaf_page.bulk_push(active_records);
                    let len = {
                        let (_init_len, p) = new_leaf
                            .as_leaf_page();

                        p.bulk_push(active_records)
                    };

                    new_leaf.commit_init(len, true);

                    BlockSplit::ByVersion(new_leaf)
                }
                false => { // VERSION SPLIT InternalPage
                    if VERBOSE {
                        println!("Version Split: Internal");
                    }

                    let (key_intervals, versions, pointers) = block
                        .keys_versions_pointers();

                    let active_entries = key_intervals
                        .iter()
                        .zip(versions.iter())
                        .zip(pointers.iter())
                        .filter(|((_, v), _)|
                            v.is_active())
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

                    let new_internal_page = self.block_manager
                        .new_empty_index_block();

                    let len = {
                        let (_init_len, p)
                            = new_internal_page.as_internal_page();

                        p.bulk_push(active_entries)
                    };

                    new_internal_page.commit_init(len, false);

                    BlockSplit::ByVersion(new_internal_page)
                }
            }
        }
    }

    #[inline]
    pub(crate) fn merge_root<'a>(
        &self,
        master_guard: RootIndexGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        root_guard: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        height: Height,
    ) -> Result<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
        if VERBOSE {
            println!("merge root");
        }

        let (root_len, root_internal_page)
            = root_guard.as_internal_page();

        let child_block = root_internal_page
            .last_child(root_len);

        let latch_child
            = child_block.borrow_write();

        if let None = latch_child {
            return Err(())
        }

        if VERBOSE {
            println!("Old root height = {}, new height = {}", height, height - 1);
        }

        let root_block
            = self.split_root(master_guard, child_block, height - 1);

        if VERBOSE {
            let (active, dead)
                = root_block.active_dead_cell();

            println!("active dead count: ({} / {})", active, dead);
        }

        Ok(root_block)
    }

    #[inline]
    pub(crate) fn split_root<'a>(
        &self,
        _master_guard: RootIndexGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        root_guard: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        height: Height,
    ) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        match self.split(root_guard, &Interval::new(self.min_key, self.max_key)) {
            BlockSplit::ByKey(left_fence,
                              left,
                              right_fence,
                              right
            ) => {
                let new_root_block = self
                    .block_manager
                    .new_empty_index_block();

                let version
                    = self.start_tx_commit();

                {
                    let (_init_len, p) = new_root_block
                        .as_internal_page();

                    p.push_uncommitted(left_fence, version, left, 0);
                    p.push_uncommitted(right_fence, version, right, 1);
                }
                // let (_init_root_len, root_internal_page)
                //     = new_root_block.as_internal_page();
                //
                // root_internal_page
                //     .push_uncommitted(left_fence, version, left, 0);
                //
                // root_internal_page
                //     .push_uncommitted(right_fence, version, right, 1);
                new_root_block.commit_init(2, false);

                let old_v = _master_guard.version();
                self.root.append_root(
                    Root::new(new_root_block.clone(), version, height + 1));

                self.end_tx_commit(version);

                self.block_manager.register_dead(
                    old_v, root_guard.clone());

                new_root_block
            }
            BlockSplit::ByVersion(new_root_block) => {
                let version
                    = self.start_tx_commit();

                let old_v = _master_guard.version();
                self.root.append_root(
                    Root::new(new_root_block.clone(), version, height));

                self.end_tx_commit(version);

                self.block_manager.register_dead(
                    old_v, root_guard.clone());

                new_root_block
            }
        }
    }
}