use std::collections::VecDeque;
use std::hash::Hash;
use std::mem;
use itertools::Itertools;
use crate::block::block::{BlockGuard, BlockSplit, BlockUnsafeDegree};
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::page_model::{Attempts, BlockRef, Height, Level};
use crate::page_model::internal_page::{InternalPage, TimeMatcher};
use crate::page_model::node::Node;
use crate::record_model::record_point::RecordPointResult;
use crate::record_model::version_info::Version;
use crate::tree::bplus_tree::{BPlusTree, LockLevel, MAX_TREE_HEIGHT, SmartRoot, RootItemGuard, MergeResult};
use crate::utils::interval::Interval;
use crate::utils::smart_cell::sched_yield;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static
> BPlusTree<FAN_OUT, NUM_RECORDS, Key>
{
    pub(crate) fn retrieve_root_for(&self, lookup_version: Version) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key> {
        let mut root_anker
            = self.root.clone();

        loop {
            let root_item
                = root_anker.unsafe_borrow();

            if root_item.version().match_version(lookup_version) {
                break root_anker;
            } else {
                root_anker = match root_item.prev.as_ref() {
                    None => unreachable!(),
                    Some(s) => s.clone()
                };
            }
        }
    }

    pub(crate) fn retrieve_root_latest(&self) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key> {
        self.retrieve_root_for(Version::MAX)
    }

    fn traverse_read_key(
        mut curr: BlockRef<FAN_OUT, NUM_RECORDS, Key>,
        key: Key,
        version: Version)
        -> BlockRef<FAN_OUT, NUM_RECORDS, Key>
    {
        while let Node::Index(internal_page) = curr
            .unsafe_borrow()
            .as_ref()
        {
            let (keys_page, versions_page) = internal_page
                .keys_versions();

            // assert_eq!(versions_page.len(), keys_page.len());

            curr = versions_page
                .iter()
                .enumerate()
                .rev()
                .zip(keys_page
                    .iter()
                    .rev())
                .skip_while(|((.., v), ..)| !v.match_version(version))
                .find(|(.., range)| range.contains(key))
                .map(|((pos, ..), ..)| internal_page.get_pointer(pos))
                .unwrap()
                .clone();
        }

        curr
    }

    #[inline]
    fn traverse_read_key_range(
        mut curr: BlockRef<FAN_OUT, NUM_RECORDS, Key>,
        lookup_range: &Interval<Key>,
        version: Version)
        -> Vec<BlockRef<FAN_OUT, NUM_RECORDS, Key>>
    {
        let mut blocks
            = VecDeque::new();

        blocks.push_back(curr);

        let mut leafs
            = vec![];

        while !blocks.is_empty() {
            curr = blocks.pop_front().unwrap();

            match curr.unsafe_borrow().as_ref() {
                Node::Index(internal_page) => {
                    let (keys_page, versions_page) = internal_page
                        .keys_versions();

                    blocks.extend(versions_page
                        .iter()
                        .enumerate()
                        .rev()
                        .zip(keys_page
                            .iter()
                            .rev())
                        .skip_while(|((.., v), ..)| !v.match_version(version))
                        .filter(|(.., range)| lookup_range.overlap(range))
                        .map(|((pos, ..), ..)| internal_page
                            .get_pointer(pos)
                            .clone())
                    );
                }
                _ => leafs.push(curr)
            }
        }

        leafs
    }

    #[inline]
    pub(crate) fn key_point_read_from_root(
        root: SmartRoot<FAN_OUT, NUM_RECORDS, Key>,
        key: Key,
        version: Version)
        -> CRUDOperationResult<Key>
    {
        let leaf = Self::traverse_read_key(
            root.unsafe_borrow().block(),
            key,
            version);

        match leaf
            .unsafe_borrow()
            .as_records()
            .iter()
            .rev()
            .skip_while(|r| !r.version().matches(version))
            .find(|r| r.key() == key)
        {
            None => CRUDOperationResult::MatchedRecords(Vec::with_capacity(0)),
            Some(result) => CRUDOperationResult::MatchedRecords(vec![RecordPointResult::from(result)])
        }
    }

    #[inline(always)]
    pub(crate) fn snapshot(&self, version: Version) -> CRUDOperationResult<Key> {
        Self::key_range_read_from_root(
            self.retrieve_root_for(version),
            &Interval::new(self.min_key, self.max_key),
            version)
    }

    #[inline]
    pub(crate) fn key_range_read_from_root(
        root: SmartRoot<FAN_OUT, NUM_RECORDS, Key>,
        range: &Interval<Key>,
        version: Version)
        -> CRUDOperationResult<Key>
    {
        let blocks = Self::traverse_read_key_range(
            root.unsafe_borrow().block(),
            range,
            version);

        CRUDOperationResult::MatchedRecords(blocks
            .into_iter()
            .map(|leaf| leaf
                .unsafe_borrow()
                .as_records()
                .iter()
                .rev()
                .skip_while(|r| !r.version().matches(version))
                .take_while(|r| r.version().matches(version))
                .filter(|r| range.contains(r.key()))
                // .cloned()
                .map(|r| RecordPointResult::from(r))
                .collect::<Vec<_>>())
            .flatten()
            .collect())
    }

    #[inline]
    pub(crate) fn traversal_write(&self, key: Key) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key> {
        let mut attempt = 0;
        let mut lock_level = MAX_TREE_HEIGHT;

        loop {
            match self.traversal_write_internal(key, attempt, lock_level) {
                Err((n_lock_level, n_attempt)) => {
                    attempt = n_attempt;
                    lock_level = n_lock_level;
                }
                Ok(guard) => break guard,
            }
        }
    }

    fn retrieve_root_write(
        &self,
        mut attempts: Attempts,
    ) -> (RootItemGuard<FAN_OUT, NUM_RECORDS, Key>,
          BlockRef<FAN_OUT, NUM_RECORDS, Key>,
          BlockGuard<FAN_OUT, NUM_RECORDS, Key>,
          Height,
          Attempts)
    {
        loop {
            match self.retrieve_root_write_internal(attempts) {
                Ok((master, block, guard, height)) =>
                    break (master, block, guard, height, attempts),
                _ => attempts += 1
            }
        }
    }

    pub(crate) fn split_root<'a>(
        &self,
        mut master_guard: RootItemGuard<'a, FAN_OUT, NUM_RECORDS, Key>,
        mut root_guard: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key>,
        mut height: Height,
    ) -> (RootItemGuard<'a, FAN_OUT, NUM_RECORDS, Key>,
          BlockRef<FAN_OUT, NUM_RECORDS, Key>,
          BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key>,
          Height)
    {
        let root_guard_deref_mut
            = root_guard.deref_mut().unwrap();

        let (height, root_block) = match self.split(root_guard_deref_mut, &Interval::new(self.min_key, self.max_key)) {
            BlockSplit::ByKey(left_fence,
                              left,
                              right_fence,
                              right
            ) => {
                let new_root_block = self
                    .block_manager
                    .new_empty_index_block()
                    .into_cell(self.locking_strategy.latch_type());

                let root_internal_page = new_root_block
                    .unsafe_borrow_mut()
                    .as_mut()
                    .as_internal_page();

                let copy_root = Self::make_smart_root(
                    self.locking_strategy.latch_type(),
                    self.root.unsafe_borrow().deep_clone(self.locking_strategy().latch_type()));

                let mut commit_handle
                    = self.begin_commit();

                let commit_version = commit_handle
                    .read_handle_version();

                root_internal_page
                    .push_uncommitted(left_fence, commit_version, left, 0);

                root_internal_page
                    .push_uncommitted(right_fence, commit_version, right, 1);

                let mut commit_attempts
                    = 0;

                let committed = loop {
                    match self.try_end_commit(commit_handle) {
                        Ok(commit) if commit_attempts > 0 => unsafe {
                            let versions
                                = root_internal_page.versions_mut();

                            *versions.get_unchecked_mut(0) = commit;
                            *versions.get_unchecked_mut(1) = commit;

                            break commit;
                        },
                        Ok(commit) => break commit,
                        Err(opt) => {
                            commit_attempts += 1;
                            sched_yield(commit_attempts);
                            commit_handle = opt
                        }
                    }
                };

                let old_root
                    = self.root.unsafe_borrow_mut();

                old_root.root.height
                    = height + 1;

                old_root.prev
                    = Some(copy_root);

                old_root.root.version
                    = committed;

                root_internal_page.commit_until(1);

                *old_root.root.block.unsafe_borrow_mut()
                    = mem::take(new_root_block.unsafe_borrow_mut());

                (height + 1, master_guard.deref_mut().unwrap().block())
            }
            BlockSplit::ByVersion(new_root_block) => unsafe {
                let copy_root = Self::make_smart_root(
                    self.locking_strategy.latch_type(),
                    self.root.unsafe_borrow().deep_clone(self.locking_strategy().latch_type()));

                let new_root_block_mut
                    = new_root_block.unsafe_borrow_mut();

                let root_can_shrink
                    = !new_root_block_mut.is_leaf() && new_root_block_mut.active_count() == 1;

                let old_root
                    = self.root.unsafe_borrow_mut();

                if root_can_shrink {
                    let new_root_internal_page
                        = new_root_block_mut.as_internal_page();

                    let (key_intervals,
                        versions,
                        pointers
                    ) = new_root_internal_page
                        .get_pointer(0)
                        .unsafe_borrow()
                        .as_internal_page_ref()
                        .keys_versions_pointers();

                    let active_entries = key_intervals
                        .iter()
                        .zip(versions)
                        .zip(pointers)
                        .filter(|((.., v), ..)| v.is_active())
                        .collect_vec();

                    new_root_internal_page
                        .override_clone(active_entries);
                } else {
                    height += 1;
                    old_root.root.height = height;
                }

                let mut commit_handle
                    = self.begin_commit();

                let mut commit_attempts
                    = 0;

                let committed = loop {
                    match self.try_end_commit(commit_handle) {
                        Ok(commit) =>
                            break commit,
                        Err(opt) => {
                            commit_attempts += 1;
                            sched_yield(commit_attempts);
                            commit_handle = opt
                        }
                    }
                };

                old_root.prev
                    = Some(copy_root);

                old_root.root.version
                    = committed;

                *old_root.root.block.unsafe_borrow_mut()
                    = mem::take(new_root_block_mut);

                (height, master_guard.deref_mut().unwrap().block())
            }
        };

        (master_guard, root_block, root_guard, height)
    }

    #[inline]
    fn retrieve_root_write_internal(&self, attempts: Attempts) -> Result<
        (RootItemGuard<FAN_OUT, NUM_RECORDS, Key>,
         BlockRef<FAN_OUT, NUM_RECORDS, Key>,
         BlockGuard<FAN_OUT, NUM_RECORDS, Key>,
         Height), ()>
    {
        let height
            = self.root.unsafe_borrow().height();

        match self.is_lock(attempts, height) {
            true => {
                let master_guard
                    = self.root.borrow_mut();

                let root_block
                    = master_guard.deref_mut().unwrap().block();

                let root_guard
                    = root_block.borrow_mut();

                match root_guard.deref().unwrap().unsafe_degree() {
                    BlockUnsafeDegree::Overflow =>
                        Ok(self.split_root(master_guard, root_guard, height)),
                    _ => Ok((master_guard, root_block, root_guard, height))
                }
            }
            false if !self.locking_strategy.is_mono_writer() => {
                let mut master_guard
                    = self.root.borrow_read();

                let root_block
                    = master_guard.deref().unwrap().block();

                let mut root_guard
                    = root_block.borrow_read();

                match root_guard.deref().unwrap().unsafe_degree() {
                    BlockUnsafeDegree::Overflow
                    if master_guard.upgrade_write_lock() && root_guard.upgrade_write_lock() =>
                        Ok(self.split_root(master_guard, root_guard, height)),
                    BlockUnsafeDegree::Overflow => Err(()),
                    _ => Ok((master_guard, root_block, root_guard, height))
                }
            }
            _ => {
                let master_guard
                    = self.root.borrow_free();

                let root_block
                    = master_guard.deref_mut().unwrap().block();

                let root_guard
                    = root_block.borrow_free();

                match root_guard.deref().unwrap().unsafe_degree() {
                    BlockUnsafeDegree::Overflow =>
                        Ok(self.split_root(master_guard, root_guard, height)),
                    _ => Ok((master_guard, root_block, root_guard, height)),
                }
            }
        }
    }

    #[inline]
    fn traversal_write_internal(&self, key: Key, attempts: Attempts, max_level: Level)
                                -> Result<BlockGuard<FAN_OUT, NUM_RECORDS, Key>, (LockLevel, Attempts)>
    {
        let (_master,
            mut curr_block,
            mut curr_guard,
            height,
            attempts) = self.retrieve_root_write(attempts);

        let mut curr_level
            = 1 as Height;

        loop {
            match curr_guard.deref().unwrap().as_ref() {
                Node::Index(internal_page) => {
                    let (keys_page, versions_page) = internal_page
                        .keys_versions();

                    let index = versions_page
                        .iter()
                        .enumerate()
                        .rev()
                        .zip(keys_page
                            .iter()
                            .rev())
                        .find(|(.., range)| range.contains(key))
                        .map(|((pos, ..), ..)| pos)
                        .unwrap();

                    let next_curr_block = internal_page
                        .get_pointer(index)
                        .clone();

                    let mut next_curr_guard = self.apply_for_ref(
                        &next_curr_block,
                        height,
                        curr_level,
                        attempts,
                        max_level);

                    let curr_len
                        = keys_page.len();

                    match next_curr_guard.deref().unwrap().unsafe_degree() {
                        BlockUnsafeDegree::Overflow
                        if next_curr_guard.upgrade_write_lock() && curr_guard.upgrade_write_lock()
                            && curr_len == curr_guard.deref().unwrap().len() =>
                            curr_guard = self.on_overflow_node(curr_guard, next_curr_guard, index),
                        BlockUnsafeDegree::ActiveUnderflow
                        if next_curr_guard.upgrade_write_lock() && curr_guard.upgrade_write_lock()
                            && curr_len == curr_guard.deref().unwrap().len() =>
                            curr_guard = self.on_underflow_node(curr_guard, next_curr_guard, index),
                        BlockUnsafeDegree::Ok => {
                            curr_level += 1;
                            curr_guard = next_curr_guard;
                            curr_block = next_curr_block;
                        }
                        _ => return Err((curr_level, attempts + 1))
                    }
                }
                _ => return Ok(curr_guard) // cant have it both ways in non optimistic latching protocol
            }
        }
    }

    pub(crate) fn on_overflow_node<'a>(
        &self,
        mufasa: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key>,
        simba: BlockGuard<FAN_OUT, NUM_RECORDS, Key>,
        child_index: usize) -> BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key>
    {
        let mufasa_deref_mut
            = mufasa.deref_mut().unwrap();

        let internal_page
            = mufasa_deref_mut.as_internal_page();

        let fence = internal_page
            .get_key(child_index)
            .clone();

        let current_len
            = internal_page.len();

        match self.split(simba.deref().unwrap(), &fence) {
            BlockSplit::ByKey(left_fence,
                              left,
                              right_fence,
                              right
            ) => {
                let mut commit_handle
                    = self.begin_commit();

                internal_page.push_uncommitted(
                    left_fence,
                    commit_handle.read_handle_version(),
                    left,
                    current_len);

                internal_page.push_uncommitted(
                    right_fence,
                    commit_handle.read_handle_version(),
                    right,
                    current_len + 1);

                let mut commit_attempts
                    = 0;

                loop {
                    match self.try_end_commit(commit_handle) {
                        Ok(commit) if commit_attempts > 0 => unsafe {
                            let versions
                                = internal_page.versions_mut();

                            *versions.get_unchecked_mut(current_len) = commit;
                            *versions.get_unchecked_mut(current_len + 1) = commit;

                            break;
                        }
                        Ok(..) => break,
                        Err(opt) => {
                            commit_attempts += 1;
                            sched_yield(commit_attempts);
                            commit_handle = opt
                        }
                    }
                }

                internal_page.commit_until(current_len + 1)
            }
            BlockSplit::ByVersion(fresh) => {
                let mut commit_handle
                    = self.begin_commit();

                internal_page.push_uncommitted(
                    fence,
                    commit_handle.read_handle_version(),
                    fresh.clone(),
                    current_len);

                let mut commit_attempts
                    = 0;

                loop {
                    match self.try_end_commit(commit_handle) {
                        Ok(commit) if commit_attempts > 0 => {
                            *internal_page
                                .get_version_mut(current_len) = commit;

                            break;
                        }
                        Ok(..) => break,
                        Err(opt) => {
                            commit_attempts += 1;
                            sched_yield(commit_attempts);
                            commit_handle = opt
                        }
                    }
                }

                internal_page.commit_until(current_len)
            }
        }

        mufasa
    }

    pub(crate) fn on_underflow_node<'a>(
        &self,
        mufasa: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key>,
        simba: BlockGuard<FAN_OUT, NUM_RECORDS, Key>,
        index_simba: usize)
        -> BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key>
    {
        let mufasa_deref_mut
            = mufasa.deref_mut().unwrap();

        match self.merge(mufasa_deref_mut, simba.deref().unwrap(), index_simba) {
            MergeResult::Merged(
                index_sibling,
                fence_sibling,
                merged_block,
                _candidate_guard
            ) => {
                let mufasa_internal_page = mufasa_deref_mut
                    .as_internal_page();

                let mufasa_len
                    = mufasa_internal_page.len();

                let mut merged_fence = mufasa_internal_page
                    .get_key(index_simba)
                    .clone();

                merged_fence.merged(&fence_sibling);

                let mut commit_handle
                    = self.begin_commit();

                mufasa_internal_page.push_uncommitted(
                    merged_fence,
                    commit_handle.read_handle_version(),
                    merged_block,
                    mufasa_len);

                let mut commit_attempts
                    = 0;

                loop {
                    match self.try_end_commit(commit_handle) {
                        Ok(commit) if commit_attempts > 0 => {
                            *mufasa_internal_page
                                .get_version_mut(mufasa_len) = commit;

                            break;
                        }
                        Ok(..) => break,
                        Err(opt) => {
                            commit_attempts += 1;
                            sched_yield(commit_attempts);
                            commit_handle = opt
                        }
                    }
                }

                mufasa_internal_page
                    .mark_version_obsolete(index_sibling);

                mufasa_internal_page
                    .mark_version_obsolete(index_simba);

                mufasa_internal_page
                    .commit_until(mufasa_len);

                mufasa
            }
            MergeResult::KeySplit(
                index_sibling,
                BlockSplit::ByKey(left_interval,
                                  left,
                                  right_interval,
                                  right),
                _candidate_guard
            ) => {
                let mufasa_internal_page = mufasa_deref_mut
                    .as_internal_page();

                let mufasa_len
                    = mufasa_internal_page.len();

                let mut commit_handle
                    = self.begin_commit();

                mufasa_internal_page.push_uncommitted(
                    left_interval,
                    commit_handle.read_handle_version(),
                    left,
                    mufasa_len);

                mufasa_internal_page.push_uncommitted(
                    right_interval,
                    commit_handle.read_handle_version(),
                    right,
                    mufasa_len + 1);

                let mut commit_attempts
                    = 0;

                loop {
                    match self.try_end_commit(commit_handle) {
                        Ok(commit) if commit_attempts > 0 => {
                            *mufasa_internal_page
                                .get_version_mut(mufasa_len) = commit;

                            *mufasa_internal_page
                                .get_version_mut(mufasa_len + 1) = commit;

                            break;
                        }
                        Ok(..) => break,
                        Err(opt) => {
                            commit_attempts += 1;
                            sched_yield(commit_attempts);
                            commit_handle = opt
                        }
                    }
                }

                mufasa_internal_page
                    .mark_version_obsolete(index_sibling);

                mufasa_internal_page
                    .mark_version_obsolete(index_simba);

                mufasa_internal_page
                    .commit_until(mufasa_len + 1);

                mufasa
            }
            _ => unreachable!("Jesus Christ! Call a Priest!")
        }
    }
}