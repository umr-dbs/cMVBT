use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::{Acquire, SeqCst};
use itertools::Itertools;
use crate::block::block::{Block, BlockGuard, BlockSplit, BlockUnsafeDegree};
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::page_model::{Attempts, BlockRef, Height, Level};
use crate::page_model::internal_page::TimeMatcher;
use crate::page_model::node::PageType;
use crate::record_model::record_point::RecordPointResult;
use crate::record_model::version_info::Version;
use crate::tree::mvbplus_tree::{MVBPlusTree, LockLevel, MAX_TREE_HEIGHT, SmartRoot, RootItemGuard, MergeResult};
use crate::tx_model::transaction::SnapShot;
use crate::tx_model::tx_api::IsolatedSnapShot;
use crate::utils::interval::Interval;
use crate::utils::smart_cell::{sched_yield, SmartCell};

pub struct RangeQueryIter<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static + Display,
    Payload: Clone + Default + 'static
> {
    pub(crate) isolated_snapshot: IsolatedSnapShot<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub(crate) range: Interval<Key>,
    path: Vec<(Interval<Key>, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)>,
    buff: VecDeque<RecordPointResult<Key, Payload>>,
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static + Display,
    Payload: Clone + Default
> RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub const fn new(tree: &'a MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>, version: Version, range: Interval<Key>) -> Self {
        Self {
            isolated_snapshot: IsolatedSnapShot(version, tree),
            range,
            path: vec![],
            buff: VecDeque::new(),
        }
    }

    #[inline(always)]
    pub const fn si(&self) -> &IsolatedSnapShot<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
        &self.isolated_snapshot
    }

    #[inline(always)]
    pub const fn snapshot(&self) -> SnapShot {
        self.si().snapshot()
    }

    #[inline(always)]
    pub const fn mv_tree(&self) -> &MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.si().mv_tree()
    }
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static + Display,
    Payload: Clone + Default + 'static
> Iterator for RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
    type Item = RecordPointResult<Key, Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.buff.is_empty() {
            return self.buff.pop_front();
        }

        if self.range.lower > self.range.upper {
            return None;
        }

        if self.path.is_empty() {
            let smart_root = self
                .mv_tree()
                .retrieve_root_for(self.snapshot());

            self.path.push((
                Interval::new(self.mv_tree().min_key,
                              self.mv_tree().max_key),
                smart_root.unsafe_borrow().block())
            );
        }

        loop {
            let (mut fence, mut block)
                = self.path.pop().unwrap();

            while fence.upper < self.range.lower {
                match self.path.pop() {
                    Some((f, b)) => {
                        fence = f;
                        block = b;
                    }
                    _ => return None,
                }
            }

            self.path.push((fence.clone(), block.clone()));
            match block.unsafe_borrow().as_ref().as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    let (keys_page, versions_page) = internal_page
                        .keys_versions();

                    match versions_page
                        .iter()
                        .enumerate()
                        .rev()
                        .zip(keys_page
                            .iter()
                            .rev())
                        .filter(|((.., v), ..)| v.matched(self.snapshot()))
                        .find(|(.., range)| range.contains(self.range.lower))
                        .map(|((pos, ..), key)| (key.clone(), internal_page
                            .get_pointer(pos)
                            .clone()))
                    {
                        Some(next) => self.path.push(next),
                        _ => self.range.lower = (self.isolated_snapshot.mv_tree().inc_key)(fence.upper)
                    }
                }
                PageType::LeafRef(leaf_page) => {
                    self.buff.extend(leaf_page
                        .as_records()
                        .iter()
                        .rev()
                        .filter(|r| r.version().matches(self.snapshot()))
                        .filter(|r| self.range.contains(r.key()))
                        .sorted_by_key(|r| r.key())
                        .map(|r| RecordPointResult::from(r)));

                    self.range.lower = (self.isolated_snapshot.mv_tree().inc_key)(fence.upper);
                    self.path.pop();
                    break;
                }
                _ => unreachable!()
            }
        }

        self.buff.pop_front()
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static + Display,
    Payload: Clone + Default + 'static
> MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub(crate) fn retrieve_root_for(&self, lookup_version: Version) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let mut root_anker
            = self.root.clone();

        loop {
            let root_item
                = root_anker.unsafe_borrow();

            if root_item.version().le_other_any(lookup_version) {
                break root_anker;
            } else {
                root_anker = match root_item.prev {
                    Some(ref p_root) => p_root.clone(),
                    _ => unreachable!()
                };
            }
        }
    }

    // pub(crate) fn retrieve_root_latest(&self) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key> {
    //     self.retrieve_root_for(Version::MAX)
    // }

    fn traverse_read_key(
        mut curr: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        key: Key,
        lookup_version: Version)
        -> Result<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
        while let PageType::IndexRef(internal_page) = curr
            .unsafe_borrow()
            .as_page_ref()
        {
            let (keys_page, versions_page) = internal_page
                .keys_versions();

            curr = versions_page
                .iter()
                .zip(keys_page)
                .enumerate()
                .rev()
                .filter(|(.., (v, ..))| v.matched(lookup_version))
                .find(|(.., (.., range))| range.contains(key))
                .map(|(pos, ..)| internal_page.get_pointer(pos))
                .cloned()
                .ok_or(())?
        }

        Ok(curr)
    }

    #[inline]
    fn traverse_read_key_range(
        mut curr: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        lookup_range: &Interval<Key>,
        lookup_version: Version)
        -> Vec<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>>
    {
        let mut blocks
            = VecDeque::new();

        blocks.push_back(curr);

        let mut leafs
            = vec![];

        while !blocks.is_empty() {
            curr = blocks.pop_front().unwrap();

            match curr.unsafe_borrow().as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    let (keys_page, versions_page) = internal_page
                        .keys_versions();

                    blocks.extend(versions_page
                        .iter()
                        .enumerate()
                        .rev()
                        .zip(keys_page
                            .iter()
                            .rev())
                        .filter(|((.., v), ..)| v.matched(lookup_version))
                        .filter(|(.., range)| lookup_range.overlap(range))
                        .unique_by(|(.., range)| range.lower())
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
        root: SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload>,
        key: Key,
        lookup_version: Version)
        -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        match Self::traverse_read_key(
            root.unsafe_borrow().block(),
            key,
            lookup_version)
        {
            Ok(leaf) => match leaf
                .unsafe_borrow()
                .as_records()
                .iter()
                .rev()
                .filter(|r| r.key() == key)
                .find(|r| r.version().matches(lookup_version))
            {
                None => CRUDOperationResult::MatchedRecords(Vec::with_capacity(0)),
                Some(result) => CRUDOperationResult::MatchedRecords(vec![RecordPointResult::from(result)])
            },
            Err(..) => CRUDOperationResult::MatchedRecords(Vec::with_capacity(0))
        }
    }

    #[inline]
    pub(crate) fn key_range_read_from_root(
        root: SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload>,
        lookup_range: Interval<Key>,
        lookup_version: Version)
        -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let blocks = Self::traverse_read_key_range(
            root.unsafe_borrow().block(),
            &lookup_range,
            lookup_version);

        CRUDOperationResult::MatchedRecords(blocks
            .into_iter()
            .map(|leaf| leaf
                .unsafe_borrow()
                .as_records()
                .iter()
                .rev()
                .filter(|r| lookup_range.contains(r.key()))
                .filter(|r| r.version().matches(lookup_version))
                .sorted_by_key(|r| r.key())
                .map(|r| RecordPointResult::from(r))
                .collect::<Vec<_>>())
            .flatten()
            .collect())
    }

    #[inline]
    pub(crate) fn traversal_write(&self, key: Key) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
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
    ) -> (
        //RootItemGuard<FAN_OUT, NUM_RECORDS, Key>,
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        Height,
        Attempts)
    {
        loop {
            match self.retrieve_root_write_internal(attempts) {
                Ok((block, guard, height)) =>
                    break (block, guard, height, attempts),
                _ => attempts += 1
            }
        }
    }

    pub(crate) fn split_root<'a>(
        &self,
        mut master_guard: RootItemGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        mut root_guard: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        mut height: Height,
    ) -> (
        //RootItemGuard<'a, FAN_OUT, NUM_RECORDS, Key>,
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
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
                    .new_empty_index_block(self.locking_strategy.latch_type());

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

                // let root_can_shrink
                //     = !new_root_block_mut.is_leaf() && new_root_block_mut.active_count() == 1;

                let old_root
                    = self.root.unsafe_borrow_mut();

                // if root_can_shrink {
                //     *new_root_block_mut = new_root_block_mut
                //         .as_internal_page_ref()
                //         .get_pointer(0)
                //         .unsafe_borrow()
                //         .clone();
                //
                //     height -= 1;
                // }

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

                // Do we need a release fence here?
                old_root.root.version
                    = committed;

                old_root.root.height
                    = height;

                *old_root.root.block.unsafe_borrow_mut()
                    = mem::take(new_root_block_mut);

                (height, master_guard.deref_mut().unwrap().block())
            }
        };

        (root_block, root_guard, height)
    }

    #[inline]
    fn retrieve_root_write_internal(&self, attempts: Attempts) -> Result<
        (
            //RootItemGuard<FAN_OUT, NUM_RECORDS, Key>,
            BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
            BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
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
                    _ => Ok((root_block, root_guard, height))
                }
            }
            false if !self.locking_strategy.is_mono_writer() => {
                let mut master_guard
                    = self.root.borrow_read();

                let master_v
                    = master_guard.deref().unwrap().version();

                let root_block
                    = master_guard.deref().unwrap().block();

                let mut root_guard
                    = root_block.borrow_read();

                let len = root_guard.deref().unwrap().len();
                match root_guard.deref().unwrap().unsafe_degree() {
                    BlockUnsafeDegree::Overflow
                    if master_guard.upgrade_write_lock() &&
                        root_guard.upgrade_write_lock() &&
                        len == root_guard.deref().unwrap().len() &&
                        master_v == master_guard.deref().unwrap().version()
                    => Ok(self.split_root(master_guard, root_guard, height)),
                    BlockUnsafeDegree::Overflow => Err(()),
                    _ => {
                        fence(Acquire);
                        Ok((root_block, root_guard, height))
                    }
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
                    _ => Ok((root_block, root_guard, height)),
                }
            }
        }
    }

    #[inline]
    fn traversal_write_internal(&self, key: Key, attempts: Attempts, max_level: Level)
                                -> Result<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, (LockLevel, Attempts)>
    {
        let (mut curr_block,
            mut curr_guard,
            height,
            attempts) = self.retrieve_root_write(attempts);

        let mut curr_level
            = 1 as Height;

        loop {
            match curr_guard.deref().unwrap().as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    // fence(Acquire);

                    let keys_page = internal_page
                        .keys();

                    let index = keys_page
                        .iter()
                        .enumerate()
                        .rev()
                        .find(|(.., range)| range.contains(key))
                        .map(|(pos, ..)| pos);

                    if let None = index {
                        return Err((curr_level, attempts + 1))
                    }

                    let index
                        = index.unwrap();

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

                    if let None = next_curr_guard.deref() {
                        return Err((curr_level, attempts + 1))
                    }

                    match next_curr_guard.deref().unwrap().unsafe_degree() {
                        BlockUnsafeDegree::Overflow
                        if next_curr_guard.upgrade_write_lock() && curr_guard.upgrade_write_lock()
                            && curr_len == curr_guard.deref().unwrap().len() =>
                            curr_guard = self.on_overflow_node(curr_guard, next_curr_guard, index),
                        BlockUnsafeDegree::ActiveUnderflow
                        if next_curr_guard.upgrade_write_lock() && curr_guard.upgrade_write_lock()
                            && curr_len == curr_guard.deref().unwrap().len() =>
                            curr_guard = self.on_underflow_node(curr_guard, next_curr_guard, index)
                                .unwrap(),
                        BlockUnsafeDegree::Ok if curr_guard.deref().unwrap().len() == curr_len => {
                            curr_level += 1;
                            curr_guard = next_curr_guard;
                            curr_block = next_curr_block;
                        }
                        _ => return Err((curr_level, attempts + 1))
                    }
                    fence(SeqCst);
                }
                _ => return if curr_guard.upgrade_write_lock() {
                    Ok(curr_guard)
                } else {
                    Err((curr_level, attempts + 1))
                }
            }
        }
    }

    pub(crate) fn on_overflow_node<'a>(
        &self,
        mufasa: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        child_index: usize) -> BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
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

                internal_page.mark_version_obsolete(child_index);
                internal_page.commit_until(current_len + 1);
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

                internal_page.mark_version_obsolete(child_index);
                internal_page.commit_until(current_len);
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
        simba: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        index_simba: usize)
        -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
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
}