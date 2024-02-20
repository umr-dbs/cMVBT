use std::fmt::Display;
use std::hash::Hash;
use itertools::Itertools;
use crate::block::block::{BlockGuard, BlockUnsafeDegree};
use crate::page_model::{Attempts, BlockRef, Height, Level};
use crate::page_model::internal_page::TimeMatcher;
use crate::page_model::node::{Node, PageType};
use crate::tree::mvbplus_tree::{MVBPlusTree, LockLevel, MAX_TREE_HEIGHT, RootItemGuard};
use crate::utils::smart_cell::sched_yield;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static + Display
> MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>
{
    #[inline]
    pub(crate) fn traversal_write_olc(&self, key: Key) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key> {
        let mut attempt = 0;
        let mut lock_level = MAX_TREE_HEIGHT;

        loop {
            match self.traversal_write_internal_olc(key, attempt, lock_level) {
                Err((n_lock_level, n_attempt)) => {
                    attempt = n_attempt;
                    lock_level = n_lock_level;

                    sched_yield(attempt);
                }
                Ok(guard) => break guard,
            }
        }
    }

    fn retrieve_root_write_olc(
        &self,
        mut attempts: Attempts,
    ) -> (RootItemGuard<FAN_OUT, NUM_RECORDS, Key>,
          BlockRef<FAN_OUT, NUM_RECORDS, Key>,
          BlockGuard<FAN_OUT, NUM_RECORDS, Key>,
          Height,
          Attempts)
    {
        loop {
            match self.retrieve_root_write_internal_olc(attempts) {
                Ok((master, block, guard, height)) =>
                    break (master, block, guard, height, attempts),
                _ => {
                    attempts += 1;
                    sched_yield(attempts);
                }
            }
        }
    }

    #[inline]
    fn retrieve_root_write_internal_olc(&self, attempts: Attempts) -> Result<
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

                if !master_guard.is_valid() {
                    return Err(());
                }

                let root_block
                    = master_guard.deref_mut().unwrap().block();

                let root_guard
                    = root_block.borrow_mut();

                if !root_guard.is_valid() {
                    return Err(())
                }

                match root_guard.deref().unwrap().unsafe_degree() {
                    BlockUnsafeDegree::Overflow =>
                        Ok(self.split_root(master_guard, root_guard, height)),
                    _ => Ok((master_guard, root_block, root_guard, height))
                }
            }
            false => {
                let mut master_guard
                    = self.root.borrow_read();

                let master_guard_result
                    = master_guard.deref();

                if let None = master_guard_result {
                    return Err(());
                }

                let root_block
                    = master_guard_result.unwrap().block();

                let mut root_guard
                    = root_block.borrow_read();

                let root_guard_result
                    = root_guard.deref();

                if let None = root_guard_result {
                    return Err(());
                }

                let guard_deref_ref
                    = root_guard_result.unwrap();

                let curr_len = guard_deref_ref
                    .len();

                match guard_deref_ref.unsafe_degree() {
                    BlockUnsafeDegree::Overflow
                    if master_guard.upgrade_write_lock() && // only deadlock free cuz non-blocking; orwc fails here
                        root_guard.upgrade_write_lock() &&
                        root_guard.deref().unwrap().len() == curr_len
                    => Ok(self.split_root(master_guard, root_guard, height)),
                    BlockUnsafeDegree::Overflow => Err(()),
                    _ => Ok((master_guard, root_block, root_guard, height))
                }
            }
        }
    }

    #[inline]
    fn traversal_write_internal_olc(&self, key: Key, attempts: Attempts, max_level: Level)
                                    -> Result<BlockGuard<FAN_OUT, NUM_RECORDS, Key>, (LockLevel, Attempts)>
    {
        let (_master,
            mut curr_block,
            mut curr_guard,
            height,
            attempts) = self.retrieve_root_write_olc(attempts);

        let mut curr_level
            = 1 as Height;

        loop {
            let curr_guard_result
                = curr_guard.deref();

            if let None = curr_guard_result {
                return Err((curr_level, attempts + 1));
            }

            match curr_guard_result.unwrap().as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    let keys_page = internal_page
                        .keys();

                    let index = keys_page
                        .iter()
                        .enumerate()
                        .rev()
                        .find(|(.., range)| range.contains(key))
                        .map(|(pos, ..)| pos)
                        .unwrap();

                    let next_curr_block = internal_page
                        .get_pointer(index)
                        .clone();

                    if !curr_guard.is_valid() {
                        return Err((curr_level, attempts + 1));
                    }

                    let mut next_curr_guard = self.apply_for_ref(
                        &next_curr_block,
                        height,
                        curr_level,
                        attempts,
                        max_level);

                    let next_curr_guard_result
                        = next_curr_guard.deref();

                    if let None = next_curr_guard_result {
                        return Err((curr_level, attempts + 1));
                    }

                    let curr_len
                        = keys_page.len();

                    match next_curr_guard_result.unwrap().unsafe_degree() {
                        BlockUnsafeDegree::Overflow
                        if next_curr_guard.upgrade_write_lock() && curr_guard.upgrade_write_lock()
                            && curr_len == curr_guard.deref().unwrap().len() =>
                            curr_guard = self.on_overflow_node(curr_guard, next_curr_guard, index),
                        BlockUnsafeDegree::ActiveUnderflow
                        if next_curr_guard.upgrade_write_lock() && curr_guard.upgrade_write_lock()
                            && curr_len == curr_guard.deref().unwrap().len() =>
                            match self.on_underflow_node(curr_guard, next_curr_guard, index) {
                                Ok(guard) => curr_guard = guard,
                                Err(..) => return Err((curr_level - 1, attempts + 1))
                            },
                        BlockUnsafeDegree::Ok => {
                            curr_level += 1;
                            curr_guard = next_curr_guard;
                            curr_block = next_curr_block;
                        }
                        _ => return Err((curr_level - 1, attempts + 1))
                    }
                }
                _ => return if curr_guard.upgrade_write_lock() {
                    Ok(curr_guard)
                } else {
                    Err((curr_level - 1, attempts + 1))
                }
            }
        }
    }
}