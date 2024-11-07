use std::fmt::Display;
use std::hash::Hash;
use crate::mv_block::block::{BlockGuard, BlockUnsafeDegree};
use crate::mv_page_model::{Attempts, BlockRef, Height, Level};
use crate::mv_page_model::node::PageType;
use crate::mv_tree::mvbplus_tree::{MVBPlusTree, LockLevel, MAX_TREE_HEIGHT};
use crate::mv_utils::smart_cell::sched_yield;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static + Display,
    Payload: Clone + Default + 'static
> MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    pub(crate) fn traversal_write_olc(&self, key: Key) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
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
    ) -> (
        //RootItemGuard<FAN_OUT, NUM_RECORDS, Key>,
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        Height,
        Attempts)
    {
        loop {
            match self.retrieve_root_write_internal_olc(attempts) {
                Ok((block, guard, height)) =>
                    break (block, guard, height, attempts),
                _ => {
                    attempts += 1;
                    sched_yield(attempts);
                }
            }
        }
    }

    #[inline]
    fn retrieve_root_write_internal_olc(&self, _attempts: Attempts) -> Result<
        (
            //RootItemGuard<FAN_OUT, NUM_RECORDS, Key>,
            BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
            BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
            Height), ()>
    {
        let height
            = self.root.unsafe_borrow().height();
        
        let mut master_guard
            = self.root.borrow_read();

        let root_block
            = master_guard.deref().unwrap().block();

        let mut root_guard
            = root_block.borrow_read();

        match root_guard.deref().unwrap().unsafe_degree() {
            BlockUnsafeDegree::Overflow
            if master_guard.upgrade_write_lock() && // only deadlock free cuz non-blocking
                root_guard.upgrade_write_lock()
            => Ok(self.split_root(master_guard, root_guard, height)),
            _ if master_guard.is_valid() => Ok((root_block, root_guard, height)),
            _ => Err(()),
        }
    }

    #[inline]
    fn traversal_write_internal_olc(&self, key: Key, attempts: Attempts, max_level: Level)
                                    -> Result<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, (LockLevel, Attempts)>
    {
        let (mut _curr_block,
            mut curr_guard,
            height,
            attempts) = self.retrieve_root_write_olc(attempts);

        let mut curr_level
            = 1 as Height;

        loop {
            let curr_guard_result
                = curr_guard.deref();

            match curr_guard_result.unwrap().as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    let keys_page = internal_page
                        .keys();

                    let index = keys_page
                        .iter()
                        .enumerate()
                        .rev()
                        .find(|(.., range)| range.contains(key))
                        .map(|(pos, ..)| pos);

                    if let None = index {
                        return Err((curr_level, attempts + 1));
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

                    match next_curr_guard.deref().unwrap().unsafe_degree() {
                        BlockUnsafeDegree::Overflow
                        if next_curr_guard.upgrade_write_lock() && curr_guard.upgrade_write_lock()
                        /*&& curr_len == curr_guard.deref().unwrap().len()*/ =>
                            curr_guard = self.on_overflow_node(curr_guard, next_curr_guard, index),
                        BlockUnsafeDegree::ActiveUnderflow
                        if next_curr_guard.upgrade_write_lock() && curr_guard.upgrade_write_lock()
                        /*&& curr_len == curr_guard.deref().unwrap().len()*/ =>
                            match self.on_underflow_node(curr_guard, next_curr_guard, index) {
                                Ok(guard) => curr_guard = guard,
                                Err(..) => return Err((curr_level - 1, attempts + 1))
                            },
                        BlockUnsafeDegree::Ok => {
                            curr_level += 1;
                            curr_guard = next_curr_guard;
                            _curr_block = next_curr_block;
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