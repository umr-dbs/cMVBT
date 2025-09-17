use std::fmt::Display;
use std::hash::Hash;

use crate::mv_block::block::{BlockGuard, BlockUnsafeDegree};
use crate::mv_page_model::{Attempts, BlockRef};
use crate::mv_page_model::internal_page::TimeMatcher;
use crate::mv_page_model::node::PageType;
use crate::mv_test;
use crate::mv_test::{LOG_REORG, VERBOSE};
use crate::mv_tree::mvbplus_tree::MVBPlusTree;
use crate::mv_sync::smart_cell::sched_yield;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    pub(crate) fn traversal_write_olc(&self, key: Key) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let mut attempt = 0;

        loop {
            match self.traversal_write_internal_olc(key, attempt) {
                Err(n_attempt) => {
                    attempt = n_attempt;

                    sched_yield(attempt);
                }
                Ok(guard) => break guard,
            }
        }
    }

    pub(crate) fn retrieve_root_write_olc(
        &self,
        mut attempts: Attempts,
    ) -> (
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        Attempts)
    {
        loop {
            match self.retrieve_root_write_internal_olc(attempts) {
                Ok((block, guard)) =>
                    break (block, guard, attempts),
                _ => {
                    attempts += 1;
                    if VERBOSE {
                        println!("retrieve_root_write_internal_olc: attempts {:?}", attempts);
                    }
                    sched_yield(attempts);
                }
            }
        }
    }

    #[inline]
    fn retrieve_root_write_internal_olc(&self, attempts: Attempts) -> Result<
        (BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
         BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>), ()>
    {
        let root 
            = self.root.clone();

        let height
            = root.height();

        let mut master_guard
            = root.borrow_read();

        let root_block
            = master_guard.block();

        let mut root_guard
            = root_block.borrow_read();

        if LOG_REORG {
            let r
                = root_guard.deref().unwrap().unsafe_degree_root();

            match r {
                BlockUnsafeDegree::Overflow => unsafe {
                    mv_test::SPLITS_ROOT_COUNTER.lock().push(self.current_version())
                }
                BlockUnsafeDegree::ActiveUnderflow => unsafe {
                    mv_test::MERGE_ROOT_COUNTER.lock().push(self.current_version())
                }
                _ => {}
            }
        }
        match root_guard.deref().unwrap().unsafe_degree_root() {
            BlockUnsafeDegree::Overflow
            if master_guard.upgrade_write_lock() && // only deadlock free cuz non-blocking
                root_guard.upgrade_write_lock()
            => Ok(self.split_root(master_guard, root_guard, height)),
            BlockUnsafeDegree::ActiveUnderflow
            if master_guard.upgrade_write_lock() && root_guard.upgrade_write_lock() =>
                self.merge_root(master_guard, root_guard, height)
                    .or(self.retrieve_root_write_internal_olc(attempts + 1)),
            BlockUnsafeDegree::Ok
            => Ok((root_block, root_guard)),
            _ => Err(()),
        }
    }

    #[inline]
    fn traversal_write_internal_olc(&self, key: Key, attempts: Attempts)
    -> Result<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, Attempts>
    {
        let (mut _curr_block,
            mut curr_guard,
            attempts) = self.retrieve_root_write_olc(attempts);

        let mut i =  0;
        loop {
            if VERBOSE {
                println!("traversal_write_internal_olc: Loop: {i}, attempts {attempts}, key: {key}");
                i += 1;
            }
            let curr_guard_result
                = curr_guard.deref();

            match curr_guard_result.unwrap().as_page_ref() {
                PageType::IndexRef(internal_page) => unsafe {
                    let (keys_page, versions_page) = internal_page
                        .keys_versions();

                    let index = keys_page
                        .iter()
                        .enumerate()
                        .rfind(|(pos, range)|
                            versions_page.get_unchecked(*pos).is_active() &&
                                range.contains(key))
                        .map(|(pos, ..)| pos);

                    if let None = index {
                        if VERBOSE {
                            println!("traversal_write_internal_olc: None Index");
                        }
                        return Err(attempts + 1);
                    }

                    let index
                        = index.unwrap();

                    let next_curr_block = internal_page
                        .get_pointer(index)
                        .clone();

                    let mut next_curr_guard
                        = next_curr_block.borrow_read();

                    if LOG_REORG {
                        let r
                            = next_curr_guard.deref().unwrap().unsafe_degree();

                        match r {
                            BlockUnsafeDegree::Overflow =>
                                mv_test::SPLITS_COUNTER.lock().push(self.current_version()),
                            BlockUnsafeDegree::ActiveUnderflow =>
                                mv_test::MERGES_COUNTER.lock().push(self.current_version()),
                            _ => {}
                        }
                    }
                    match next_curr_guard.deref().unwrap().unsafe_degree() {
                        BlockUnsafeDegree::Overflow
                        if next_curr_guard.upgrade_write_lock() && curr_guard.upgrade_write_lock()
                            => curr_guard = self.on_overflow_node(curr_guard, next_curr_guard, index),
                        BlockUnsafeDegree::ActiveUnderflow
                        if next_curr_guard.upgrade_write_lock() && curr_guard.upgrade_write_lock()
                        => match self.on_underflow_node(curr_guard, next_curr_guard, index) {
                                Ok(guard) => curr_guard = guard,
                                Err(..) => {
                                    if VERBOSE {
                                        println!("traversal_write_internal_olc: on_underflow_node Err()");
                                    }
                                    return Err(attempts + 1)
                                }
                            },
                        BlockUnsafeDegree::Ok => {
                            curr_guard = next_curr_guard;
                            _curr_block = next_curr_block;
                        }
                        _ => return Err(attempts + 1)
                    }
                }
                _ => return if curr_guard.upgrade_write_lock() {
                    Ok(curr_guard)
                } else {
                    if VERBOSE {
                        println!("traversal_write_internal_olc: upgrade_write_lock Err()");
                    }
                    Err(attempts + 1)
                }
            }
        }
    }
}