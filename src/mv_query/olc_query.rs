use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use crate::mv_block::block::BlockGuard;
use crate::mv_page_model::{Attempts, BlockRef};
use crate::mv_page_model::internal_page::TimeMatcher;
use crate::mv_page_model::node::PageType;
use crate::mv_test;
use crate::mv_test::{LOG_REORG, VERBOSE};
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_sync::smart_cell::sched_yield;
use crate::mv_tree::smo::BlockUnsafeDegree;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
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
                Ok(guard) => {
                    // RESTARTS_COUNTER
                    //     .get(attempt as usize)
                    //     .inspect(|a| { a.fetch_add(1, Relaxed); });
                    break guard
                },
            }
        }
    }

    #[inline]
    pub(crate) fn retrieve_root_write_olc(
        &self,
        mut attempts: Attempts,
    ) -> (BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        Attempts)
    {
        loop {
            match self.retrieve_root_write_internal_olc(attempts) {
                Ok(guard) =>
                    break (guard, attempts),
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
    fn retrieve_root_write_internal_olc(&self, mut attempts: Attempts) -> Result<
        BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
        let root 
            = self.root.clone();

        let height
            = root.height();

        let mut master_guard
            = root.borrow_read();

        let root_block
            = master_guard.block();

        let root_guard
            = root_block.borrow_read();

        if LOG_REORG {
            let r
                = root_guard.deref().unsafe_degree_root();

            match r {
                BlockUnsafeDegree::Overflow => unsafe {
                    mv_test::SPLITS_ROOT_COUNTER.lock().push(self.current_version_for_reader())
                }
                BlockUnsafeDegree::ActiveUnderflow => unsafe {
                    mv_test::MERGE_ROOT_COUNTER.lock().push(self.current_version_for_reader())
                }
                _ => {}
            }
        }
        match root_guard.deref().unsafe_degree_root() {
            BlockUnsafeDegree::Overflow
            if master_guard.upgrade_write_lock()
            => Ok(self.split_root(master_guard, root_guard, height)),
            BlockUnsafeDegree::ActiveUnderflow
            if master_guard.upgrade_write_lock() =>
                self.merge_root(master_guard, root_guard, height),
            BlockUnsafeDegree::Ok
            => Ok(root_guard),
            _ => Err(()),
        }
    }

    #[inline]
    fn traversal_write_internal_olc(&self, key: Key, attempts: Attempts)
    -> Result<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, Attempts>
    {
        let (mut curr_guard,
            attempts) = self.retrieve_root_write_olc(attempts);

        let mut i =  0;
        loop {
            if VERBOSE {
                println!("traversal_write_internal_olc: Loop: {i}, attempts {attempts}, key: {key}");
                i += 1;
            }
            let curr_guard_result
                = curr_guard.deref();

            match curr_guard_result.as_page_ref() {
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

                    let next_curr_guard
                        = internal_page
                        .get_pointer(index).borrow_read();

                    if LOG_REORG {
                        let r
                            = next_curr_guard.deref().unsafe_degree();

                        match r {
                            BlockUnsafeDegree::Overflow =>
                                mv_test::SPLITS_COUNTER.lock().push(self.current_version_for_reader()),
                            BlockUnsafeDegree::ActiveUnderflow =>
                                mv_test::MERGES_COUNTER.lock().push(self.current_version_for_reader()),
                            _ => {}
                        }
                    }
                    match next_curr_guard.unsafe_degree() {
                        BlockUnsafeDegree::Overflow // next_curr_guard.upgrade_write_lock() &&
                        if curr_guard.upgrade_write_lock()
                            => curr_guard = self.on_overflow_node(curr_guard, next_curr_guard, index),
                        BlockUnsafeDegree::ActiveUnderflow // next_curr_guard.upgrade_write_lock() &&
                        if  curr_guard.upgrade_write_lock()
                        => match self.on_underflow_node(curr_guard, next_curr_guard, index) {
                                Ok(guard) => curr_guard = guard,
                                Err(..) => {
                                    if VERBOSE {
                                        println!("traversal_write_internal_olc: on_underflow_node Err()");
                                    }
                                    return Err(attempts + 1)
                                }
                            },
                        BlockUnsafeDegree::Ok => curr_guard = next_curr_guard,
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