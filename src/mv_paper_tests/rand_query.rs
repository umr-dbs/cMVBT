use std::fmt::Display;
use std::hash::Hash;
use crate::mv_block::block::{BlockGuard, BlockUnsafeDegree};
use crate::mv_page_model::{Attempts, BlockRef};
use crate::mv_page_model::internal_page::{Fence, TimeMatcher};
use crate::mv_page_model::node::PageType;
use crate::mv_test;
use crate::mv_test::{LOG_REORG, VERBOSE};
use crate::mv_tree::mvbplus_tree::MVBPlusTree;
use crate::mv_utils::interval::Interval;
use crate::mv_utils::smart_cell::sched_yield;

pub const RAND_ATTEMPTS_MAX: Attempts = 10; // for insertion generation upper bound

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    pub(crate) fn traversal_write_rand_query(&self) -> (Fence<Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>) {
        let mut attempt = 0;

        loop {
            match self.traversal_write_internal_rand(attempt) {
                Err(n_attempt) => {
                    attempt = n_attempt;

                    sched_yield(attempt);
                }
                Ok(guard) => break guard,
            }
        }
    }

    #[inline]
    fn traversal_write_internal_rand(&self, attempts: Attempts)
    -> Result<(Fence<Key>, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>), Attempts>
    {
        let (mut _curr_block,
            mut curr_guard,
            attempts) = self.retrieve_root_write_olc(attempts);

        let mut curr_fence
            = Fence::new(self.min_key, self.max_key);

        let mut traversal_loops =  0;
        loop {
            let curr_guard_result
                = curr_guard.deref();

            let curr_page_ref
                = curr_guard_result.unwrap();

            let (live_n, _dead_n)
                = curr_page_ref.active_dead_count();

            let mut index
                = rand::random_range(0..live_n as usize);

            if VERBOSE {
                println!("traversal_write_internal_olc: Loop: {traversal_loops}, attempts {attempts}, live_index: {index}");
                traversal_loops += 1;
            }

            match curr_guard_result.unwrap().as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    for (k, version) in internal_page.versions().iter().enumerate() {
                        if version.is_active() {
                            if index == 0 {
                                index = k;
                                break
                            }
                            index -= 1;
                        }
                    }

                    debug_assert!(index < _dead_n as usize + live_n as usize);
                    debug_assert!(!internal_page.get_version(index).is_obsolete(),
                            "Accessed obsolete version!");

                    curr_fence = internal_page.get_key(index).clone();
                    let next_curr_block = internal_page
                        .get_pointer(index)
                        .clone();

                    let mut next_curr_guard
                        = next_curr_block.borrow_read();

                    if LOG_REORG {
                        let r
                            = next_curr_guard.deref().unwrap().unsafe_degree();

                        match r {
                            BlockUnsafeDegree::Overflow => unsafe {
                                mv_test::SPLITS_COUNTER.lock().push(self.current_version())
                            }
                            BlockUnsafeDegree::ActiveUnderflow => unsafe {
                                mv_test::MERGES_COUNTER.lock().push(self.current_version())
                            }
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
                    Ok((curr_fence, curr_guard))
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