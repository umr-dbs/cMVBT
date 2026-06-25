use crate::mv_block::block::BlockGuard;
use crate::mv_page_model::Attempts;
use crate::mv_query::time_matcher::TimeMatcher;
use crate::mv_test::{LOG_REORG, VERBOSE};
use crate::mv_tree::mvbt::MVBTSt;
use std::fmt::Display;
use std::hash::Hash;

use crate::mv_sync::smart_cell::{PageType, sched_yield};
use crate::mv_test;
use crate::mv_tree::smo::BlockUnsafeDegree;
use crate::mv_utils::interval::Interval;

pub const RAND_ATTEMPTS_MAX: Attempts = 10; // for insertion generation upper bound
pub type Fence<Key> = Interval<Key>;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
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
        let (_root_block,
            attempts) = self.retrieve_root_write_olc(attempts);

        let mut curr_fence
            = Fence::new(self.min_key, self.max_key);

        let mut cursor
            = _root_block.borrow_read();

        let mut traversal_loops =  0;
        loop {
            let (live_n, _dead_n)
                = cursor.active_dead();

            let mut index
                = if live_n == 0 { 0 } else { rand::random_range(0..live_n as usize) };

            if VERBOSE {
                println!("traversal_write_internal_olc: Loop: {traversal_loops}, attempts {attempts}, live_index: {index}");
                traversal_loops += 1;
            }

            let (len, curr_page_ref)
                = cursor.as_page_ref();

            match curr_page_ref {
                PageType::IndexRef(internal_page) => {
                    for (k, version) in internal_page.versions(len).iter().enumerate() {
                        if version.is_active() {
                            if index == 0 {
                                index = k;
                                break
                            }
                            index -= 1;
                        }
                    }
                    // if index >= 127 {
                    //     return self.traversal_write_internal_rand(attempts);
                    // }
                    assert!(index < _dead_n as usize + live_n as usize);
                    assert!(!internal_page.get_version(index).is_obsolete(),
                            "Accessed obsolete version!");

                    curr_fence = internal_page.get_key(index).clone();
                    let next_curr_block = internal_page
                        .get_pointer(index);

                    if LOG_REORG {
                        let r
                            = next_curr_block.unsafe_degree();

                        match r {
                            BlockUnsafeDegree::Overflow => unsafe {
                                mv_test::SPLITS_COUNTER.lock().push(self.current_version_for_reader())
                            }
                            BlockUnsafeDegree::ActiveUnderflow => unsafe {
                                mv_test::MERGES_COUNTER.lock().push(self.current_version_for_reader())
                            }
                            _ => {}
                        }
                    }
                    
                    match next_curr_block.unsafe_degree() {
                        BlockUnsafeDegree::Overflow
                        if cursor.upgrade_write_lock()
                        => cursor = self.on_overflow_node(cursor, next_curr_block, index),
                        BlockUnsafeDegree::ActiveUnderflow
                        if cursor.upgrade_write_lock()
                        => match self.on_underflow_node(cursor, next_curr_block, index) {
                            Ok(next) => cursor = next,
                            Err(..) => {
                                if VERBOSE {
                                    println!("traversal_write_internal_olc: on_underflow_node Err()");
                                }
                                return Err(attempts + 1)
                            }
                        },
                        BlockUnsafeDegree::Ok => cursor = next_curr_block.borrow_read(),
                        _ => return Err(attempts + 1)
                    };
                }
                _ => return if cursor.upgrade_write_lock() {
                    Ok((curr_fence, cursor))
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