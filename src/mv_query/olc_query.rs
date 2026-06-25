use std::fmt::Display;
use std::hash::Hash;

use crate::mv_block::block::BlockGuard;
use crate::mv_page_model::{Attempts, BlockRef};
use crate::mv_query::time_matcher::TimeMatcher;
use crate::mv_sync::smart_cell::{PageType, sched_yield};
use crate::mv_test;
use crate::mv_test::{LOG_REORG, VERBOSE};
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_tree::smo::BlockUnsafeDegree;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    pub(crate) fn traversal_write_olc(&self, key: Key) -> BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload> {
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
    ) -> (BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        Attempts)
    {
        loop {
            match self.retrieve_root_write_internal_olc() {
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
    fn retrieve_root_write_internal_olc(&self) -> Result<
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
        let mut master_guard
            = self.root.borrow_read();

        let root
            = self.root.current_root();

        let height
            = root.height();

        if LOG_REORG {
            let r
                = root.block.unsafe_degree_root();

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

        match root.block.unsafe_degree_root() {
            BlockUnsafeDegree::Overflow
            if master_guard.upgrade_write_lock()
            => Ok(self.split_root(master_guard, &root.block, height)),
            BlockUnsafeDegree::ActiveUnderflow
            if master_guard.upgrade_write_lock() =>
                self.merge_root(master_guard, &root.block, height),
            BlockUnsafeDegree::Ok
            => Ok(root.block),
            _ => Err(()),
        }
    }

    #[inline]
    fn traversal_write_internal_olc(&'_ self, key: Key, attempts: Attempts)
    -> Result<BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>, Attempts>
    {
        let (root_block,
            attempts) = self.retrieve_root_write_olc(attempts);

        let mut cursor
            = root_block.borrow_read();

        let mut i =  0;
        loop {
            if VERBOSE {
                println!("traversal_write_internal_olc: Loop: {i}, attempts {attempts}, key: {key}");
                i += 1;
            }

            let (len, curr_page)
                = cursor.as_page_ref();

            match curr_page {
                PageType::IndexRef(internal_page) => unsafe {
                    let (keys_page, versions_page) = internal_page
                        .keys_versions(len);

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

                    let next_curr_guard = internal_page
                        .get_pointer(index)
                        .borrow_read();

                    if LOG_REORG {
                        let r
                            = next_curr_guard.unsafe_degree();

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
                        if cursor.upgrade_write_lock()
                            => cursor = self.on_overflow_node(cursor, next_curr_guard.cell(), index),
                        BlockUnsafeDegree::ActiveUnderflow // next_curr_guard.upgrade_write_lock() &&
                        if cursor.upgrade_write_lock()
                        => match self.on_underflow_node(cursor, next_curr_guard.cell(), index) {
                                Ok(next) => cursor = next,
                                Err(..) => {
                                    if VERBOSE {
                                        println!("traversal_write_internal_olc: on_underflow_node Err()");
                                    }
                                    return Err(attempts + 1)
                                }
                            },
                        BlockUnsafeDegree::Ok => cursor = next_curr_guard.borrow_read(),
                        _ => return Err(attempts + 1)
                    }
                }
                _ => return if cursor.upgrade_write_lock() {
                    Ok(cursor)
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