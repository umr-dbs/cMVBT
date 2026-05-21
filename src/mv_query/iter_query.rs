use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use itertools::Itertools;
use crate::mv_page_model::BlockRef;
use crate::mv_page_model::internal_page::TimeMatcher;
use crate::mv_page_model::node::PageType;
use crate::mv_record_model::record_point::RecordPointResult;
use crate::mv_record_model::version_info::Version;
use crate::mv_tree::mvtree::MVTreeSt;
use crate::mv_tx_model::transaction_result::SnapShot;
use crate::mv_tx_query::tx_api::IsolatedSnapShot;
use crate::mv_utils::interval::Interval;

pub struct RangeQueryIter<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> {
    pub(crate) isolated_snapshot: IsolatedSnapShot<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub(crate) range: Interval<Key>,
    path: Vec<(Interval<Key>, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)>,
    buff: VecDeque<RecordPointResult<Key, Payload>>,
    is_completed: bool,
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> Drop for RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn drop(&mut self) { // ensure snapshot is released even if user didn't consume all data
        if !self.is_completed {
            self.mv_tree()
                .on_release_reader_snapshot(self.snapshot().into())
        }
    }
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub fn new(tree: &'a MVTreeSt<FAN_OUT, NUM_RECORDS, Key, Payload>, version: Version, range: Interval<Key>) -> Self {
        tree.on_acquire_reader_snapshot(version);

        Self {
            isolated_snapshot: IsolatedSnapShot(version, tree),
            range,
            path: vec![(Interval::new(tree.min_key, tree.max_key),
                        tree.snapshot_current().mv_tree().retrieve_root_for(version))],
            buff: VecDeque::new(),
            is_completed: false,
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
    pub const fn mv_tree(&self) -> &MVTreeSt<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.si().mv_tree()
    }
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> Iterator for RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
    type Item = RecordPointResult<Key, Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.buff.is_empty() {
            return self.buff.pop_front();
        }

        let si
            = self.snapshot();

        let inc
            = self.mv_tree().inc_key;

        loop {
            if self.path.is_empty() || self.range.lower > self.range.upper {
                self.mv_tree()
                    .on_release_reader_snapshot(self.snapshot());

                self.is_completed = true;
                return None
            }
            let (curr_fence, curr_block)
                = self.path.last().cloned().unwrap();

            match curr_block.borrow_read().as_ref().as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    let (keys_page, versions_page) = internal_page
                        .keys_versions();

                    match versions_page
                        .iter()
                        .zip(keys_page.iter())
                        .enumerate()
                        .rev()
                        .find_map(|(pos, (v, range))|
                            if range.contains(self.range.lower) && v.matched(si){
                                Some((range.clone(), internal_page.get_pointer(pos).clone()))
                            } else {
                                None
                            })
                    {
                        Some(next) => self.path.push(next),
                        _ => {
                            self.path.pop();
                            self.range.lower = inc(curr_fence.upper);
                        }
                    }
                }
                PageType::LeafRef(leaf_page) => {
                    let records = leaf_page
                        .as_records();

                    self.buff.extend(records
                        .iter()
                        .filter(|r|
                            r.version().matches(si) && self.range.contains(r.key()))
                        .map(RecordPointResult::from));

                    self.path.pop();

                    self.range.lower = inc(curr_fence.upper);
                    if !self.buff.is_empty() || self.range.lower == self.mv_tree().max_key {
                        return self.buff.pop_front()
                    }
                }
                _ => unreachable!()
            }
        }
    }
}