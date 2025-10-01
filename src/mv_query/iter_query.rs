use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
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
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub fn new(tree: &'a MVTreeSt<FAN_OUT, NUM_RECORDS, Key, Payload>, version: Version, range: Interval<Key>) -> Self {
        Self {
            isolated_snapshot: IsolatedSnapShot(version, tree),
            range,
            path: vec![(Interval::new(tree.min_key, tree.max_key),
                        tree.snapshot_current().mv_tree().retrieve_root_for(version))],
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
            if self.path.is_empty() || self.range.lower > self.range.upper || self.range.lower == self.mv_tree().max_key {
                return None
            }
            let (curr_fence, curr_block)
                = self.path.last().cloned().unwrap();

            match curr_block.borrow_read().deref().unwrap().as_ref().as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    let (keys_page, versions_page) = internal_page
                        .keys_versions();

                    let start_pos_si = versions_page.len() -
                        versions_page.binary_search_by(|v| v.into_cmp().cmp(&si))
                            .unwrap_or_else(|pos| pos);

                    match versions_page
                        .iter()
                        .zip(keys_page)
                        .enumerate()
                        .rev()
                        .skip(start_pos_si)
                        .filter_map(|(pos, (v, range))|
                            if range.contains(self.range.lower) && v.matched(si) {
                                Some((range.clone(), internal_page.get_pointer(pos).clone()))
                            } else {
                                None
                            })
                        .next()
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

                    let start_pos_si = records.len() -
                        records.binary_search_by(|r|
                            r.version.insert_version.cmp(&si)
                        ).unwrap_or_else(|pos| pos);

                    self.buff.extend(records
                        .iter()
                        .rev()
                        .skip(start_pos_si)
                        .filter(|r|
                            r.version().matches(si) && self.range.contains(r.key()))
                        .map(RecordPointResult::from));

                    self.range.lower = inc(curr_fence.upper);
                    self.path.pop();

                    if !self.buff.is_empty() {
                        return self.buff.pop_front()
                    }
                }
                _ => unreachable!()
            }
        }
    }
}