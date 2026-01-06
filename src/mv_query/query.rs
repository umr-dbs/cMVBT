use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use itertools::Itertools;
use crate::mv_block::block::BlockGuard;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_page_model::BlockRef;
use crate::mv_page_model::internal_page::TimeMatcher;
use crate::mv_page_model::node::PageType;
use crate::mv_record_model::record_point::RecordPointResult;
use crate::mv_record_model::version_info::Version;
use crate::mv_tree::mvtree::{MVTreeSt};
use crate::mv_tree::smo::BlockUnsafeDegree;
use crate::mv_utils::interval::Interval;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVTreeSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub fn retrieve_root_for(&self, lookup_version: Version)
                                    -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        self.root
            .root_for(lookup_version)
            .block
    }

    #[inline]
    fn traverse_read_key(
        mut curr: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        key: Key,
        lookup_version: Version)
        -> Result<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
        while let PageType::IndexRef(internal_page) = curr
            .borrow_read()
            .deref()
            .unwrap()
            .as_page_ref()
        {
            let (keys_page, versions_page) = internal_page
                .keys_versions();

            curr = versions_page
                .iter()
                .zip(keys_page)
                .enumerate()
                .rfind(|(_, (v, range))|
                    v.matched(lookup_version) && range.contains(key))
                .map(|(pos, _)| internal_page.get_pointer(pos).clone())
                .ok_or(())?
        }

        Ok(curr)
    }

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

            let curr_read
                = curr.borrow_read();

            match curr_read.deref().unwrap().as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    let (keys_page, versions_page) = internal_page
                        .keys_versions();

                    let start_pos_si = versions_page.len() -
                        versions_page.binary_search_by(|v| v.into_cmp().cmp(&lookup_version))
                            .unwrap_or_else(|pos| pos);

                    versions_page
                        .iter()
                        .enumerate()
                        .zip(keys_page.iter())
                        .rev()
                        .skip(start_pos_si)
                        .filter(|((.., v), range)| //v.matched(lookup_version) &&
                            v.matched(lookup_version) && lookup_range.overlap(range))
                        .unique_by(|(.., range)| range.lower())
                        .unique_by(|(.., range)| range.upper())
                        .for_each(|((pos, ..), ..)|
                            blocks.push_back(internal_page.get_pointer(pos).clone()));
                }
                _ => leafs.push(curr)
            }
        }

        leafs
    }

    #[inline]
    pub(crate) fn key_point_read_from_root(
        root: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        key: Key,
        lookup_version: Version)
        -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        match Self::traverse_read_key(
            root,
            key,
            lookup_version)
        {
            Ok(leaf) => match leaf
                .borrow_read()
                .deref()
                .unwrap()
                .as_records()
                .iter()
                .rfind(|r| r.key() == key && r.version().matches(lookup_version))
            {
                None => CRUDOperationResult::MatchedRecords(Vec::with_capacity(0)),
                Some(result) => CRUDOperationResult::MatchedRecords(vec![RecordPointResult::from(result)])
            },
            Err(..) => CRUDOperationResult::MatchedRecords(Vec::with_capacity(0))
        }
    }

    pub(crate) fn key_range_read_from_root(
        root: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        lookup_range: Interval<Key>,
        lookup_version: Version)
        -> CRUDOperationResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let blocks = Self::traverse_read_key_range(
            root,
            &lookup_range,
            lookup_version);

        CRUDOperationResult::MatchedRecords(blocks
            .into_iter()
            .map(|leaf| {
                let leaf_guard =  leaf
                    .borrow_read();

                let records = leaf_guard
                    .deref()
                    .unwrap()
                    .as_records();

                let start_pos_si = records.len() -
                    records.binary_search_by(|r|
                        r.version.insert_version.cmp(&lookup_version)
                    ).unwrap_or_else(|pos| pos);

               records
                   .iter()
                   .rev()
                   .skip(start_pos_si)
                   .filter(|r|
                       r.version().matches(lookup_version) &&
                           lookup_range.contains(r.key()))
                   // .sorted_by_key(|r| r.key())
                   .map(RecordPointResult::from)
                   .collect::<Vec<_>>()
            })
            // .filter(|set| !set.is_empty())
            // .sorted_by_key(|set|
            //     unsafe { set.get_unchecked(0).key })
            .flatten()
            .collect())
    }

    #[inline]
    fn retrieve_root_write(&self) ->
        (BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
         BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)
    {
        let height
            = self.root.height();

        let master_guard
            = self.root.borrow_read();

        let root_block
            = master_guard.block();

        let root_guard
            = root_block.borrow_read();

        match master_guard.unsafe_degree_root() {
            BlockUnsafeDegree::Overflow => self.split_root(master_guard, root_guard, height),
            BlockUnsafeDegree::ActiveUnderflow => self.merge_root(master_guard, root_guard, height)
                .unwrap(),
            _ => (root_block, root_guard),
        }
    }

    #[inline]
    pub(crate) fn traversal_write(&self, key: Key)
                                  -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let (mut _curr_block,
            mut curr_guard) = self.retrieve_root_write();

        loop {
            match curr_guard.deref().unwrap().as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    let keys_page = internal_page
                        .keys();

                    let index = keys_page
                        .iter()
                        .enumerate()
                        // .rev()
                        .rfind(|(.., range)| range.contains(key))
                        .map(|(pos, ..)| pos)
                        .unwrap();

                    let next_curr_block = internal_page
                        .get_pointer(index)
                        .clone();

                    let next_curr_guard
                        = next_curr_block.borrow_free();

                    match next_curr_guard.deref().unwrap().unsafe_degree() {
                        BlockUnsafeDegree::Overflow =>
                            curr_guard = self.on_overflow_node(curr_guard, next_curr_guard, index),
                        BlockUnsafeDegree::ActiveUnderflow =>
                            curr_guard = self.on_underflow_node(curr_guard, next_curr_guard, index)
                                .unwrap(),
                        BlockUnsafeDegree::Ok => {
                            curr_guard = next_curr_guard;
                            _curr_block = next_curr_block;
                        }
                    }
                }
                _ => return curr_guard
            }
        }
    }
}