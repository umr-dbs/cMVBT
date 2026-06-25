use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_page_model::BlockRef;
use crate::mv_page_model::leaf_page::LeafPage;
use crate::mv_query::time_matcher::TimeMatcher;
use crate::mv_record_model::record_point::RecordPointResult;
use crate::mv_record_model::version_info::Version;
use crate::mv_root::index_root::RootIndex;
use crate::mv_sync::smart_cell::PageType;
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_utils::interval::Interval;
use itertools::Itertools;
use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub fn retrieve_root_number_for(&self, lookup_version: Version) -> (usize, usize) {
        match self.root {
            RootIndex::FrugalList(ref fg) => {
                let roots = fg;
                let roots_all
                    = roots.iter().collect_vec();

                let root_count = roots_all.len();

                (roots_all.iter()
                     .enumerate()
                     .rev()
                     .find_map(|(pos, r)|
                         (r.insert_version <= lookup_version).then(|| pos + 1))
                     .unwrap(), root_count)
            }
            RootIndex::LinkedList(ref ll) => {
                let roots = ll.clone();
                let root_count = roots.len();

                (roots.iter()
                    .enumerate()
                    .rev()
                    .find_map(|(pos, r)| (r.version <= lookup_version)
                        .then(|| pos + 1)
                        .or(Some(1)))
                     .unwrap(), root_count)
            }
            _ => (0,0)
        }
    }

    #[inline(always)]
    pub fn retrieve_root_for(&self, lookup_version: Version)
                                    -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        self.root
            .root_for(lookup_version)
            .block
    }

    #[inline]
    fn traverse_read_key<'a>(
        curr: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        key: Key,
        lookup_version: Version)
        -> (usize, &'a LeafPage<NUM_RECORDS, Key, Payload>)
    {
        let (mut len, mut page)
            = curr.as_page_ref();
        
        while let PageType::IndexRef(internal_page) = page
        {
            let (keys_page, versions_page) = internal_page
                .keys_versions(len);

            (len, page) = versions_page
                .iter()
                .zip(keys_page)
                .enumerate()
                .rfind(|(_, (v, range))|
                    v.matched(lookup_version) && range.contains(key))
                .map(|(pos, _)| internal_page.get_pointer(pos).as_page_ref())
                .unwrap()
        }

        (len, page.as_leaf_page())
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

            let (len, page) 
                = curr.as_page_ref();
            
            match page {
                PageType::IndexRef(internal_page) => {
                    let (keys_page, versions_page) = internal_page
                        .keys_versions(len);

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
    pub(crate) fn key_point_read_from_root<'a>(
        root: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        key: Key,
        lookup_version: Version)
        -> CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let (len, leaf_page)
            = Self::traverse_read_key(root, key, lookup_version);

        match leaf_page
            .as_records(len)
            .iter()
            .rev()
            .skip_while(|r| r.version.insert_version > lookup_version)
            .find(|r|
                r.key() == key && r.version().matches(lookup_version))
        {
            None => CRUDOperationResult::MatchedRecords(Vec::with_capacity(0)),
            Some(result) =>
                CRUDOperationResult::MatchedRecords(vec![RecordPointResult::from(result)])
        }
    }

    pub(crate) fn key_range_read_from_root<'a>(
        root: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        lookup_range: Interval<Key>,
        lookup_version: Version)
        -> CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let blocks = Self::traverse_read_key_range(
            root,
            &lookup_range,
            lookup_version);

        CRUDOperationResult::MatchedRecords(blocks
            .into_iter()
            .map(|leaf| {
                let records
                    = leaf.as_records();

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
}