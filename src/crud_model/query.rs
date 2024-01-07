use std::collections::VecDeque;
use std::hash::Hash;
use itertools::Itertools;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::page_model::BlockRef;
use crate::page_model::node::{Node, NodeUnsafeDegree};
use crate::record_model::version_info::{TimeMatcher, Version};
use crate::tree::bplus_tree::{BPlusTree, SmartRoot};
use crate::utils::interval::Interval;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> BPlusTree<FAN_OUT, NUM_RECORDS, Key>
{
    #[inline(always)]
    pub(crate) fn has_overflow(&self, node: &Node<FAN_OUT, NUM_RECORDS, Key>) -> bool {
        match node.is_leaf() {
            true => node.is_overflow(self.block_manager.allocation_leaf()),
            false => node.is_overflow(self.block_manager.allocation_directory())
        }
    }

    fn has_underflow(&self, node: &Node<FAN_OUT, NUM_RECORDS, Key>) -> bool {
        match node.is_leaf() {
            true => node.is_underflow(self.block_manager.allocation_leaf()),
            false => node.is_underflow(self.block_manager.allocation_directory())
        }
    }

    fn unsafe_degree_of(&self, node: &Node<FAN_OUT, NUM_RECORDS, Key>) -> NodeUnsafeDegree {
        match node.is_leaf() {
            true => node.unsafe_degree(self.block_manager.allocation_leaf()),
            false => node.unsafe_degree(self.block_manager.allocation_directory()),
        }
    }

    pub(crate) fn retrieve_root_for(&self, lookup_version: Version) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key> {
        let mut root_anker
            = self.root.clone();

        loop {
            let root_item
                = root_anker.unsafe_borrow();

            if root_item.version() <= lookup_version {
                break root_anker;
            } else {
                root_anker = match root_item.prev.as_ref() {
                    None => unreachable!(),
                    Some(s) => s.clone()
                };
            }
        }
    }

    pub(crate) fn retrieve_root_latest(&self) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key> {
        self.retrieve_root_for(Version::MAX)
    }

    fn traverse_read_key(
        mut curr: BlockRef<FAN_OUT, NUM_RECORDS, Key>,
        key: Key,
        version: Version)
        -> BlockRef<FAN_OUT, NUM_RECORDS, Key>
    {
        while let Node::Index(internal_page) = curr
            .unsafe_borrow()
            .as_ref()
        {
            let (keys_page, versions_page) = internal_page
                .keys_versions();

            assert_eq!(versions_page.len(), keys_page.len());

            curr = versions_page
                .iter()
                .enumerate()
                .rev()
                .zip(keys_page
                    .iter()
                    .rev())
                .skip_while(|((.., v), ..)| !v.match_version(version))
                // .take_while(|((.., v), ..)| v.match_version(version))
                .find(|(.., range)| range.contains(key))
                .map(|((pos, ..), ..)| internal_page.get_pointer(pos))
                .unwrap()
                .clone();
        }

        curr
    }

    #[inline]
    fn traverse_read_key_range(
        mut curr: BlockRef<FAN_OUT, NUM_RECORDS, Key>,
        lookup_range: &Interval<Key>,
        version: Version)
        -> Vec<BlockRef<FAN_OUT, NUM_RECORDS, Key>>
    {
        let mut blocks
            = VecDeque::new();

        blocks.push_back(curr);

        let mut leafs
            = vec![];

        while !blocks.is_empty() {
            curr = blocks.pop_front().unwrap();

            match curr.unsafe_borrow().as_ref() {
                Node::Index(internal_page) => {
                    let (keys_page, versions_page) = internal_page
                        .keys_versions();

                    assert_eq!(versions_page.len(), keys_page.len());

                    blocks.extend(versions_page
                        .iter()
                        .enumerate()
                        .rev()
                        .zip(keys_page
                            .iter()
                            .rev())
                        .skip_while(|((.., v), ..)| !v.match_version(version))
                        // .take_while(|((.., v), ..)| v.match_version(version))
                        .filter(|(.., range)| lookup_range.overlap(range))
                        .map(|((pos, ..), range)| internal_page
                            .get_pointer(pos)
                            .clone())
                    );
                }
                _ => leafs.push(curr)
            }
        }

        leafs
    }

    pub(crate) fn key_point_read_from_root(
        root: SmartRoot<FAN_OUT, NUM_RECORDS, Key>,
        key: Key,
        version: Version)
        -> CRUDOperationResult<Key>
    {
        let leaf = Self::traverse_read_key(
            root.unsafe_borrow().block(),
            key,
            version);

        match leaf
            .unsafe_borrow()
            .as_records()
            .iter()
            .rev()
            .skip_while(|r| !r.version().matches(version))
            .take_while(|r| r.version().matches(version))
            .find(|r| r.key() == key)
        {
            None => CRUDOperationResult::MatchedRecords(Vec::with_capacity(0)),
            Some(result) => CRUDOperationResult::MatchedRecords(vec![result.clone()])
        }
    }

    pub(crate) fn key_range_read_from_root(
        root: SmartRoot<FAN_OUT, NUM_RECORDS, Key>,
        range: &Interval<Key>,
        version: Version)
        -> CRUDOperationResult<Key>
    {
        let blocks = Self::traverse_read_key_range(
            root.unsafe_borrow().block(),
            range,
            version);

        CRUDOperationResult::MatchedRecords(blocks
            .into_iter()
            .map(|leaf| leaf
                .unsafe_borrow()
                .as_records()
                .iter()
                .rev()
                .skip_while(|r| !r.version().matches(version))
                .take_while(|r| r.version().matches(version))
                .filter(|r| range.contains(r.key()))
                .cloned()
                .collect::<Vec<_>>())
            .flatten()
            .collect())
    }

    #[inline]
    fn traverse_write(&self, key: Key) -> BlockRef<FAN_OUT, NUM_RECORDS, Key> {
        let mut curr
            = self.retrieve_root_latest().unsafe_borrow().block();

        // TODO: Consider latch type
        while let Node::Index(internal_page) = curr
            .unsafe_borrow()
            .as_ref()
        {
            let (keys_page, versions_page) = internal_page
                .keys_versions();

            assert_eq!(versions_page.len(), keys_page.len());

            let index = versions_page
                .iter()
                .rev()
                .zip(keys_page
                    .iter()
                    .rev())
                .find_position(|(.., range)| range.contains(key))
                .map(|(pos, ..)| versions_page.len() - pos - 1)
                .unwrap();

            curr = internal_page
                .get_pointer(index)
                .clone();
        }

        curr
    }
}