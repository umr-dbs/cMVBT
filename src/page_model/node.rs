use std::fmt::{Debug, Display};
use std::hash::Hash;
use crate::page_model::BlockRef;
use crate::page_model::internal_page::InternalPage;
use crate::page_model::leaf_page::LeafPage;
use crate::record_model::record_point::{Payload, RecordPoint};
use crate::record_model::version_info::{Version, VersionInfo};
use crate::utils::interval::Interval;

#[derive(Clone)]
pub enum Node<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> {
    Index(InternalPage<FAN_OUT, NUM_RECORDS, Key>),
    Leaf(LeafPage<NUM_RECORDS, Key>),
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> Node<FAN_OUT, NUM_RECORDS, Key> {
    #[inline(always)]
    pub const fn is_leaf(&self) -> bool {
        match self {
            Node::Index(..) => false,
            _ => true
        }
    }

    #[inline(always)]
    pub fn as_records(&self) -> &[RecordPoint<Key>] {
        match self {
            Node::Leaf(records_page) =>
                records_page.as_records(),
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .as_records")
        }
    }

    #[inline(always)]
    pub fn keys_versions(&self) -> (&[Interval<Key>], &[Version]) {
        match self {
            Node::Index(internal_page) =>
                internal_page.keys_versions(),
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .keys_versions")
        }
    }

    #[inline(always)]
    pub unsafe fn keys(&self) -> &[Interval<Key>] {
        match self {
            Node::Index(internal_page) =>
                internal_page.keys(),
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .keys")
        }
    }

    #[inline(always)]
    pub fn children(&self) -> &[BlockRef<FAN_OUT, NUM_RECORDS, Key>] {
        match self {
            Node::Index(internal_page) =>
                internal_page.children(),
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .children")
        }
    }

    #[inline(always)]
    pub fn keys_versions_pointers(&self) -> (&[Interval<Key>], &[Version], &[BlockRef<FAN_OUT, NUM_RECORDS, Key>]) {
        match self {
            Node::Index(internal_page) =>
                internal_page.keys_versions_pointers(),
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .keys_versions_pointers")
        }
    }

    #[inline]
    pub fn delete_key(&mut self, key: Key, del: Version) -> Option<VersionInfo> {
        match self {
            Node::Leaf(records) =>
                records.delete(key, del),
            _ => None
        }
    }

    #[inline(always)]
    pub fn as_leaf_page(&mut self) -> &mut LeafPage<NUM_RECORDS, Key> {
        match self {
            Node::Leaf(records_page) => records_page,
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn as_internal_page(&mut self) -> &mut InternalPage<FAN_OUT, NUM_RECORDS, Key>{
        match self {
            Node::Index(internal_page) => internal_page,
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn as_internal_page_ref(&self) -> & InternalPage<FAN_OUT, NUM_RECORDS, Key>{
        match self {
            Node::Index(internal_page) => internal_page,
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            Node::Index(index_page) => index_page.len(),
            Node::Leaf(records_page) => records_page.len(),
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> AsRef<Node<FAN_OUT, NUM_RECORDS, Key>> for Node<FAN_OUT, NUM_RECORDS, Key> {
    fn as_ref(&self) -> &Node<FAN_OUT, NUM_RECORDS, Key> {
        &self
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> Default for Node<FAN_OUT, NUM_RECORDS, Key> {
    fn default() -> Self {
        Self::Leaf(LeafPage::default())
    }
}