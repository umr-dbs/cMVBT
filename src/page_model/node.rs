use std::fmt::{Debug, Display};
use std::hash::Hash;
use crate::page_model::internal_page::InternalPage;
use crate::page_model::leaf_page::LeafPage;
use crate::record_model::record_point::{Payload, RecordPoint};
use crate::record_model::version_info::{Version, VersionInfo};

pub enum Node<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> {
    Index(InternalPage<FAN_OUT, NUM_RECORDS, Key>),
    Leaf(LeafPage<NUM_RECORDS, Key>),
}

#[repr(u8)]
pub enum NodeUnsafeDegree {
    Ok,
    Overflow,
    Underflow,
}

impl NodeUnsafeDegree {
    pub const fn is_ok(&self) -> bool {
        match self {
            Self::Ok => true,
            _ => false
        }
    }

    pub const fn is_overflow(&self) -> bool {
        match self {
            Self::Overflow => true,
            _ => false
        }
    }

    pub const fn is_underflow(&self) -> bool {
        match self {
            Self::Underflow => true,
            _ => false
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> Node<FAN_OUT, NUM_RECORDS, Key> {
    #[inline(always)]
    pub fn unsafe_degree(&self, allocation: usize) -> NodeUnsafeDegree {
        let len = self.len();

        if len >= allocation {
            NodeUnsafeDegree::Overflow
        } else if len < allocation / 2 {
            NodeUnsafeDegree::Underflow
        } else {
            NodeUnsafeDegree::Ok
        }
    }

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
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .records_mut")
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

    #[inline]
    pub fn push(&mut self, key: Key, version: Version, payload: Payload) {
        match self {
            Node::Leaf(records_page) => records_page
                .push(RecordPoint::new(key, VersionInfo::new(version), payload)),
            _ => {}
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