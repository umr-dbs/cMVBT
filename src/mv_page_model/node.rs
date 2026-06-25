use std::fmt::Display;
use std::hash::Hash;
use std::mem::ManuallyDrop;

use crate::mv_page_model::internal_page::InternalPage;
use crate::mv_page_model::leaf_page::LeafPage;
use crate::mv_sync::safe_cell::SafeCell;

// const PADDING_SIZE: usize = 56;
pub struct Node<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    // _pad: [u8; PADDING_SIZE],
    pub(crate) page: SafeCell<InnerPage<FAN_OUT, NUM_RECORDS, Key, Payload>>,
}

pub union InnerPage<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    pub(crate) internal: ManuallyDrop<InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    pub(crate) leaf: ManuallyDrop<LeafPage<NUM_RECORDS, Key, Payload>>,
}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Sync for InnerPage<FAN_OUT, NUM_RECORDS, Key, Payload> { }

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub const fn new_leaf() -> Self {
        Self {
            // _pad: [0u8; PADDING_SIZE],
            page: SafeCell::new(InnerPage {
                leaf: ManuallyDrop::new(LeafPage::new())
            }),
        }
    }

    #[inline(always)]
    pub const fn new_internal() -> Self {
        Self {
            // _pad: [0u8; PADDING_SIZE],
            page: SafeCell::new(InnerPage {
                internal: ManuallyDrop::new(InternalPage::new())
            }),
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> AsRef<Node<FAN_OUT, NUM_RECORDS, Key, Payload>> for Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn as_ref(&self) -> &Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
        &self
    }
}