use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use CCBPlusTree::crud_model::crud_api::CRUDDispatcher;
use CCBPlusTree::crud_model::crud_operation::CRUDOperation;
use CCBPlusTree::crud_model::crud_operation_result::CRUDOperationResult;
use CCBPlusTree::locking::locking_strategy::LockingStrategy;
use CCBPlusTree::locking::locking_strategy::LockingStrategy::OLC;
use CCBPlusTree::tree::bplus_tree::BPlusTree;
use crate::mv_gc::db_tracker::AUX_PROTOCOL;
use crate::mv_page_model::BlockRef;
use crate::mv_record_model::version_info::Version;
use crate::mv_test::{dec_key, inc_key};
use crate::mv_utils::safe_cell::SafeCell;

const AUX_DP_FAN_OUT: usize = 250;
const AUX_DP_LEAF_SIZE: usize = 250;

type DeadPageKey = Version;
pub(crate) type DeadPageValue<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload>
= BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>;

pub(crate) struct BzTrace<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static>
(SafeCell<BPlusTree<AUX_DP_FAN_OUT, AUX_DP_LEAF_SIZE, DeadPageKey, DeadPageValue<P_F, P_N, Key, Payload>>>);

impl<const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static> Deref for BzTrace<P_F, P_N, Key, Payload>
{
    type Target = BPlusTree<AUX_DP_FAN_OUT, AUX_DP_LEAF_SIZE, DeadPageKey, DeadPageValue<P_F, P_N, Key, Payload>>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}


impl<const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default> BzTrace<P_F, P_N, Key, Payload>
{
    pub(crate) fn new() -> Self {
        Self(SafeCell::new(BPlusTree::new_with(
            AUX_PROTOCOL,
            Version::MIN,
            Version::MAX,
            inc_key,
            dec_key,
        )))
    }

    #[inline(always)]
    pub(crate) fn pop_min(&self) -> Option<(Version, BlockRef<P_F, P_N, Key, Payload>)> {
        match self.dispatch(CRUDOperation::PopMin) {
            (.., CRUDOperationResult::Deleted(version, block)) =>
                Some((version, block)),
            _ => None
        }
    }

    #[inline(always)]
    pub(crate) fn peek_min(&self) -> Option<Version> {
        match self.dispatch(CRUDOperation::PeekMin) {
            (.., CRUDOperationResult::MatchedRecord(Some(block))) =>
                Some(block.key),
            _ => None
        }
    }

    #[inline(always)]
    pub(crate) fn register_died_page(&self, page_version: Version, page: DeadPageValue<P_F, P_N, Key, Payload>) {
        let _ = self.dispatch(CRUDOperation::Insert(page_version, page));
    }

    #[inline(always)]
    pub(crate) fn register_died_page_col(&self, dead_pages: [(Version, BlockRef<P_F, P_N, Key, Payload>); 2]) {
        dead_pages
            .into_iter()
            .for_each(|(d_v, d_p)| self.register_died_page(d_v, d_p))
    }
}