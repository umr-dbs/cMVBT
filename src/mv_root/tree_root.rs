use std::fmt::{Display, Formatter};
use std::hash::Hash;
use CCBPlusTree::crud_model::crud_api::CRUDDispatcher;
use CCBPlusTree::crud_model::crud_operation::CRUDOperation;
use CCBPlusTree::crud_model::crud_operation_result::CRUDOperationResult;
use CCBPlusTree::tree::bplus_tree::BPlusTree;

use crate::mv_page_model::{BlockRef, Height};
use crate::mv_record_model::version_info::Version;
use crate::mv_root::root::Root;
use crate::mv_tree::mvtree::INIT_TREE_HEIGHT;
use crate::mv_sync::smart_cell::LatchType;
use crate::mv_sync::version_handle;
use crate::mv_tx_model::transaction_result::SnapShot;
// pub(crate) fn make_start_value_root_inner_tree<
//     const F: usize,
//     const N: usize,
//     Key: Display + Default + Ord + Copy + Hash,
//     Payload: Default + Clone>(bk: &BlockManager<F, N, Key, Payload>, latch_type: LatchType)
//                               -> (ValueRootInner<F, N, Key, Payload>, SnapShot)
// {
//     (ValueRootInner::initial(bk.new_empty_leaf(latch_type)), VersionManager::START_VERSION)
// }

pub(crate) const TREE_ROOT_MIN_KEY: Version = version_handle::START_VERSION;
pub(crate) const TREE_ROOT_MAX_KEY: Version = Version::MAX;

#[derive(Clone, Default)]
pub(crate) struct ValueRootInner<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash,
    Payload: Default + Clone>
(
    pub BlockRef<FANOUT, NUM_RECORDS, Key, Payload>,
    pub Height
);

impl<const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash,
    Payload: Default + Clone> ValueRootInner<FANOUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub const fn initial(block: BlockRef<FANOUT, NUM_RECORDS, Key, Payload>) -> Self {
        ValueRootInner(block, INIT_TREE_HEIGHT) // is default
    }

    #[inline(always)]
    pub const fn from(block: BlockRef<FANOUT, NUM_RECORDS, Key, Payload>, height: Height) -> Self {
        ValueRootInner(block, height)
    }

    #[inline(always)]
    pub fn block(&self) -> BlockRef<FANOUT, NUM_RECORDS, Key, Payload> {
        self.0.clone()
    }
    #[inline(always)]
    pub fn height(&self) -> Height {
        self.1
    }
}

unsafe impl<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static> Sync
for ValueRootInner<FANOUT, NUM_RECORDS, Key, Payload> {}

unsafe impl<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync,
    Payload: Display + Default + Clone + Sync> Send
for ValueRootInner<FANOUT, NUM_RECORDS, Key, Payload> {}

impl<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static> Display for ValueRootInner<FANOUT, NUM_RECORDS, Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InnerRoot<F: {FANOUT}, N: {NUM_RECORDS}> Height: {}", self.1)
    }
}

pub struct RootTree<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static>
(pub(crate) BPlusTree<FANOUT, NUM_RECORDS, SnapShot, ValueRootInner<FANOUT, NUM_RECORDS, Key, Payload>>);


impl<const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static> RootTree<FANOUT, NUM_RECORDS, Key, Payload>
{
    pub fn new(latch_type: LatchType) -> Self {
        Self(BPlusTree::new_with(
            latch_type.into_cc_locking_strategy(),
            TREE_ROOT_MIN_KEY,
            TREE_ROOT_MAX_KEY,
            |v| v.checked_add(1).unwrap_or(SnapShot::MAX),
            |v| v.checked_sub(1).unwrap_or(SnapShot::MIN)))
    }

    #[inline(always)]
    pub fn height(&self) -> Height {
        self.0.height()
    }

    #[inline(always)]
    pub fn current_root(&self) -> Root<FANOUT, NUM_RECORDS, Key, Payload> {
        let (_nv, crud)
            = self.0.dispatch(CRUDOperation::PeekMax);

        if let CRUDOperationResult::MatchedRecord(Some(latest_root)) = crud {
            Root::new(latest_root.payload.0, latest_root.key, latest_root.payload.1)
        }
        else {
            unreachable!("Failed access current_root!")
        }
    }

    #[inline(always)]
    pub fn root_for(&self, si: SnapShot) -> Root<FANOUT, NUM_RECORDS, Key, Payload> {
        let (_nv, crud)
            = self.0.dispatch(CRUDOperation::Pred(si));

        if let CRUDOperationResult::MatchedRecord(Some(si_root)) = crud {
            Root::new(si_root.payload.0, si_root.key, si_root.payload.1)
        }
        else {
            unreachable!("Failed access root_for!")
        }
    }

    #[inline(always)]
    pub fn append_root(&self, root: Root<FANOUT, NUM_RECORDS, Key, Payload>) -> bool {
        let (_nv, crud)
            = self.0.dispatch(CRUDOperation::Insert(root.version, ValueRootInner(root.block, root.height)));

        if let CRUDOperationResult::Inserted(..) = crud {
            true
        }
        else {
            false
        }
    }
}