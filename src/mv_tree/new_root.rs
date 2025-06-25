use std::fmt::{Display, Formatter};
use std::hash::Hash;
use CCBPlusTree::crud_model::crud_api::CRUDDispatcher;
use CCBPlusTree::crud_model::crud_operation::CRUDOperation;
use CCBPlusTree::crud_model::crud_operation_result::CRUDOperationResult;
use CCBPlusTree::locking::locking_strategy::LockingStrategy;
use CCBPlusTree::tree::bplus_tree::BPlusTree;
use crate::mv_page_model::{BlockRef, Height};
use crate::mv_record_model::version_info::Version;
use crate::mv_tree::root::Root;
use crate::mv_tree::version_manager::VersionManager;
use crate::mv_tx_model::transaction::SnapShot;

#[derive(Clone, Default)]
struct InnerRoot<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static>
(
    BlockRef<FANOUT, NUM_RECORDS, Key, Payload>,
    Height
);

impl<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static> Display for InnerRoot<FANOUT, NUM_RECORDS, Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InnerRoot<F: {FANOUT}, N: {NUM_RECORDS}> Height: {}", self.1)
    }
}

pub struct RootIndex<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static>
{
    index: BPlusTree<FANOUT, NUM_RECORDS, SnapShot, InnerRoot<FANOUT, NUM_RECORDS, Key, Payload>>,
}

impl<const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static> RootIndex<FANOUT, NUM_RECORDS, Key, Payload>
{
    pub fn new() -> Self {
        Self {
            index: BPlusTree::new_with(
                LockingStrategy::OLC,
                VersionManager::START_VERSION,
                SnapShot::MAX,
                |v| v + 1,
                |v| v - 1)
        }
    }

    #[inline(always)]
    pub fn current_root(&self) -> Root<FANOUT, NUM_RECORDS, Key, Payload> {
        let (_nv, crud)
            = self.index.dispatch(CRUDOperation::PeekMax);

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
            = self.index.dispatch(CRUDOperation::Pred(si));

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
            = self.index.dispatch(CRUDOperation::Insert(root.version, InnerRoot(root.block, root.height)));

        if let CRUDOperationResult::Inserted(..) = crud {
            true
        }
        else {
            false
        }
    }
}