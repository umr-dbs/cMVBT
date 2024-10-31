use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use CCBPlusTree::crud_model::crud_api::CRUDDispatcher;
use CCBPlusTree::crud_model::crud_operation::CRUDOperation;
use CCBPlusTree::crud_model::crud_operation_result::CRUDOperationResult;
use CCBPlusTree::locking::locking_strategy::LockingStrategy::OLC;
use CCBPlusTree::locking::locking_strategy::LockingStrategy;
use CCBPlusTree::tree::bplus_tree::BPlusTree;
use crate::mv_page_model::BlockRef;
use crate::mv_page_model::internal_page::TimeMatcher;
use crate::mv_record_model::version_info::Version;
use crate::mv_test::{dec_key, inc_key};
use crate::mv_tx_model::transaction::SnapShot;
use crate::mv_utils::safe_cell::SafeCell;

const AUX_ATX_FAN_OUT: usize = 250;
const AUX_DP_FAN_OUT: usize = 250;
const AUX_ATX_LEAF_SIZE: usize = 499;
const AUX_DP_LEAF_SIZE: usize = 250;

const AUX_PROTOCOL: LockingStrategy = OLC; // LHL works too, but readers are disjoint with writers!

type TxLiveKey = SnapShot;
type TxLiveValue = NullValue;
type DeadPageKey = Version;
type DeadPageValue<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload>
= BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>;

#[derive(Default, Clone)]
struct NullValue;

impl Display for NullValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "()")
    }
}

struct DeadPagesTrace<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static>
(SafeCell<BPlusTree<AUX_DP_FAN_OUT, AUX_DP_LEAF_SIZE, DeadPageKey, DeadPageValue<P_F, P_N, Key, Payload>>>);

impl<const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static> Deref for DeadPagesTrace<P_F, P_N, Key, Payload>
{
    type Target = BPlusTree<AUX_DP_FAN_OUT, AUX_DP_LEAF_SIZE, DeadPageKey, DeadPageValue<P_F, P_N, Key, Payload>>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

struct TxTrace
(SafeCell<BPlusTree<AUX_ATX_FAN_OUT, AUX_ATX_LEAF_SIZE, TxLiveKey, TxLiveValue>>);

impl Deref for TxTrace {
    type Target = BPlusTree<AUX_ATX_FAN_OUT, AUX_ATX_LEAF_SIZE, TxLiveKey, TxLiveValue>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl TxTrace {
    fn new() -> Self {
        Self(SafeCell::new(BPlusTree::new_with(
            AUX_PROTOCOL,
            SnapShot::MIN,
            SnapShot::MAX,
            inc_key,
            dec_key,
        )))
    }

    #[inline(always)]
    fn peek_min(&self) -> SnapShot {
        match self.dispatch(CRUDOperation::PeekMin) {
            (_, CRUDOperationResult::MatchedRecord(Some(r))) => r.key(),
            _ => SnapShot::MAX
        }
    }

    #[inline(always)]
    fn on_tx_start(&self, snapshot: SnapShot) -> bool {
        match self.dispatch(CRUDOperation::Insert(snapshot, NullValue)) {
            (.., CRUDOperationResult::Inserted(..)) => true,
            _ => false
        }
    }

    #[inline(always)]
    fn on_tx_completed(&self, snap_shot: SnapShot) {
        let _ = self.dispatch(CRUDOperation::Delete(snap_shot));
    }
}

impl<const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default> DeadPagesTrace<P_F, P_N, Key, Payload>
{
    fn new() -> Self {
        Self(SafeCell::new(BPlusTree::new_with(
            AUX_PROTOCOL,
            Version::MIN,
            Version::MAX,
            inc_key,
            dec_key,
        )))
    }

    #[inline(always)]
    fn pop_min(&self) -> Option<(Version, BlockRef<P_F, P_N, Key, Payload>)> {
        match self.dispatch(CRUDOperation::PopMin) {
            (.., CRUDOperationResult::Deleted(version, block)) =>
                Some((version, block)),
            _ => None
        }
    }

    #[inline(always)]
    fn register_died_page(&self, page_version: Version, page: DeadPageValue<P_F, P_N, Key, Payload>) {
        let _ = self.dispatch(CRUDOperation::Insert(page_version, page));
    }

    #[inline(always)]
    fn register_died_page_col(&self, dead_pages: [(Version, BlockRef<P_F, P_N, Key, Payload>); 2]) {
        dead_pages
            .into_iter()
            .for_each(|(d_v, d_p)| self.register_died_page(d_v, d_p))
    }
}

pub(crate) type MDBTracker<
    const P_F: usize,
    const P_N: usize,
    Key,
    Payload> = Arc<DBTracker<P_F, P_N, Key, Payload>>;

pub(crate) struct DBTracker<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static>
{
    live_tx: TxTrace,
    dead_blocks: DeadPagesTrace<P_F, P_N, Key, Payload>,
}

impl<const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default> DBTracker<P_F, P_N, Key, Payload>
{
    pub fn new() -> Self {
        Self {
            live_tx: TxTrace::new(),
            dead_blocks: DeadPagesTrace::new(),
        }
    }

    #[inline]
    pub fn on_tx_start(&self, snap_shot: SnapShot) -> bool {
        self.live_tx.on_tx_start(snap_shot)
    }

    #[inline]
    pub fn on_tx_completed(&self, snap_shot: SnapShot) {
        self.live_tx.on_tx_completed(snap_shot);
    }

    #[inline]
    pub fn register_died_page(&self, page_version: Version, page: DeadPageValue<P_F, P_N, Key, Payload>) {
        self.dead_blocks.register_died_page(page_version, page)
    }

    #[inline]
    pub fn register_died_page_col(&self, dead_pages: [(Version, BlockRef<P_F, P_N, Key, Payload>); 2]) {
        self.dead_blocks.register_died_page_col(dead_pages)
    }

    #[inline]
    pub fn free_block(&self) -> Option<BlockRef<P_F, P_N, Key, Payload>> {
        match self.dead_blocks.pop_min() {
            Some((dead_v, dead_block)) if dead_v.lt_self_any(self.live_tx.peek_min()) =>
                Some(dead_block),
            Some((live_v, live_block)) => {
                self.register_died_page(live_v, live_block);
                None
            }
            _ => None
        }
    }
}