use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::SeqCst;

use crate::mv_gc::block_tracer::{DeadPageValue, BlockTrace};
use crate::mv_gc::query_tracer::TransactionTrace;
use crate::mv_page_model::BlockRef;
use crate::mv_sync::clock::committed_snapshot;
use crate::mv_record_model::version_info::Version;
use crate::mv_tx_model::transaction_result::SnapShot;

pub type TrackerHandle<
    const P_F: usize,
    const P_N: usize,
    Key,
    Payload> = Arc<TrackerHandleSt<P_F, P_N, Key, Payload>>;

pub struct TrackerHandleSt<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static>
{
    live_tx: TransactionTrace,
    dead_blocks: BlockTrace<P_F, P_N, Key, Payload>,
}

impl<const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default> TrackerHandleSt<P_F, P_N, Key, Payload>
{
    pub fn new() -> Self {
        Self {
            live_tx: TransactionTrace::new(),
            dead_blocks: BlockTrace::new(),
        }
    }

    #[inline]
    pub fn on_tx_start(&self, snap_shot: SnapShot) {
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

    // #[inline]
    // pub fn oldest_live_si(&self) -> Option<SnapShot> {
    //     let min_si = self.live_tx.peek_min();
    //     if min_si == Version::MAX {
    //         None
    //     }
    //     else {
    //         Some(min_si)
    //     }
    // }

    #[inline]
    pub fn newest_live_si(&self) -> Option<SnapShot> {
        self.live_tx.peek_max()
    }

    /// Pins the calling reader *before* it draws its snapshot; see `TransactionTrace::pin`.
    #[inline]
    pub(crate) fn pin_reader(&self, lower_bound: SnapShot) -> (SnapShot, u64) {
        let ticket = self.live_tx.pin(lower_bound);
        fence(SeqCst);
        ticket
    }

    #[inline]
    pub(crate) fn unpin_reader(&self, ticket: &(SnapShot, u64)) {
        self.live_tx.unpin(ticket)
    }

    /// Hands out the oldest dead block no reader can reach anymore, if any.
    ///
    /// A block that died at version `d` is still reachable by every reader with snapshot `< d`.
    /// The bound is composed of the global committed snapshot, which lower-bounds every reader
    /// that has not announced itself yet, and the oldest announced reader. The committed
    /// snapshot MUST be read first: a reader announcing itself after our scan of the live
    /// readers draws its snapshot after our read of the committed snapshot, hence cannot
    /// observe a smaller one.
    #[inline]
    pub fn free_block(&self) -> Option<BlockRef<P_F, P_N, Key, Payload>> {
        let committed = committed_snapshot(Version::MAX);
        fence(SeqCst);
        let bound = self.live_tx
            .peek_min()
            .map_or(committed, |live_min| live_min.min(committed));

        self.dead_blocks
            .pop_min_if(|death_version| death_version < bound)
    }
}
