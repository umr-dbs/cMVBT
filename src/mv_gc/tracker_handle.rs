use crate::mv_page_model::internal_page::TimeMatcher;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;

use crate::mv_gc::block_tracer::{DeadPageValue, BlockTrace};
use crate::mv_gc::query_tracer::TransactionTrace;
use crate::mv_page_model::BlockRef;
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

    #[inline]// TODO: Just for checks
    pub fn free_block(&self) -> Option<BlockRef<P_F, P_N, Key, Payload>> {
        self.dead_blocks.pop_min().map(|s| s.1)
        // if let Some((dead_v, dead_block)) = self.dead_blocks.pop_min() {
        //     match self.live_tx.peek_min() {
        //         Some(live_min_snapshot) if dead_v.lt_self_any(live_min_snapshot) =>
        //             return Some(dead_block),
        //         _ => self.register_died_page(dead_v, dead_block)
        //     }
        // }
        //
        // None
    }
}