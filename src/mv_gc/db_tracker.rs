use crate::mv_page_model::internal_page::TimeMatcher;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use CCBPlusTree::locking::locking_strategy::LockingStrategy;
use CCBPlusTree::locking::locking_strategy::LockingStrategy::OLC;
use crate::mv_gc::bz_tracer::{DeadPageValue, BzTrace};
use crate::mv_gc::tx_tracer::TxTrace;
use crate::mv_page_model::BlockRef;
use crate::mv_record_model::version_info::Version;
use crate::mv_tx_model::transaction::SnapShot;

pub(crate) const AUX_PROTOCOL: LockingStrategy = OLC;

pub type MDBTracker<
    const P_F: usize,
    const P_N: usize,
    Key,
    Payload> = Arc<DBTracker<P_F, P_N, Key, Payload>>;

pub struct DBTracker<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static>
{
    live_tx: TxTrace,
    dead_blocks: BzTrace<P_F, P_N, Key, Payload>,
}

impl<const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default> DBTracker<P_F, P_N, Key, Payload>
{
    pub fn new() -> Self {
        Self {
            live_tx: TxTrace::new(),
            dead_blocks: BzTrace::new(),
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
        let max_si = self.live_tx.peek_max();
        if max_si == Version::MIN {
            None
        }
        else {
            Some(max_si)
        }
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