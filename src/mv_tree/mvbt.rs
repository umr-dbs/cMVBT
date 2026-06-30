use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use crate::mv_block::block_handle::BlockAllocManager;
use crate::mv_gc::tracker_handle::{TrackerHandle, TrackerHandleSt};
use crate::mv_page_model::{Height, ObjectCount};
use crate::mv_root::index_root::{RootIndex, RootIndexType};
use crate::mv_sync::clock::GlobalClock;

pub const FAN_OUT: usize        = 125;
pub const NUM_RECORDS: usize    = 125;
pub type Key                    = u64;
pub type Payload                = u64;
// pub type Payload = PayloadIndirection;
pub type MVBT                   = MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>;

pub const INIT_TREE_HEIGHT: Height = 1;
// pub const MAX_TREE_HEIGHT: Height = Height::MAX;

pub struct MVBTSt<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> {
    pub(crate) root: RootIndex<FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub block_manager: BlockAllocManager<FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub(crate) global_clock: GlobalClock,
    pub(crate) inc_key: fn(Key) -> Key,
    pub(crate) dec_key: fn(Key) -> Key,
    pub(crate) min_key: Key,
    pub(crate) max_key: Key,
}

unsafe impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Clone + Default + Display + Sync + 'static
> Sync for MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> {}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> Send for MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> {}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Payload: Display + Clone + Default + Sync + 'static
> Default for MVBTSt<FAN_OUT, NUM_RECORDS, u64, Payload> {
    fn default() -> Self {
        Self::make_standard(RootIndexType::default())
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, u64, Payload>
{
    pub fn count_roots(&self) -> usize {
        self.root.count_roots()
    }
    
    #[inline]
    pub fn make_standard(
        root_index_type: RootIndexType) -> Self
    {
        fn inc_key(k: u64) -> u64 {
            k.checked_add(1).unwrap_or(u64::MAX)
        }

        fn dec_key(k: u64) -> u64 {
            k.checked_sub(1).unwrap_or(u64::MIN)
        }

        Self::make(root_index_type, inc_key, dec_key, u64::MIN, u64::MAX)
    }

    // pub fn olc() -> Self {
    //     Self::make_standard(OLC(), ClockType::SYNC)
    // }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub fn enable_gc(&self, update_in_place: bool) {
        self.block_manager.pass_aux_tx_tracker(Some(Arc::new(TrackerHandleSt::new())));
        self.block_manager.set_update_in_place(update_in_place);
    }

    pub fn disable_gc(&self) {
        self.block_manager.del_aux()
    }

    pub fn root_star_index(&self) -> RootIndexType {
        self.root.index_type()
    }

    #[inline(always)]
    pub(crate) fn tracker(&self) -> Option<TrackerHandle<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        self.block_manager.tracker()
    }

    #[inline(always)]
    pub(crate) fn has_update_in_place(&self) -> bool {
        self.block_manager.has_update_in_place()
    }

    #[inline]
    fn make(root_index_type: RootIndexType,
            inc_key: fn(Key) -> Key,
            dec_key: fn(Key) -> Key,
            min_key: Key,
            max_key: Key,
    ) -> Self {
        let bm = BlockAllocManager::new();
        Self {
            root: RootIndex::new(root_index_type, &bm),
            block_manager: bm,
            global_clock: GlobalClock::new(),
            inc_key,
            dec_key,
            min_key,
            max_key,
        }
    }
}