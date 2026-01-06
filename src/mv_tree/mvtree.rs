use std::fmt::Display;
use std::hash::Hash;
use crate::mv_block::block_handle::BlockHandle;
use crate::mv_gc::tracker_handle::TrackerHandle;
use crate::mv_page_model::{Height, ObjectCount};
use crate::mv_root::index_root::{RootIndex, RootIndexType};
use crate::mv_sync::clock::GlobalClock;
use crate::mv_sync::latch_protocol::{LatchProtocol, OLC};

pub type LockLevel = ObjectCount;

pub const INIT_TREE_HEIGHT: Height = 1;
// pub const MAX_TREE_HEIGHT: Height = Height::MAX;

pub struct MVTreeSt<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> {
    pub(crate) root: RootIndex<FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub locking_strategy: LatchProtocol,
    pub block_manager: BlockHandle<FAN_OUT, NUM_RECORDS, Key, Payload>,
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
> Sync for MVTreeSt<FAN_OUT, NUM_RECORDS, Key, Payload> {}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> Send for MVTreeSt<FAN_OUT, NUM_RECORDS, Key, Payload> {}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Payload: Display + Clone + Default + Sync + 'static
> Default for MVTreeSt<FAN_OUT, NUM_RECORDS, u64, Payload> {
    fn default() -> Self {
        Self::olc_optimistic_clock(RootIndexType::default())
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Payload: Display + Clone + Default + Sync + 'static
> MVTreeSt<FAN_OUT, NUM_RECORDS, u64, Payload>
{
    pub fn count_roots(&self) -> usize {
        self.root.count_roots()
    }
    
    #[inline]
    pub fn make_standard(locking_strategy: LatchProtocol,
                         root_index_type: RootIndexType) -> Self
    {
        fn inc_key(k: u64) -> u64 {
            k.checked_add(1).unwrap_or(u64::MAX)
        }

        fn dec_key(k: u64) -> u64 {
            k.checked_sub(1).unwrap_or(u64::MIN)
        }

        Self::make(locking_strategy, root_index_type, inc_key, dec_key, u64::MIN, u64::MAX)
    }

    // pub fn standard() -> Self {
    //     Self::make_standard(LockingStrategy::MonoWriter, ClockType::FREE)
    // }

    pub fn olc_optimistic_clock(root_index_type: RootIndexType) -> Self {
        Self::make_standard(OLC(), root_index_type)
    }

    // pub fn olc() -> Self {
    //     Self::make_standard(OLC(), ClockType::SYNC)
    // }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync,
    Payload: Display + Clone + Default + Sync + 'static
> MVTreeSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub fn root_star_index(&self) -> RootIndexType {
        self.root.index_type(self.locking_strategy.latch_type())
    }

    #[inline(always)]
    pub(crate) fn tracker(&self) -> Option<TrackerHandle<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        self.block_manager.tracker()
    }

    #[inline]
    fn make(locking_strategy: LatchProtocol,
            root_index_type: RootIndexType,
            inc_key: fn(Key) -> Key,
            dec_key: fn(Key) -> Key,
            min_key: Key,
            max_key: Key,
    ) -> Self {
        let bm = BlockHandle::new();
        Self {
            root: RootIndex::new(root_index_type, &bm),
            block_manager: bm,
            global_clock: GlobalClock::new(),
            locking_strategy,
            inc_key,
            dec_key,
            min_key,
            max_key,
        }
    }
}