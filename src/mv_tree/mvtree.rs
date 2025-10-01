use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

use crate::mv_block::block_handle::BlockHandle;
use crate::mv_gc::tracker_handle::TrackerHandle;
use crate::mv_page_model::{BlockRef, Height, ObjectCount};
use crate::mv_record_model::version_info::Version;
use crate::mv_root::index_root::{RootIndex, RootIndexType};
use crate::mv_root::root::Root;
use crate::mv_sync::clock::{ClockType, GlobalClock};
use crate::mv_sync::latch_protocol::{LatchProtocol, OLC};
use crate::mv_sync::version_handle::VersionHandle;
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::smart_cell::{LatchType, OptCell, SmartCell, SmartFlavor, SmartGuard};

pub type LockLevel = ObjectCount;

pub const INIT_TREE_HEIGHT: Height = 1;
pub const MAX_TREE_HEIGHT: Height = Height::MAX;


// #[derive(Default, Clone)]
// pub(crate) struct RootItem<
//     const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Ord + Copy + Hash + Display,
//     Payload: Clone + Default
// > {
//     pub(crate) root: Root<FAN_OUT, NUM_RECORDS, Key, Payload>,
//     pub(crate) prev: Option<SmartCell<RootItem<FAN_OUT, NUM_RECORDS, Key, Payload>>>,
// }
//
// impl<const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Ord + Copy + Hash + Display,
//     Payload: Clone + Default
// > Deref for RootItem<FAN_OUT, NUM_RECORDS, Key, Payload> {
//     type Target = Root<FAN_OUT, NUM_RECORDS, Key, Payload>;
//
//     fn deref(&self) -> &Self::Target {
//         &self.root
//     }
// }
//
// impl<const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Ord + Copy + Hash + Display + 'static,
//     Payload: Clone + Default + 'static
// > RootItem<FAN_OUT, NUM_RECORDS, Key, Payload> {
//     pub(crate) fn deep_clone(&self, latch_type: LatchType) -> Self {
//         Self {
//             root: Root {
//                 block: self.root.block.unsafe_borrow().clone().into_cell(latch_type),
//                 version: self.version,
//                 height: self.height,
//             },
//             prev: self.prev.clone(),
//         }
//     }
// }
//
// pub(crate) type SmartRoot<
//     const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key,
//     Payload
// > = SmartCell<RootItem<FAN_OUT, NUM_RECORDS, Key, Payload>>;
//
// pub(crate) type RootItemGuard<
//     const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key,
//     Payload
// > = SmartGuard<RootItem<FAN_OUT, NUM_RECORDS, Key, Payload>>;

pub struct MVTreeSt<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> {
    // pub(crate) root: UnCell<SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    pub(crate) root: RootIndex<FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub locking_strategy: LatchProtocol,
    pub block_manager: BlockHandle<FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub(crate) version_manager: VersionHandle,
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
    #[inline]
    pub fn make_standard(locking_strategy: LatchProtocol,
                         clock_type: ClockType,
                         root_index_type: RootIndexType) -> Self
    {
        fn inc_key(k: u64) -> u64 {
            k.checked_add(1).unwrap_or(u64::MAX)
        }

        fn dec_key(k: u64) -> u64 {
            k.checked_sub(1).unwrap_or(u64::MIN)
        }

        Self::make(locking_strategy, clock_type, root_index_type, inc_key, dec_key, u64::MIN, u64::MAX)
    }

    // pub fn standard() -> Self {
    //     Self::make_standard(LockingStrategy::MonoWriter, ClockType::FREE)
    // }

    pub fn olc_optimistic_clock(root_index_type: RootIndexType) -> Self {
        Self::make_standard(OLC(), ClockType::OPT, root_index_type)
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

    // pub(crate) fn from(locking_strategy: &LockingStrategy,
    //                    block: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    //                    version: Version,
    //                    height: Height,
    //                    prev: Option<SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    // ) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload>
    // {
    //     let root_item = RootItem {
    //         root: Root::new(
    //             block,
    //             version,
    //             height,
    //         ),
    //         prev,
    //     };
    //
    //     Self::make_smart_root(locking_strategy.latch_type(), root_item)
    // }

    pub fn clock_type(&self) -> ClockType {
        match self.version_manager.committed_version {
            GlobalClock::Locked(_) => ClockType::SYNC,
            GlobalClock::Atomic(_) => ClockType::OPT,
            GlobalClock::Free(_) => ClockType::FREE
        }
    }

    #[inline(always)]
    pub(crate) fn tracker(&self) -> Option<TrackerHandle<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        self.block_manager.tracker()
    }

    // pub(crate) fn make_smart_root(latch_type: LatchType, root_item: RootItem<FAN_OUT, NUM_RECORDS, Key, Payload>) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload> {
    //     SmartCell(Arc::new(match latch_type {
    //         LatchType::Optimistic => SmartFlavor::OLCCell(
    //             OptCell::new(root_item)),
    //         LatchType::None => SmartFlavor::FreeCell(
    //             SafeCell::new(root_item))
    //     }))
    // }
    //
    // pub(crate) fn make_root_item(locking_strategy: &LockingStrategy,
    //                              block_manager: &BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload>,
    //                              version: Version,
    //                              height: Height,
    //                              prev: Option<SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    // ) -> SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload>
    // {
    //     let root_item = RootItem {
    //         root: Root::new(
    //             block_manager.new_empty_leaf(locking_strategy.latch_type()),
    //             version,
    //             height,
    //         ),
    //         prev,
    //     };
    //
    //     SmartCell(Arc::new(match locking_strategy.latch_type() {
    //         LatchType::Optimistic => SmartFlavor::OLCCell(
    //             OptCell::new(root_item)),
    //         LatchType::None => SmartFlavor::FreeCell(
    //             SafeCell::new(root_item))
    //     }))
    // }

    #[inline]
    fn make(locking_strategy: LatchProtocol,
            clock_type: ClockType,
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
            version_manager: match clock_type {
                ClockType::FREE => VersionHandle::new_free(),
                ClockType::OPT => VersionHandle::new_optimistic(),
                ClockType::SYNC => VersionHandle::new_locked()
            },
            locking_strategy,
            inc_key,
            dec_key,
            min_key,
            max_key,
        }
    }
}