use std::cell::Cell;
use std::collections::LinkedList;
use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use parking_lot::Mutex;
use crate::mv_block::block::Block;
use crate::mv_page_model::node::Node;
use crate::mv_page_model::{BlockRef, ObjectCount};
use crate::mv_record_model::version_info::Version;
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::smart_cell::SmartCell;
use crate::mv_gc::tracker_handle::TrackerHandle;

const ENABLE_SMALL_BLOCK: bool = false;
const MAX_ZEROS_PER_BLOCK: usize = 3964; // = data region in a mv_block // outdated due to omitted mv_block-id

/// Default starting numerical value for a valid BlockID.
// pub const START_BLOCK_ID: BlockID = BlockID::MIN;

pub const _1KB: usize = 1024;
pub const _2KB: usize = 2 * _1KB;
pub const _4KB: usize = 4 * _1KB;
pub const _8KB: usize = 8 * _1KB;
pub const _16KB: usize = 16 * _1KB;
pub const _32KB: usize = 32 * _1KB;

pub const fn bsz_alignment_min<Key, Payload>() -> usize
where
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Default + Clone,
{
    mem::align_of::<Arc<()>>() + // ptr size
        mem::align_of::<usize>() + // dispatcher alignment
        mem::size_of::<usize>() * 2 + // arc extras in data area in Tree
        mem::align_of::<Block<0, 0, Key, Payload>>() + // alignment for mv_block
        mem::size_of::<ObjectCount>() + // len indicator
        mem::size_of::<usize>() * 2 + // arc extras in data area
        // mem::size_of::<SmartFlavor<()>>() + // align of SmartFlavor = size of empty data
        mem::size_of::<SmartCell<()>>() // align of SmartCell = size of usize
}

// pub const fn bsz_alignment<Key, Payload>() -> usize
//     where Key: Default + Ord + Copy + Hash + Display,
//           Payload: Default + Clone
// {
//     bsz_alignment_min::<Key, Payload>() +
//         if ENABLE_SMALL_BLOCK { MAX_ZEROS_PER_BLOCK } else { 0 }
// }

type DeadPages<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key,
    Payload>
= Arc<Mutex<LinkedList<(Version, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)>>>;

// type DeadPages<const FAN_OUT: usize, const NUM_RECORDS: usize, Key>
// = Arc<SafeCell<BPlusTree<250, 250, Version, BlockRef<FAN_OUT, NUM_RECORDS, Key>>>>;

// pub static NODES_REQUEST: AtomicUsize = AtomicUsize::new(0);
pub struct BlockAllocManager<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static
> {
    tracker: SafeCell<Option<TrackerHandle<FAN_OUT, NUM_RECORDS, Key, Payload>>>,
    update_in_place: Cell<bool>,
    // pub reuse_count: AtomicUsize,
    // pub alloc_count: AtomicUsize,
    // block_id_counter: AtomicBlockID,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Clone for BlockAllocManager<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn clone(&self) -> Self {
        Self {
            // block_id_counter: AtomicBlockID::new(START_BLOCK_ID),
            tracker: SafeCell::new(None),
            update_in_place: Cell::new(false),
            // reuse_count: AtomicUsize::new(0),
            // alloc_count: AtomicUsize::new(0),
        }
    }
}

/// Default implementation for BlockManager with default BlockSettings.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default
> Default for BlockAllocManager<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn default() -> Self {
        BlockAllocManager::new()
    }
}

/// Main functionality implementation for BlockManager.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static
> BlockAllocManager<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    // /// Generates and returns a new atomic (unique across callers) BlockID.
    // #[inline(always)]
    // pub(crate) fn next_block_id(&self) -> BlockID {
    //     self.block_id_counter.fetch_add(1, Ordering::Relaxed)
    // }

    pub fn reset_alloc_reuse_counts(&self) {
        // self.reuse_count.store(0, SeqCst);
        // self.alloc_count.store(0, SeqCst);
    }
    
    #[inline(always)]
    pub(crate) fn tracker(&self) -> Option<TrackerHandle<FAN_OUT, NUM_RECORDS, Key, Payload>>  {
        self.tracker.clone()
    }

    #[inline(always)]
    pub(crate) const fn has_update_in_place(&self) -> bool {
        self.update_in_place.get()
    }

    #[inline(always)]
    pub const fn allocation_leaf(&self) -> usize {
        NUM_RECORDS
    }

    #[inline(always)]
    pub const fn allocation_directory(&self) -> usize {
        FAN_OUT
    }

    #[inline(always)]
    pub const fn max_records() -> usize {
        NUM_RECORDS
    }

    #[inline(always)]
    pub const fn overflow_records_count() -> usize {
        Self::max_records()
    }

    // #[inline(always)]
    // pub const fn min_active_records() -> usize { // 20%
    //     Self::max_records() / 5
    // }

    // #[inline(always)]
    // pub const fn min_active_keys() -> usize { // 20%
    //     Self::max_keys() / 5
    // }

    #[inline(always)]
    pub const fn max_keys() -> usize {
        FAN_OUT
    }

    #[inline(always)]
    pub const fn overflow_keys_count() -> usize {
        Self::max_keys() - 1
    }

    /// Main Constructor requiring supplied BlockSettings.
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            // block_id_counter: AtomicBlockID::new(START_BLOCK_ID),
            tracker: SafeCell::new(None),
            update_in_place: Cell::new(false),
            // reuse_count: AtomicUsize::new(0),
            // alloc_count: AtomicUsize::new(0),
        }
    }

    // #[inline(always)]
    // pub fn new_with_gc(db_tracker: TrackerHandle<FAN_OUT, NUM_RECORDS, Key, Payload>) -> Self {
    //     Self {
    //         // block_id_counter: AtomicBlockID::new(START_BLOCK_ID),
    //         tracker: SafeCell::new(Some(db_tracker)),
    //         reuse_count: AtomicUsize::new(0),
    //         alloc_count: AtomicUsize::new(0),
    //     }
    // }

    pub fn set_update_in_place(&self, update_in_place: bool) {
        self.update_in_place.set(update_in_place);
    }

    pub fn pass_aux_tx_tracker(&self, db_tracker: Option<TrackerHandle<FAN_OUT, NUM_RECORDS, Key, Payload>>) {
        *self.tracker.get_mut() = db_tracker;
    }
    
    pub fn del_aux(&self) {
        self.tracker.get_mut().take();
    }

    #[inline(always)]
    pub fn register_dead_col(&self, dead: [(Version, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>); 2]) {
        self.tracker
            .as_ref()
            .as_ref()
            .map(|tracker|
                tracker.register_died_page_col(dead));
    }

    #[inline(always)]
    pub fn register_dead(&self, dead_v: Version, dead_p: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>) {
        self.tracker
            .as_ref()
            .as_ref()
            .map(|tracker|
                tracker.register_died_page(dead_v, dead_p));
    }

    #[inline(always)]
    fn alloc_block(&self, leaf: bool) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        // NODES_REQUEST.fetch_add(1, Relaxed);
        match self.tracker.as_ref().as_ref().map(|tracker| tracker.free_block()) {
            Some(Some(block)) => {
                // self.reuse_count.fetch_add(1, Relaxed);

                let m_page
                    = block.unsafe_borrow_mut().node_data.get_mut();

                // println!("Reuse");
                m_page.on_reuse();

                if leaf {
                    m_page.mark_leaf()
                } else {
                    m_page.mark_internal()
                }

                // fence(Acquire);
                block
            }
            _ => {
                // self.alloc_count.fetch_add(1, Relaxed);
                // println!("Alloc");
                Block {
                    // block_id: self.next_block_id(),
                    node_data: SafeCell::new(if leaf { Node::new_leaf() } else { Node::new_internal() })
                }.into_cell()
            }
        }
    }


    // #[inline(always)]
    // fn alloc_block_index(&self, latch_type: LatchType, leaf: bool) -> BlockRef<FAN_OUT, NUM_RECORDS, Key> {
    //     if let (Some(active_tx), Some(dead_pages))
    //         = (self.active_tx.as_ref(), self.dead_pages.as_ref())
    //     {
    //         // println!("Enter bb {:?}", SystemTime::now());
    //         let (.., oldest_dead_page)
    //             = dead_pages.dispatch(CRUDOperation::PopMin);
    //
    //         match oldest_dead_page {
    //             CRUDOperationResult::MatchedRecord(
    //                 Some(RecordPoint {
    //                          key: dead_version,
    //                          payload: dead_block
    //                      })
    //             ) => match active_tx.dispatch(CRUDOperation::PeekMin) {
    //                 (.., CRUDOperationResult::MatchedRecord(smallest_si)) => {
    //                     if smallest_si.is_none() || dead_version.lt_self_any(smallest_si.unwrap().key()) {
    //                         // println!("Enter cc {:?}", SystemTime::now());
    //                         let m_page
    //                             = dead_block.unsafe_borrow_mut().node_data.get_mut();
    //
    //                         m_page.on_reuse();
    //
    //                         if leaf {
    //                             m_page.mark_leaf()
    //                         } else {
    //                             m_page.mark_internal()
    //                         }
    //
    //                         // println!("Leave cc {:?}", SystemTime::now());
    //                         return dead_block;
    //                     } else {
    //                         // println!("Enter aa {:?}", SystemTime::now());
    //                         let _ = dead_pages.dispatch(
    //                             CRUDOperation::Insert(dead_version, dead_block));
    //                         // println!("Leave aa {:?}", SystemTime::now());
    //                     }
    //                 }
    //                 _ => unreachable!()
    //             },
    //             _ => {}
    //         }
    //     }
    //
    //     // println!("Alloc {:?}", SystemTime::now());
    //     Block {
    //         // block_id: self.next_block_id(),
    //         node_data: SafeCell::new(if leaf { Node::new_leaf() } else { Node::new_internal() })
    //     }.into_cell(latch_type)
    // }
    //
    // #[inline(always)]
    // fn alloc_block(&self, latch_type: LatchType, leaf: bool) -> BlockRef<FAN_OUT, NUM_RECORDS, Key> {
    //     if let (Some(active_tx), Some(dead_pages))
    //         = (self.active_tx.as_ref(), self.dead_pages.as_ref())
    //     {
    //         match dead_pages.try_lock() {
    //             Some(mut guard) => {
    //                 let front
    //                     = guard.pop_front();
    //
    //                 mem::drop(guard);
    //
    //                 match front {
    //                     Some((m_version, page)) => match active_tx.try_lock() {
    //                         Some(guard_tx_si) => {
    //                             let smallest_si = guard_tx_si
    //                                 .peek()
    //                                 .cloned();
    //
    //                             mem::drop(guard_tx_si);
    //
    //                             if smallest_si.is_none() || m_version.lt_self_any(smallest_si.unwrap()) {
    //                                 let m_page
    //                                     = page.unsafe_borrow_mut().node_data.get_mut();
    //
    //                                 m_page.on_reuse();
    //
    //                                 if leaf {
    //                                     m_page.mark_leaf()
    //                                 } else {
    //                                     m_page.mark_internal()
    //                                 }
    //
    //                                 return page;
    //                             } else {
    //                                 dead_pages.lock().push_back((m_version, page))
    //                             }
    //                         }
    //                         _ => {}
    //                     },
    //                     _ => {}
    //                 }
    //             }
    //             _ => {}
    //         }
    //     }
    //
    //     Block {
    //         // block_id: self.next_block_id(),
    //         node_data: SafeCell::new(if leaf { Node::new_leaf() } else { Node::new_internal() })
    //     }.into_cell(latch_type)
    // }

    #[inline]
    pub(crate) fn new_empty_leaf(&self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.alloc_block(true)
    }

    /// Crafts a new aligned Index-Block.
    #[inline]
    pub(crate) fn new_empty_index_block(&self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.alloc_block(false)
    }
}