use std::collections::LinkedList;
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::{mem, ptr};
use std::sync::Arc;
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::time::SystemTime;
use cc_bplustree::crud_model::crud_api::CRUDDispatcher;
use cc_bplustree::crud_model::crud_operation::CRUDOperation;
use cc_bplustree::crud_model::crud_operation_result::CRUDOperationResult;
// use cc_bplustree::crud_model::crud_api::CRUDDispatcher;
// use cc_bplustree::crud_model::crud_operation::CRUDOperation;
// use cc_bplustree::crud_model::crud_operation_result::CRUDOperationResult;
// use cc_bplustree::locking::locking_strategy::LockingStrategy::OLC;
// use cc_bplustree::locking::locking_strategy::orwc;
// use cc_bplustree::record_model::record_point::RecordPoint;
// use cc_bplustree::tree::bplus_tree::BPlusTree;
use parking_lot::{Mutex, RawMutex};
use parking_lot::lock_api::MutexGuard;
use rb_tree::RBTree;
use crate::block::block::Block;
use crate::page_model::internal_page::{InternalPage, TimeMatcher};
use crate::page_model::leaf_page::LeafPage;
use crate::page_model::node::Node;
use crate::page_model::{BlockRef, ObjectCount};
use crate::record_model::version_info::Version;
use crate::test::{dec_key, inc_key};
use crate::tx_model::transaction::SnapShot;
use crate::tx_model::tx_manager::ActiveTransactions;
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::{LatchType, OBSOLETE_FLAG_VERSION, SmartCell, SmartFlavor};

const ENABLE_SMALL_BLOCK: bool = false;
const MAX_ZEROS_PER_BLOCK: usize = 3964; // = data region in a block // outdated due to omitted block-id

/// Default starting numerical value for a valid BlockID.
// pub const START_BLOCK_ID: BlockID = BlockID::MIN;

pub const _1KB: usize = 1024;
pub const _2KB: usize = 2 * _1KB;
pub const _4KB: usize = 4 * _1KB;
pub const _8KB: usize = 8 * _1KB;
pub const _16KB: usize = 16 * _1KB;
pub const _32KB: usize = 32 * _1KB;

pub const fn bsz_alignment_min<Key, Payload>() -> usize
    where Key: Default + Ord + Copy + Hash + Display,
          Payload: Default + Clone
{
    mem::align_of::<Arc<()>>() + // ptr size
        mem::align_of::<usize>() + // dispatcher alignment
        mem::size_of::<usize>() * 2 + // arc extras in data area in Tree
        mem::align_of::<Block<0, 0, Key, Payload>>() + // alignment for block
        mem::size_of::<ObjectCount>() + // len indicator
        mem::size_of::<usize>() * 2 + // arc extras in data area
        mem::size_of::<SmartFlavor<()>>() + // align of SmartFlavor = size of empty data
        mem::size_of::<SmartCell<()>>() // align of SmartCell = size of usize
}

pub const fn bsz_alignment<Key, Payload>() -> usize
    where Key: Default + Ord + Copy + Hash + Display,
          Payload: Default + Clone
{
    bsz_alignment_min::<Key, Payload>() +
        if ENABLE_SMALL_BLOCK { MAX_ZEROS_PER_BLOCK } else { 0 }
}

type DeadPages<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key,
    Payload>
= Arc<Mutex<LinkedList<(Version, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)>>>;

// type DeadPages<const FAN_OUT: usize, const NUM_RECORDS: usize, Key>
// = Arc<SafeCell<BPlusTree<250, 250, Version, BlockRef<FAN_OUT, NUM_RECORDS, Key>>>>;


pub struct BlockManager<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    active_tx: Option<ActiveTransactions>,
    dead_pages: Option<DeadPages<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    _marker: PhantomData<Key>,
    // block_id_counter: AtomicBlockID,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Clone for BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn clone(&self) -> Self {
        Self {
            // block_id_counter: AtomicBlockID::new(START_BLOCK_ID),
            active_tx: None,
            dead_pages: None,
            _marker: PhantomData,
        }
    }
}

/// Default implementation for BlockManager with default BlockSettings.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default
> Default for BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn default() -> Self {
        BlockManager::new()
    }
}

/// Main functionality implementation for BlockManager.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default
> BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    // /// Generates and returns a new atomic (unique across callers) BlockID.
    // #[inline(always)]
    // pub(crate) fn next_block_id(&self) -> BlockID {
    //     self.block_id_counter.fetch_add(1, Ordering::Relaxed)
    // }

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
    pub const fn max_records_safe() -> usize {
        Self::max_records()
    }

    #[inline(always)]
    pub const fn min_active_records() -> usize { // 20%
        Self::max_records() / 5
    }

    #[inline(always)]
    pub const fn min_active_keys() -> usize { // 20%
        (Self::max_keys()) / 5
    }

    #[inline(always)]
    pub const fn max_keys() -> usize {
        FAN_OUT
    }

    #[inline(always)]
    pub const fn max_keys_safe() -> usize {
        Self::max_keys() - 1
    }

    /// Main Constructor requiring supplied BlockSettings.
    #[inline(always)]
    pub(crate) fn new() -> Self {
        Self {
            // block_id_counter: AtomicBlockID::new(START_BLOCK_ID),
            active_tx: None,
            dead_pages: None,
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn new_with_gc(active_tx: ActiveTransactions) -> Self {
        Self {
            // block_id_counter: AtomicBlockID::new(START_BLOCK_ID),
            dead_pages: Some(Arc::new(Mutex::new(LinkedList::new()))),
            active_tx: Some(active_tx),
            _marker: PhantomData,
        }
    }

    pub(crate) fn set_active_tx_for_gc(&mut self, active_tx: Option<ActiveTransactions>) {
        if active_tx.is_some() {
            self.active_tx = active_tx;
            self.dead_pages = Some(Arc::new(Mutex::new(LinkedList::new())))
        } else {
            self.active_tx.take();
            self.dead_pages.take();
        }
    }

    #[inline(always)]
    pub(crate) fn register_dead_col(&self, dead: [(Version, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>); 2]) {
        if let Some(ref dead_pages) = self.dead_pages {
            // dead.into_iter().for_each(|(v, p)| {
            //     match dead_pages.dispatch(CRUDOperation::Insert(v, p)) {
            //         (_, CRUDOperationResult::Inserted(..)) => {}
            //         _ => unreachable!()
            //     }
            // });
            dead_pages.lock().extend(dead);
        }
    }

    #[inline(always)]
    pub(crate) fn register_dead(&self, dead: (Version, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)) {
        if let Some(ref dead_pages) = self.dead_pages {
            // match dead_pages.dispatch(CRUDOperation::Insert(dead.0, dead.1)) {
            //     (_, CRUDOperationResult::Inserted(..)) => {}
            //     _ => unreachable!()
            // }
            dead_pages.lock().push_back(dead);
        }
    }

    // #[inline]
    // pub fn mark_version_ooo(v: *mut Version) {
    //     unsafe {
    //         *v = *v | OOO_REUSED_VERSION_MARK;
    //     }
    // }

    #[inline(always)]
    fn alloc_block(&self, latch_type: LatchType, leaf: bool) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        if let (Some(active_tx), Some(dead_pages))
            = (self.active_tx.as_ref(), self.dead_pages.as_ref())
        {
            match dead_pages.try_lock() {
                Some(mut guard) => {
                    let front
                        = guard.pop_front();

                    mem::drop(guard);

                    match front {
                        Some((dead_version, dead_block)) =>
                            match active_tx.dispatch(CRUDOperation::PeekMin) {
                                (.., CRUDOperationResult::MatchedRecord(smallest_si)) => {
                                    if smallest_si.is_none() || dead_version.lt_self_any(smallest_si.unwrap().key()) {
                                        // println!("Enter cc {:?}", SystemTime::now());
                                        let m_page
                                            = dead_block.unsafe_borrow_mut().node_data.get_mut();

                                        m_page.on_reuse();

                                        if leaf {
                                            m_page.mark_leaf()
                                        } else {
                                            m_page.mark_internal()
                                        }

                                        fence(Acquire);
                                        // println!("Leave cc {:?}", SystemTime::now());
                                        return dead_block;
                                    } else {
                                        // println!("Enter aa {:?}", SystemTime::now());
                                        dead_pages.lock().push_front((dead_version, dead_block));
                                        // println!("Leave aa {:?}", SystemTime::now());
                                    }
                                }
                                _ => unreachable!()
                            },
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        Block {
            // block_id: self.next_block_id(),
            node_data: SafeCell::new(if leaf { Node::new_leaf() } else { Node::new_internal() })
        }.into_cell(latch_type)
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
    pub(crate) fn new_empty_leaf(&self, latch_type: LatchType) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.alloc_block(latch_type, true)
    }

    /// Crafts a new aligned Index-Block.
    #[inline]
    pub(crate) fn new_empty_index_block(&self, latch_type: LatchType) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.alloc_block(latch_type, false)
    }
}