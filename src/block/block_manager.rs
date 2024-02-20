use std::collections::LinkedList;
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::{mem, ptr};
use std::sync::Arc;
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::{Release, SeqCst};
use parking_lot::Mutex;
use crate::block::block::Block;
use crate::page_model::internal_page::{InternalPage, TimeMatcher};
use crate::page_model::leaf_page::LeafPage;
use crate::page_model::node::Node;
use crate::page_model::{BlockRef, ObjectCount};
use crate::record_model::version_info::Version;
use crate::tx_model::transaction::SnapShot;
use crate::tx_model::tx_manager::ActiveTransactions;
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::{LatchType, SmartCell, SmartFlavor};

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
        mem::align_of::<Block<0, 0, Key>>() + // alignment for block
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

type DeadPages = Arc<Mutex<LinkedList<SnapShot>>>;

pub struct BlockManager<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display
> {
    active_tx: Option<ActiveTransactions>,
    dead_pages: Option<Arc<Mutex<LinkedList<(Version, BlockRef<FAN_OUT, NUM_RECORDS, Key>)>>>>,
    _marker: PhantomData<Key>,
    // block_id_counter: AtomicBlockID,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display
> Clone for BlockManager<FAN_OUT, NUM_RECORDS, Key> {
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
    Key: Default + Ord + Copy + Hash + Display
> Default for BlockManager<FAN_OUT, NUM_RECORDS, Key> {
    fn default() -> Self {
        BlockManager::new()
    }
}

/// Main functionality implementation for BlockManager.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display
> BlockManager<FAN_OUT, NUM_RECORDS, Key>
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
            dead_pages: Some(Arc::new(Default::default())),
            active_tx: Some(active_tx),
            _marker: PhantomData,
        }
    }

    pub(crate) fn set_active_tx_for_gc(&mut self, active_tx: Option<ActiveTransactions>) {
        if active_tx.is_some() {
            self.active_tx = active_tx;
            self.dead_pages = Some(Arc::new(Default::default()))
        }
        else {
            self.active_tx.take();
            self.dead_pages.take();
        }
    }

    #[inline(always)]
    pub(crate) fn register_dead_col(&self, dead: [(Version, BlockRef<FAN_OUT, NUM_RECORDS, Key>); 2]) {
        if let Some(ref dead_pages) = self.dead_pages {
            dead_pages.lock().extend(dead);
        }
    }

    #[inline(always)]
    pub(crate) fn register_dead(&self, dead: (Version, BlockRef<FAN_OUT, NUM_RECORDS, Key>)) {
        if let Some(ref dead_pages) = self.dead_pages {
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
    pub(crate) fn new_empty_leaf(&self, latch_type: LatchType) -> BlockRef<FAN_OUT, NUM_RECORDS, Key> {
        if let (Some(active_tx), Some(dead_pages))
            = (self.active_tx.as_ref(), self.dead_pages.as_ref())
        {
            let mut handle = dead_pages.lock();
            let front = handle.pop_front();
            mem::drop(handle);

            if let Some((version, page)) = front {
                let handle = active_tx.lock();
                let front = handle.peek().cloned();
                mem::drop(handle);

                if let Some(smallest_si) = front {
                    if version.lt(smallest_si) {
                        let m_page
                            = page.unsafe_borrow_mut().node_data.get_mut();

                        m_page.on_reuse();
                        m_page.mark_leaf();

                        assert!(m_page.is_leaf());
                        return page;
                    }
                } else {
                    let m_page
                        = page.unsafe_borrow_mut().node_data.get_mut();

                    m_page.on_reuse();
                    m_page.mark_leaf();

                    assert!(m_page.is_leaf());
                    return page;
                }
                dead_pages.lock().push_back((version, page));
            }
        }

        Block {
            // block_id: self.next_block_id(),
            node_data: SafeCell::new(Node::new_leaf())
        }.into_cell(latch_type)
    }

    /// Crafts a new aligned Index-Block.
    #[inline(always)]
    pub(crate) fn new_empty_index_block(&self, latch_type: LatchType) -> BlockRef<FAN_OUT, NUM_RECORDS, Key> {
        if let (Some(active_tx), Some(dead_pages))
            = (self.active_tx.as_ref(), self.dead_pages.as_ref())
        {
            let mut handle = dead_pages.lock();
            let front = handle.pop_front();
            mem::drop(handle);

            if let Some((version, page)) = front {
                let mut handle = active_tx.lock();
                let front = handle.peek().cloned();
                mem::drop(handle);

                if let Some(smallest_si) = front {
                    if version.lt(smallest_si) {
                        let m_page
                            = &mut page.unsafe_borrow_mut().node_data;

                        m_page.on_reuse();
                        m_page.mark_internal();

                        assert!(!m_page.is_leaf());
                        return page
                    }
                    else {
                        dead_pages.lock().push_front((version, page));
                    }
                } else {
                    let m_page
                        = &mut page.unsafe_borrow_mut().node_data;

                    m_page.on_reuse();
                    m_page.mark_internal();

                    assert!(!m_page.is_leaf());
                    return page
                }
            }
        }

        // println!("End Alloc Index");
        Block {
            // block_id: self.next_block_id(),
            node_data: SafeCell::new(Node::new_internal())
        }.into_cell(latch_type)
    }
}