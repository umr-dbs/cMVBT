/// Buffer Pool for the cMVBT (Concurrent Multiversion B-Tree)
///
/// # Correctness contract
///
/// Readers in the cMVBT NEVER latch and NEVER validate a version after reading.
/// They read only the *committed section* of a node, whose size is given by the
/// live/dead counters in the node header. The buffer pool must therefore guarantee:
///
///   (R1) A page that a reader is currently traversing MUST remain in its frame.
///        → We use an *epoch-based* pin scheme: a reader registers an epoch on
///          entry and deregisters on exit. Eviction is only allowed for frames
///          whose last-access epoch is older than the oldest active reader epoch.
///
///   (R2) The frame a reader found via a BlockId pointer MUST still contain that
///        BlockId when it reads the committed section.
///        → Frames carry an atomic `block_id`. A reader validates block_id after
///          computing the address but before reading entries. If it has changed
///          (eviction + reload of different page), the reader restarts.
///          This is the ONLY "validation" readers ever do, and it is one single
///          atomic load — far cheaper than OLC version checks.
///
///   (W1) Writers pin the frame explicitly (fetch_page → unpin_page) around the
///        latch acquire / Append_and_Commit / unlatch sequence.
///
///   (W2) A frame is only evicted when pin_count == 0 AND no active reader epoch
///        overlaps with the frame's last-access epoch.
///
/// # Layout of per-frame metadata
///
///   latch_version : AtomicU64   — OLC seqlock. Even = unlocked, odd = locked.
///   node_type     : AtomicU8    — write-once; set at node creation. (shrunk from
///                                 AtomicUsize to save 7 bytes, node type fits u8)
///   len           : AtomicU32   — high 16 bits = live count, low 16 bits = dead count
///   block_id      : AtomicU64   — which page is loaded; u64::MAX = empty
///   pin_count     : AtomicU32   — explicit writer pins (readers use epoch table)
///   is_dirty      : AtomicBool
///   last_epoch    : AtomicU64   — epoch at which this frame was last accessed

use std::{fs::{File, OpenOptions}, io, mem, os::unix::fs::FileExt, path::Path, sync::{
    atomic::{
        AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
    },
    Mutex,
}};
use crate::mv_block::block::Block;
use crate::mv_page_model::BlockID;
use crate::mv_page_model::node::{active_len, dead_len};
use crate::mv_tree::mvbt::FAN_OUT;
use crate::mv_tree::mvbt::NUM_RECORDS;
use crate::mv_tree::mvbt::Key;
use crate::mv_tree::mvbt::Payload;

// This is a claude first prompt to proceed to a buffer pool impl. TODO:
// ── Constants ────────────────────────────────────────────────────────────────

pub const PAGE_SIZE: usize = size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>();
pub const EMPTY_PAGE: u64 = u64::MAX;

pub type FrameId = u32;

// ── Node type encoding (write-once per frame) ────────────────────────────────

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Internal = 0,
    Leaf     = 1,
    Root     = 2,
}

impl TryFrom<u8> for NodeType {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::Internal),
            1 => Ok(Self::Leaf),
            2 => Ok(Self::Root),
            _ => Err(()),
        }
    }
}


// ── Frame ────────────────────────────────────────────────────────────────────

/// One slot in the buffer pool. Holds exactly one page worth of data plus all
/// in-RAM metadata that must NOT be persisted to disk.
#[repr(C)]
pub struct Frame {
    // ── Concurrency metadata (in-RAM only, never serialised) ─────────────
    /// OLC seqlock. Even = unlocked; odd = write-locked.
    pub latch:      OlcLatch,

    /// Write-once after node creation. Stored as AtomicU8 to save 7 bytes
    /// vs AtomicUsize while still being safely shareable across threads.
    pub node_type:  AtomicU8,

    /// Combined live/dead counter: high 16 = live, low 16 = dead.
    /// Updated atomically by the writer holding the latch; readers load it
    /// once to know how many committed entries to scan.
    pub len:        AtomicU32,

    // ── Buffer pool bookkeeping ───────────────────────────────────────────
    /// Which BlockID is currently in `data`. EMPTY_PAGE when this frame is free.
    /// Readers validate this after computing the frame pointer (contract R2).
    pub block_id:   BlockID,

    /// Explicit pin count — incremented by writers on fetch_page, decremented
    /// on unpin_page. Eviction is only possible when this is 0 *and* no active
    /// reader epoch covers this frame (see EpochTable).
    pub pin_count:  AtomicU32,

    /// True if the in-RAM data has been modified since the last flush.
    pub is_dirty:   AtomicBool,

    /// The global epoch at which this frame was last accessed.
    /// Used by the epoch-based eviction guard for readers.
    pub last_epoch: AtomicU64,

    // ── Page data ─────────────────────────────────────────────────────────
    /// Raw page bytes. Access is governed by the latch for writers.
    /// Readers access only the committed section (bytes up to the committed
    /// boundary encoded in the page header) without any latch.
    ///
    /// SAFETY: UnsafeCell is required because readers and writers share the
    /// frame. The correctness invariant is:
    ///   - The *committed section* (entries 0..n_live+n_dead as counted in
    ///     `len`) is immutable once committed. Writers only append beyond it.
    ///   - The *uncommitted section* is written only by the latch holder and
    ///     becomes visible only after write_unlock() (Release ordering).
    pub data: std::cell::UnsafeCell<Box<[u8; PAGE_SIZE]>>,
}

// SAFETY: The UnsafeCell in `data` is safe to share across threads because:
//   1. Writes to the uncommitted section happen only under the write latch.
//   2. The committed section is truly immutable (cMVBT append-only invariant).
//   3. Readers only ever touch the committed section.
unsafe impl Sync for Frame {}
unsafe impl Send for Frame {}

impl Frame {
    fn new() -> Self {
        Self {
            latch:      OlcLatch::new(),
            node_type:  AtomicU8::new(NodeType::Leaf as u8),
            len:        AtomicU32::new(0),
            block_id:   AtomicU64::new(EMPTY_PAGE),
            pin_count:  AtomicU32::new(0),
            is_dirty:   AtomicBool::new(false),
            last_epoch: AtomicU64::new(0),
            data:       std::cell::UnsafeCell::new(Box::new([0u8; PAGE_SIZE])),
        }
    }

    /// Reset all in-RAM metadata. Called when this frame is reused for a
    /// different page. Must only be called with pin_count == 0 and no active
    /// readers (verified by the epoch table before eviction).
    fn reset_metadata(&self, new_block_id: BlockID, epoch: u64) {
        self.latch.reset();
        self.is_dirty.store(false,           Ordering::Release);
        self.pin_count.store(0,              Ordering::Release);
        self.last_epoch.store(epoch,         Ordering::Release);
        // block_id is set last so that any thread that races a lookup and
        // finds this frame already sees the correct block_id after the data
        // is fully written (see fetch_page's double-checked pattern).
        self.block_id.store(new_block_id,    Ordering::Release);
    }

    /// Rebuild the live/dead counter from the on-disk page header.
    /// Called after loading a page from disk. The latch is not yet published
    /// (block_id is still EMPTY_PAGE), so this is single-threaded.
    fn rebuild_len_from_header(&self) {
        let data = unsafe { &**self.data.get() };
        let live = u16::from_le_bytes([data[PAGE_HEADER_LIVE_OFF],
            data[PAGE_HEADER_LIVE_OFF + 1]]);
        let dead = u16::from_le_bytes([data[PAGE_HEADER_DEAD_OFF],
            data[PAGE_HEADER_DEAD_OFF + 1]]);
        self.len.store(make_len(live, dead), Ordering::Release);
        self.node_type.store(data[PAGE_HEADER_TYPE_OFF], Ordering::Release);
    }

    /// Sync the live/dead counters back into the page header before flushing.
    /// Must be called with the latch released (data is in a stable committed
    /// state — no uncommitted section is pending).
    fn sync_header_for_flush(&self) {
        debug_assert!(
            self.latch.version() % 2 == 0,
            "sync_header_for_flush called while latch is held — \
             uncommitted data would be written to disk"
        );
        let data = unsafe { &mut **self.data.get() };
        let len  = self.len.load(Ordering::Acquire);
        let live = (active_len(len) as u16).to_le_bytes();
        let dead = (dead_len(len)   as u16).to_le_bytes();
        data[PAGE_HEADER_LIVE_OFF]     = live[0];
        data[PAGE_HEADER_LIVE_OFF + 1] = live[1];
        data[PAGE_HEADER_DEAD_OFF]     = dead[0];
        data[PAGE_HEADER_DEAD_OFF + 1] = dead[1];
        data[PAGE_HEADER_TYPE_OFF]     = self.node_type.load(Ordering::Relaxed);
    }
}

// ── Page header offsets (first PAGE_HEADER_SIZE bytes of every page) ─────────
//
//  offset  size  field
//  0       8     block_id        (u64 le)
//  8       1     node_type       (u8)
//  9       2     n_live          (u16 le)
//  11      2     n_dead          (u16 le)
//  13      3     _padding
//  16      …     committed entries start here

pub const PAGE_HEADER_SIZE:     usize = 16;
pub const PAGE_HEADER_LIVE_OFF: usize = 9;
pub const PAGE_HEADER_DEAD_OFF: usize = 11;
pub const PAGE_HEADER_TYPE_OFF: usize = 8;

// ── Epoch table (reader safety without latching) ─────────────────────────────
//
// Every reader thread registers its start epoch before it begins a traversal
// and deregisters when done. The eviction path checks that no registered
// epoch is ≤ the frame's last_epoch. This is the standard "safe epoch"
// (hazard epoch) pattern used in LeanStore and other systems.
//
// The table is a fixed-size array indexed by thread-local thread ID.
// MAX_READER_THREADS must be ≥ the actual number of concurrent reader threads.

pub const MAX_THREADS: usize = 256;
const EPOCH_SENTINEL: u64 = u64::MAX; // means "not in a read section"

pub struct EpochTable {
    /// Per-thread registered epoch. EPOCH_SENTINEL = inactive.
    slots:        Box<[AtomicU64; MAX_THREADS]>,
    /// Monotonically increasing global epoch. Incremented on every page fetch.
    global_epoch: AtomicU64,
}

impl EpochTable {
    pub fn new() -> Self {
        // AtomicU64 does not implement Copy, so we cannot use array init syntax.
        // Build via a Vec and convert.
        let slots: Vec<AtomicU64> = (0..MAX_THREADS)
            .map(|_| AtomicU64::new(EPOCH_SENTINEL))
            .collect();
        Self {
            slots:        slots.try_into().ok().unwrap(),
            global_epoch: AtomicU64::new(0),
        }
    }

    /// Returns the current global epoch (monotonically increasing).
    #[inline]
    pub fn current(&self) -> u64 {
        self.global_epoch.load(Ordering::Acquire)
    }

    /// Advance the global epoch and return the new value.
    /// Called once per page fetch so that frame.last_epoch tracks recency.
    #[inline]
    pub fn advance(&self) -> u64 {
        self.global_epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// A reader thread calls this BEFORE it starts traversing any node.
    /// `thread_id` must be unique per OS thread (e.g. from a thread-local).
    #[inline]
    pub fn enter(&self, thread_id: usize) -> u64 {
        let epoch = self.current();
        // Store with Release so that any subsequent reads of frame data
        // in this thread happen after the epoch is published.
        self.slots[thread_id].store(epoch, Ordering::Release);
        std::sync::atomic::fence(Ordering::SeqCst);
        epoch
    }

    /// A reader thread calls this AFTER it has finished its traversal.
    #[inline]
    pub fn exit(&self, thread_id: usize) {
        self.slots[thread_id].store(EPOCH_SENTINEL, Ordering::Release);
    }

    /// Returns the minimum epoch across all active readers, or u64::MAX if
    /// no reader is active. Eviction is safe for frames with last_epoch <
    /// this value.
    pub fn min_active_epoch(&self) -> u64 {
        let mut min = u64::MAX;
        for slot in self.slots.iter() {
            let e = slot.load(Ordering::Acquire);
            if e != EPOCH_SENTINEL && e < min {
                min = e;
            }
        }
        min
    }

    /// True if it is safe to evict a frame last accessed at `frame_epoch`.
    #[inline]
    pub fn safe_to_evict(&self, frame_epoch: u64) -> bool {
        // If no reader is active, min_active_epoch returns MAX — always safe.
        // Otherwise, we need frame_epoch < min active epoch so that no reader
        // that registered before or during frame_epoch is still running.
        frame_epoch < self.min_active_epoch()
    }
}

// ── CLOCK replacer ───────────────────────────────────────────────────────────

struct ClockReplacer {
    hand:      usize,
    ref_bit:   Vec<AtomicBool>,
    evictable: Vec<AtomicBool>,
    n:         usize,
}

impl ClockReplacer {
    fn new(n: usize) -> Self {
        Self {
            hand:      0,
            ref_bit:   (0..n).map(|_| AtomicBool::new(false)).collect(),
            evictable: (0..n).map(|_| AtomicBool::new(false)).collect(),
            n,
        }
    }

    fn record_access(&self, fid: FrameId) {
        self.ref_bit[fid as usize].store(true, Ordering::Relaxed);
    }

    fn set_evictable(&self, fid: FrameId, evictable: bool) {
        self.evictable[fid as usize].store(evictable, Ordering::Release);
    }

    /// Find a victim frame. Returns None if all frames are pinned.
    /// The *caller* must verify epoch safety before actually evicting.
    fn evict_candidate(&mut self) -> Option<FrameId> {
        for _ in 0..2 * self.n {
            let f = self.hand % self.n;
            self.hand = (self.hand + 1) % self.n;
            if !self.evictable[f].load(Ordering::Acquire) { continue; }
            if self.ref_bit[f].load(Ordering::Relaxed) {
                self.ref_bit[f].store(false, Ordering::Relaxed); // second chance
            } else {
                return Some(f as FrameId);
            }
        }
        None
    }
}

// ── Disk manager ─────────────────────────────────────────────────────────────

pub struct DiskManager {
    file: File,
}

impl DiskManager {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true).write(true).create(true)
            .open(path)?;
        Ok(Self { file })
    }

    pub fn read_page(&self, id: BlockID, buf: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
        self.file.read_exact_at(buf, id * PAGE_SIZE as u64)
    }

    pub fn write_page(&self, id: BlockID, buf: &[u8; PAGE_SIZE]) -> io::Result<()> {
        self.file.write_all_at(buf, id * PAGE_SIZE as u64)
    }
}

// ── Buffer Pool ───────────────────────────────────────────────────────────────

pub struct BufferPool {
    frames:    Box<[Frame]>,

    /// BlockID → FrameId. DashMap shards the lock to reduce contention.
    /// We avoid holding a DashMap reference across any blocking operation.
    page_table: dashmap::DashMap<BlockID, FrameId>,

    free_list:  Mutex<std::collections::VecDeque<FrameId>>,
    replacer:   Mutex<ClockReplacer>,

    pub epochs: EpochTable,
    disk:       DiskManager,
}

impl BufferPool {
    pub fn new(num_frames: usize, path: impl AsRef<Path>) -> io::Result<Self> {
        let frames: Box<[Frame]> = (0..num_frames)
            .map(|_| Frame::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let free_list = (0..num_frames as FrameId).collect();
        Ok(Self {
            frames,
            page_table: dashmap::DashMap::new(),
            free_list:  Mutex::new(free_list),
            replacer:   Mutex::new(ClockReplacer::new(num_frames)),
            epochs:     EpochTable::new(),
            disk:       DiskManager::open(path)?,
        })
    }

    // ── Public API for writers ────────────────────────────────────────────

    /// Fetch a page into the pool and pin it (increments pin_count).
    ///
    /// Writers call this before attempting XLatchOpt. The returned FrameId
    /// is valid until the matching `unpin_page` call.
    ///
    /// CORRECTNESS: We use `page_table.entry()` to serialise concurrent
    /// fetches of the same BlockID, preventing two threads from loading the
    /// same page into different frames (the "ABA on eviction" race).
    pub fn fetch_page(&self, block_id: BlockID) -> Option<FrameId> {
        // Fast path: already in pool. We can use a shared (read) reference
        // here because DashMap's get() holds only a shard read-lock.
        if let Some(r) = self.page_table.get(&block_id) {
            let fid = *r;
            let frame = &self.frames[fid as usize];
            frame.pin_count.fetch_add(1, Ordering::AcqRel);
            let epoch = self.epochs.advance();
            frame.last_epoch.store(epoch, Ordering::Release);
            self.replacer.lock().unwrap().set_evictable(fid, false);
            self.replacer.lock().unwrap().record_access(fid);
            return Some(fid);
        }

        // Slow path: load from disk. Hold an entry() guard for the duration
        // so no other thread can race to load the same block.
        use dashmap::mapref::entry::Entry;
        match self.page_table.entry(block_id) {
            Entry::Occupied(o) => {
                // Another thread loaded it while we were waiting for the shard lock.
                let fid = *o.get();
                let frame = &self.frames[fid as usize];
                frame.pin_count.fetch_add(1, Ordering::AcqRel);
                let epoch = self.epochs.advance();
                frame.last_epoch.store(epoch, Ordering::Release);
                self.replacer.lock().unwrap().set_evictable(fid, false);
                self.replacer.lock().unwrap().record_access(fid);
                Some(fid)
            }
            Entry::Vacant(v) => {
                let fid = self.alloc_frame(block_id)?;
                let frame = &self.frames[fid as usize];

                // Load page data from disk. frame.block_id is still EMPTY_PAGE
                // here, so no reader can accidentally find this frame yet.
                {
                    let data = unsafe { &mut **frame.data.get() };
                    self.disk.read_page(block_id, data).ok()?;
                }

                // Rebuild in-RAM metadata from the page header.
                frame.rebuild_len_from_header();

                // Publish: set block_id last, with Release, so all writes
                // above are visible to any thread that subsequently reads
                // frame.block_id and finds our block_id.
                let epoch = self.epochs.advance();
                frame.last_epoch.store(epoch, Ordering::Release);
                frame.pin_count.store(1,        Ordering::Release);
                frame.block_id.store(block_id,  Ordering::Release);

                // Insert into page table before releasing the entry guard,
                // so other threads doing fetch_page(block_id) will find it.
                v.insert(fid);

                self.replacer.lock().unwrap().set_evictable(fid, false);
                self.replacer.lock().unwrap().record_access(fid);
                Some(fid)
            }
        }
    }

    /// Allocate a brand-new page (called when cMVBT creates a new node).
    ///
    /// Behaviour differs from `fetch_page` in two ways:
    ///   1. No disk read — the page is zeroed and initialised in-RAM.
    ///   2. The page is immediately dirty.
    pub fn alloc_new_page(
        &self,
        block_id:  BlockID,
        node_type: NodeType,
    ) -> Option<FrameId> {
        use dashmap::mapref::entry::Entry;
        let Entry::Vacant(v) = self.page_table.entry(block_id) else {
            // block_id is already in the pool — the caller reused a graveyard
            // id that hasn't been evicted yet. This is valid: just return the
            // existing frame after resetting it for the new node.
            //
            // In practice this should be rare; the graveyard list calls
            // force_evict_if_unpinned before handing a BlockID back.
            return self.reinit_existing(block_id, node_type);
        };

        let fid = self.alloc_frame(block_id)?;
        let frame = &self.frames[fid as usize];

        // Zero the page and write the type byte into the header.
        {
            let data = unsafe { &mut **frame.data.get() };
            data.fill(0);
            data[PAGE_HEADER_TYPE_OFF] = node_type as u8;
            // block_id in header
            data[0..8].copy_from_slice(&block_id.to_le_bytes());
        }

        frame.node_type.store(node_type as u8, Ordering::Release);
        frame.len.store(0,          Ordering::Release);
        frame.is_dirty.store(true,  Ordering::Release);
        frame.pin_count.store(1,    Ordering::Release);
        let epoch = self.epochs.advance();
        frame.last_epoch.store(epoch, Ordering::Release);
        frame.block_id.store(block_id, Ordering::Release);

        v.insert(fid);
        self.replacer.lock().unwrap().set_evictable(fid, false);
        Some(fid)
    }

    /// Decrement pin count. Once it reaches 0, the frame becomes a candidate
    /// for eviction (subject to the epoch guard).
    ///
    /// `dirty` — pass true if this writer modified the page (i.e. after a
    /// successful Append_and_Commit).
    pub fn unpin_page(&self, block_id: BlockID, dirty: bool) {
        if let Some(r) = self.page_table.get(&block_id) {
            let fid   = *r;
            let frame = &self.frames[fid as usize];
            if dirty {
                frame.is_dirty.store(true, Ordering::Release);
            }
            let prev = frame.pin_count.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(prev > 0, "unpin_page called with pin_count == 0");
            if prev == 1 {
                self.replacer.lock().unwrap().set_evictable(fid, true);
            }
        }
    }

    /// Flush a specific page to disk without evicting it.
    /// Called during checkpointing or before recycling a dead node.
    pub fn flush_page(&self, block_id: BlockID) -> io::Result<()> {
        if let Some(r) = self.page_table.get(&block_id) {
            let fid   = *r;
            let frame = &self.frames[fid as usize];
            if frame.is_dirty.load(Ordering::Acquire) {
                // Sync live/dead counters into the page header before writing.
                frame.sync_header_for_flush();
                let data = unsafe { &**frame.data.get() };
                self.disk.write_page(block_id, data)?;
                frame.is_dirty.store(false, Ordering::Release);
            }
        }
        Ok(())
    }

    // ── Public API for readers ────────────────────────────────────────────

    /// Resolve a BlockID to a FrameId for a READER (no pin, no latch).
    ///
    /// Returns the FrameId AND the block_id that was in the frame at lookup
    /// time. The reader must call `validate_frame` after reading the committed
    /// section to confirm the frame still holds the same page (contract R2).
    ///
    /// If the page is not in the pool this returns None — the reader should
    /// then call `fetch_page` (which loads it) and proceed as a writer would
    /// for the buffer-pool interaction, but without acquiring the tree latch.
    ///
    /// NOTE: Readers must have called `epochs.enter(thread_id)` before any
    /// traversal step and `epochs.exit(thread_id)` at the end (or on restart).
    pub fn resolve_for_reader(&self, block_id: BlockID) -> Option<FrameId> {
        let r   = self.page_table.get(&block_id)?;
        let fid = *r;
        // Update access epoch so the eviction guard knows this frame is hot.
        // We do NOT increment pin_count — readers rely on the epoch table.
        let epoch = self.epochs.advance();
        self.frames[fid as usize].last_epoch.store(epoch, Ordering::Release);
        Some(fid)
    }

    /// After a reader has consumed committed entries from a frame, it calls
    /// this to confirm the frame still holds the expected page.
    ///
    /// Returns false if the page was evicted and a different page loaded
    /// in its place — the reader must restart its traversal from the root.
    #[inline]
    pub fn validate_frame(&self, fid: FrameId, expected_block_id: BlockID) -> bool {
        self.frames[fid as usize].block_id.load(Ordering::Acquire) == expected_block_id
    }

    /// Get a raw pointer to the frame's page data for reading.
    ///
    /// SAFETY: The caller must ensure it only reads within the committed
    /// section (bytes [PAGE_HEADER_SIZE .. PAGE_HEADER_SIZE + entry_bytes])
    /// and that it has validated block_id before and after reading
    /// (via validate_frame). See contract R2.
    #[inline]
    pub unsafe fn reader_data(&self, fid: FrameId) -> *const u8 {
        (*self.frames[fid as usize].data.get()).as_ptr()
    }

    /// Get a mutable pointer to the frame's page data for writing.
    ///
    /// SAFETY: Caller must hold the write latch (OlcLatch) for this frame.
    #[inline]
    pub unsafe fn writer_data(&self, fid: FrameId) -> *mut u8 {
        (*self.frames[fid as usize].data.get()).as_mut_ptr()
    }

    // ── Graveyard / GC integration ────────────────────────────────────────

    /// Called by the graveyard list before recycling a BlockID.
    /// Evicts the frame if it is in the pool and unpinned, and no active
    /// reader epoch covers it.
    ///
    /// Returns true if the frame was evicted (or was not in the pool).
    /// Returns false if the frame is still pinned or covered by an active
    /// reader epoch — the graveyard should retry later.
    pub fn force_evict_if_safe(&self, block_id: BlockID) -> bool {
        let Some(r) = self.page_table.get(&block_id) else {
            return true; // not in pool at all
        };
        let fid   = *r;
        let frame = &self.frames[fid as usize];

        // Check pin count
        if frame.pin_count.load(Ordering::Acquire) > 0 {
            return false;
        }

        // Check epoch safety
        let frame_epoch = frame.last_epoch.load(Ordering::Acquire);
        if !self.epochs.safe_to_evict(frame_epoch) {
            return false;
        }

        // Drop the shared reference before we mutate
        drop(r);

        // Dead nodes are immutable — no need to flush dirty data.
        // (The page was flushed when the reorganisation committed it as dead.)
        self.page_table.remove(&block_id);
        frame.block_id.store(EMPTY_PAGE, Ordering::Release);
        frame.is_dirty.store(false,      Ordering::Release);
        self.replacer.lock().unwrap().set_evictable(fid, false);
        self.free_list.lock().unwrap().push_back(fid);
        true
    }

    // ── Frame accessor (for tree code that has a FrameId) ─────────────────

    #[inline]
    pub fn frame(&self, fid: FrameId) -> &Frame {
        &self.frames[fid as usize]
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    /// Allocate a free frame, evicting a victim if necessary.
    /// The returned frame has block_id == EMPTY_PAGE and pin_count == 0.
    fn alloc_frame(&self, _for_block: BlockID) -> Option<FrameId> {
        // Try the free list first (O(1), no epoch check needed).
        if let Some(fid) = self.free_list.lock().unwrap().pop_front() {
            return Some(fid);
        }

        // No free frames — run the clock replacer.
        // We may need several attempts because the epoch guard can reject
        // a candidate even after the clock hand selects it.
        let n = self.frames.len();
        for _ in 0..2 * n {
            let candidate = self.replacer.lock().unwrap().evict_candidate()?;
            let frame = &self.frames[candidate as usize];

            // Double-check: must still be unpinned.
            if frame.pin_count.load(Ordering::Acquire) > 0 { continue; }

            // Epoch guard: no active reader may be touching this frame.
            let fe = frame.last_epoch.load(Ordering::Acquire);
            if !self.epochs.safe_to_evict(fe) { continue; }

            // Safe to evict. Flush if dirty.
            let old_bid = frame.block_id.load(Ordering::Acquire);
            if old_bid != EMPTY_PAGE {
                if frame.is_dirty.load(Ordering::Acquire) {
                    frame.sync_header_for_flush();
                    let data = unsafe { &**frame.data.get() };
                    self.disk.write_page(old_bid, data).ok()?;
                }
                self.page_table.remove(&old_bid);
                // Publish eviction: set block_id to EMPTY_PAGE so any thread
                // that resolved this frame via a stale pointer sees the change
                // in validate_frame (contract R2).
                frame.block_id.store(EMPTY_PAGE, Ordering::Release);
            }

            frame.reset_metadata(EMPTY_PAGE, 0); // caller will set real values
            self.replacer.lock().unwrap().set_evictable(candidate, false);
            return Some(candidate);
        }

        None // all frames are pinned or reader-covered
    }

    /// Handle the edge case in alloc_new_page where the graveyard returned a
    /// BlockID that is still cached. Re-initialise the existing frame.
    fn reinit_existing(&self, block_id: BlockID, node_type: NodeType) -> Option<FrameId> {
        let r   = self.page_table.get(&block_id)?;
        let fid = *r;
        let frame = &self.frames[fid as usize];
        {
            let data = unsafe { &mut **frame.data.get() };
            data.fill(0);
            data[PAGE_HEADER_TYPE_OFF] = node_type as u8;
            data[0..8].copy_from_slice(&block_id.to_le_bytes());
        }
        frame.latch.reset();
        frame.node_type.store(node_type as u8, Ordering::Release);
        frame.len.store(0,         Ordering::Release);
        frame.is_dirty.store(true, Ordering::Release);
        frame.pin_count.fetch_add(1, Ordering::AcqRel);
        Some(fid)
    }
}

// ── Thread-local thread ID ────────────────────────────────────────────────────
//
// Each thread needs a stable ID in [0, MAX_THREADS) for the epoch table.
// We assign IDs from an atomic counter on first use.

static NEXT_THREAD_ID: AtomicUsize = AtomicUsize::new(0);

std::thread_local! {
    static THREAD_ID: usize = NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed);
}

pub fn current_thread_id() -> usize {
    THREAD_ID.with(|id| *id)
}

// ── Reader guard (RAII) ───────────────────────────────────────────────────────
//
// Wrap reader traversal in a guard so epoch.exit() is called even on panic.

pub struct ReaderGuard<'a> {
    pool:      &'a BufferPool,
    thread_id: usize,
}

impl<'a> ReaderGuard<'a> {
    pub fn new(pool: &'a BufferPool) -> Self {
        let tid = current_thread_id();
        pool.epochs.enter(tid);
        Self { pool, thread_id: tid }
    }

    /// Resolve a BlockID to a FrameId. Returns (fid, snapshot of block_id).
    /// Call validate() after reading to confirm R2.
    pub fn resolve(&self, block_id: BlockID) -> Option<(FrameId, BlockID)> {
        let fid = self.pool.resolve_for_reader(block_id)?;
        // Read block_id AFTER resolving so we have a stable baseline.
        let confirmed_bid = self.pool.frame(fid).block_id.load(Ordering::Acquire);
        if confirmed_bid != block_id {
            return None; // page was evicted between page_table lookup and here
        }
        Some((fid, confirmed_bid))
    }

    /// Validate that frame `fid` still holds `expected_block_id`.
    /// Call this AFTER reading the committed entries. Returns false → restart.
    #[inline]
    pub fn validate(&self, fid: FrameId, expected_block_id: BlockID) -> bool {
        self.pool.validate_frame(fid, expected_block_id)
    }

    /// Access the committed section of a frame.
    ///
    /// SAFETY: You must call validate() before and after accessing data to
    /// satisfy contract R2. Only read within [0 .. PAGE_HEADER_SIZE +
    /// entry_bytes) where entry_bytes is derived from the `len` atomic.
    pub unsafe fn committed_data(&self, fid: FrameId) -> (*const u8, u32) {
        let frame = self.pool.frame(fid);
        let len   = frame.len.load(Ordering::Acquire);
        (self.pool.reader_data(fid), len)
    }
}

impl Drop for ReaderGuard<'_> {
    fn drop(&mut self) {
        self.pool.epochs.exit(self.thread_id);
    }
}

// ── Usage example showing the full read/write protocol ───────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn temp_pool(n: usize) -> BufferPool {
        let path = format!("/tmp/cmvbt_test_{}.db", std::process::id());
        BufferPool::new(n, &path).unwrap()
    }

    // ── Writer protocol ───────────────────────────────────────────────────

    /// Correct writer sequence mirroring WriteTraversal / Repair:
    ///
    ///   1. fetch_page(bid)        → pin, get fid
    ///   2. read frame.latch.version() → p_stat
    ///   3. if node is unsafe → Repair (latch parent, reorganise, unlatch)
    ///   4. if leaf → latch.try_write_lock(p_stat)
    ///   5. Append_and_Commit (write to uncommitted section, move boundary)
    ///   6. latch.write_unlock()
    ///   7. unpin_page(bid, dirty=true)
    fn writer_example(pool: &BufferPool, bid: BlockID) {
        let fid    = pool.fetch_page(bid).expect("page must be available");
        let frame  = pool.frame(fid);
        let p_stat = frame.latch.version();

        // Try to acquire write latch (XLatchOpt from Algorithm 1)
        if !frame.latch.try_write_lock(p_stat) {
            pool.unpin_page(bid, false);
            // → restart WriteTraversal in real code
            return;
        }

        // === Append_and_Commit would happen here ===
        // Write new entry into uncommitted section, then commit.
        let data = unsafe { pool.writer_data(fid) };
        // ... write bytes at data[committed_boundary..] ...
        // ... update len atomic ...
        let _ = data; // suppress unused warning in test

        frame.latch.write_unlock();
        pool.unpin_page(bid, true);
    }

    // ── Reader protocol ───────────────────────────────────────────────────

    /// Correct reader sequence mirroring Section 4.3 of the paper:
    ///
    ///   1. ReaderGuard::new(pool)      → register epoch
    ///   2. guard.resolve(bid)          → (fid, confirmed_bid)
    ///   3. read frame.len              → how many committed entries to scan
    ///   4. scan committed entries (latch-free)
    ///   5. guard.validate(fid, bid)    → block_id still correct? (contract R2)
    ///   6. if !valid → restart from root
    ///   7. guard drops                 → epoch deregistered
    fn reader_example(pool: &BufferPool, bid: BlockID) -> bool {
        let guard = ReaderGuard::new(pool);

        let Some((fid, confirmed_bid)) = guard.resolve(bid) else {
            // Page not in pool. In real code: fetch_page without latching the
            // tree, re-register epoch, retry.
            return false;
        };

        // Read the live/dead counts from the atomic (no latch needed).
        let (data_ptr, len) = unsafe { guard.committed_data(fid) };
        let n_live = active_len(len);
        let n_dead = dead_len(len);

        // Scan committed entries — completely latch-free.
        let entries_start = PAGE_HEADER_SIZE;
        let _ = (data_ptr, n_live, n_dead, entries_start);
        // ... scan bytes [entries_start .. entries_start + entry_size*(n_live+n_dead)] ...

        // Validate AFTER reading: confirm the page wasn't swapped out under us.
        if !guard.validate(fid, confirmed_bid) {
            return false; // eviction raced with our read → restart
        }

        true
    }

    #[test]
    fn test_alloc_fetch_unpin() {
        let pool = temp_pool(16);
        let bid  = 0u64;
        let fid  = pool.alloc_new_page(bid, NodeType::Leaf).unwrap();
        assert_eq!(pool.frame(fid).block_id.load(Ordering::Acquire), bid);
        pool.unpin_page(bid, true);
        assert_eq!(pool.frame(fid).pin_count.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_reader_writer_no_conflict() {
        let pool = Arc::new(temp_pool(32));

        // Alloc and fill a page
        let bid = 0u64;
        let fid = pool.alloc_new_page(bid, NodeType::Leaf).unwrap();
        pool.unpin_page(bid, true);

        let pool_r = Arc::clone(&pool);
        let pool_w = Arc::clone(&pool);

        let reader = std::thread::spawn(move || {
            for _ in 0..100 {
                reader_example(&pool_r, bid);
            }
        });

        let writer = std::thread::spawn(move || {
            for _ in 0..100 {
                writer_example(&pool_w, bid);
            }
        });

        reader.join().unwrap();
        writer.join().unwrap();
    }

    #[test]
    fn test_epoch_prevents_eviction_during_read() {
        let pool  = Arc::new(temp_pool(4));
        let bid   = 42u64;
        pool.alloc_new_page(bid, NodeType::Leaf).unwrap();
        pool.unpin_page(bid, false);

        // Register an active reader epoch BEFORE attempting eviction
        let tid = current_thread_id();
        pool.epochs.enter(tid);
        let frame_epoch = pool.frame(
            *pool.page_table.get(&bid).unwrap()
        ).last_epoch.load(Ordering::Acquire);

        // Epoch guard should block eviction
        assert!(
            !pool.epochs.safe_to_evict(frame_epoch),
            "epoch guard must prevent eviction while reader is active"
        );

        pool.epochs.exit(tid);

        // After exit, eviction should be allowed
        assert!(
            pool.epochs.safe_to_evict(frame_epoch),
            "epoch guard must allow eviction after reader exits"
        );
    }

    #[test]
    fn test_r2_validate_detects_eviction() {
        // Use a pool with just 1 frame so we can force eviction
        let pool = temp_pool(1);
        let bid0 = 0u64;
        let bid1 = 1u64;

        // Load page 0
        let fid0 = pool.alloc_new_page(bid0, NodeType::Leaf).unwrap();
        pool.unpin_page(bid0, false);

        // Snapshot block_id as a "reader" would
        let confirmed = pool.frame(fid0).block_id.load(Ordering::Acquire);
        assert_eq!(confirmed, bid0);

        // Force evict page 0 by loading page 1 (pool has only 1 frame)
        let _ = pool.alloc_new_page(bid1, NodeType::Leaf);

        // R2 check: frame 0 now holds a different block_id
        assert!(
            !pool.validate_frame(fid0, bid0),
            "validate_frame must return false after eviction"
        );
    }
}