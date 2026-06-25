use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::{fs, mem};
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Weak};
use arc_swap::Cache;
use dashmap::DashMap;
use parking_lot::RwLock;
use smallvec::SmallVec;
use crate::mv_block::block::Block;
use crate::mv_page_model::{BlockID, BlockRef};
use crate::mv_record_model::version_info::Version;
use crate::mv_sync::smart_cell::OptCell;
use crate::mv_tree::mvbt::MVBTSt;

pub const BLOCK_SZ: usize = 1024 * 4;
const IO_HANDLES_ON_STACK: usize = 5;

fn data_file(data_dir: &str, number: usize) -> File {
    let _ = fs::remove_dir_all(data_dir);
    let _ = fs::create_dir_all(data_dir);

    OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(format!("{}/cMVBT.{}", data_dir, number))
        .unwrap()
}

pub struct CachedBlock<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Clone + Default + Display + Sync + 'static,
> {
    pub block_id: BlockID,
    pub disk_version: Version,
    pub block_ref: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    index: &'static MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Clone + Default + Display + Sync + 'static,
> Drop for CachedBlock<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn drop(&mut self) {
        let entry = self.index.buffer_pool.live
            .get(&self.block_id)
            .map(|e| e.value().clone())
            .unwrap();

        let _guard = entry.write();  // exclusive

        if self.disk_version != self.block_ref.load_version()  {
            self.index.persist_block(&self);
        }
        self.index.buffer_pool.live.remove(&self.block_id);
    }
}

pub type CachedBlockRef<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key,
    Payload,
> = Arc<CachedBlock<FAN_OUT, NUM_RECORDS, Key, Payload>>;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Clone + Default + Display + Sync + 'static,
> Deref for CachedBlock<FAN_OUT, NUM_RECORDS, Key, Payload> {
    type Target = OptCell<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>;
    fn deref(&self) -> &Self::Target {
        &self.block_ref
    }
}

pub struct BufferPool<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static>
{
    cache: Cache<BlockID, CachedBlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    live: DashMap<BlockID, Arc<RwLock<Weak<CachedBlock<FAN_OUT, NUM_RECORDS, Key, Payload>>>>>,
    data_file_handles: SmallVec<[File; IO_HANDLES_ON_STACK]>
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static> BufferPool<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub fn new(data_dir: &str, number_files: usize, cache_cap: usize) -> Self {
        Self {
            cache: Cache::builder()
                .initial_capacity(cache_cap)
                .max_capacity(cache_cap as u64)
                .eviction_listener(Self::on_eviction)
                .build(),
            live: DashMap::with_capacity(cache_cap),
            data_file_handles: SmallVec::from_vec((0..number_files)
                .map(|i| data_file(data_dir, i)).collect()),
        }
    }

    fn on_new_block(&self, cached_block: CachedBlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>) {
        let io_lock
            = Arc::new(RwLock::new(Arc::downgrade(&cached_block)));

        let _guard
            = io_lock.write_arc();

        self.live.insert(cached_block.block_id, io_lock);
        self.cache.insert(cached_block.block_id, cached_block);
    }

    fn on_eviction(block_id: Arc<BlockID>,
                   _block: CachedBlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
                   cause: moka::notification::RemovalCause)
    {
        println!("onEviction_Callback: BlockID = {}, Cause: {}", block_id, match cause {
            moka::notification::RemovalCause::Expired => "The entry's expiration timestamp has passed.",
            moka::notification::RemovalCause::Explicit => "The entry was manually removed by the user.",
            moka::notification::RemovalCause::Replaced => "The entry itself was not actually removed, but its value was replaced by the user.",
            moka::notification::RemovalCause::Size => "The entry was evicted due to size constraints"
        });
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static>
    MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub(crate) fn persist_block(&self, cached_block: &CachedBlock<FAN_OUT, NUM_RECORDS, Key, Payload>) {
        let files_count = self.buffer_pool.data_file_handles.len();
        let block_id = cached_block.block_ref.unsafe_borrow().block_id();
        let file_id = block_id as usize % files_count;
        let local_index = block_id as usize / files_count;
        let size_block = size_of::<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>>();

        let test1 = size_of::<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>>();
        debug_assert!(test1 == size_block && size_block == BLOCK_SZ);

        self.buffer_pool.data_file_handles.get(file_id).unwrap().write_all_at(unsafe {
            mem::transmute::<_, &[u8; BLOCK_SZ]>(&cached_block.block_ref)
        }, local_index as u64 * size_block as u64).unwrap();
    }

    // NOT allowed to be called anywhere but block_ref for atomicy!
    fn _block_disk_loader(&self, block_id: BlockID)
        -> CachedBlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let files_count = self.buffer_pool.data_file_handles.len();
        let file_id = block_id as usize % files_count;
        let local_index = block_id as usize / files_count;

        let size_block = size_of::<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>>();
        let local_offset
            = local_index * size_block;

        let mut block_ref = unsafe {
            MaybeUninit::<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>>::uninit()
                .assume_init()
        };

        let test1 = size_of::<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>>();
        debug_assert!(test1 == BLOCK_SZ);

        self.buffer_pool.data_file_handles.get(file_id).unwrap()
            .read_exact_at(unsafe {
                mem::transmute::<_, &mut [u8; BLOCK_SZ]>(&mut block_ref) }, local_offset as u64)
            .unwrap();

        Arc::new(CachedBlock {
            block_id,
            disk_version: block_ref.load_version_relaxed(),
            block_ref,
            index: unsafe { mem::transmute(self) },
        })
    }

    pub(crate) fn on_new_block_allocation(
        &self, block: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
        -> CachedBlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let cached_ref = Arc::new(CachedBlock {
            block_id: block.cell.block_id(),
            disk_version: block.load_version_relaxed(),
            block_ref: block,
            index: unsafe { mem::transmute(self) },
        });

        self.buffer_pool.on_new_block(cached_ref.clone());
        cached_ref
    }

    pub(crate) fn block_ref_latched(&self, id: BlockID) -> CachedBlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.block_ref_internal(id, true)
    }

    pub(crate) fn block_ref(&self, id: BlockID) -> CachedBlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.block_ref_internal(id, false)
    }

    pub(crate) fn block_ref_internal(&self, id: BlockID, latched: bool) -> CachedBlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        if let Some(entry) = self.buffer_pool.live.get(&id) {
            let guard = entry.read();
            if let Some(cached_block) = guard.upgrade() {
                if latched { cached_block.write_lock_store(); }
                return cached_block;  // fast path, node still alive
            }
            // weak is dead, fall through under read lock
            drop(guard);
        }

        // slow path
        let io_lock = self.buffer_pool.live
            .get(&id)
            .map(|e|
                e.value().clone())  // clone Arc, DashMap lock released
            .unwrap_or_default();

        let _guard
            = io_lock.read();  // now safe, DashMap lock not held

        self.buffer_pool.cache.get_with(id, || {
            let cached_block_ref
                = self._block_disk_loader(id);

            if latched { cached_block_ref.write_lock_store(); }

            // update the Weak inside the RwLock
            *self.buffer_pool.live.get(&id)
                .unwrap()
                .write() = Arc::downgrade(&cached_block_ref);

            cached_block_ref
        })
    }
}



