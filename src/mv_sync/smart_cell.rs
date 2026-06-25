use crate::mv_page_model::internal_page::InternalPage;
use crate::mv_page_model::leaf_page::LeafPage;
use crate::mv_page_model::{Attempts, BlockRef};
use crate::mv_record_model::AtomicVersion;
use crate::mv_record_model::version_info::Version;
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::smart_cell::SmartGuard::{Reader, Writer};

use crate::mv_block::block::Block;
use crate::mv_block::block_handle::BlockAllocManager;
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_sync::clock::PaddedAtomicVersion;
use crate::mv_test;
use crate::mv_test::{Key, Payload};
use crate::mv_tree::smo::BlockUnsafeDegree;
use crate::mv_utils::interval::Interval;

use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::{hint, mem, ptr};

const BLOCK_FLAG_VERSION: LatchVersion = 0x8_000000000000000;
const TYPE_FLAG_VERSION: LatchVersion   = 0x4_000000000000000;
const PAGE_TYPE_INTERNAL: LatchVersion  = 0;
const PAGE_TYPE_LEAF: LatchVersion      = TYPE_FLAG_VERSION;

const WRITE_FLAG_VERSION: LatchVersion  = 0x2_000000000000000;
const LATCH_SECTOR_MASK: LatchVersion   = 0xFF_FF_FF_FF_00_00_00_00;
const COMMIT_SECTOR_MASK: LatchVersion  = 0x00_00_00_00_FF_FF_FF_FF;

#[cfg(all(feature = "hardware-lock-elision", any(target_arch = "x86", target_arch = "x86_64")))]
pub trait AtomicElisionExt {
    fn elision_compare_exchange_acquire(
        &self,
        current: Version,
        new: Version,
    ) -> Result<Version, Version>;
}

#[cfg(all(feature = "hardware-lock-elision", any(target_arch = "x86", target_arch = "x86_64")))]
impl AtomicElisionExt for AtomicVersion {
    #[inline(always)]
    fn elision_compare_exchange_acquire(&self, current: Version, new: Version) -> Result<Version, Version> {
        unsafe {
            use core::arch::asm;
            let prev: Version;
            #[cfg(target_pointer_width = "32")]
            asm!(
            "xacquire",
            "lock",
            "cmpxchg [{:e}], {:e}",
            in(reg) self,
            in(reg) new,
            inout("eax") current => prev,
            );
            #[cfg(target_pointer_width = "64")]
            asm!(
            "xacquire",
            "lock",
            "cmpxchg [{}], {}",
            in(reg) self,
            in(reg) new,
            inout("rax") current => prev,
            );
            if prev == current {
                Ok(prev)
            } else {
                Err(prev)
            }
        }
    }
}

// pub static mut COUNTERS: (AtomicUsize, AtomicUsize) =
//     (AtomicUsize::new(0), AtomicUsize::new(0));

#[inline(always)]
#[cfg(target_os = "linux")]
pub fn sched_yield(attempt: Attempts) {
    if attempt > 3 {
        unsafe {
            // COUNTERS.1.fetch_add(1, Relaxed);
            libc::sched_yield();
        }
    } else {
        // unsafe { COUNTERS.0.fetch_add(1, Relaxed); }
        hint::spin_loop();
    }
}

pub const FORCE_YIELD: Attempts = 4;

#[inline(always)]
#[cfg(not(target_os = "linux"))]
pub fn sched_yield(attempt: Attempts) {
    if attempt > 3 {
        std::thread::yield_now();
    } else {
        hint::spin_loop();
    }
}

type LatchVersion = Version;
type IsRead = bool;

pub struct OptCell<E> {
    pub cell_version: PaddedAtomicVersion,
    pub cell: SafeCell<E>
}

impl< E> Drop for OptCell<E> {
    fn drop(&mut self) {
        const FAN_OUT: usize = mv_test::FAN_OUT;
        const NUM_RECORDS: usize = mv_test::NUM_RECORDS;

        let v
            = self.cell_version.load(Relaxed);

        if v & BLOCK_FLAG_VERSION != 0 {
            let len_sum
                = from_len_sum((v & COMMIT_SECTOR_MASK) as _);

            let block =  unsafe {
                mem::transmute::<_, &mut Block<FAN_OUT, NUM_RECORDS, Key, Payload>>(
                    SafeCell::get_mut(&self.cell))
            };

            let is_leaf = v & TYPE_FLAG_VERSION == PAGE_TYPE_LEAF;
            if is_leaf {
                unsafe { &mut block.page.leaf }.on_reuse(len_sum);
            }
            else {
                unsafe { &mut block.page.internal }.on_reuse(len_sum);
            }
        }
    }
}


impl<E: Display> Display for OptCell<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OptCell {{\ncell: {}\n\t\tcell_version: {}\n\t}}", self.cell.get_mut(), self.cell_version.load(Relaxed))
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> OptCell<Block<FAN_OUT, NUM_RECORDS, Key, Payload>> {
    pub const fn new_block(block: Block<FAN_OUT, NUM_RECORDS, Key, Payload>, is_leaf: bool) -> Self {
        let markers = BLOCK_FLAG_VERSION |
            if is_leaf { PAGE_TYPE_LEAF } else { PAGE_TYPE_INTERNAL };

        OptCell {
            cell: SafeCell::new(block),
            cell_version: PaddedAtomicVersion(AtomicVersion::new(markers)),
        }
    }
}

impl<E> OptCell<E> {
    const CELL_START_VERSION: LatchVersion = 0;

    #[inline(always)]
    pub const fn new_blank(data: E) -> Self {
        Self {
            cell: SafeCell::new(data),
            cell_version: PaddedAtomicVersion(AtomicVersion::new(Self::CELL_START_VERSION)),
        }
    }

    #[cfg(not(all(feature = "hardware-lock-elision", any(
        target_arch = "x86",
        target_arch = "x86_64"
    ))))]
    #[inline(always)]
    pub fn write_lock(&self, read_version: LatchVersion) -> Option<LatchVersion> {
        match self.cell_version.compare_exchange_weak(
            read_version,
            WRITE_FLAG_VERSION | read_version,
            Acquire,
            Relaxed)
        {
            Ok(..) => Some(WRITE_FLAG_VERSION | read_version),
            Err(..) => None
        }
    }

    #[cfg(all(feature = "hardware-lock-elision", any(target_arch = "x86", target_arch = "x86_64")))]
    #[inline(always)]
    pub fn write_lock(&self, read_version: LatchVersion) -> Option<LatchVersion> {
        match self.cell_version.elision_compare_exchange_acquire(
            read_version,
            WRITE_FLAG_VERSION | read_version)
        {
            Ok(..) => Some(WRITE_FLAG_VERSION | read_version),
            Err(..) => None
        }
    }
}

pub struct SmartCell<E>(pub Arc<OptCell<E>>);

impl<E> Clone for SmartCell<E> {
    #[inline(always)]
    fn clone(&self) -> Self {
        SmartCell(self.0.clone())
    }
}

pub enum SmartGuard<'a, E> {
    Reader(&'a SmartCell<E>, SafeCell<LatchVersion>),
    Writer(SmartCell<E>, SafeCell<LatchVersion>),
    // Zeroed
}

// impl<'a, E: 'static> Clone for SmartGuard<'a, E> {
//     fn clone(&self) -> Self {
//         match self {
//             Reader(cell, latch) => Reader(*cell, *latch),
//             _ => unreachable!()
//         }
//     }
// }

impl<'a, E: 'static> Deref for SmartGuard<'a, E> {
    type Target = SmartCell<E>;
    fn deref(&self) -> &Self::Target {
        self.cell()
    }
}

pub type Active = u32;
pub type Dead = u32;
pub type LenP = u32;

#[inline(always)]
pub const fn from_len_sum(len: LenP) -> usize {
    (active_len(len) + dead_len(len)) as usize
}
#[inline(always)]
pub const fn from_len(len: LenP) -> (Active, Dead) {
    (active_len(len), dead_len(len))
}
#[inline(always)]
pub const fn active_len(len: LenP) -> Active {
    len >> 16
}
#[inline(always)]
pub const fn dead_len(len: LenP) -> Dead {
    len & 0xFF_FF
}
#[inline(always)]
pub const fn from_active_dead(active: Active, dead: Dead) -> LatchVersion {
    ((active << 16) | dead) as _
}

pub enum PageType<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    LeafRef(&'a LeafPage<NUM_RECORDS, Key, Payload>),
    IndexRef(&'a InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>),
    LeafMut(&'a mut LeafPage<NUM_RECORDS, Key, Payload>),
    IndexMut(&'a mut InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>),
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> PageType<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
    pub fn as_leaf_page(self) -> &'a LeafPage<NUM_RECORDS, Key, Payload> {
        match self {
            PageType::LeafRef(leaf) => leaf,
            PageType::LeafMut(leaf) => leaf,
            _ => panic!("not a leaf page"),
        }
    }
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static
> SmartGuard<'a, Block<FAN_OUT, NUM_RECORDS, Key, Payload>>  {
    #[inline(always)]
    pub fn meta_block(&self) -> (bool, LatchVersion, Active, Dead) {
        let v = match self {
            Reader(_, v) |
            Writer(_, v) => v.get()
        };

        let commit_sector = (v & COMMIT_SECTOR_MASK) as LenP;

        (v & TYPE_FLAG_VERSION == PAGE_TYPE_LEAF,
         v & LATCH_SECTOR_MASK,
         active_len(commit_sector),
         dead_len(commit_sector))
    }

    #[inline(always)]
    pub fn meta_block_is_leaf_latched(&self) -> (bool, bool, Active, Dead) {
        let v = match self {
            Reader(_, v) |
            Writer(_, v) => v.get()
        };
        let commit_sector = (v & COMMIT_SECTOR_MASK) as LenP;

        (v & TYPE_FLAG_VERSION == PAGE_TYPE_LEAF,
         v & WRITE_FLAG_VERSION == WRITE_FLAG_VERSION,
         active_len(commit_sector),
         dead_len(commit_sector))
    }

    #[inline(always)]
    pub fn keys_versions_pointers(&self)
        -> (&[Interval<Key>], &[Version], &[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>])
    {
        let len_sum = match self {
            Reader(_, v) | Writer(_, v) =>
                from_len_sum((v.get() & COMMIT_SECTOR_MASK) as LenP)
        };

        unsafe {
            let deref
                = &self.page.internal;

            deref.keys_versions_pointers(len_sum)
        }
    }

    #[inline]
    pub fn commit_delta(&self, active_delta: i32, dead_delta: i32) {
        let (latch, len, editor) = match self {
            Writer(_, v) => {
                let vi = v.get();
                (vi & LATCH_SECTOR_MASK, (vi & COMMIT_SECTOR_MASK) as LenP, v)
            }
            _ => unreachable!()
        };

        let latch_version = self.commit_delta_from(
            active_delta,
            dead_delta,
            latch,
            len);

        *editor.get_mut() = latch_version;
    }

    #[inline(always)]
    pub fn as_page_ref(&self) -> (usize, PageType<'static, FAN_OUT, NUM_RECORDS, Key, Payload>) {
        let (is_leaf, len_sum) = match self {
            Reader(_, v) |
            Writer(_, v) => {
                let v = v.get();
                let len_sum
                    = from_len_sum((v & COMMIT_SECTOR_MASK) as LenP);

                (v & TYPE_FLAG_VERSION == PAGE_TYPE_LEAF, len_sum)
            }
        };

        if is_leaf {
            (len_sum, PageType::LeafRef(unsafe { mem::transmute(&self.0.cell.page.leaf) }))
        }
        else {
            (len_sum, PageType::IndexRef(unsafe { mem::transmute(&self.0.cell.page.internal) }))
        }
    }

    #[inline(always)]
    pub fn as_internal_page(&self) -> (usize, &mut InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>) { ;
        let len_sum = match self {
            Writer(_, v) =>
                from_len_sum((v.get() & COMMIT_SECTOR_MASK) as LenP),
            _ => unreachable!()
        };
        (len_sum, unsafe { &mut self.page.get_mut().internal })
    }

    #[inline(always)]
    pub fn as_leaf_page(&self) -> (usize, &mut LeafPage<NUM_RECORDS, Key, Payload>) {
        let len_sum = match self {
            Reader(_, v) |
            Writer(_, v) =>
                from_len_sum((v.get() & COMMIT_SECTOR_MASK) as LenP)
        };
        (len_sum, unsafe { &mut self.page.get_mut().leaf })
    }
}

impl<'a, E: 'static> SmartGuard<'a, E> {
    #[inline(always)]
    pub fn upgrade_write_lock(&mut self) -> bool {
        match self {
            Reader(cell, read_latch) => unsafe {
                if let Some(write_latch)
                    = cell.0.write_lock(read_latch.get())
                {
                    let writer = Writer(cell.clone(), SafeCell::new(write_latch));
                    ptr::write(self, writer);
                    return true;
                }
                false
            }
            _ => true
        }
    }

    #[inline(always)]
    pub fn active_dead(&self) -> (Active, Dead) {
        match self {
            Reader(_, v) |
            Writer(_, v) =>
                from_len((v.get() & COMMIT_SECTOR_MASK) as _)
        }
    }

    #[inline(always)]
    pub fn cell(&self) -> &SmartCell<E> {
        match self {
            Reader(cell, ..) => *cell,
            Writer(cell, ..) => cell,
            _ => unreachable!()
        }
    }
}

impl<E> Deref for SmartCell<E> {
    type Target = E;

    fn deref(&self) -> &Self::Target {
        self.0.cell.as_ref()
    }
}

impl<E> SmartCell<E> {
    #[inline(always)]
    pub fn len(&self) -> usize {
        from_len_sum((self.0.cell_version.load(Relaxed) & COMMIT_SECTOR_MASK) as _)
    }

    #[inline(always)]
    pub fn active_dead_cell(&self) -> (Active, Dead) {
        from_len((self.0.cell_version.load(Relaxed) & COMMIT_SECTOR_MASK) as _)
    }

    #[inline(always)]
    pub fn active_dead_is_leaf(&self) -> (Active, Dead, bool) {
        let v = self.0.cell_version.load(Relaxed);
        let (active, dead)
            = from_len((v & COMMIT_SECTOR_MASK) as _);

        (active, dead, v & TYPE_FLAG_VERSION == PAGE_TYPE_LEAF)
    }

    #[inline(always)]
    pub fn meta(&self) -> (bool, PageTypePrimitive, LatchVersion, usize) {
        let v = self.0.cell_version.load(Relaxed);

        (v & BLOCK_FLAG_VERSION != 0,
         v & TYPE_FLAG_VERSION,
         v & LATCH_SECTOR_MASK,
         from_len_sum((v & COMMIT_SECTOR_MASK) as LenP))
    }

    #[inline]
    pub fn commit_init(&self, active_delta: usize, is_leaf: bool) {
        debug_assert!(active_delta > 0);

        let extra = BLOCK_FLAG_VERSION |
            if is_leaf {
                PAGE_TYPE_LEAF
            } else {
                PAGE_TYPE_INTERNAL
            };

        let latch = extra |
            from_active_dead(
                active_delta as Active,
                0 as Dead);

        self.0.cell_version.store(latch, Relaxed);
    }

    #[inline]
    fn commit_delta_from(&self,
                         active_delta: i32,
                         dead_delta: i32,
                         latch: LatchVersion,
                         len: LenP) -> LatchVersion
    {
        debug_assert!(latch & WRITE_FLAG_VERSION != 0);

        let active = active_len(len) as i32 + active_delta;
        let dead = dead_len(len) as i32 + dead_delta;

        let latch
            = latch | from_active_dead(active as Active, dead as Dead);

        self.0.cell_version.store(latch, Relaxed);
        latch
    }
}

type PageTypePrimitive = Version;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static
> SmartCell<Block<FAN_OUT, NUM_RECORDS, Key, Payload>> {
    #[inline(always)]
    pub fn meta_block(&self) -> (bool, LatchVersion, Active, Dead) {
        let v = self.0.cell_version.load(Relaxed);
        let commit_sector = (v & COMMIT_SECTOR_MASK) as LenP;

        (v & TYPE_FLAG_VERSION == PAGE_TYPE_LEAF,
         v & LATCH_SECTOR_MASK,
         active_len(commit_sector),
         dead_len(commit_sector))
    }

    pub fn meta_block_is_leaf_latched_load(&self) -> (bool, bool, Active, Dead) {
        let v = self.0.cell_version.load(Relaxed);
        let commit_sector = (v & COMMIT_SECTOR_MASK) as LenP;

        (v & TYPE_FLAG_VERSION == PAGE_TYPE_LEAF,
         v & WRITE_FLAG_VERSION == WRITE_FLAG_VERSION,
         active_len(commit_sector),
         dead_len(commit_sector))
    }

    #[inline(always)]
    pub fn as_page_ref(&self) -> (usize, PageType<'static, FAN_OUT, NUM_RECORDS, Key, Payload>) {
        let (is_leaf, _latch, active, dead)
            = self.meta_block();

        if is_leaf {
            ((active + dead) as _, PageType::LeafRef(unsafe { mem::transmute(&self.0.cell.page.leaf) }))
        }
        else {
            ((active + dead) as _, PageType::IndexRef(unsafe { mem::transmute(&self.0.cell.page.internal) }))
        }
    }

    #[inline(always)]
    pub fn as_internal_page(&self) -> (usize, &mut InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>) {
        (self.len(), unsafe { &mut self.page.get_mut().internal })
    }

    #[inline(always)]
    pub fn as_leaf_page(&self) -> (usize, &mut LeafPage<NUM_RECORDS, Key, Payload>) {
        (self.len(), unsafe { &mut self.page.get_mut().leaf })
    }

    #[inline(always)]
    pub fn is_leaf(&self) -> bool {
        let (_is_block, block_type, _latch, _len_sum)
            = self.meta();

        block_type == PAGE_TYPE_LEAF
    }

    #[inline(always)]
    pub fn keys_versions_pointers(&self)
        -> (&[Interval<Key>], &[Version], &[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>])
    {
        let len_sum
            = self.len();

        unsafe {
            let deref
                = &self.page.internal;

            deref.keys_versions_pointers(len_sum)
        }
    }

    #[inline(always)]
    pub fn as_records(&self) -> &[RecordPoint<Key, Payload>] {
        let len_sum
            = self.len();

        unsafe {
            let deref
                = &self.page.leaf;

            deref.as_records(len_sum)
        }
    }

    #[inline(always)]
    pub fn on_reuse(&self, block_type: Version, len_sum: usize, new_block_type_leaf: bool) {
        match block_type  {
            PAGE_TYPE_INTERNAL => unsafe {
                let derefmut
                    = &mut self.0.cell.page.get_mut().internal;

                derefmut.on_reuse(len_sum);

            },
            _ => unsafe {
                let derefmut
                    = &mut self.0.cell.page.get_mut().leaf;

                derefmut.on_reuse(len_sum);
            }
        }

        let meta_additional = BLOCK_FLAG_VERSION |
            if new_block_type_leaf { PAGE_TYPE_LEAF }
            else { PAGE_TYPE_INTERNAL };

        self.0.cell_version.store(meta_additional, Relaxed);
    }

    #[inline(always)]
    pub fn unsafe_degree_root(&self) -> BlockUnsafeDegree {
        let (active, dead, is_leaf)
            = self.active_dead_is_leaf();

        let (active, dead)
            = (active as usize,  dead as usize);

        if active == 1 && !is_leaf { // single child
            BlockUnsafeDegree::ActiveUnderflow
        }
        else if active + dead >= self.overflow_units_count(is_leaf) {
            BlockUnsafeDegree::Overflow
        }
        else {
            BlockUnsafeDegree::Ok
        }
    }

    #[inline(always)]
    pub fn unsafe_degree(&self) -> BlockUnsafeDegree {
        let (is_leaf, _latch, active, dead)
            = self.meta_block();

        let (active, dead)
            = (active as usize,  dead as usize);

        if active <= self.filling_20_percent(is_leaf) {
            BlockUnsafeDegree::ActiveUnderflow
        }
        else {
            let overflow_units_count
                = self.overflow_units_count(is_leaf);

            let is_overflow
                = active + dead >= overflow_units_count;

            if is_overflow && active <= self.filling_40_percent(is_leaf) {
                BlockUnsafeDegree::ActiveUnderflow
            } else if is_overflow {
                BlockUnsafeDegree::Overflow
            } else {
                BlockUnsafeDegree::Ok
            }
        }
    }

    #[inline(always)]
    pub fn overflow_units_count(&self, is_leaf: bool) -> usize {
        match is_leaf {
            true => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::overflow_records_count(),
            false => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::overflow_keys_count()
        }
    }
}

impl<E> SmartCell<E> {
    #[inline(always)]
    pub fn unsafe_borrow(&self) -> &E {
        self.0.cell.as_ref()
    }

    #[inline(always)]
    pub fn unsafe_borrow_mut(&self) -> &mut E {
        self.0.cell.get_mut()
    }

    #[inline(always)]
    pub fn borrow_read(&self) -> SmartGuard<'static, E> {
        unsafe {
            mem::transmute(Reader(
                self,
                SafeCell::new(self.0.cell_version.load(Relaxed) & !WRITE_FLAG_VERSION)))
        }
    }

    #[inline(always)]
    pub fn borrow_write(&self) -> Option<SmartGuard<'static, E>> {
        let mut guard =
            self.borrow_read();

        guard
            .upgrade_write_lock()
            .then(|| guard)
    }
}

impl<E> Drop for SmartGuard<'_, E> {
    fn drop(&mut self) {
        match self {
            Writer(cell, w) =>
                cell.0.cell_version.store(w.get() ^ WRITE_FLAG_VERSION, Relaxed),
            _ => { }
        }
    }
}

unsafe impl<'a, E: Default> Sync for SmartGuard<'a, E> {}
unsafe impl<'a, E: Default> Send for SmartGuard<'a, E> {}