use std::fmt::{Display, Formatter};
use std::{hint, mem, ptr};
use std::mem::transmute_copy;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use CCBPlusTree::locking::locking_strategy::LockingStrategy;
use crate::mv_page_model::Attempts;
use crate::mv_record_model::AtomicVersion;
use crate::mv_record_model::version_info::Version;
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::smart_cell::SmartGuard::{Reader, Writer};

pub const OBSOLETE_FLAG_VERSION: LatchVersion = 0x8_000000000000000;
const WRITE_FLAG_VERSION: LatchVersion = 0x4_000000000000000;

const WRITE_OBSOLETE_FLAG_VERSION: LatchVersion = 0xC_000000000000000;
const WRITE_PIN_FLAG_VERSION: LatchVersion = 0x6_000000000000000;
const WRITE_PIN_OBSOLETE_FLAG_VERSION: LatchVersion = 0xE_000000000000000;

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

pub struct OptCell<E: Default> {
    pub cell: SafeCell<E>,
    pub cell_version: AtomicVersion,
}

impl<E: Default + Display> Display for OptCell<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OptCell {{\ncell: {}\n\t\tcell_version: {}\n\t}}", self.cell.get_mut(), self.cell_version.load(Relaxed))
    }
}

impl<E: Default> Default for OptCell<E> {
    fn default() -> Self {
        Self::new(E::default())
    }
}

impl<E: Default> OptCell<E> {
    const CELL_START_VERSION: LatchVersion = 0;

    #[inline(always)]
    pub const fn new(data: E) -> Self {
        Self {
            cell: SafeCell::new(data),
            cell_version: AtomicVersion::new(Self::CELL_START_VERSION),
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
            AcqRel,
            Acquire)
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

#[derive(Default)]
pub struct SmartCell<E: Default>(pub Arc<OptCell<E>>);

impl<E: Default> Clone for SmartCell<E> {
    #[inline(always)]
    fn clone(&self) -> Self {
        SmartCell(self.0.clone())
    }
}

pub enum SmartGuard<'a, E: Default> {
    Reader(&'a SmartCell<E>, LatchVersion),
    Writer(SmartCell<E>, LatchVersion),
}

impl<'a, E: Default + 'static> Clone for SmartGuard<'a, E> {
    fn clone(&self) -> Self {
        match self {
            Reader(cell, latch) => Reader(*cell, *latch),
            _ => unreachable!()
        }
    }
}

impl<'a, E: Default + 'static> Deref for SmartGuard<'a, E> {
    type Target = E;
    fn deref(&self) -> &Self::Target {
        match self {
            Reader(cell, ..) => cell.0.cell.as_ref(),
            Writer(cell, ..) => cell.0.cell.as_ref(),
        }
    }
}

impl<'a, E: Default + 'static> SmartGuard<'a, E> {
    #[inline(always)]
    pub fn upgrade_write_lock(&mut self) -> bool {
        match self {
            Reader(cell, read_latch) => unsafe {
                if let Some(write_latch)
                    = cell.0.write_lock(*read_latch & !WRITE_OBSOLETE_FLAG_VERSION)
                {
                    let writer = Writer(cell.clone(), write_latch);
                    ptr::write(self, writer);
                    return true;
                }
                false
            }
            _ => true
        }
    }

    pub fn inner_cell(self) -> SmartCell<E> {
        match self {
            Reader(cell, ..) => cell.clone(),
            Writer(ref cell, ..) => cell.clone(),
        }
    }

    // pub fn inner_cell(mut self) -> SmartCell<E> { // requires manual unlatch on reuse
    //     match self {
    //         Reader(cell, ..) => cell.clone(),
    //         Writer(ref cell, latch) => unsafe {
    //             let cell = transmute_copy(cell);
    //             ptr::write(&mut self, Reader(mem::transmute(&cell),
    //                                          latch & !WRITE_OBSOLETE_FLAG_VERSION));
    //             cell
    //         }
    //     }
    // }

    #[inline(always)]
    pub fn deref_mut(&self) -> &mut E {
        match self {
            Writer(cell, ..) => cell.unsafe_borrow_mut(),
            Reader(cell, ..) => cell.unsafe_borrow_mut(),
        }
    }
}

impl<E: Default> Deref for SmartCell<E> {
    type Target = E;

    fn deref(&self) -> &Self::Target {
        self.0.cell.as_ref()
    }
}

impl<E: Default> SmartCell<E> {
    #[inline(always)]
    pub fn unsafe_borrow(&self) -> &E {
        self.deref()
    }

    #[inline(always)]
    pub fn unsafe_borrow_mut(&self) -> &mut E {
        self.0.cell.get_mut()
    }


    #[inline(always)]
    pub fn borrow_read(&self) -> SmartGuard<'static, E> {
        unsafe {
            mem::transmute(
                Reader(self, self.0.cell_version.load(Relaxed) & !WRITE_OBSOLETE_FLAG_VERSION)
            )
        }
    }
}

impl<'a, E: Default> Drop for SmartGuard<'a, E> {
    fn drop(&mut self) {
        match self {
            Writer(cell, write_version) =>
                cell.0.cell_version.store((*write_version + 1) ^ WRITE_FLAG_VERSION, Release),
            _ => {}
        }
    }
}

unsafe impl<'a, E: Default> Sync for SmartGuard<'a, E> {}
unsafe impl<'a, E: Default> Send for SmartGuard<'a, E> {}