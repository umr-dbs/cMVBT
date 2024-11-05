use std::fmt::{Display, Formatter};
use std::{hint, ptr};
use std::mem::{transmute, transmute_copy};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use crate::mv_page_model::Attempts;
use crate::mv_record_model::AtomicVersion;
use crate::mv_record_model::version_info::Version;
use crate::mv_utils::safe_cell::SafeCell;
use crate::mv_utils::smart_cell::SmartFlavor::{FreeCell, OLCCell, ReadersWriterCell};
use crate::mv_utils::smart_cell::SmartGuard::{LockFree, OLCReader, OLCWriter, RwReaderFree, RwWriterMut};

pub const CPU_THREADS: bool = true;
pub const ENABLE_YIELD: bool = !CPU_THREADS;

pub(crate) const OBSOLETE_FLAG_VERSION: LatchVersion = 0x8_000000000000000;
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

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum LatchType {
    ReadersWriter,
    Optimistic,
    None,
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
    pub(crate) cell: SafeCell<E>,
    pub(crate) cell_version: AtomicVersion,
}

impl<E: Default + Display> Display for OptCell<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OptCell {{\ncell: {}\n\t\tcell_version: {}\n\t}}", self.cell.get_mut(), self.load_version())
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

    #[inline(always)]
    pub fn load_version(&self) -> LatchVersion {
        self.cell_version.load(Acquire)
    }

    #[inline(always)]
    pub fn read_lock(&self) -> (IsRead, LatchVersion) {
        let read_version
            = self.load_version();

        (read_version & WRITE_OBSOLETE_FLAG_VERSION == 0, read_version)
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

    #[inline(always)]
    pub fn write_unlock(&self, write_version: LatchVersion) {
        debug_assert!(write_version & WRITE_PIN_FLAG_VERSION == WRITE_FLAG_VERSION);

        self.cell_version.store((write_version + 1) ^ WRITE_FLAG_VERSION, Relaxed)
    }

    #[inline(always)]
    pub fn write_obsolete(&self) {
        self.cell_version.store(OBSOLETE_FLAG_VERSION, Relaxed)
    }

    #[inline(always)]
    pub fn write_obsolete_with_latch(&self, latch: LatchVersion) {
        self.cell_version.store(OBSOLETE_FLAG_VERSION | latch, Release)
    }

    #[inline(always)]
    pub fn is_obsolete(&self) -> bool {
        self.load_version() == OBSOLETE_FLAG_VERSION
    }

    #[inline(always)]
    pub fn is_write(&self) -> bool {
        self.load_version() & WRITE_FLAG_VERSION == WRITE_FLAG_VERSION
    }
}

#[derive(Default)]
pub struct SmartCell<E: Default>(pub Arc<SmartFlavor<E>>);

impl<E: Default> Clone for SmartCell<E> {
    #[inline(always)]
    fn clone(&self) -> Self {
        SmartCell(self.0.clone())
    }
}

pub enum SmartFlavor<E: Default> {
    FreeCell(SafeCell<E>),
    ReadersWriterCell(Mutex<()>, SafeCell<E>),
    OLCCell(OptCell<E>),
}

impl<E: Default> Default for SmartFlavor<E> {
    fn default() -> Self {
        FreeCell(SafeCell::new(
            E::default()))
    }
}

impl<E: Default + 'static> Deref for SmartFlavor<E> {
    type Target = E;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match self {
            OLCCell(opt) =>
                opt.cell.as_ref(),
            FreeCell(ptr) => ptr.get_mut(),
            ReadersWriterCell(.., ptr) => ptr.get_mut(),
        }
    }
}

impl<E: Default + 'static> DerefMut for SmartFlavor<E> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            OLCCell(opt) => opt.cell.get_mut(),
            FreeCell(ptr) => ptr.get_mut(),
            ReadersWriterCell(.., ptr) => ptr.get_mut()
        }
    }
}

pub enum SmartGuard<'a, E: Default> {
    LockFree(*mut E),
    RwReaderFree(SmartCell<E>),
    RwWriterMut(MutexGuard<'a, RawMutex, ()>, SmartCell<E>),
    OLCReader(Option<(SmartCell<E>, LatchVersion)>),
    OLCWriter(SmartCell<E>, LatchVersion),
}

impl<'a, E: Default + 'static> Clone for SmartGuard<'_, E> {
    fn clone(&self) -> Self {
        match self {
            OLCReader(inner) => OLCReader(inner.clone()),
            LockFree(ptr) => LockFree(*ptr),
            RwReaderFree(rw) => RwReaderFree(rw.clone()),
            _ => OLCReader(None)
        }
    }
}

impl<'a, E: Default + 'static> SmartGuard<'_, E> {
    // #[inline(always)]
    // pub(crate) fn mark_obsolete(&self) {
    //     match self {
    //         OLCWriter(cell, ..) => match cell.0.as_ref() {
    //             OLCCell(opt) => opt.write_obsolete(),
    //             _ => {}
    //         }
    //         _ => {}
    //     }
    // }

    #[inline(always)]
    pub fn downgrade(&mut self) {
        match self {
            RwWriterMut(guard, cell) => unsafe {
                let mutex
                    = MutexGuard::mutex(guard);

                mutex.force_unlock();

                let s_guard
                    = RwReaderFree(cell.clone());

                ptr::write(self, s_guard)
            }
            _ => {}
        }
    }

    #[inline(always)]
    pub fn upgrade_write_lock(&mut self) -> bool {
        match self {
            LockFree(_) => true,
            RwWriterMut(..) => true,
            OLCWriter(..) => true,
            RwReaderFree(ref cell) => unsafe {
                if let ReadersWriterCell(mutex, ..) = cell.0.as_ref() {
                    let mutex: &'a Mutex<()>
                        = transmute(mutex);

                    if let Some(writer) = mutex.try_lock() {
                        let next
                            = transmute(RwWriterMut::<E>(writer, transmute_copy(cell)));

                        ptr::write(self, next);
                        return true;
                    }
                }

                false
            }
            OLCReader(Some((ref cell, read_latch))) => unsafe {
                match cell.0.as_ref() {
                    OLCCell(opt) => if let Some(write_latch)
                        = opt.write_lock(*read_latch & !WRITE_FLAG_VERSION)
                    {
                        let writer = OLCWriter(transmute_copy(cell), write_latch);
                        ptr::write(self, writer);
                        return true;
                    },
                    _ => {}
                }

                false
            }
            _ => false
        }
    }

    #[inline(always)]
    pub const fn is_write_lock(&self) -> bool {
        match self {
            RwWriterMut(..) => true,
            OLCWriter(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        match self {
            OLCReader(None) => false,
            OLCReader(Some((opt, version))) => {
                if let OLCCell(opt) = opt.0.as_ref() {
                    let loaded = opt.load_version();
                    loaded & WRITE_FLAG_VERSION == 0 && loaded == *version
                }
                else {
                    false
                }
            }
            _ => true
        }
    }

    #[inline(always)]
    pub fn deref(&self) -> Option<&'_ E> {
        match self {
            LockFree(ptr) => unsafe { ptr.as_ref() },
            RwReaderFree(.., ptr) => Some(ptr.unsafe_borrow()),
            RwWriterMut(.., ptr) => Some(ptr.unsafe_borrow()),
            OLCReader(Some((cell, ..))) => Some(cell.0.as_ref()),
            OLCWriter(cell, ..) => Some(cell.0.as_ref()),
            _ => None
        }
    }

    // #[inline(always)]
    // pub unsafe fn deref_unsafe(&self) -> Option<&'_ E> {
    //     match self {
    //         LockFree(ptr) => ptr.as_ref(),
    //         RwReaderFree(.., ptr) => Some(ptr.unsafe_borrow()),
    //         RwWriterMut(.., ptr) => Some(ptr.unsafe_borrow()),
    //         OLCReader(Some((cell, ..))) => Some(cell.0.as_ref()),
    //         OLCWriter(cell, ..) => Some(cell.0.as_ref()),
    //         _ => None
    //     }
    // }

    #[inline(always)]
    pub fn deref_mut(&self) -> Option<&mut E> {
        match self {
            LockFree(ptr) => unsafe { ptr.as_mut() },
            RwWriterMut(.., ptr) => Some(ptr.unsafe_borrow_mut()),
            OLCWriter(cell, ..) => Some(cell.unsafe_borrow_mut()),
            _ => None
        }
    }
}

impl<E: Default> SmartCell<E> {
    #[inline(always)]
    pub(crate) fn unsafe_borrow(&self) -> &E {
        match self.0.as_ref() {
            OLCCell(opt) => opt.cell.as_ref(),
            FreeCell(ptr) => ptr.as_ref(),
            ReadersWriterCell(.., ptr) => ptr.as_ref(),
        }
    }

    #[inline(always)]
    pub fn unsafe_borrow_mut(&self) -> &mut E {
        match self.0.as_ref() {
            OLCCell(opt) => opt.cell.get_mut(),
            FreeCell(ptr) => ptr.get_mut(),
            ReadersWriterCell(.., ptr) => ptr.get_mut()
        }
    }

    #[inline(always)]
    pub fn borrow_free(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            FreeCell(ptr) => LockFree(ptr.get_mut()),
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn borrow_opt(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            OLCCell(opt) => OLCReader(Some((self.clone(), opt.load_version()))),
            _ => OLCReader(None)
        }
    }

    #[inline(always)]
    pub fn borrow_read(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            OLCCell(opt) => {
                let (success, read)
                    = opt.read_lock();

                OLCReader(success.then(|| (self.clone(), read)))
            }
            ReadersWriterCell(..) => {
                let ret = RwReaderFree(self.clone());
                fence(Acquire);
                ret
            }
            FreeCell(ptr) => LockFree(ptr.get_mut()),
        }
    }

    #[inline(always)]
    pub fn borrow_mut(&self) -> SmartGuard<'static, E> {
        match self.0.deref() {
            FreeCell(ptr) => LockFree(ptr.get_mut()),
            ReadersWriterCell(rw, ..) => unsafe {
                let lock = rw.lock();
                transmute(RwWriterMut(
                    transmute(lock),
                    self.clone(),
                ))
            },
            OLCCell(opt) => {
                let read_version
                    = opt.load_version();

                if read_version & WRITE_PIN_OBSOLETE_FLAG_VERSION != 0 {
                    OLCReader(None)
                } else if let Some(latched) = opt.write_lock(read_version) {
                    OLCWriter(self.clone(), latched)
                } else {
                    OLCReader(None)
                }
            }
        }
    }
}

impl<'a, E: Default> Drop for SmartGuard<'a, E> {
    fn drop(&mut self) {
        match self {
            OLCWriter(cell, write_version) =>
               if let OLCCell(opt) = cell.0.as_ref() {
                    opt.write_unlock(*write_version)
                }
            _ => {}
        }
    }
}

unsafe impl<'a, E: Default + 'a> Sync for SmartGuard<'a, E> {}

unsafe impl<'a, E: Default + 'a> Send for SmartGuard<'a, E> {}