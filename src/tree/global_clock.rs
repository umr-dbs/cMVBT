use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::Relaxed;
use crate::record_model::version_info::{AtomicVersion, Version};
use crate::tree::version_manager::VersionManager;
use crate::utils::safe_cell::SafeCell;

pub(crate) enum GlobalClock {
    Locked(Mutex<Version>),
    Atomic(AtomicVersion),
    Free(SafeCell<Version>)
}

impl Clone for GlobalClock {
    fn clone(&self) -> Self {
        match self {
            GlobalClock::Locked(_) => Self::Locked(Mutex::new(VersionManager::START_VERSION)),
            GlobalClock::Atomic(_) => Self::Atomic(AtomicVersion::new(VersionManager::START_VERSION)),
            GlobalClock::Free(_) => Self::Free(SafeCell::new(VersionManager::START_VERSION))
        }
    }
}

/// Holds Version Commit Clock atomic strategy, either locked in multi-threaded or
/// single writer mode.
// #[repr(u8)]
pub enum ClockHandle<'a> {
    Locked(MutexGuard<'a, RawMutex, Version>),
    Free(&'a mut Version),
    Optimistic(&'a AtomicVersion, Version)
}

/// Implements variant checkers for VCClock.
impl ClockHandle<'_> {
    /// Returns true, if this clock is not locked.
    /// /// Returns false, otherwise.
    pub(crate) const fn is_free(&self) -> bool {
        match self {
            Self::Free(..) => true,
            _ => false,
        }
    }

    #[inline]
    pub(crate) fn read_handle_version(&self) -> Version {
        match self {
            ClockHandle::Locked(guard) => *guard.deref(),
            ClockHandle::Free(v) => **v,
            ClockHandle::Optimistic(.., seen) => *seen
        }
    }

    /// Returns true, if this clock is optimistic.
    /// /// Returns false, otherwise.
    pub(crate) const fn is_optimistic(&self) -> bool {
        match self {
            Self::Optimistic(..) => true,
            _ => false,
        }
    }

    /// Returns true, if this clock is locked.
    /// Returns false, otherwise.
    pub(crate) const fn is_locked(&self) -> bool {
        !self.is_free()
    }
}