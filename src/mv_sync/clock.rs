use std::fmt::{Display, Formatter};
use std::hash::Hash;
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use std::ops::Deref;
use serde::{Deserialize, Serialize};
use crate::mv_record_model::version_info::{AtomicVersion, Version};
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::version_handle::VersionHandle;
use crate::mv_tree::mvtree::MVTreeSt;

#[derive(Clone, Serialize, Deserialize)]
pub enum ClockType {
    FREE,
    OPT,
    SYNC,
}

impl Display for ClockType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClockType::FREE => write!(f, "FREE"),
            ClockType::OPT => write!(f, "OPT"),
            ClockType::SYNC => write!(f, "SYNC"),
        }
    }
}

pub(crate) enum GlobalClock {
    Locked(Mutex<Version>),
    Atomic(AtomicVersion),
    Free(SafeCell<Version>)
}

impl Clone for GlobalClock {
    fn clone(&self) -> Self {
        match self {
            GlobalClock::Locked(_) => Self::Locked(Mutex::new(VersionHandle::START_VERSION)),
            GlobalClock::Atomic(_) => Self::Atomic(AtomicVersion::new(VersionHandle::START_VERSION)),
            GlobalClock::Free(_) => Self::Free(SafeCell::new(VersionHandle::START_VERSION))
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

    #[inline]
    pub(crate) fn end_commit<
        const FAN_OUT: usize,
        const NUMBER_RECORDS: usize,
        Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
        Payload: Display + Clone + Default + Sync + 'static
    >(self, index: &MVTreeSt<FAN_OUT, NUMBER_RECORDS, Key, Payload>) -> Version
    {
        index.end_commit(self)
    }
}