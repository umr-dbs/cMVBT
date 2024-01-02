use parking_lot::lock_api::MutexGuard;
use parking_lot::RawMutex;
use std::ops::{Deref, DerefMut};
use crate::record_model::version_info::Version;

/// Holds Version Commit Clock atomic strategy, either locked in multi-threaded or
/// single writer mode.
// #[repr(u8)]
pub(crate) enum GlobalClock<'a> {
    Locked(MutexGuard<'a, RawMutex, Version>),
    Free(&'a mut Version),
}

/// Implements variant checkers for VCClock.
impl GlobalClock<'_> {
    /// Returns true, if this clock is not locked.
    /// /// Returns false, otherwise.
    pub(crate) const fn is_free(&self) -> bool {
        match self {
            Self::Free(..) => true,
            _ => false,
        }
    }

    /// Returns true, if this cliock is locked.
    /// Returns false, otherwise.
    pub(crate) const fn is_locked(&self) -> bool {
        !self.is_free()
    }
}

/// Implements sugar for auto deref, i.e. access the VC Clock.
impl Deref for GlobalClock<'_> {
    type Target = Version;

    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            GlobalClock::Locked(vc) => vc.deref(),
            GlobalClock::Free(vc) => vc,
        }
    }
}

/// Implements sugar, used for automatic commit, regardless of underlying mode.
impl DerefMut for GlobalClock<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            GlobalClock::Locked(vc) => vc.deref_mut(),
            GlobalClock::Free(vc) => vc,
        }
    }
}
