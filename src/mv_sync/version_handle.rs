use std::fmt::Display;
use std::hash::Hash;
use parking_lot::Mutex;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, SeqCst};
use crate::mv_record_model::version_info::{AtomicVersion, Version};
use crate::mv_tree::mvtree::MVTreeSt;
use crate::mv_sync::clock::{ClockHandle, GlobalClock};
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::smart_cell::sched_yield;

/// Structure wrapping the VC counter via a FairMutex.
#[derive(Clone)]
pub struct VersionHandle {
    pub committed_version: GlobalClock,
}

/// Implements default initializer for VersionHandle.
impl Default for VersionHandle {
    fn default() -> Self {
        VersionHandle::new_locked()
    }
}

/// Implements core functions for VersionManager.
impl VersionHandle {
    /// Default first version.
    pub const START_VERSION: Version = 1;

    /// Default first committed version.
    pub(crate) const START_COMMITTED_VERSION: Version = Self::START_VERSION;

    /// Peek committed version.
    pub fn committed_version(&self) -> Version {
        match &self.committed_version {
            GlobalClock::Locked(claw) => *claw.lock(),
            GlobalClock::Atomic(opt) => opt.load(Acquire),
            GlobalClock::Free(inspector) => *inspector.get_mut()
        }
    }

    /// Basic constructor.
    pub fn new_optimistic() -> Self {
        Self {
            committed_version: GlobalClock::Atomic(AtomicVersion::new(Self::START_VERSION))
        }
    }

    /// Basic constructor.
    pub fn new_locked() -> Self {
        Self {
            committed_version: GlobalClock::Locked(Mutex::new(Self::START_VERSION))
        }
    }

    /// Basic constructor.
    pub fn new_free() -> Self {
        Self {
            committed_version: GlobalClock::Free(SafeCell::new(Self::START_VERSION))
        }
    }
}

/// Extended "Index" implementation, i.e. including version specific methods.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVTreeSt<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub fn current_version(&self) -> Version {
        self.version_manager.committed_version()
    }

    /// Applies adaptive lock on commit version counter.
    #[inline]
    pub(crate) fn begin_commit(&self) -> ClockHandle<'_> {
        match &self.version_manager.committed_version {
            GlobalClock::Locked(claw) => ClockHandle::Locked(claw.lock()),
            GlobalClock::Atomic(opt) => ClockHandle::Optimistic(&opt, opt.load(Acquire)),
            GlobalClock::Free(inspector) => ClockHandle::Free(inspector.get_mut())
        }
    }

    /// Uses supplied lock to increment commit counter and releasing it afterward.
    #[inline]
    pub(crate) fn try_end_commit<'a>(&self, guard: ClockHandle<'a>) -> Result<Version, ClockHandle<'a>> {
        match guard {
            ClockHandle::Locked(mut claw) => {
                *claw = *claw + 1;
                Ok(*claw)
            },
            ClockHandle::Free(version) => {
                *version = *version + 1;
                Ok(*version)
            },
            ClockHandle::Optimistic(atomic, ..) =>
                Ok(atomic.fetch_add(1, SeqCst)),
        }
    }

    #[inline]
    pub(crate) fn end_commit(&self, mut commit_handle: ClockHandle) -> Version {
        let mut commit_attempts = 0;
        loop {
            match self.try_end_commit(commit_handle) {
                Ok(commit) if commit_attempts > 0 =>
                    break commit,
                Ok(commit) =>
                    break commit,
                Err(opt) => {
                    commit_attempts += 1;
                    sched_yield(commit_attempts);
                    commit_handle = opt
                }
            }
        }
    }

    #[inline(always)]
    pub(crate) fn commit(&self) -> Version {
        self.begin_commit()
            .end_commit(self)
    }
}