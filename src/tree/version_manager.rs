use std::hash::Hash;
use parking_lot::Mutex;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use crate::record_model::version_info::{AtomicVersion, Version};
use crate::tree::bplus_tree::BPlusTree;
use crate::tree::global_clock::{ClockHandle, GlobalClock};
use crate::utils::safe_cell::SafeCell;

/// Structure wrapping the VC counter via a FairMutex.
#[derive(Clone)]
pub struct VersionManager {
    pub committed_version: GlobalClock,
}

/// Implements default initializer for VersionManager.
impl Default for VersionManager {
    fn default() -> Self {
        VersionManager::new_locked()
    }
}

/// Implements core functions for VersionManager.
impl VersionManager {
    /// Default first version.
    pub(crate) const START_VERSION: Version = Version::MIN;

    /// Default first committed version.
    pub(crate) const START_COMMITTED_VERSION: Version = Version::MIN;

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
    Key: Default + Ord + Copy + Hash
> BPlusTree<FAN_OUT, NUM_RECORDS, Key> {
    /// Applies adaptive lock on commit version counter.
    #[inline]
    pub(crate) fn begin_commit(&self) -> ClockHandle {
        match &self.version_manager.committed_version {
            GlobalClock::Locked(claw) => ClockHandle::Locked(claw.lock()),
            GlobalClock::Atomic(opt) => ClockHandle::Optimistic(&opt, opt.load(Acquire)),
            GlobalClock::Free(inspector) => ClockHandle::Free(inspector.get_mut())
        }
    }

    /// Uses supplied lock to increment commit counter and releasing it afterwards.
    #[inline]
    pub(crate) fn try_end_commit<'a>(&self, mut guard: ClockHandle<'a>) -> Result<Version, ClockHandle<'a>> {
        match guard {
            ClockHandle::Locked(mut claw) => {
                *claw = *claw + 1;
                Ok(*claw)
            },
            ClockHandle::Free(version) => {
                *version = *version + 1;
                Ok(*version)
            },
            ClockHandle::Optimistic(atomic, seen) =>
                match atomic.compare_exchange_weak(seen, seen + 1, AcqRel, Acquire) {
                    Ok(prev) => Ok(prev + 1),
                    Err(curr) => Err(ClockHandle::Optimistic(atomic, curr))
                }
        }
    }
}