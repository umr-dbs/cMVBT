use std::hash::Hash;
use parking_lot::Mutex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::mem;
use crate::record_model::version_info::Version;
use crate::tree::bplus_tree::BPlusTree;
use crate::tree::global_clock::GlobalClock;

/// Structure wrapping the VC counter via a FairMutex.
pub struct VersionManager {
    pub committed_version: Mutex<Version>,
}

/// Safely implementing serde::Serialize for VersionManager.
impl Serialize for VersionManager {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.committed_version())
    }
}

/// Safely implementing serde::Deserialize for VersionManager.
impl<'de> Deserialize<'de> for VersionManager {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::from(u64::deserialize(deserializer)?))
    }
}

/// Implements default initializer for VersionManager.
impl Default for VersionManager {
    fn default() -> Self {
        VersionManager::new()
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
        unsafe { *self.committed_version.data_ptr() }
    }

    /// Basic constructor.
    pub fn new() -> Self {
        Self {
            committed_version: Mutex::new(Self::START_COMMITTED_VERSION),
        }
    }

    /// Internal reconstruction method.
    /// Mainly for a deserialize support via serde.
    pub(crate) fn from(version: Version) -> Self {
        Self {
            committed_version: Mutex::new(version),
        }
    }
}

/// Extended "Index" implementation, i.e. including version specific methods.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash // + 'static
> BPlusTree<FAN_OUT, NUM_RECORDS, Key> {
    /// Applies adaptive lock on commit version counter.
    #[inline]
    pub(crate) fn begin_commit(&self) -> GlobalClock {
        match self.locking_strategy.version_commit_lock_required() {
            false => unsafe {
                GlobalClock::Free(&mut *self.version_manager.committed_version.data_ptr())
            },
            true => GlobalClock::Locked(self.version_manager.committed_version.lock()),
        }
    }

    /// Uses supplied lock to increment commit counter and releasing it afterwards.
    #[inline]
    pub(crate) fn end_commit(&self, mut guard: GlobalClock) {
        *guard = *guard + 1;

        mem::drop(guard)
    }
}