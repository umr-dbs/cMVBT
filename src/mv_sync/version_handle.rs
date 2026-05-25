use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use crate::mv_record_model::version_info::Version;
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_sync::clock::committed_read;

pub(crate) const START_VERSION: Version = 1;

/// Extended "Index" implementation, i.e., including version-specific methods.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub fn current_version_for_reader(&self) -> Version {
        committed_read(self.global_clock.0.load(Relaxed))
    }

    #[inline(always)]
    pub(crate) fn current_version(&self) -> Version {
        self.global_clock.current_version()
    }

    #[inline(always)]
    pub(crate) fn start_tx_commit(&self) -> Version {
        self.global_clock.start_commit()
    }

    #[inline(always)]
    pub(crate) fn end_tx_commit(&self, version: Version) {
        self.global_clock.end_commit(version);
    }
}