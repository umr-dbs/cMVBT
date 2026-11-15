use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use crate::mv_record_model::version_info::Version;
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_sync::clock::{CommitTicket, GlobalClock, WriteGuard};

pub(crate) const START_VERSION: Version = 1;

/// Extended "Index" implementation, i.e., including version-specific methods.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload> {
    /// The newest snapshot all of whose writes have taken effect (see `mv_sync::clock`).
    #[inline(always)]
    pub fn current_version_for_reader(&self) -> Version {
        self.global_clock.snapshot()
    }

    /// The next version to be handed out (not a reader snapshot).
    #[inline(always)]
    pub(crate) fn current_version(&self) -> Version {
        self.global_clock.current_version()
    }

    /// Starts a write. Its version is published to readers once the returned ticket is dropped
    /// or published, which must happen after the soft commit.
    #[inline(always)]
    pub(crate) fn start_tx_commit(&self) -> CommitTicket<'_> {
        self.global_clock.start_commit()
    }

    /// Starts a write operation; hold the guard until the operation's soft commit.
    #[inline(always)]
    pub(crate) fn begin_write(&self) -> WriteGuard<'_> {
        self.global_clock.begin_write()
    }

    /// See `GlobalClock::retire_stamp`.
    #[inline(always)]
    pub(crate) fn retire_stamp(&self) -> Version {
        self.global_clock.retire_stamp()
    }

    #[inline(always)]
    pub(crate) fn clock(&self) -> &Arc<GlobalClock> {
        &self.global_clock
    }
}
