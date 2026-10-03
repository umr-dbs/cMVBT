use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::{Relaxed, Release};
use std::sync::OnceLock;
use std::thread::{spawn, yield_now, JoinHandle};
use parking_lot::Mutex;
use crate::dexa::mvb_record_model::Version;
use crate::dexa::mvb_record_model::version_info::AtomicVersion;
use crate::dexa::mvb_tree::bplus_tree::MVBPlusTree;
use crate::dexa::mvb_utils::safe_cell::SafeCell;

pub const START_VERSION: Version = 1;

thread_local! {
    static STATE: ThreadState = ThreadState::new();
}

static GLOBAL_MIN: PaddedAtomicVersion
    = PaddedAtomicVersion(AtomicVersion::new(START_VERSION));

static GLOBAL_DIRTY: PaddedAtomicBool
    = PaddedAtomicBool(AtomicBool::new(false));

const MAX_READS_IN_ROW_EPSILON: usize
    = 100;

const READ_IN_ROW_INACTIVE: usize
    = usize::MAX;

const INACTIVE_COMMIT_VERSION_MAX: Version
    = Version::MAX;

static THREAD_ID: AtomicUsize
    = AtomicUsize::new(0);

/// Commit slots of all threads. Fixed in size and never reallocated: threads read the whole array lock-free, so a
/// growing `Vec` would free the buffer under their feet (use-after-free when more threads than cores registered).
/// Thread ids are recycled when threads end.
const MAX_THREADS: usize = 4096;

static SLOTS: [PaddedAtomicVersion; MAX_THREADS]
    = [const { PaddedAtomicVersion(AtomicVersion::new(INACTIVE_COMMIT_VERSION_MAX)) }; MAX_THREADS];

static FREE_TIDS: Mutex<Vec<usize>> = Mutex::new(Vec::new());

#[repr(align(64))]
struct PaddedAtomicVersion(AtomicVersion);
impl Deref for PaddedAtomicVersion {
    type Target = AtomicVersion;
    fn deref(&self) -> &AtomicVersion { &self.0 }
}

#[repr(align(64))]
struct PaddedAtomicBool(AtomicBool);
impl Deref for PaddedAtomicBool {
    type Target = AtomicBool;
    fn deref(&self) -> &AtomicBool { &self.0 }
}

struct ThreadState {
    tid: usize,
    reads_in_row: SafeCell<usize>,
}

impl ThreadState {
    #[inline(always)]
    fn has_active_commit(&self) -> bool { *self.reads_in_row != READ_IN_ROW_INACTIVE }

    #[inline(always)]
    fn set_inactive_commit(&self) {
        *self.reads_in_row.get_mut() = READ_IN_ROW_INACTIVE;
        thread_local_commit_inactive(self.tid);
        // let committed
        //     = committed();
        //
        // let my_min = unsafe { committed.get_unchecked(self.tid).load(Relaxed) };
        //
        // let curr_min = committed
        //     .iter()
        //     .enumerate()
        //     .filter(|(pos, ..)| *pos != self.tid)
        //     .min_by(|(.., l_commit_0), (.., l_commit_1)|
        //         l_commit_0.load(Relaxed).cmp(&l_commit_1.load(Relaxed)))
        //     .map(|(_, l_commit_min)| l_commit_min.load(Relaxed))
        //     .unwrap_or(my_min);
        //
        // if curr_min > my_min {
        //     *self.reads_in_row.get_mut() = READ_IN_ROW_INACTIVE;
        //     thread_local_commit_inactive(self.tid);
        // }
        // else {
        //     self.inc_reads()
        // }
    }

    #[inline(always)]
    fn has_reads_in_row_max(&self) -> bool {
        *self.reads_in_row > MAX_READS_IN_ROW_EPSILON
    }

    #[inline(always)]
    fn inc_reads(&self) {
        *self.reads_in_row.get_mut() = *self.reads_in_row + 1
    }

    #[inline(always)]
    fn reset_reads(&self) {
        *self.reads_in_row.get_mut() = 0
    }

    fn new() -> Self {
        let tid = FREE_TIDS.lock().pop().unwrap_or_else(|| {
            // The atomic only reserves a unique index; every slot is statically initialized.
            let tid = THREAD_ID.fetch_add(1, Relaxed);
            assert!(tid < MAX_THREADS, "more than {MAX_THREADS} threads use the commit clock at the same time");
            tid
        });

        ThreadState {
            tid,
            reads_in_row: SafeCell::new(0),
        }
    }
}

impl Drop for ThreadState {
    fn drop(&mut self) {
        thread_local_commit_inactive(self.tid);
        FREE_TIDS.lock().push(self.tid);
    }
}

#[inline(always)]
fn committed() -> &'static [PaddedAtomicVersion] {
    &SLOTS
}

#[inline]
pub(crate) fn committed_read(clock_time: Version) -> Version {
    let _ = STATE.try_with(|state| if state.has_active_commit() {
        if state.has_reads_in_row_max() {
            state.set_inactive_commit();
        }
        else {
            state.inc_reads()
        }
    });

    if !GLOBAL_DIRTY.load(Relaxed) {
        GLOBAL_MIN.load(Relaxed)
    }
    else {
        let agg_min_commit = committed()
            .iter()
            .take(THREAD_ID.load(Relaxed))
            .fold(clock_time,
                  |acc, l_commit| acc.min(l_commit.load(Relaxed)));

        let oo_min
            = GLOBAL_MIN.fetch_max(agg_min_commit, Relaxed);

        GLOBAL_DIRTY.store(false, Relaxed);
        agg_min_commit.max(oo_min)
    }
}

#[inline(always)]
fn thread_local_commit_inactive(id: usize) {
    thread_local_commit(id, INACTIVE_COMMIT_VERSION_MAX);
}

#[inline]
fn thread_local_commit(id: usize, version: Version) {
    unsafe {
        debug_assert!(committed().len() > id, "committed.len()={}, id={}", committed().len(), id);
        committed().get_unchecked(id).store(version, Release);
        GLOBAL_DIRTY.store(true, Release);
    }
}

pub(crate) struct GlobalClock(pub(crate) AtomicVersion);

impl GlobalClock {
    pub(crate) fn new() -> GlobalClock {
        GlobalClock(AtomicVersion::new(START_VERSION))
    }

    // pushes completed work to visible, for readers
    #[inline(always)]
    pub(crate) fn end_commit(&self, version: Version) {
        STATE.with(|t_state| thread_local_commit(t_state.tid, version))
    }

    // global commit counter, e.g., to apply work
    #[inline(always)]
    pub(crate) fn start_commit(&self) -> Version {
        STATE.with(|t_state| t_state.reset_reads());
        // Atomic modification order provides unique versions. Page publication uses Release/Acquire separately.
        self.0.fetch_add(1, Relaxed)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Send + Clone + Default + Sync + 'static
> MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub fn current_version_for_reader(&self) -> Version {
        committed_read(self.global_clock.0.load(Relaxed))
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

#[cfg(test)]
mod tests {
    use super::{GlobalClock, Version};
    use std::collections::HashSet;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn relaxed_dexa_clock_allocates_unique_contiguous_versions_concurrently() {
        const THREADS: usize = 8;
        const VERSIONS_PER_THREAD: usize = 1_000;

        let clock = Arc::new(GlobalClock::new());
        let start = clock.0.load(std::sync::atomic::Ordering::Relaxed);
        let barrier = Arc::new(Barrier::new(THREADS));
        let mut workers = Vec::with_capacity(THREADS);

        for _ in 0..THREADS {
            let clock = Arc::clone(&clock);
            let barrier = Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                barrier.wait();
                (0..VERSIONS_PER_THREAD)
                    .map(|_| {
                        let version = clock.start_commit();
                        clock.end_commit(version);
                        version
                    })
                    .collect::<Vec<Version>>()
            }));
        }

        let versions = workers
            .into_iter()
            .flat_map(|worker| worker.join().expect("DEXA clock worker panicked"))
            .collect::<Vec<_>>();
        let unique = versions.iter().copied().collect::<HashSet<_>>();
        let allocated = (THREADS * VERSIONS_PER_THREAD) as Version;

        assert_eq!(versions.len(), allocated as usize);
        assert_eq!(unique.len(), versions.len());
        assert_eq!(versions.iter().copied().min(), Some(start));
        assert_eq!(versions.iter().copied().max(), Some(start + allocated - 1));
        assert_eq!(
            clock.0.load(std::sync::atomic::Ordering::Relaxed),
            start + allocated
        );
    }
}
