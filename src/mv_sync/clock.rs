use std::ops::Deref;
use std::sync::atomic::{AtomicU8, AtomicUsize};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::OnceLock;
use std::thread::{spawn, yield_now, JoinHandle};
use parking_lot::Mutex;
use crate::mv_record_model::version_info::{AtomicVersion, Version};
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::version_handle;

pub type Tid = usize;

thread_local! {
    static STATE: ThreadState = ThreadState::new();
}

#[inline]
pub fn __tid() -> Tid {
    STATE.with(|tid| tid.tid)
}

static GLOBAL_MIN: PaddedAtomicVersion
    = PaddedAtomicVersion(AtomicVersion::new(version_handle::START_VERSION));

const CLEAN: u8 = 0;
const DIRTY: u8 = 1;
const SCANNING: u8 = 2;

static GLOBAL_DIRTY: PaddedAtomicU8
    = PaddedAtomicU8(AtomicU8::new(CLEAN));

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
struct PaddedAtomicU8(AtomicU8);
impl Deref for PaddedAtomicU8 {
    type Target = AtomicU8;
    fn deref(&self) -> &AtomicU8 { &self.0 }
}

struct ThreadState {
    tid: Tid,
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

    committed_snapshot(clock_time)
}

/// Side-effect free (w.r.t. the calling thread's commit slot) variant of `committed_read`.
/// The result is monotonically non-decreasing over time, hence it lower-bounds every snapshot
/// a reader obtains afterwards. Pass `Version::MAX` when no clock reading is at hand.
#[inline]
pub(crate) fn committed_snapshot(clock_time: Version) -> Version {
    let dirty = GLOBAL_DIRTY.load(Acquire);
    if dirty == CLEAN {
        GLOBAL_MIN.load(Relaxed)
    }
    else {
        // One reader owns the clean transition. Other readers remain latch-free: they aggregate
        // independently rather than waiting for the owner. A concurrent commit changes SCANNING
        // back to DIRTY, so the owner's final compare-exchange cannot erase its notification.
        let owns_scan = dirty == DIRTY
            && GLOBAL_DIRTY.compare_exchange(DIRTY, SCANNING, AcqRel, Acquire).is_ok();
        let agg_min_commit = committed()
            .iter()
            .take(THREAD_ID.load(Relaxed))
            .fold(clock_time,
                  |acc, l_commit| acc.min(l_commit.load(Relaxed)));

        let oo_min
            = GLOBAL_MIN.fetch_max(agg_min_commit, Relaxed);

        if owns_scan {
            let _ = GLOBAL_DIRTY.compare_exchange(SCANNING, CLEAN, Release, Relaxed);
        }
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
        committed().get_unchecked(id).store(version, Release);
        GLOBAL_DIRTY.store(DIRTY, Release);
    }
}

/// Explicitly publishes worker completion. Joining a Rust thread waits for its closure, but its
/// native thread-local destructors may run slightly later, so phase boundaries must not rely on
/// `ThreadState::drop` to release the last committed version.
#[inline]
pub(crate) fn release_thread_commit() {
    let _ = STATE.try_with(|state| state.set_inactive_commit());
}

pub(crate) struct GlobalClock(pub(crate) AtomicVersion);

impl GlobalClock {
    pub(crate) fn new() -> GlobalClock {
        GlobalClock(AtomicVersion::new(version_handle::START_VERSION))
    }

    #[inline(always)]
    pub(crate) fn current_version(&self) -> Version {
        self.0.load(Relaxed)
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

#[cfg(test)]
mod tests {
    use super::{committed, GlobalClock, Version, INACTIVE_COMMIT_VERSION_MAX};
    use std::collections::HashSet;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn explicit_worker_finish_releases_commit_slot_before_join() {
        let clock = Arc::new(GlobalClock::new());
        let worker_clock = Arc::clone(&clock);
        let tid = thread::spawn(move || {
            let version = worker_clock.start_commit();
            worker_clock.end_commit(version);
            let tid = super::__tid();
            super::release_thread_commit();
            tid
        })
        .join()
        .unwrap();
        assert_eq!(committed()[tid].load(std::sync::atomic::Ordering::Relaxed), INACTIVE_COMMIT_VERSION_MAX);
    }

    #[test]
    fn relaxed_clock_allocates_unique_contiguous_versions_concurrently() {
        const THREADS: usize = 8;
        const VERSIONS_PER_THREAD: usize = 1_000;

        let clock = Arc::new(GlobalClock::new());
        let start = clock.current_version();
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
            .flat_map(|worker| worker.join().expect("clock worker panicked"))
            .collect::<Vec<_>>();
        let unique = versions.iter().copied().collect::<HashSet<_>>();
        let allocated = (THREADS * VERSIONS_PER_THREAD) as Version;

        assert_eq!(versions.len(), allocated as usize);
        assert_eq!(unique.len(), versions.len());
        assert_eq!(versions.iter().copied().min(), Some(start));
        assert_eq!(versions.iter().copied().max(), Some(start + allocated - 1));
        assert_eq!(clock.current_version(), start + allocated);
    }
}
