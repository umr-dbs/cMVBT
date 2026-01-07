use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::OnceLock;
use std::thread::yield_now;
use parking_lot::Mutex;
use crate::mv_record_model::version_info::{AtomicVersion, Version};
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::version_handle;

thread_local! {
    static STATE: ThreadState = ThreadState::new();
}

static GLOBAL_MIN: AtomicVersion
    = AtomicVersion::new(version_handle::START_VERSION);

static GLOBAL_DIRTY: AtomicBool
    = AtomicBool::new(true);

const MAX_READS_IN_ROW_EPSILON: usize
    = 10;

static THREAD_ID: AtomicUsize
    = AtomicUsize::new(0);

static SHARDED_COMMITTED: OnceLock<Mutex<Vec<PaddedAtomic>>>
    = OnceLock::new();

#[repr(align(64))]
struct PaddedAtomic(AtomicVersion);
impl Deref for PaddedAtomic {
    type Target = AtomicVersion;
    fn deref(&self) -> &AtomicVersion { &self.0 }
}

struct ThreadState {
    tid: usize,
    reads_in_row: SafeCell<usize>,
}

impl ThreadState {
    fn new() -> Self {
        loop {
            let tid_curr = THREAD_ID.load(SeqCst);
            if tid_curr >= committed().len() {
                match SHARDED_COMMITTED
                    .get()
                    .unwrap()
                    .try_lock()
                {
                    Some(mut lock) => lock
                        .extend((0..num_cpus::get_physical())
                            .map(|_| PaddedAtomic(AtomicVersion::new(Version::MAX)))),
                    _ => {
                        yield_now();
                        continue
                    }
                }
            }
            else {
                break ThreadState {
                    tid: THREAD_ID.fetch_add(1, Relaxed),
                    reads_in_row: SafeCell::new(0),
                }
            }
        }
    }
}

impl Drop for ThreadState {
    fn drop(&mut self) {
        thread_local_commit(self.tid, Version::MAX)
    }
}
#[inline]
fn committed() -> &'static [PaddedAtomic] {
    unsafe {
        &*SHARDED_COMMITTED.get_or_init(||
            Mutex::new(
                (0..num_cpus::get_physical())
                    .map(|_| PaddedAtomic(AtomicVersion::new(Version::MAX)))
                    .collect()))
            .data_ptr()
    }
}

#[inline]
pub(crate) fn committed_read() -> Version {
    let _ = STATE.try_with(|state| {
        if *state.reads_in_row != usize::MAX {
            if *state.reads_in_row > MAX_READS_IN_ROW_EPSILON {
                thread_local_commit(state.tid, Version::MAX);
                *state.reads_in_row.get_mut() = usize::MAX;
            }
            else {
                *state.reads_in_row.get_mut() = *state.reads_in_row + 1;
            }
        }
    });

    if !GLOBAL_DIRTY.load(Relaxed) {
        GLOBAL_MIN.load(Relaxed)
    }
    else {
        let agg_min_commit = committed()
            .iter()
            .take(THREAD_ID.load(Relaxed) + 1)
            .fold(Version::MAX,
                  |acc, l_commit| acc.min(l_commit.load(Relaxed)));

        if agg_min_commit == Version::MAX {
            GLOBAL_MIN.store(version_handle::START_VERSION, Relaxed);
            GLOBAL_DIRTY.store(false, Relaxed);

            version_handle::START_VERSION // empty root -> matched
        }
        else {
            GLOBAL_MIN.store(agg_min_commit, Relaxed);
            GLOBAL_DIRTY.store(false, Relaxed);
            agg_min_commit
        }
    }
}

#[inline]
fn thread_local_commit(id: usize, version: Version) {
    unsafe {
        committed().get_unchecked(id).store(version, Relaxed);
        GLOBAL_DIRTY.store(true, Relaxed);
    }
}

pub(crate) struct GlobalClock(pub(crate) AtomicVersion);

impl GlobalClock {
    pub(crate) const fn new() -> GlobalClock {
        GlobalClock(AtomicVersion::new(version_handle::START_VERSION))
    }

    // pushes completed work to visible, for readers
    #[inline(always)]
    pub(crate) fn end_commit(&self, version: Version) {
        STATE.with(|t_state| thread_local_commit(t_state.tid, version))
    }

    // global commit counter, e.g., to apply work
    #[inline(always)]
    pub(crate) fn start_commit(&self) -> Version {
        STATE.with(|t_state| *t_state.reads_in_row.get_mut() = 0);
        self.0.fetch_add(1, SeqCst)
    }
}