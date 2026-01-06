use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use std::sync::OnceLock;
use std::thread::AccessError;
use crate::mv_record_model::version_info::{AtomicVersion, Version};
use crate::mv_sync::version_handle;

thread_local! {
    static STATE: ThreadState = ThreadState::new();
}

struct ThreadState {
    tid: usize
}

impl ThreadState {
    fn new() -> Self {
        ThreadState {
            tid: THREAD_ID.fetch_add(1, Relaxed),
        }
    }

    const fn tid(&self) -> usize { self.tid }
}

impl Drop for ThreadState {
    fn drop(&mut self) {
        unsafe {
            committed()
                .get_unchecked(self.tid)
                .store(committed_max(), Relaxed);
        }
    }
}

static THREAD_ID: AtomicUsize = AtomicUsize::new(0);

#[repr(align(64))]
struct PaddedAtomic(AtomicVersion);
impl Deref for PaddedAtomic {
    type Target = AtomicVersion;
    fn deref(&self) -> &AtomicVersion { &self.0 }
}

static SHARDED_COMMITTED: OnceLock<Vec<PaddedAtomic>> = OnceLock::new();

#[inline]
fn committed() -> &'static [PaddedAtomic] {
    SHARDED_COMMITTED.get_or_init(||
        (0..num_cpus::get_physical()).map(|_| PaddedAtomic(AtomicVersion::new(Version::MAX))).collect())
}

fn committed_max() -> Version {
    committed()
        .iter()
        .max_by_key(|a| a.load(SeqCst))
        .unwrap()
        .load(Relaxed)
}

#[inline]
pub(crate) fn committed_read() -> Version {
    // STATE.with(|_| { }); // never init. the state for readers, only writers via commit
    if let Ok(tid) = STATE.try_with(|state| state.tid) {
        thread_local_commit(tid, committed_max())
    }

    let v = committed()
        .iter()
        .take(THREAD_ID.load(Relaxed) + 1)
        .fold(Version::MAX,
              |a, b| a.min(b.load(Relaxed)));

    if v == Version::MAX {
        version_handle::START_VERSION // empty root -> matched
    }
    else {
        v
    }
}

#[inline]
pub fn thread_local_commit(id: usize, version: Version) {
    unsafe {
        committed().get_unchecked(id).store(version, Relaxed);
    }
}

pub(crate) struct GlobalClock(pub(crate) AtomicVersion);

impl Clone for GlobalClock {
    fn clone(&self) -> Self {
        GlobalClock(AtomicVersion::new(self.0.load(SeqCst)))
    }
}

impl GlobalClock {
    pub(crate) const fn new() -> GlobalClock {
        GlobalClock(AtomicVersion::new(version_handle::START_VERSION))
    }

    // pushes completed work to visible, for readers
    #[inline(always)]
    pub(crate) fn end_commit(&self, version: Version) {
        STATE.with(|t_state| thread_local_commit(t_state.tid(), version))
    }

    // global commit counter, e.g., to apply work
    #[inline(always)]
    pub(crate) fn start_commit(&self) -> Version {
         self.0.fetch_add(1, SeqCst)
    }
}