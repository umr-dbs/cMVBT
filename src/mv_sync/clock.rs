//! Logical commit clock, snapshot publication and protection of write traversals.
//!
//! Every write takes a unique version from a per-tree counter `next`. A reader may only be handed
//! a snapshot `S` such that every write with a version `<= S` has taken effect (Section 5 of the
//! paper). Versions are allocated before the write is applied, so the clock alone does not give
//! such a bound: a writer may hold version `v` while a writer with version `v + 1` has already
//! finished.
//!
//! The protocol therefore tracks the *in-flight* write operations:
//!
//! 1. When a write operation starts (before its traversal), the thread publishes the reservation
//!    `r = next - 1` into its slot.
//! 2. Every version the operation allocates (including those of node reorganizations performed
//!    on its way down) is `v = next.fetch_add(1) > r`.
//! 3. When the operation ends, after its soft commit, the slot is cleared (`IDLE`).
//!
//! A reader computes `S = min(next - 1, min over all slots)`. All accesses of steps 1-2 and of the
//! reader's loads are sequentially consistent, which gives:
//!
//! * If the reader's load of `next` observes an allocation `v`, the reservation store precedes it
//!   in the single total order, so the reader observes the reservation (or the later `IDLE`,
//!   stored after the operation's soft commit) and `S <= r < v`.
//! * Otherwise the reader loaded `next <= v`, so `S <= v - 1 < v`.
//!
//! In both cases an in-flight version is never part of a snapshot, and every version `<= S` has
//! taken effect and is visible to the reader. Completed operations do not hold back snapshots.
//!
//! The same reservation protects optimistic write traversals against node reuse: a node retired
//! with stamp `t = next` (read after it was unlinked) is reused only when `t` is below every
//! reservation, i.e., when every in-flight write operation started after the unlink.
//!
//! The state is per clock (per tree): reservations of one tree never influence another tree.

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicU8, AtomicUsize};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, SeqCst};
use parking_lot::Mutex;
use crate::mv_record_model::version_info::{AtomicVersion, Version};
use crate::mv_sync::version_handle;

pub type Tid = usize;

thread_local! {
    static STATE: ThreadState = ThreadState::new();
}

#[inline]
pub fn __tid() -> Tid {
    STATE.with(|tid| tid.tid)
}

/// Slot value of a thread without an in-flight write.
const IDLE: Version = Version::MAX;

const CLEAN: u8 = 0;
const DIRTY: u8 = 1;
const SCANNING: u8 = 2;

/// Upper bound of concurrently registered threads. Thread ids are recycled when threads end.
const MAX_THREADS: usize = 4096;

/// High-water mark of handed out thread ids; readers only inspect slots below it.
static THREAD_ID: AtomicUsize = AtomicUsize::new(0);

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
}

impl ThreadState {
    fn new() -> Self {
        let tid = FREE_TIDS.lock().pop().unwrap_or_else(|| {
            let tid = THREAD_ID.fetch_add(1, SeqCst);
            assert!(tid < MAX_THREADS, "more than {MAX_THREADS} threads use the commit clock at the same time");
            tid
        });
        ThreadState { tid }
    }
}

impl Drop for ThreadState {
    fn drop(&mut self) {
        // A thread cannot end while holding a `CommitTicket` (tickets are not `Send` and release
        // their slot on drop, also during unwinding), so all its slots are `IDLE` here.
        FREE_TIDS.lock().push(self.tid);
    }
}

/// No-op, kept for callers written against the former protocol, in which a thread had to
/// explicitly release the last version it committed. Completed writes no longer hold back
/// snapshots.
#[inline(always)]
pub(crate) fn release_thread_commit() {}

pub(crate) struct GlobalClock {
    /// The next version to be handed out.
    next: PaddedAtomicVersion,
    /// Cached snapshot; monotonically non-decreasing. Valid whenever `state == CLEAN`.
    snapshot: PaddedAtomicVersion,
    /// CLEAN: no write started or ended since `snapshot` was computed.
    state: PaddedAtomicU8,
    /// Reservation of the in-flight write of each thread, or `IDLE`.
    slots: Box<[PaddedAtomicVersion]>,
}

impl GlobalClock {
    pub(crate) fn new() -> GlobalClock {
        GlobalClock {
            // `START_VERSION` is the version of the initial, empty tree (its root is registered
            // at it), hence the snapshot of a tree nobody has written to yet.
            next: PaddedAtomicVersion(AtomicVersion::new(version_handle::START_VERSION + 1)),
            snapshot: PaddedAtomicVersion(AtomicVersion::new(version_handle::START_VERSION)),
            state: PaddedAtomicU8(AtomicU8::new(CLEAN)),
            slots: (0..MAX_THREADS).map(|_| PaddedAtomicVersion(AtomicVersion::new(IDLE))).collect(),
        }
    }

    /// The next version to be handed out. Not a snapshot: versions below it may be in flight.
    #[inline(always)]
    pub(crate) fn current_version(&self) -> Version {
        self.next.load(Relaxed)
    }

    /// Starts a write operation: publishes the reservation of the calling thread unless it is
    /// already inside a write operation on this clock. The operation ends when the outermost
    /// guard is dropped, which must happen after its soft commit.
    #[inline(always)]
    pub(crate) fn begin_write(&self) -> WriteGuard<'_> {
        let tid = __tid();
        let slot = &self.slots[tid];
        // Only the owning thread writes its slot.
        let outermost = slot.load(Relaxed) == IDLE;
        if outermost {
            slot.store(self.next.load(SeqCst).saturating_sub(1), SeqCst);
            self.state.store(DIRTY, SeqCst);
        }
        WriteGuard { clock: self, tid, outermost, _not_send: PhantomData }
    }

    /// Allocates a version for a write. Inside a write operation the version is published when
    /// the operation ends; otherwise the ticket forms a write operation of its own.
    #[inline(always)]
    pub(crate) fn start_commit(&self) -> CommitTicket<'_> {
        let guard = self.begin_write();
        // Strictly above the reservation published by `begin_write`.
        let version = self.next.fetch_add(1, SeqCst);
        CommitTicket { _guard: guard, version }
    }

    /// Stamp of a node that has just been unlinked by a reorganization. Larger than the
    /// reorganization's version; the node is safe to reuse once the reader bound exceeds it.
    #[inline(always)]
    pub(crate) fn retire_stamp(&self) -> Version {
        self.next.load(SeqCst)
    }

    #[inline(always)]
    fn end_write(&self, tid: Tid) {
        // SeqCst (a release) orders the operation's soft commit before the slot becomes `IDLE`.
        self.slots[tid].store(IDLE, SeqCst);
        self.state.store(DIRTY, SeqCst);
    }

    /// The newest snapshot all of whose writes have taken effect.
    #[inline]
    pub(crate) fn snapshot(&self) -> Version {
        // The clock must be read before the slots (see the module documentation).
        let next = self.next.load(SeqCst);
        if self.state.load(SeqCst) == CLEAN {
            return self.snapshot.load(Acquire)
        }

        // One reader owns the clean transition. Other readers stay latch-free and aggregate
        // independently. A write starting or ending meanwhile sets DIRTY again, so the owner's
        // final compare-exchange fails and the cache is not declared clean.
        let owns_scan = self.state
            .compare_exchange(DIRTY, SCANNING, SeqCst, SeqCst)
            .is_ok();

        let snapshot = self.slots
            .iter()
            .take(THREAD_ID.load(SeqCst).min(MAX_THREADS))
            .fold(next.saturating_sub(1), |acc, slot| acc.min(slot.load(SeqCst)));

        // Snapshots only grow: once every write `<= s` has taken effect, this stays true.
        let previous = self.snapshot.fetch_max(snapshot, AcqRel);

        if owns_scan {
            let _ = self.state.compare_exchange(SCANNING, CLEAN, SeqCst, Relaxed);
        }
        snapshot.max(previous)
    }
}

/// An in-flight write operation. Dropping the outermost guard marks the operation as completed,
/// on every path including early returns of failed operations and unwinding.
#[must_use = "dropping the guard immediately ends the write operation"]
pub(crate) struct WriteGuard<'a> {
    clock: &'a GlobalClock,
    tid: Tid,
    outermost: bool,
    /// The guard refers to the slot of the creating thread.
    _not_send: PhantomData<*const ()>,
}

impl Drop for WriteGuard<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        if self.outermost {
            self.clock.end_write(self.tid)
        }
    }
}

/// A version allocated for a write. See [`GlobalClock::start_commit`].
#[must_use = "dropping the ticket immediately publishes the write as completed"]
pub(crate) struct CommitTicket<'a> {
    _guard: WriteGuard<'a>,
    version: Version,
}

impl CommitTicket<'_> {
    #[inline(always)]
    pub(crate) fn version(&self) -> Version {
        self.version
    }

    /// Ends the ticket; call after the write's soft commit. Publishes the write unless it is part
    /// of an enclosing write operation, which then publishes it when it ends.
    #[inline(always)]
    pub(crate) fn publish(self) {}
}


#[cfg(test)]
mod tests {
    use super::{GlobalClock, Version};
    use std::collections::HashSet;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn clock_allocates_unique_contiguous_versions_concurrently() {
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
                    .map(|_| clock.start_commit().version())
                    .collect::<Vec<Version>>()
            }));
        }

        let versions = workers
            .into_iter()
            .flat_map(|worker| worker.join().expect("clock worker panicked"))
            .collect::<Vec<_>>();
        let unique = versions.iter().copied().collect::<HashSet<_>>();
        let allocated = (THREADS * VERSIONS_PER_THREAD) as Version;

        assert_eq!(unique.len(), versions.len());
        assert_eq!(versions.iter().copied().min(), Some(start));
        assert_eq!(versions.iter().copied().max(), Some(start + allocated - 1));
        assert_eq!(clock.current_version(), start + allocated);
        assert_eq!(clock.snapshot(), start + allocated - 1, "all writes completed");
    }

    /// Reviewer scenario: a write that took an older version and has not finished must bound
    /// every snapshot, even when a newer write has completed.
    #[test]
    fn in_flight_write_bounds_snapshots() {
        let clock = Arc::new(GlobalClock::new());
        for _ in 0..10 {
            clock.start_commit().publish();
        }
        let base = clock.snapshot();
        assert_eq!(base, clock.current_version() - 1);

        let (taken_tx, taken_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
        let slow = {
            let clock = Arc::clone(&clock);
            thread::spawn(move || {
                let ticket = clock.start_commit();
                taken_tx.send(ticket.version()).unwrap();
                resume_rx.recv().unwrap();
                ticket.publish();
            })
        };
        let slow_version = taken_rx.recv().unwrap();
        assert_eq!(slow_version, base + 1);

        let fast = clock.start_commit();
        assert_eq!(fast.version(), base + 2);
        fast.publish();

        assert!(clock.snapshot() < slow_version, "snapshot includes the in-flight write");

        resume_tx.send(()).unwrap();
        slow.join().unwrap();
        assert_eq!(clock.snapshot(), base + 2);
    }

    /// A failed operation drops its ticket without an explicit publish; its reservation must not
    /// hold back later snapshots.
    #[test]
    fn dropped_ticket_releases_reservation() {
        let clock = GlobalClock::new();
        {
            let _failed = clock.start_commit();
        }
        let last = clock.start_commit();
        let last_version = last.version();
        last.publish();
        assert_eq!(clock.snapshot(), last_version);
    }

    /// Snapshots of one tree are independent of in-flight writes on another tree.
    #[test]
    fn clocks_are_independent() {
        let a = GlobalClock::new();
        let b = GlobalClock::new();
        for _ in 0..100 {
            a.start_commit().publish();
        }
        let a_completed = a.snapshot();
        let _in_flight_on_a = a.start_commit();
        let on_b = b.start_commit();
        let b_version = on_b.version();
        on_b.publish();
        assert_eq!(b.snapshot(), b_version);
        assert_eq!(a.snapshot(), a_completed);
    }

    #[test]
    fn nested_commits_are_published_with_the_enclosing_operation() {
        let clock = GlobalClock::new();
        clock.start_commit().publish();
        let before = clock.snapshot();
        let op = clock.begin_write();
        let reorganization = clock.start_commit();
        let reorganization_version = reorganization.version();
        reorganization.publish();
        let own = clock.start_commit();
        assert_eq!(own.version(), reorganization_version + 1);
        own.publish();
        assert_eq!(clock.snapshot(), before, "versions of an unfinished operation are visible");
        assert!(clock.retire_stamp() > reorganization_version);
        drop(op);
        assert_eq!(clock.snapshot(), before + 2);
    }

    #[test]
    fn snapshots_are_monotone_under_concurrency() {
        const WRITERS: usize = 6;
        const OPS: usize = 20_000;
        let clock = Arc::new(GlobalClock::new());
        let barrier = Arc::new(Barrier::new(WRITERS + 1));
        let writers: Vec<_> = (0..WRITERS).map(|_| {
            let clock = Arc::clone(&clock);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..OPS {
                    clock.start_commit().publish();
                }
            })
        }).collect();
        barrier.wait();
        let mut last = 0;
        for _ in 0..OPS {
            let s = clock.snapshot();
            assert!(s >= last, "snapshot went backwards: {s} < {last}");
            assert!(s < clock.current_version());
            last = s;
        }
        writers.into_iter().for_each(|w| w.join().unwrap());
        assert_eq!(clock.snapshot(), clock.current_version() - 1);
    }
}
