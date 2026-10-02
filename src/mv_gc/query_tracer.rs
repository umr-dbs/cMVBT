use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use crossbeam_skiplist::SkipSet;
use crate::mv_gc::tracker_handle::TrackerHandle;
use crate::mv_sync::clock::committed_snapshot;
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_tx_model::transaction_result::SnapShot;

/// A live reader: its snapshot plus a registration sequence number, so that concurrent readers
/// sharing a snapshot are tracked individually (multiset). Registration and release are matched
/// by snapshot only, hence they may happen on different threads.
#[derive(Ord, Eq, PartialEq, PartialOrd, Clone)]
pub(crate) struct ReaderQuery(SnapShot, u64);

impl ReaderQuery {
    #[inline]
    const fn snapshot(&self) -> SnapShot {
        self.0
    }
}
type QueryTracer = SkipSet<ReaderQuery>;

// #[derive(Default, Clone)]
// pub struct NullValue;
//
// impl Display for NullValue {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         write!(f, "()")
//     }
// }

/// Readers announced *before* they draw their snapshot. The announced value lower-bounds the
/// snapshot, so GC cannot reclaim anything the reader will need in the window between drawing
/// the snapshot and registering it in the `QueryTracer`.
type PinTracer = SkipSet<(SnapShot, u64)>;

pub(crate) struct TransactionTrace(QueryTracer, PinTracer, AtomicU64);

impl Deref for TransactionTrace {
    type Target = QueryTracer;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TransactionTrace {
    pub(crate) fn new() -> Self {
        Self(QueryTracer::new(), PinTracer::new(), AtomicU64::new(0))
    }

    #[inline(always)]
    pub(crate) fn peek_min(&self) -> Option<SnapShot> {
        let registered = self.front()
            .map(|entry| entry.snapshot());

        let pinned = self.1.front()
            .map(|entry| entry.0);

        match (registered, pinned) {
            (Some(r), Some(p)) => Some(r.min(p)),
            (r, p) => r.or(p)
        }
    }

    /// Announces a reader whose snapshot will be >= `lower_bound`. Returns the ticket for `unpin`.
    #[inline(always)]
    pub(crate) fn pin(&self, lower_bound: SnapShot) -> (SnapShot, u64) {
        let ticket = (lower_bound, self.2.fetch_add(1, Relaxed));
        self.1.insert(ticket);
        ticket
    }

    #[inline(always)]
    pub(crate) fn unpin(&self, ticket: &(SnapShot, u64)) {
        self.1.remove(ticket);
    }

    #[inline(always)]
    pub(crate) fn peek_max(&self) -> Option<SnapShot> {
        if !self.1.is_empty() {
            // A pinned reader is about to draw a snapshot of unknown height.
            return Some(SnapShot::MAX)
        }

        self.back()
            .map(|entry| entry.snapshot())
    }

    #[inline(always)]
    pub(crate) fn on_tx_start(&self, snapshot: SnapShot) {
        self.0.insert(ReaderQuery(snapshot, self.2.fetch_add(1, Relaxed)));
    }

    /// Releases one registration of `snapshot`; a release without registration is a no-op.
    #[inline(always)]
    pub(crate) fn on_tx_completed(&self, snapshot: SnapShot) {
        let mut candidates = self.0
            .range(ReaderQuery(snapshot, 0)..=ReaderQuery(snapshot, u64::MAX));

        while let Some(entry) = candidates.next() {
            if entry.remove() {
                return
            }
        }
    }
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    pub(crate) fn on_acquire_reader_snapshot(&self, snapshot: SnapShot) {
        // if let Some(snapshot) = snapshot {
        // println!("[{:?}] - Enter", thread::current().id());
        self.tracker()
            .inspect(|tracker|
                tracker.on_tx_start(snapshot));
        // }
    }

    #[inline]
    pub(crate) fn on_release_reader_snapshot(&self, snapshot: SnapShot) {
        // if let Some(snapshot) = snapshot {
        // println!("[{:?}] - Exit", thread::current().id());
        self.tracker()
            .inspect(|tracker|
                tracker.on_tx_completed(snapshot));
        // }
    }
}

/// Keeps GC from reclaiming anything a reader drawing its snapshot under this pin may need.
/// Hold it until the query has finished.
pub(crate) struct ReaderPin<const P_F: usize, const P_N: usize, Key: Copy + Default + Hash + Ord + Display + 'static, Payload: Clone + Default + 'static>(
    Option<(TrackerHandle<P_F, P_N, Key, Payload>, (SnapShot, u64))>);

impl<const P_F: usize, const P_N: usize, Key: Copy + Default + Hash + Ord + Display + 'static, Payload: Clone + Default + 'static> Drop for ReaderPin<P_F, P_N, Key, Payload> {
    fn drop(&mut self) {
        if let Some((tracker, ticket)) = self.0.take() {
            tracker.unpin_reader(&ticket)
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// Draws a reader snapshot such that the GC cannot reclaim nodes it needs between drawing
    /// and registration (which the query dispatch performs on its own).
    #[inline]
    pub(crate) fn pinned_reader_snapshot(&self) -> (SnapShot, ReaderPin<FAN_OUT, NUM_RECORDS, Key, Payload>) {
        let pin = self.tracker().map(|tracker| {
            let ticket = tracker.pin_reader(committed_snapshot(self.current_version()));
            (tracker, ticket)
        });

        (self.current_version_for_reader(), ReaderPin(pin))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readers_sharing_a_snapshot_are_tracked_individually() {
        let trace = TransactionTrace::new();
        trace.on_tx_start(7);
        trace.on_tx_start(7);
        trace.on_tx_start(9);

        trace.on_tx_completed(7);
        assert_eq!(trace.peek_min(), Some(7));

        std::thread::scope(|s| { s.spawn(|| trace.on_tx_completed(7)); }); // other thread
        assert_eq!(trace.peek_min(), Some(9));

        trace.on_tx_completed(8); // never registered: no-op
        assert_eq!(trace.peek_min(), Some(9));
    }

    #[test]
    fn pinned_reader_blocks_in_place_updates() {
        let trace = TransactionTrace::new();
        trace.on_tx_start(5);
        assert_eq!(trace.peek_max(), Some(5));

        let ticket = trace.pin(3);
        assert_eq!(trace.peek_max(), Some(SnapShot::MAX));
        assert_eq!(trace.peek_min(), Some(3));

        trace.unpin(&ticket);
        assert_eq!(trace.peek_max(), Some(5));
    }
}
