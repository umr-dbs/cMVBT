use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use crossbeam_skiplist::SkipSet;
use crate::mv_sync::clock::{__tid, Tid};
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_tx_model::transaction_result::SnapShot;

#[derive(Ord, Eq, PartialEq, PartialOrd, Clone)]
pub(crate) struct ReaderQuery(SnapShot, Tid);

// impl PartialOrd for ReaderQuery {
//     fn partial_cmp(&self, other: &ReaderQuery) -> Option<Ordering> {
//         Some(self.0.cmp(&other.0))
//     }
// }
//
// impl Ord for ReaderQuery {
//     fn cmp(&self, other: &Self) -> Ordering {
//         self.0.cmp(&other.0)
//     }
// }

impl Into<ReaderQuery> for SnapShot {
    fn into(self) -> ReaderQuery {
        ReaderQuery::new(self)
    }
}

impl ReaderQuery {
    #[inline]
    fn new(version: SnapShot) -> ReaderQuery {
        Self(version, __tid())
    }

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

pub(crate) struct TransactionTrace(QueryTracer);

impl Deref for TransactionTrace {
    type Target = QueryTracer;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TransactionTrace {
    pub(crate) fn new() -> Self {
        Self(QueryTracer::new())
    }

    #[inline(always)]
    pub(crate) fn peek_min(&self) -> Option<SnapShot> {
        self.front()
            .map(|entry| entry.snapshot())
    }

    #[inline(always)]
    pub(crate) fn peek_max(&self) -> Option<SnapShot> {
        self.back()
            .map(|entry| entry.snapshot())
    }

    #[inline(always)]
    pub(crate) fn on_tx_start(&self, snapshot: SnapShot) {
        let reader_query: ReaderQuery = snapshot.into();
        let res = self.insert(reader_query.clone());
        // println!("[{:?}] - Inserted ReaderQuery: (v: {}, tid: {})",
        //          thread::current().id(),
        //          res.0, res.1);
    }

    #[inline(always)]
    pub(crate) fn on_tx_completed(&self, snap_shot: SnapShot) {
        let reader_query = snap_shot.into();
        if let None = self.remove(&reader_query) {
            // println!("[{:?}] - Failed Reader Snapshot Removal of: (v: {}, tid: {}) was not found",
            //          thread::current().id(),
            //          reader_query.0,
            //          reader_query.1);
        }
        else {
            // println!("[{:?}] - Successful Reader Snapshot Removal of: (v: {}, tid: {}).",
            //          thread::current().id(),
            //          reader_query.0,
            //          reader_query.1);
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