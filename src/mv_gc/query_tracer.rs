use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use crossbeam_skiplist::SkipSet;
use crate::mv_sync::clock::{__tid, Tid};
use crate::mv_tree::mvtree::MVTreeSt;
use crate::mv_tx_model::transaction_result::SnapShot;

#[derive(Ord, Eq, PartialEq, PartialOrd)]
pub(crate) struct ReaderQuery(SnapShot, Tid);

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
        let _ = self.insert(snapshot.into());
    }

    #[inline(always)]
    pub(crate) fn on_tx_completed(&self, snap_shot: SnapShot) {
        let _ = self.remove(&snap_shot.into());
    }
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> MVTreeSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    pub(crate) fn on_enter_crud_dispatch(&self, snapshot: Option<SnapShot>) {
        if let Some(snapshot) = snapshot {
            self.tracker()
                .inspect(|tracker|
                    tracker.on_tx_start(snapshot));
        }
    }

    #[inline]
    pub(crate) fn on_exit_crud_dispatch(&self, snapshot: Option<SnapShot>) {
        if let Some(snapshot) = snapshot {
            self.tracker()
                .inspect(|tracker|
                    tracker.on_tx_completed(snapshot));
        }
    }
}