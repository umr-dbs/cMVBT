use std::ops::Deref;
use crossbeam_skiplist::SkipSet;
use crate::mv_sync::clock::{__tid, Tid};
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
    pub(crate) fn on_tx_start(&self, snapshot: SnapShot) -> bool {
        let _ = self.insert(snapshot.into());
        true
    }

    #[inline(always)]
    pub(crate) fn on_tx_completed(&self, snap_shot: SnapShot) {
        let _ = self.remove(&snap_shot.into());
    }
}