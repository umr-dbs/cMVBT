use std::fmt::{Display, Formatter};
use std::ops::Deref;

use crossbeam_skiplist::SkipSet;

use crate::mv_tx_model::transaction_result::SnapShot;

type QueryTracer = SkipSet<SnapShot>;

#[derive(Default, Clone)]
pub struct NullValue;

impl Display for NullValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "()")
    }
}

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
    pub(crate) fn peek_min(&self) -> SnapShot {
        self.front()
            .map_or(SnapShot::MAX, |entry| *entry)
    }

    #[inline(always)]
    pub(crate) fn peek_max(&self) -> SnapShot {
        self.back()
            .map_or(SnapShot::MIN, |entry| *entry)
    }

    #[inline(always)]
    pub(crate) fn on_tx_start(&self, snapshot: SnapShot) -> bool {
        let _ = self.insert(snapshot);
        true
    }

    #[inline(always)]
    pub(crate) fn on_tx_completed(&self, snap_shot: SnapShot) {
        let _ = self.remove(&snap_shot);
    }
}