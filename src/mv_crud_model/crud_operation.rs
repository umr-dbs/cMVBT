use std::fmt::{Display, Formatter};
use std::hash::Hash;
use serde::{Deserialize, Serialize};
use crate::mv_utils::interval::Interval;
use crate::mv_crud_model::crud_operation::CRUDOperation::{Empty, Delete, Point, Insert, Range, Update, PointSi, RangeSi, RangeIter, RangeIterSi};
use crate::mv_record_model::version_info::Version;

pub type TxAtomicOperation<Key, Payload> = CRUDOperation<Key, Payload>;

/// Transactions definitions.
/// Empty variant indicates an initiation error and/or a default stack allocation.
#[derive(Clone, Default, Serialize, Deserialize)]
pub enum CRUDOperation<Key: Ord + Copy + Hash + Display, Payload: Clone> {
    #[default]
    Empty,

    // Writers
    Insert(Key, Payload),
    Update(Key, Payload),
    Delete(Key),

    // Readers
    Point(Key, Version),
    PointSi(Key),

    Range(Interval<Key>, Version),
    RangeSi(Interval<Key>),
    RangeIter(Interval<Key>, Version),
    RangeIterSi(Interval<Key>),

    // Rand Writers
    UpdateRand,
    DeleteRand,
    InsertRand
}

/// Explicitly support move-semantics for Transaction.
unsafe impl<Key: Ord + Copy + Hash + Display, Payload: Clone> Send for CRUDOperation<Key, Payload> {}
unsafe impl<Key: Ord + Copy + Hash + Display, Payload: Clone> Sync for CRUDOperation<Key, Payload> {}
/// Implements Display for Transaction, i.e. pretty printers.
impl<Key: Display + Ord + Copy + Hash, Payload: Clone> Display for CRUDOperation<Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Insert(key, payload) =>
                write!(f, "Insert(Key: {})", key),
            Update(key, payload) =>
                write!(f, "Update(key: {})", key),
            Delete(key) =>
                write!(f, "Delete(Key: {})", key),
            Point(key, version) =>
                write!(f, "Point(Key: {}, version: {})", key, version),
            PointSi(key) =>
                write!(f, "Point(Key: {}, version: Si)", key),
            Range(key, version) =>
                write!(f, "Range(Keys: [{}, {}], version: {})", key.lower(), key.upper(), version),
            RangeSi(key) =>
                write!(f, "Range(Keys: [{}, {}], version: Si)", key.lower(), key.upper()),
            RangeIter(key, version) =>
                write!(f, "Range(Keys: [{}, {}], version: {})", key.lower(), key.upper(), version),
            RangeIterSi(key) =>
                write!(f, "RangeIterSi(Keys: [{}, {}], version: Si)", key.lower(), key.upper()),
            Empty => write!(f, "Empty"),
            CRUDOperation::UpdateRand =>
                write!(f, "UpdateRand"),
            CRUDOperation::DeleteRand =>
                write!(f, "DeleteRand"),
            CRUDOperation::InsertRand =>
                write!(f, "InsertRand"),
        }
    }
}

/// Main implementation mv_block for Transaction.
impl<Key: Ord + Hash + Copy + Display, Payload: Clone> CRUDOperation<Key, Payload> {
    /// Returns true, only if the Transaction does not require write access when executing.
    /// Returns false, otherwise.
    #[inline(always)]
    pub const fn is_read(&self) -> bool {
        match self {
            Insert(..) | Delete(..) | Update(..) |
            CRUDOperation::UpdateRand |
            CRUDOperation::DeleteRand => false,
            _ => true,
        }
    }

    /// Returns true, only if the Transaction requires write access when executing.
    /// Returns false, otherwise.
    #[inline(always)]
    pub const fn is_write(&self) -> bool {
        !self.is_read()
    }
}
