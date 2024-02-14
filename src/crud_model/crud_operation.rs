use std::fmt::{Display, Formatter};
use std::hash::Hash;
use crate::utils::interval::Interval;
use crate::crud_model::crud_operation::CRUDOperation::{Empty, Delete, Point, Insert, Range, Update, PointSi, RangeSi, RangeIter, RangeIterSi};
use crate::record_model::record_point::Payload;
use crate::record_model::version_info::Version;

pub type TxAtomicOperation<Key> = CRUDOperation<Key>;

/// Transactions definitions.
/// Empty variant indicates an initiation error and/or a default stack allocation.
#[derive(Clone, Default)]
pub enum CRUDOperation<Key: Ord + Copy + Hash + Display> {
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
    RangeIterSi(Interval<Key>)
}

/// Explicitly support move-semantics for Transaction.
unsafe impl<Key: Ord + Copy + Hash + Display> Send for CRUDOperation<Key> {}
unsafe impl<Key: Ord + Copy + Hash + Display> Sync for CRUDOperation<Key> {}
/// Implements Display for Transaction, i.e. pretty printers.
impl<Key: Display + Ord + Copy + Hash> Display for CRUDOperation<Key> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Insert(key, payload) =>
                write!(f, "Insert(Key: {}, Payload: {:?})", key, payload),
            Update(key, payload) =>
                write!(f, "Update(key: {}, payload: {:?})", key, payload),
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
        }
    }
}

/// Main implementation block for Transaction.
impl<Key: Ord + Hash + Copy + Display> CRUDOperation<Key> {
    /// Returns true, only if the Transaction does not require write access when executing.
    /// Returns false, otherwise.
    #[inline(always)]
    pub const fn is_read(&self) -> bool {
        match self {
            Insert(..) | Delete(..) | Update(..) => false,
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
