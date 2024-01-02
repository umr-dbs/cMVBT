use std::fmt::{Display, Formatter};
use std::hash::Hash;
use crate::utils::interval::Interval;
use crate::crud_model::crud_operation::CRUDOperation::{Empty, Delete, Point, Insert, Range, Update};
use crate::record_model::record_point::Payload;

/// Transactions definitions.
/// Empty variant indicates an initiation error and/or a default stack allocation.
#[derive(Clone, Default)]
pub enum CRUDOperation<Key: Ord + Copy + Hash> {
    #[default]
    Empty,

    Insert(Key, Payload),
    Update(Key, Payload),
    Delete(Key),
    Point(Key),
    Range(Interval<Key>),
}

/// Explicitly support move-semantics for Transaction.
unsafe impl<Key: Ord + Copy + Hash> Send for CRUDOperation<Key> {}
unsafe impl<Key: Ord + Copy + Hash> Sync for CRUDOperation<Key> {}
/// Implements Display for Transaction, i.e. pretty printers.
impl<Key: Display + Ord + Copy + Hash> Display for CRUDOperation<Key> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Insert(key, payload) =>
                write!(f, "Insert(Key: {}, Payload: {})", key, payload),
            Update(key, payload) =>
                write!(f, "Update(key: {}, payload: {})", key, payload),
            Delete(key) =>
                write!(f, "Delete(Key: {})", key),
            Point(key) =>
                write!(f, "Point(Key: {})", key),
            Range(key) =>
                write!(f, "Range(Keys: [{}, {}])", key.lower(), key.upper()),
            Empty => write!(f, "Empty"),
        }
    }
}

/// Main implementation block for Transaction.
impl<Key: Ord + Hash + Copy> CRUDOperation<Key> {
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
