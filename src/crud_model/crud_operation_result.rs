use std::fmt::{Display, Formatter, write};
use std::hash::Hash;
use crate::record_model::record_point::{RecordPoint, RecordPointResult};
use crate::crud_model::crud_operation_result::CRUDOperationResult::{Deleted, Inserted, MatchedRecordIter, MatchedRecords, Updated};
use crate::crud_model::query::RangeQueryIter;
use crate::record_model::version_info::{Version, VersionInfo};

/// Defines possible Transaction execution result.
/// *Error*, indicates execution error.
/// *Inserted*, indicates that the Transaction executed was successful and the (key, version) pair
/// of matching record is held.
/// *MatchedRecord*, indicates that the Transaction executed was successful and the result of
/// a potential match is held.
/// *MatchedRecords*, indicates that the Transaction executed was successful and the result of
/// matches is held.
#[derive(Default)]
pub enum CRUDOperationResult<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static + Display
> {
    MatchedRecords(Vec<RecordPointResult<Key>>),
    MatchedRecordIter(RangeQueryIter<'a, FAN_OUT, NUM_RECORDS, Key>),
    Inserted(Version),
    Updated(Version),
    Deleted(Version),

    #[default]
    Error, // flatten no good
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static + Display
> CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key> {
    #[inline(always)]
    pub const fn is_err(&self) -> bool {
        match self {
            CRUDOperationResult::Error => true,
            _ => false
        }
    }

    #[inline(always)]
    pub const fn is_ok(&self) -> bool {
        !self.is_err()
    }
}

/// Implements pretty printers for TransactionResult.
impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static + Display
> Display for CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CRUDOperationResult::Error =>
                write!(f, "Error"),
            MatchedRecords(records) => {
                write!(f, "MatchedRecords[len={}]\n", records.len());
                records.iter().for_each(|record| {
                    write!(f, "{}\n", record);
                });
                write!(f, "]")
            }
            Inserted(version) =>
                write!(f, "Inserted(version: {})", version),
            Updated(version) =>
                write!(f, "Updated(version: {})", version),
            Deleted(version) =>
                write!(f, "Deleted(version: {})", version),
            MatchedRecordIter(iter) =>
                write!(f, "RangeQueryIterator(low: {}, high: {}, version: {})",
                       iter.range.lower(),
                       iter.range.upper(),
                       iter.isolated_snapshot.snapshot())
        }
    }
}

/// Sugar implementation, wrapping collection of records to a RecordPointResult.
impl<Key: Ord + Hash + Copy + Default> Into<RecordPointResult<Key>> for RecordPoint<Key> {
    fn into(self) -> RecordPointResult<Key> {
        RecordPointResult::from(&self)
    }
}