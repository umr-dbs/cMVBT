use std::fmt::{Display, Formatter};
use std::hash::Hash;
use crate::record_model::record_point::RecordPoint;
use crate::crud_model::crud_operation_result::CRUDOperationResult::{Deleted, Inserted, MatchedRecords, Updated};
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
pub enum CRUDOperationResult<Key: Ord + Hash + Copy + Default> {
    MatchedRecords(Vec<RecordPoint<Key>>),
    Inserted(Version),
    Updated(Version),
    Deleted(VersionInfo),

    #[default]
    Error, // flatten no good
}

/// Implements pretty printers for TransactionResult.
impl<Key: Display + Ord + Hash + Copy + Default> Display for CRUDOperationResult<Key> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CRUDOperationResult::Error =>
                write!(f, "Error"),
            MatchedRecords(records) => {
                write!(f, "MatchedRecords[len={}\n", records.len());
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
        }
    }
}

/// Sugar implementation, wrapping collection of records to a TransactionResult.
impl<Key: Ord + Hash + Copy + Default> Into<CRUDOperationResult<Key>>
for
Vec<RecordPoint<Key>> {
    fn into(self) -> CRUDOperationResult<Key> {
        MatchedRecords(self)
    }
}