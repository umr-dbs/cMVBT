//! The systems of the paper behind one interface for the experiment drivers.

use std::sync::Arc;

use super::{FileOp, Key};
use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_tree::mvbt::{MVBTSt, FAN_OUT, NUM_RECORDS};
use crate::mv_utils::interval::Interval;

pub const SYSTEMS: &str = "cmvbt|chain|frugal|vweaver|skiplist";

pub trait PaperIndex: Send + Sync {
    /// Applies an operation of the workload file; `false` if it did not take effect (e.g. update of an absent key).
    fn apply(&self, op: FileOp) -> bool;
    /// Like `apply`, but returns the version the operation committed at (`None` if it did not take effect).
    fn apply_versioned(&self, op: FileOp) -> Option<u64>;
    /// Number of records of the freshest visible snapshot, scanned entirely.
    fn scan_fresh(&self) -> usize;
    /// Scans the entire data set of `version`.
    fn scan_at(&self, version: u64) -> usize;
    /// The newest version a scan may ask for.
    fn newest_version(&self) -> u64;
    fn reset_alloc_counts(&self);
    /// (nodes from the memory manager, nodes reused by the GC); (0, 0) for systems without node recycling.
    fn alloc_counts(&self) -> (u64, u64);
}

pub fn make_system(name: &str, root_index: &str, gc: bool) -> Result<Arc<dyn PaperIndex>, String> {
    match name {
        "cmvbt" => {
            let root = crate::ycsb::parse_root_index(root_index).unwrap_or_default();
            let index = MVBTSt::<FAN_OUT, NUM_RECORDS, Key, u64>::make_standard(root);
            if gc {
                index.enable_gc(false);
            }
            Ok(Arc::new(CMvbt(index)))
        }
        #[cfg(feature = "dexa")]
        "chain" | "frugal" | "vweaver" | "skiplist" => Ok(Arc::new(dexa_system::VersionLists::new(name, gc))),
        #[cfg(not(feature = "dexa"))]
        "chain" | "frugal" | "vweaver" | "skiplist" => Err(format!("system '{name}' needs the `dexa` cargo feature")),
        other => Err(format!("unknown system '{other}'")),
    }
}

struct CMvbt(MVBTSt<FAN_OUT, NUM_RECORDS, Key, u64>);

impl PaperIndex for CMvbt {
    fn apply(&self, op: FileOp) -> bool {
        self.apply_versioned(op).is_some()
    }

    fn apply_versioned(&self, op: FileOp) -> Option<u64> {
        let crud = match op {
            FileOp::Insert(k) => CRUDOperation::Insert(k, 0),
            FileOp::Update(k) => CRUDOperation::Update(k, 0),
            FileOp::Delete(k) => CRUDOperation::Delete(k),
        };
        match self.0.dispatch_crud(crud) {
            CRUDOperationResult::Inserted(v) | CRUDOperationResult::Updated(v) | CRUDOperationResult::Deleted(v) => Some(v),
            _ => None,
        }
    }

    fn scan_fresh(&self) -> usize {
        let (snapshot, _pin) = self.0.pinned_reader_snapshot();
        self.scan_at(snapshot)
    }

    fn scan_at(&self, version: u64) -> usize {
        match self.0.dispatch_crud(CRUDOperation::Range(Interval::new(Key::MIN, Key::MAX), version)) {
            CRUDOperationResult::MatchedRecords(found) => found.len(),
            _ => 0,
        }
    }

    fn newest_version(&self) -> u64 {
        self.0.current_version_for_reader()
    }

    fn reset_alloc_counts(&self) {
        self.0.block_manager.reset_alloc_reuse_counts()
    }

    fn alloc_counts(&self) -> (u64, u64) {
        self.0.block_manager.alloc_reuse_counts()
    }
}

#[cfg(feature = "dexa")]
mod dexa_system {
    use super::*;
    use crate::dexa::mvb_crud_model::crud_api::CRUDDispatcher as DexaDispatcher;
    use crate::dexa::mvb_crud_model::crud_operation::CRUDOperation as DexaOp;
    use crate::dexa::mvb_crud_model::crud_operation_result::CRUDOperationResult as DexaResult;
    use crate::dexa::mvb_locking::locking_strategy::LockingStrategy;
    use crate::dexa::mvb_record_model::v_record_point::VersionIndexType;
    use crate::dexa::mvb_tree::bplus_tree::new_INDEX;
    use crate::dexa::mvb_utils::interval::Interval as DexaInterval;
    use crate::dexa::types::INDEX;

    pub struct VersionLists(INDEX);

    impl VersionLists {
        pub fn new(name: &str, gc: bool) -> Self {
            let kind = match name {
                "chain" => VersionIndexType::VANILLA,
                "frugal" => VersionIndexType::FrugalSkipList,
                "vweaver" => VersionIndexType::VWEAVER,
                _ => VersionIndexType::SkipList,
            };
            Self(new_INDEX(LockingStrategy::OLC, kind, gc))
        }
    }

    impl PaperIndex for VersionLists {
        fn apply(&self, op: FileOp) -> bool {
            self.apply_versioned(op).is_some()
        }

        fn apply_versioned(&self, op: FileOp) -> Option<u64> {
            let crud = match op {
                FileOp::Insert(k) => DexaOp::Insert(k, 0),
                FileOp::Update(k) => DexaOp::Update(k, 0),
                FileOp::Delete(k) => DexaOp::Delete(k),
            };
            match self.0.dispatch(crud).1 {
                DexaResult::Inserted(v) | DexaResult::Updated(v) | DexaResult::Deleted(v) => Some(v),
                _ => None,
            }
        }

        fn scan_fresh(&self) -> usize {
            self.scan_at(self.0.current_version_for_reader())
        }

        fn scan_at(&self, version: u64) -> usize {
            match self.0.dispatch(DexaOp::Range(DexaInterval::new(Key::MIN, Key::MAX), version)).1 {
                DexaResult::MatchedRecords(found) => found.len(),
                _ => 0,
            }
        }

        fn newest_version(&self) -> u64 {
            self.0.current_version_for_reader()
        }

        fn reset_alloc_counts(&self) {}

        fn alloc_counts(&self) -> (u64, u64) {
            (0, 0)
        }
    }
}
