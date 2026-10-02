//! The systems under test behind one interface. The benchmark drives them identically; the
//! workload invariant `payload == key` lets every read and scan verify what it got.

use std::sync::Arc;

use super::workload::Key;
use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_root::index_root::RootIndexType;
use crate::mv_tree::mvbt::{MVBTSt, FAN_OUT, NUM_RECORDS};
use crate::mv_utils::interval::Interval;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReadOutcome {
    Hit,
    Miss,
    /// Found, but the payload violates the workload invariant.
    Corrupt,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct ScanOutcome {
    pub records: u64,
    pub violations: u64,
}

pub trait YcsbIndex: Send + Sync {
    fn read(&self, key: Key) -> ReadOutcome;
    fn insert(&self, key: Key) -> bool;
    fn update(&self, key: Key) -> bool;
    fn delete(&self, key: Key) -> bool;
    /// Scans the `len` keys `[start, start + len)` of a consistent snapshot.
    fn scan(&self, start: Key, len: u64) -> ScanOutcome;
}

pub const SYSTEMS: &str = "cmvbt|chain|frugal|vweaver|skiplist";

#[derive(Clone, Copy)]
pub struct SystemOptions {
    pub gc: bool,
    pub root_index: RootIndexType,
}

pub fn parse_root_index(name: &str) -> Result<RootIndexType, String> {
    match name {
        "sk" => Ok(RootIndexType::SkipList),
        "ll" => Ok(RootIndexType::LinkedList),
        "fg" => Ok(RootIndexType::FrugalList),
        "bt" => Ok(RootIndexType::BTree),
        other => Err(format!("unknown root* index '{other}' (fg|ll|sk|bt)")),
    }
}

pub fn make_system(name: &str, opts: SystemOptions) -> Result<Arc<dyn YcsbIndex>, String> {
    match name {
        "cmvbt" => {
            let index = MVBTSt::<FAN_OUT, NUM_RECORDS, Key, u64>::make_standard(opts.root_index);
            if opts.gc {
                index.enable_gc(false);
            }
            Ok(Arc::new(CMvbt(index)))
        }
        #[cfg(feature = "dexa")]
        "chain" | "frugal" | "vweaver" | "skiplist" => Ok(Arc::new(dexa_system::VersionLists::new(name, opts.gc))),
        #[cfg(not(feature = "dexa"))]
        "chain" | "frugal" | "vweaver" | "skiplist" =>
            Err(format!("system '{name}' needs the `dexa` cargo feature")),
        other => Err(format!("unknown system '{other}' ({SYSTEMS})")),
    }
}

struct CMvbt(MVBTSt<FAN_OUT, NUM_RECORDS, Key, u64>);

impl YcsbIndex for CMvbt {
    fn read(&self, key: Key) -> ReadOutcome {
        let (snapshot, _pin) = self.0.pinned_reader_snapshot();
        match self.0.dispatch_crud(CRUDOperation::Point(key, snapshot)) {
            CRUDOperationResult::MatchedRecords(found) => match found.first() {
                Some(r) if r.payload == key => ReadOutcome::Hit,
                Some(_) => ReadOutcome::Corrupt,
                None => ReadOutcome::Miss,
            },
            _ => ReadOutcome::Miss,
        }
    }

    fn insert(&self, key: Key) -> bool {
        matches!(self.0.dispatch_crud(CRUDOperation::Insert(key, key)), CRUDOperationResult::Inserted(..))
    }

    fn update(&self, key: Key) -> bool {
        matches!(self.0.dispatch_crud(CRUDOperation::Update(key, key)), CRUDOperationResult::Updated(..))
    }

    fn delete(&self, key: Key) -> bool {
        matches!(self.0.dispatch_crud(CRUDOperation::Delete(key)), CRUDOperationResult::Deleted(..))
    }

    fn scan(&self, start: Key, len: u64) -> ScanOutcome {
        let end = start.saturating_add(len - 1);
        let (snapshot, _pin) = self.0.pinned_reader_snapshot();
        match self.0.dispatch_crud(CRUDOperation::Range(Interval::new(start, end), snapshot)) {
            CRUDOperationResult::MatchedRecords(found) => verify_scan(
                start, end, found.iter().map(|r| (r.key, r.payload))),
            _ => ScanOutcome::default(),
        }
    }
}

fn verify_scan(start: Key, end: Key, records: impl Iterator<Item = (Key, u64)>) -> ScanOutcome {
    // Records of one leaf are not necessarily returned in key order, so only membership of
    // the range and the payload invariant are checked here.
    let mut out = ScanOutcome::default();
    for (key, payload) in records {
        out.records += 1;
        if key < start || key > end || payload != key {
            out.violations += 1;
        }
    }
    out
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

    /// Version Chains, Frugal Lists, vWeaver and skip lists as version-chain index on the
    /// same optimistic-lock-coupling B+-tree.
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

    impl YcsbIndex for VersionLists {
        fn read(&self, key: Key) -> ReadOutcome {
            match self.0.dispatch(DexaOp::Point(key, self.0.current_version_for_reader())) {
                (.., DexaResult::MatchedRecord(Some(r))) if r.payload == key => ReadOutcome::Hit,
                (.., DexaResult::MatchedRecord(Some(_))) => ReadOutcome::Corrupt,
                _ => ReadOutcome::Miss,
            }
        }

        fn insert(&self, key: Key) -> bool {
            matches!(self.0.dispatch(DexaOp::Insert(key, key)), (.., DexaResult::Inserted(..)))
        }

        fn update(&self, key: Key) -> bool {
            matches!(self.0.dispatch(DexaOp::Update(key, key)), (.., DexaResult::Updated(..)))
        }

        fn delete(&self, key: Key) -> bool {
            matches!(self.0.dispatch(DexaOp::Delete(key)), (.., DexaResult::Deleted(..)))
        }

        fn scan(&self, start: Key, len: u64) -> ScanOutcome {
            let end = start.saturating_add(len - 1);
            let snapshot = self.0.current_version_for_reader();
            match self.0.dispatch(DexaOp::Range(DexaInterval::new(start, end), snapshot)) {
                (.., DexaResult::MatchedRecords(found)) => verify_scan(
                    start, end, found.iter().map(|r| (r.key, r.payload))),
                _ => ScanOutcome::default(),
            }
        }
    }
}

#[cfg(test)]
mod model_tests {
    use super::*;
    use crate::ycsb::workload::Rng;
    use std::collections::HashSet;

    /// Single-threaded random ops checked against a plain set. Operations on absent keys must
    /// fail on every system, but a deleted key is never inserted again: the benchmark inserts
    /// fresh keys only (re-inserting after a delete hangs the vWeaver baseline).
    fn check_against_model(name: &str, keys: u64, ops: usize) {
        // The commit clocks are process-global: systems of concurrently running tests would
        // hold back each other's snapshots.
        static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let index = make_system(name, SystemOptions { gc: false, root_index: RootIndexType::FrugalList }).unwrap();
        let mut present = HashSet::new();
        let mut ever_inserted = HashSet::new();
        let mut rng = Rng::new(3);
        let mut history: Vec<(usize, &str, bool)> = vec![];
        for i in 0..ops {
            let key = rng.below(keys);
            let ctx = format!("{name}: op #{i} on key {key}");
            let (what, got, expected) = match rng.below(4) {
                0 if !ever_inserted.insert(key) => continue,
                0 => { present.insert(key); ("insert", index.insert(key), true) }
                1 => ("delete", index.delete(key), present.remove(&key)),
                2 => ("update", index.update(key), present.contains(&key)),
                _ => ("read", index.read(key) == ReadOutcome::Hit, present.contains(&key)),
            };
            history.push((key as usize, what, got));
            assert!(got == expected, "{what} {ctx}: got {got}, expected {expected}; history of key: {:?}",
                    history.iter().filter(|h| h.0 == key as usize).collect::<Vec<_>>());
        }
        let full = index.scan(0, keys);
        assert_eq!((full.records, full.violations), (present.len() as u64, 0), "{name}: final scan");
        (0..keys).for_each(|k| assert_eq!(index.read(k) == ReadOutcome::Hit, present.contains(&k), "{name}: final read {k}"));
    }

    #[test] fn cmvbt_matches_model() { check_against_model("cmvbt", 2000, 400_000) }
    #[test] fn chain_matches_model() { check_against_model("chain", 2000, 400_000) }
    #[test] fn skiplist_matches_model() { check_against_model("skiplist", 2000, 400_000) }
    #[test] fn frugal_matches_model() { check_against_model("frugal", 2000, 400_000) }
    #[test] fn vweaver_matches_model() { check_against_model("vweaver", 2000, 400_000) }
}

#[cfg(test)]
mod scan_tests {
    use super::*;

    /// A scan over a loaded, undisturbed keyspace must return every key of its range.
    #[test]
    fn scans_return_the_whole_range() {
        static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
        for name in ["cmvbt", "chain", "frugal", "vweaver", "skiplist"] {
            let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
            let index = make_system(name, SystemOptions { gc: false, root_index: RootIndexType::FrugalList }).unwrap();
            // large enough for a tree of height 3
            const N: u64 = 300_000;
            (0..N).for_each(|k| assert!(index.insert(k)));
            (0..N).for_each(|k| assert_eq!(index.read(k), ReadOutcome::Hit, "{name}: read {k}"));
            for (start, len) in [(0, 100), (777, 2000), (5000, 10_000), (150_000, 20_000), (N - 1000, 5000)] {
                let want = len.min(N - start);
                let got = index.scan(start, len);
                assert!((got.records, got.violations) == (want, 0),
                        "{name}: scan({start}, {len}) returned {} of {want} records, {} violations", got.records, got.violations);
            }
        }
    }
}


