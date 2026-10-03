//! The systems under test behind one interface. The benchmark drives them identically; the
//! workload invariant `payload == key` lets every read and scan verify what it got.

use std::sync::Arc;

use super::value::{Blob, Value, ValueKind};
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
    /// Updates the payload to an arbitrary value (the invariant `payload == key` does not hold afterwards, so
    /// scans and reads of such keys are not verified; only used by tests on dedicated indexes).
    fn update_value(&self, key: Key, value: u64) -> bool;
    fn read_value(&self, key: Key) -> Option<u64>;
}

pub const SYSTEMS: &str = "cmvbt|chain|frugal|vweaver|skiplist";

#[derive(Clone, Copy)]
pub struct SystemOptions {
    pub gc: bool,
    pub root_index: RootIndexType,
    pub value: ValueKind,
}

impl SystemOptions {
    pub fn new(gc: bool, value: ValueKind) -> Self {
        Self { gc, root_index: RootIndexType::FrugalList, value }
    }
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
    match opts.value {
        ValueKind::Inline8 => make_typed::<u64>(name, opts),
        ValueKind::Blob1K => make_typed::<Blob>(name, opts),
    }
}

fn make_typed<V: Value>(name: &str, opts: SystemOptions) -> Result<Arc<dyn YcsbIndex>, String> {
    match name {
        "cmvbt" => {
            let index = MVBTSt::<FAN_OUT, NUM_RECORDS, Key, V>::make_standard(opts.root_index);
            if opts.gc {
                index.enable_gc(false);
            }
            Ok(Arc::new(CMvbt(index)))
        }
        #[cfg(feature = "dexa")]
        "chain" | "frugal" | "vweaver" | "skiplist" => Ok(Arc::new(dexa_system::VersionLists::<V>::new(name, opts.gc))),
        #[cfg(not(feature = "dexa"))]
        "chain" | "frugal" | "vweaver" | "skiplist" =>
            Err(format!("system '{name}' needs the `dexa` cargo feature")),
        other => Err(format!("unknown system '{other}' ({SYSTEMS})")),
    }
}

struct CMvbt<V: Value>(MVBTSt<FAN_OUT, NUM_RECORDS, Key, V>);

impl<V: Value> YcsbIndex for CMvbt<V> {
    fn read(&self, key: Key) -> ReadOutcome {
        let (snapshot, _pin) = self.0.pinned_reader_snapshot();
        match self.0.dispatch_crud(CRUDOperation::Point(key, snapshot)) {
            CRUDOperationResult::MatchedRecords(found) => match found.first() {
                Some(r) if r.payload.check(key) => ReadOutcome::Hit,
                Some(_) => ReadOutcome::Corrupt,
                None => ReadOutcome::Miss,
            },
            _ => ReadOutcome::Miss,
        }
    }

    fn insert(&self, key: Key) -> bool {
        matches!(self.0.dispatch_crud(CRUDOperation::Insert(key, V::record(key))), CRUDOperationResult::Inserted(..))
    }

    fn update(&self, key: Key) -> bool {
        matches!(self.0.dispatch_crud(CRUDOperation::Update(key, V::record(key))), CRUDOperationResult::Updated(..))
    }

    fn delete(&self, key: Key) -> bool {
        matches!(self.0.dispatch_crud(CRUDOperation::Delete(key)), CRUDOperationResult::Deleted(..))
    }

    fn update_value(&self, key: Key, value: u64) -> bool {
        matches!(self.0.dispatch_crud(CRUDOperation::Update(key, V::tagged(key, value))), CRUDOperationResult::Updated(..))
    }

    fn read_value(&self, key: Key) -> Option<u64> {
        let (snapshot, _pin) = self.0.pinned_reader_snapshot();
        match self.0.dispatch_crud(CRUDOperation::Point(key, snapshot)) {
            CRUDOperationResult::MatchedRecords(found) => found.first().map(|r| r.payload.tag()),
            _ => None,
        }
    }

    fn scan(&self, start: Key, len: u64) -> ScanOutcome {
        let end = start.saturating_add(len - 1);
        let (snapshot, _pin) = self.0.pinned_reader_snapshot();
        match self.0.dispatch_crud(CRUDOperation::Range(Interval::new(start, end), snapshot)) {
            CRUDOperationResult::MatchedRecords(found) => verify_scan(
                start, end, found.iter().map(|r| (r.key, r.payload.check(r.key)))),
            _ => ScanOutcome::default(),
        }
    }
}

fn verify_scan(start: Key, end: Key, records: impl Iterator<Item = (Key, bool)>) -> ScanOutcome {
    // Records of one leaf are not necessarily returned in key order, so only membership of
    // the range and the payload invariant are checked here.
    let mut out = ScanOutcome::default();
    for (key, payload_ok) in records {
        out.records += 1;
        if key < start || key > end || !payload_ok {
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
    use crate::dexa::mvb_tree::bplus_tree::MVBPlusTree;
    use crate::dexa::mvb_utils::interval::Interval as DexaInterval;
    use crate::dexa::types::{dec_key, inc_key, FAN_OUT as DEXA_FAN_OUT, NUM_RECORDS as DEXA_NUM_RECORDS};

    /// Version Chains, Frugal Lists, vWeaver and skip lists as version-chain index on the
    /// same optimistic-lock-coupling B+-tree.
    pub struct VersionLists<V: Value>(MVBPlusTree<DEXA_FAN_OUT, DEXA_NUM_RECORDS, Key, V>);

    impl<V: Value> VersionLists<V> {
        pub fn new(name: &str, gc: bool) -> Self {
            let kind = match name {
                "chain" => VersionIndexType::VANILLA,
                "frugal" => VersionIndexType::FrugalSkipList,
                "vweaver" => VersionIndexType::VWEAVER,
                _ => VersionIndexType::SkipList,
            };
            Self(MVBPlusTree::new_with(LockingStrategy::OLC, u64::MIN, u64::MAX, inc_key, dec_key, kind, gc))
        }
    }

    impl<V: Value> YcsbIndex for VersionLists<V> {
        fn read(&self, key: Key) -> ReadOutcome {
            match self.0.dispatch(DexaOp::Point(key, self.0.current_version_for_reader())) {
                (.., DexaResult::MatchedRecord(Some(r))) if r.payload.check(key) => ReadOutcome::Hit,
                (.., DexaResult::MatchedRecord(Some(_))) => ReadOutcome::Corrupt,
                _ => ReadOutcome::Miss,
            }
        }

        fn insert(&self, key: Key) -> bool {
            matches!(self.0.dispatch(DexaOp::Insert(key, V::record(key))), (.., DexaResult::Inserted(..)))
        }

        fn update(&self, key: Key) -> bool {
            matches!(self.0.dispatch(DexaOp::Update(key, V::record(key))), (.., DexaResult::Updated(..)))
        }

        fn delete(&self, key: Key) -> bool {
            matches!(self.0.dispatch(DexaOp::Delete(key)), (.., DexaResult::Deleted(..)))
        }

        fn update_value(&self, key: Key, value: u64) -> bool {
            matches!(self.0.dispatch(DexaOp::Update(key, V::tagged(key, value))), (.., DexaResult::Updated(..)))
        }

        fn read_value(&self, key: Key) -> Option<u64> {
            match self.0.dispatch(DexaOp::Point(key, self.0.current_version_for_reader())) {
                (.., DexaResult::MatchedRecord(Some(r))) => Some(r.payload.tag()),
                _ => None,
            }
        }

        fn scan(&self, start: Key, len: u64) -> ScanOutcome {
            let end = start.saturating_add(len - 1);
            let snapshot = self.0.current_version_for_reader();
            match self.0.dispatch(DexaOp::Range(DexaInterval::new(start, end), snapshot)) {
                (.., DexaResult::MatchedRecords(found)) => verify_scan(
                    start, end, found.iter().map(|r| (r.key, r.payload.check(r.key)))),
                _ => ScanOutcome::default(),
            }
        }
    }
}

#[cfg(test)]
mod model_tests {
    use super::*;

    /// GC off/on, 8-byte inline values and 1 KB records behind a pointer.
    pub(super) const KINDS: [(bool, ValueKind); 4] = [
        (false, ValueKind::Inline8), (true, ValueKind::Inline8), (false, ValueKind::Blob1K), (true, ValueKind::Blob1K)];
    use crate::ycsb::workload::Rng;
    use std::collections::HashSet;

    /// Single-threaded random ops checked against a plain set. Operations on absent keys must
    /// fail on every system, but a deleted key is never inserted again: the benchmark inserts
    /// fresh keys only (re-inserting after a delete hangs the vWeaver baseline).
    fn check_against_model(name: &str, keys: u64, ops: usize) {
        // The commit clocks are process-global: systems of concurrently running tests would
        // hold back each other's snapshots.
        let _serial = crate::mv_sync::TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        for (gc, value) in KINDS {
        let index = make_system(name, SystemOptions::new(gc, value)).unwrap();
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
        for (name, value) in ["cmvbt", "chain", "frugal", "vweaver", "skiplist"].into_iter()
            .flat_map(|n| [(n, ValueKind::Inline8), (n, ValueKind::Blob1K)]) {
            let _serial = crate::mv_sync::TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
            let index = make_system(name, SystemOptions::new(false, value)).unwrap();
            // large enough for a tree of height 3
            const N: u64 = 300_000;
            (0..N).for_each(|k| assert!(index.insert(k)));
            (0..N).for_each(|k| assert_eq!(index.read(k), ReadOutcome::Hit, "{name}: read {k}"));
            // up to Key::MAX, like the paper's OLAP queries (must terminate and return everything)
            let all = index.scan(1, u64::MAX);
            assert_eq!((all.records, all.violations), (N - 1, 0), "{name}: scan up to Key::MAX");
            for (start, len) in [(0, 100), (777, 2000), (5000, 10_000), (150_000, 20_000), (N - 1000, 5000)] {
                let want = len.min(N - start);
                let got = index.scan(start, len);
                assert!((got.records, got.violations) == (want, 0),
                        "{name}: scan({start}, {len}) returned {} of {want} records, {} violations", got.records, got.violations);
            }
        }
    }
}




#[cfg(test)]
mod concurrent_model_tests {
    use super::*;
    use crate::ycsb::workload::Rng;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    /// Writers own disjoint key sets (key % THREADS == thread), interleaved in the same leaves, so structure
    /// modifications race while every operation's outcome stays exactly predictable from a per-thread model:
    /// writes act on the newest state, only reads may lag (visible snapshots trail the commits of other threads).
    /// Scanners run alongside and must only ever see consistent data. At the end, with all writers gone, the
    /// whole index must equal the union of the models.
    fn check(name: &str, gc: bool) {
        for value in [ValueKind::Inline8, ValueKind::Blob1K] {
            check_value(name, gc, value)
        }
    }

    fn check_value(name: &str, gc: bool, value: ValueKind) {
        let threads: u64 = std::env::var("CT_THREADS").ok().and_then(|v| v.parse().ok()).unwrap_or(8);
        const KEYS: u64 = 24_000;
        const OPS: usize = 60_000;
        let _serial = crate::mv_sync::TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());

        let index = make_system(name, SystemOptions::new(gc, value)).unwrap();
        let stop = AtomicBool::new(false);
        let scan_violations = std::sync::atomic::AtomicU64::new(0);

        let models: Vec<HashSet<u64>> = std::thread::scope(|s| {
            let n_scanners = std::env::var("CT_SCANNERS").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
            let scanners: Vec<_> = (0..n_scanners).map(|_| s.spawn(|| {
                while !stop.load(Relaxed) {
                    let out = index.scan(0, KEYS);
                    scan_violations.fetch_add(out.violations + (out.records > KEYS) as u64, Relaxed);
                }
            })).collect();

            let writers: Vec<_> = (0..threads).map(|t| {
                let index = &index;
                s.spawn(move || {
                    let mut rng = Rng::new(0xC0FFEE + t);
                    let mut present = HashSet::new();
                    let mut ever = HashSet::new();
                    let mut history: Vec<(u64, &str, bool)> = vec![];
                    for i in 0..OPS {
                        let key = rng.below(KEYS / threads) * threads + t;
                        // delete-heavy phases force merges, insert-heavy ones splits
                        let phase = (i / 5000) % 2;
                        let roll = rng.below(100);
                        let ctx = |what: &str, h: &Vec<(u64, &str, bool)>| format!(
                            "{name} gc={gc} value={value:?}: thread {t} op {i} {what}({key}); history of the key: {:?}",
                            h.iter().filter(|e| e.0 == key).collect::<Vec<_>>());
                        match (roll, phase) {
                            (0..=39, 0) | (0..=14, _) if ever.insert(key) => {
                                let ok = index.insert(key);
                                history.push((key, "insert", ok));
                                assert!(ok, "{}", ctx("insert", &history));
                                present.insert(key);
                            }
                            (40..=64, 0) | (15..=64, 1) => {
                                let got = index.delete(key);
                                history.push((key, "delete", got));
                                assert_eq!(got, present.remove(&key), "{}", ctx("delete", &history));
                            }
                            _ => {
                                let got = index.update(key);
                                history.push((key, "update", got));
                                assert_eq!(got, present.contains(&key), "{}", ctx("update", &history));
                            }
                        }
                    }
                    present
                })
            }).collect();

            let models = writers.into_iter().map(|w| w.join().unwrap()).collect();
            stop.store(true, Relaxed);
            scanners.into_iter().for_each(|h| h.join().unwrap());
            models
        });

        assert_eq!(scan_violations.load(Relaxed), 0, "{name} gc={gc}: a concurrent scan returned inconsistent data");

        let all: HashSet<u64> = models.into_iter().flatten().collect();
        let full = index.scan(0, KEYS);
        assert_eq!((full.records, full.violations), (all.len() as u64, 0), "{name} gc={gc}: final scan");
        for k in 0..KEYS {
            assert_eq!(index.read(k) == ReadOutcome::Hit, all.contains(&k), "{name} gc={gc}: final read {k}");
        }
    }

    #[test] fn cmvbt_concurrent() { check("cmvbt", false) }
    #[test] fn cmvbt_concurrent_gc() { check("cmvbt", true) }
    #[test] fn chain_concurrent() { check("chain", false) }
    #[test] fn chain_concurrent_gc() { check("chain", true) }
    #[test] fn frugal_concurrent() { check("frugal", false) }
    #[test] fn frugal_concurrent_gc() { check("frugal", true) }
    #[test] fn vweaver_concurrent() { check("vweaver", false) }
    #[test] fn vweaver_concurrent_gc() { check("vweaver", true) }
    #[test] fn skiplist_concurrent() { check("skiplist", false) }
    #[test] fn skiplist_concurrent_gc() { check("skiplist", true) }
}
