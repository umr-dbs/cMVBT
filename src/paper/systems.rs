//! The systems of the paper behind one interface for the experiment drivers.

use std::sync::Arc;

use super::{FileOp, Key};
use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_tree::mvbt::{MVBTSt, FAN_OUT, NUM_RECORDS};
use crate::mv_utils::interval::Interval;

pub const SYSTEMS: &str = "cmvbt|chain|frugal|vweaver|skiplist|mdbx";

pub trait PaperIndex: Send + Sync {
    /// Lets engines with efficient bulk ingestion replace the generic one-transaction-per-key load.
    fn bulk_load(&self, _records: u64) -> Option<Result<(), String>> {
        None
    }
    /// Pins the snapshot used by a post-workload historical scan, if the engine
    /// cannot reopen arbitrary versions later (libmdbx/CoW).
    fn prepare_historical_snapshot(&self) -> Result<(), String> {
        Ok(())
    }
    fn historical_scan_mode(&self) -> &'static str {
        "historical"
    }
    /// Applies an operation of the workload file; `false` if it did not take effect (e.g. update of an absent key).
    fn apply(&self, op: FileOp) -> bool;
    /// Like `apply`, but returns the version the operation committed at (`None` if it did not take effect).
    fn apply_versioned(&self, op: FileOp) -> Option<u64>;
    /// Number of records of the freshest visible snapshot, scanned entirely.
    fn scan_fresh(&self) -> usize;
    /// Scans the entire data set of `version`.
    fn scan_at(&self, version: u64) -> usize;
    /// Scans at most `len` consecutive keys of the freshest visible snapshot.
    fn scan_fresh_range(&self, start: Key, len: u64) -> usize;
    /// Scans at most `len` consecutive keys at `version`.
    fn scan_at_range(&self, start: Key, len: u64, version: u64) -> usize;
    /// The newest version a scan may ask for.
    fn newest_version(&self) -> u64;
    /// Releases this worker's final committed version before its result is joined.
    fn finish_thread(&self) {}
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
        "chain" | "frugal" | "vweaver" | "skiplist" => {
            Ok(Arc::new(dexa_system::VersionLists::new(name, gc)))
        }
        #[cfg(not(feature = "dexa"))]
        "chain" | "frugal" | "vweaver" | "skiplist" => {
            Err(format!("system '{name}' needs the `dexa` cargo feature"))
        }
        #[cfg(feature = "mdbx")]
        "mdbx" => Ok(Arc::new(mdbx_system::Mdbx::new()?)),
        #[cfg(not(feature = "mdbx"))]
        "mdbx" => Err("system 'mdbx' needs the `mdbx` cargo feature".into()),
        other => Err(format!("unknown system '{other}'")),
    }
}

struct CMvbt(MVBTSt<FAN_OUT, NUM_RECORDS, Key, u64>);

impl PaperIndex for CMvbt {
    fn finish_thread(&self) {
        crate::mv_sync::clock::release_thread_commit();
    }

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
            CRUDOperationResult::Inserted(v)
            | CRUDOperationResult::Updated(v)
            | CRUDOperationResult::Deleted(v) => Some(v),
            _ => None,
        }
    }

    fn scan_fresh(&self) -> usize {
        let (snapshot, _pin) = self.0.pinned_reader_snapshot();
        self.scan_at(snapshot)
    }

    fn scan_at(&self, version: u64) -> usize {
        match self.0.dispatch_crud(CRUDOperation::Range(
            Interval::new(Key::MIN, Key::MAX),
            version,
        )) {
            CRUDOperationResult::MatchedRecords(found) => found.len(),
            _ => 0,
        }
    }

    fn scan_fresh_range(&self, start: Key, len: u64) -> usize {
        let (snapshot, _pin) = self.0.pinned_reader_snapshot();
        self.scan_at_range(start, len, snapshot)
    }

    fn scan_at_range(&self, start: Key, len: u64, version: u64) -> usize {
        let end = start.saturating_add(len.saturating_sub(1));
        match self
            .0
            .dispatch_crud(CRUDOperation::Range(Interval::new(start, end), version))
        {
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

#[cfg(feature = "mdbx")]
mod mdbx_system {
    use super::*;
    use libmdbx::{
        Database, DatabaseOptions, Mode, ReadWriteOptions, SyncMode, TableFlags, Transaction,
        WriteFlags, WriteMap, RO,
    };
    use std::borrow::Cow;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    use std::sync::Mutex;

    static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);
    const LOAD_BATCH_SIZE: u64 = 10_000;

    /// libmdbx is a single-writer copy-on-write B+Tree. The pinned transaction
    /// is used only for Figure 5: it is opened after loading and kept alive
    /// while the measured writes create newer database snapshots.
    pub struct Mdbx {
        // This field must be cleared before `db` is dropped (see Drop).
        pinned: Mutex<Option<Transaction<'static, RO, WriteMap>>>,
        db: Option<Box<Database<WriteMap>>>,
        path: PathBuf,
    }

    impl Mdbx {
        pub fn new() -> Result<Self, String> {
            let base = std::env::var_os("MDBX_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(std::env::temp_dir);
            let path = base.join(format!(
                "cmvbt-paper-mdbx-{}-{}",
                std::process::id(),
                NEXT_DATABASE.fetch_add(1, Relaxed),
            ));
            if path.exists() {
                std::fs::remove_dir_all(&path).map_err(|e| {
                    format!("remove stale libmdbx directory {}: {e}", path.display())
                })?;
            }
            std::fs::create_dir_all(&path)
                .map_err(|e| format!("create libmdbx directory {}: {e}", path.display()))?;
            let options = DatabaseOptions {
                max_readers: Some(128),
                mode: Mode::ReadWrite(ReadWriteOptions {
                    sync_mode: SyncMode::UtterlyNoSync,
                    max_size: isize::try_from(1_u64 << 40).ok(),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let db = Box::new(
                Database::<WriteMap>::open_with_options(&path, options)
                    .map_err(|e| format!("open libmdbx database {}: {e}", path.display()))?,
            );
            let txn = db
                .begin_rw_txn()
                .map_err(|e| format!("libmdbx create transaction: {e}"))?;
            txn.create_table(None, TableFlags::empty())
                .map_err(|e| format!("libmdbx create table: {e}"))?;
            txn.commit()
                .map_err(|e| format!("libmdbx commit table: {e}"))?;
            Ok(Self {
                pinned: Mutex::new(None),
                db: Some(db),
                path,
            })
        }

        fn db(&self) -> &Database<WriteMap> {
            self.db
                .as_deref()
                .expect("libmdbx database already dropped")
        }

        fn scan_txn(txn: &Transaction<'_, RO, WriteMap>, start: Key, len: u64) -> usize {
            let table = txn.open_table(None).expect("libmdbx open table for scan");
            let mut cursor = txn.cursor(&table).expect("libmdbx cursor");
            let end = if start == Key::MIN && len == u64::MAX {
                Key::MAX
            } else {
                start.saturating_add(len.saturating_sub(1))
            };
            let mut item = cursor
                .set_range::<Cow<'_, [u8]>, ()>(&start.to_be_bytes())
                .expect("libmdbx cursor set_range");
            let mut count = 0;
            while let Some((key, ())) = item {
                if key.len() != 8 {
                    panic!("libmdbx paper key has {} bytes instead of 8", key.len());
                }
                let key = u64::from_be_bytes(key.as_ref().try_into().expect("checked key length"));
                if key > end {
                    break;
                }
                count += 1;
                if count as u64 >= len {
                    break;
                }
                item = cursor
                    .next::<Cow<'_, [u8]>, ()>()
                    .expect("libmdbx cursor next");
            }
            count
        }

        fn fresh_scan(&self, start: Key, len: u64) -> usize {
            let txn = self
                .db()
                .begin_ro_txn()
                .expect("libmdbx begin read transaction");
            Self::scan_txn(&txn, start, len)
        }
    }

    impl Drop for Mdbx {
        fn drop(&mut self) {
            self.pinned
                .get_mut()
                .expect("libmdbx pinned mutex poisoned")
                .take();
            self.db.take();
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    impl PaperIndex for Mdbx {
        fn bulk_load(&self, records: u64) -> Option<Result<(), String>> {
            let result = (|| {
                let mut first = 0;
                while first < records {
                    let end = (first + LOAD_BATCH_SIZE).min(records);
                    let txn = self
                        .db()
                        .begin_rw_txn()
                        .map_err(|e| format!("libmdbx begin load transaction: {e}"))?;
                    let table = txn
                        .open_table(None)
                        .map_err(|e| format!("libmdbx open load table: {e}"))?;
                    for key in first..end {
                        txn.put(
                            &table,
                            key.to_be_bytes(),
                            0_u64.to_be_bytes(),
                            WriteFlags::NO_OVERWRITE,
                        )
                        .map_err(|e| format!("libmdbx initial insert({key}): {e}"))?;
                    }
                    txn.commit()
                        .map_err(|e| format!("libmdbx commit load batch: {e}"))?;
                    first = end;
                }
                Ok(())
            })();
            Some(result)
        }

        fn prepare_historical_snapshot(&self) -> Result<(), String> {
            let txn = self
                .db()
                .begin_ro_txn()
                .map_err(|e| format!("libmdbx pin initial read transaction: {e}"))?;
            // Mdbx owns the boxed Database at a stable address and Drop clears
            // `pinned` before dropping it. Extend the borrow to encode that
            // self-referential ownership invariant.
            let txn: Transaction<'static, RO, WriteMap> = unsafe { std::mem::transmute(txn) };
            *self
                .pinned
                .lock()
                .map_err(|_| "libmdbx pinned transaction mutex poisoned")? = Some(txn);
            Ok(())
        }

        fn historical_scan_mode(&self) -> &'static str {
            "pinned_initial"
        }

        fn apply(&self, op: FileOp) -> bool {
            self.apply_versioned(op).is_some()
        }

        fn apply_versioned(&self, op: FileOp) -> Option<u64> {
            let txn = self
                .db()
                .begin_rw_txn()
                .expect("libmdbx begin write transaction");
            let version = txn.id();
            let table = txn.open_table(None).expect("libmdbx open write table");
            let key = match op {
                FileOp::Insert(key) | FileOp::Update(key) | FileOp::Delete(key) => key,
            };
            let key_bytes = key.to_be_bytes();
            let exists = txn
                .get::<()>(&table, &key_bytes)
                .expect("libmdbx lookup")
                .is_some();
            let applied = match op {
                FileOp::Insert(_) if !exists => {
                    txn.put(
                        &table,
                        key_bytes,
                        0_u64.to_be_bytes(),
                        WriteFlags::NO_OVERWRITE,
                    )
                    .expect("libmdbx insert");
                    true
                }
                FileOp::Update(_) if exists => {
                    txn.put(&table, key_bytes, version.to_be_bytes(), WriteFlags::UPSERT)
                        .expect("libmdbx update");
                    true
                }
                FileOp::Delete(_) if exists => {
                    txn.del(&table, key_bytes, None).expect("libmdbx delete")
                }
                _ => false,
            };
            if applied {
                txn.commit().expect("libmdbx commit write transaction");
                Some(version)
            } else {
                None
            }
        }

        fn scan_fresh(&self) -> usize {
            self.fresh_scan(Key::MIN, u64::MAX)
        }

        fn scan_at(&self, _version: u64) -> usize {
            let pinned = self
                .pinned
                .lock()
                .expect("libmdbx pinned transaction mutex poisoned");
            match pinned.as_ref() {
                Some(txn) => Self::scan_txn(txn, Key::MIN, u64::MAX),
                None => self.fresh_scan(Key::MIN, u64::MAX),
            }
        }

        fn scan_fresh_range(&self, start: Key, len: u64) -> usize {
            self.fresh_scan(start, len)
        }

        fn scan_at_range(&self, start: Key, len: u64, _version: u64) -> usize {
            let pinned = self
                .pinned
                .lock()
                .expect("libmdbx pinned transaction mutex poisoned");
            match pinned.as_ref() {
                Some(txn) => Self::scan_txn(txn, start, len),
                None => self.fresh_scan(start, len),
            }
        }

        fn newest_version(&self) -> u64 {
            self.db()
                .begin_ro_txn()
                .expect("libmdbx begin version transaction")
                .id()
        }

        fn reset_alloc_counts(&self) {}

        fn alloc_counts(&self) -> (u64, u64) {
            (0, 0)
        }
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
        fn finish_thread(&self) {
            crate::dexa::mvb_tree::clock::release_thread_commit();
        }

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
                DexaResult::Inserted(v) | DexaResult::Updated(v) | DexaResult::Deleted(v) => {
                    Some(v)
                }
                _ => None,
            }
        }

        fn scan_fresh(&self) -> usize {
            self.scan_at(self.0.current_version_for_reader())
        }

        fn scan_at(&self, version: u64) -> usize {
            match self
                .0
                .dispatch(DexaOp::Range(
                    DexaInterval::new(Key::MIN, Key::MAX),
                    version,
                ))
                .1
            {
                DexaResult::MatchedRecords(found) => found.len(),
                _ => 0,
            }
        }

        fn scan_fresh_range(&self, start: Key, len: u64) -> usize {
            self.scan_at_range(start, len, self.0.current_version_for_reader())
        }

        fn scan_at_range(&self, start: Key, len: u64, version: u64) -> usize {
            let end = start.saturating_add(len.saturating_sub(1));
            match self
                .0
                .dispatch(DexaOp::Range(DexaInterval::new(start, end), version))
                .1
            {
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
