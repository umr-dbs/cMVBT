//! Commit clock and reader registry of the baselines.
//!
//! The baselines use the same commit clock and snapshot publication protocol as the cMVBT
//! (`crate::mv_sync::clock`), so all systems hand out reader snapshots with identical guarantees.
//!
//! The baselines' garbage collection removes deleted keys from leaves during splits. A key whose
//! newest version is a deletion at version `d` is still visible to every reader with a snapshot
//! `< d`, so it may only be removed when `d <= horizon`, where the horizon lower-bounds the
//! snapshot of every current and future reader. Readers announce themselves in a per-tree
//! registry ([`MVBPlusTree::pin_snapshot`]); the horizon is the minimum of the newest snapshot and
//! all announced snapshots.

use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering::{Relaxed, SeqCst}};
use crate::dexa::mvb_record_model::Version;
use crate::dexa::mvb_tree::bplus_tree::MVBPlusTree;
use crate::mv_sync::clock::{__tid, Tid};
pub(crate) use crate::mv_sync::clock::{release_thread_commit, CommitTicket, GlobalClock};

const MAX_THREADS: usize = 4096;
const UNPINNED: Version = Version::MAX;

#[repr(align(64))]
struct PaddedAtomicVersion(AtomicU64);

pub(crate) struct ReaderRegistry {
    /// Snapshot announced by each thread, or `UNPINNED`.
    slots: Box<[PaddedAtomicVersion]>,
    /// Largest horizon the GC has used so far; snapshots below it may have lost deleted keys.
    collected: AtomicU64,
}

impl ReaderRegistry {
    pub(crate) fn new() -> Self {
        Self {
            slots: (0..MAX_THREADS).map(|_| PaddedAtomicVersion(AtomicU64::new(UNPINNED))).collect(),
            collected: AtomicU64::new(0),
        }
    }

    /// Announces `snapshot` for the calling thread; returns the value to restore on unpin.
    #[inline]
    fn announce(&self, tid: Tid, snapshot: Version) -> Version {
        let slot = &self.slots[tid].0;
        let previous = slot.load(Relaxed); // only the owning thread writes its slot
        slot.store(previous.min(snapshot), SeqCst);
        previous
    }

    #[inline]
    fn restore(&self, tid: Tid, previous: Version) {
        self.slots[tid].0.store(previous, SeqCst);
    }
}

/// A reader snapshot registered with the baselines' garbage collection. Deleted keys it can see
/// stay in the tree while it is held.
pub struct SnapshotPin<'a> {
    registry: &'a ReaderRegistry,
    tid: Tid,
    previous: Version,
    version: Version,
    /// The pin refers to the slot of the creating thread.
    _not_send: PhantomData<*const ()>,
}

impl SnapshotPin<'_> {
    #[inline(always)]
    pub fn version(&self) -> Version {
        self.version
    }
}

impl Drop for SnapshotPin<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        self.registry.restore(self.tid, self.previous)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Send + Clone + Default + Sync + 'static
> MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {
    /// The newest snapshot all of whose writes have taken effect. Not protected against garbage
    /// collection; a reader that scans with GC enabled should use [`Self::pin_snapshot`].
    #[inline(always)]
    pub fn current_version_for_reader(&self) -> Version {
        self.global_clock.snapshot()
    }

    /// Draws the newest snapshot and protects it against garbage collection while the pin lives.
    #[inline]
    pub fn pin_snapshot(&self) -> SnapshotPin<'_> {
        let tid = __tid();
        // Announce a lower bound first, then draw the snapshot: a collector that misses the
        // announcement read its horizon before it, hence a horizon <= the drawn snapshot.
        let lower_bound = self.global_clock.snapshot();
        let previous = self.readers.announce(tid, lower_bound);
        let version = self.global_clock.snapshot();
        SnapshotPin { registry: &self.readers, tid, previous, version, _not_send: PhantomData }
    }

    /// Protects an explicit, possibly historical snapshot. Panics if garbage collection may
    /// already have removed keys deleted after `version`.
    #[inline]
    pub fn pin_snapshot_at(&self, version: Version) -> SnapshotPin<'_> {
        let tid = __tid();
        let previous = self.readers.announce(tid, version);
        // A collector publishes its horizon before scanning the announcements: either it sees
        // ours, or we see its horizon here.
        let collected = self.readers.collected.load(SeqCst);
        let pin = SnapshotPin { registry: &self.readers, tid, previous, version, _not_send: PhantomData };
        assert!(version >= collected,
                "snapshot {version} is older than the garbage-collection horizon {collected}");
        pin
    }

    /// The version up to which deleted keys can be removed: no current or future reader holds an
    /// older snapshot. Publishes the bound before reading the announcements (see `pin_snapshot_at`).
    #[inline]
    pub(crate) fn gc_horizon(&self) -> Version {
        let newest = self.global_clock.snapshot();
        self.readers.collected.fetch_max(newest, SeqCst);
        self.readers.slots
            .iter()
            .take(MAX_THREADS)
            .fold(newest, |horizon, slot| horizon.min(slot.0.load(SeqCst)))
    }

    #[inline(always)]
    pub(crate) fn start_tx_commit(&self) -> CommitTicket<'_> {
        self.global_clock.start_commit()
    }
}

#[cfg(test)]
mod tests {
    use crate::dexa::mvb_crud_model::crud_api::CRUDDispatcher;
    use crate::dexa::mvb_crud_model::crud_operation::CRUDOperation;
    use crate::dexa::mvb_crud_model::crud_operation_result::CRUDOperationResult;
    use crate::dexa::mvb_record_model::v_record_point::VersionIndexType;
    use crate::dexa::mvb_tree::bplus_tree::new_INDEX;
    use crate::dexa::mvb_utils::interval::Interval;
    use crate::dexa::types::{INDEX, NUM_RECORDS};
    use crate::dexa::mvb_locking::locking_strategy::LockingStrategy;

    type Tree = INDEX;

    fn new_tree(index: VersionIndexType, gc: bool) -> Tree {
        new_INDEX(LockingStrategy::OLC, index, gc)
    }

    fn visible(tree: &Tree, version: u64) -> usize {
        match tree.dispatch(CRUDOperation::Range(Interval::new(u64::MIN, u64::MAX), version)).1 {
            CRUDOperationResult::MatchedRecords(records) => records.len(),
            _ => panic!("range scan failed"),
        }
    }

    /// EDBT'27 review, R3-D2: a reader holding an older snapshot must keep seeing a key that is
    /// deleted afterwards, also when the GC filters deleted keys during a leaf split.
    #[test]
    fn leaf_split_keeps_deleted_keys_of_pinned_readers() {
        for index in [VersionIndexType::VANILLA, VersionIndexType::FrugalSkipList, VersionIndexType::VWEAVER] {
            for gc in [false, true] {
                let tree = new_tree(index, gc);
                let keys = NUM_RECORDS as u64 - 1;
                for k in 1..=keys {
                    tree.dispatch(CRUDOperation::Insert(k, k as _));
                }
                let pin = tree.pin_snapshot();
                tree.dispatch(CRUDOperation::Delete(keys / 2));
                for k in keys + 1..=keys + 3 * NUM_RECORDS as u64 { // forces splits
                    tree.dispatch(CRUDOperation::Insert(k, k as _));
                }
                assert_eq!(visible(&tree, pin.version()), keys as usize,
                           "{} gc={gc}: pinned reader lost a record", index as u8);
                drop(pin);

                // Without readers, the GC may reclaim the deleted key; the newest snapshot is unaffected.
                for k in keys + 3 * NUM_RECORDS as u64 + 1..=keys + 6 * NUM_RECORDS as u64 {
                    tree.dispatch(CRUDOperation::Insert(k, k as _));
                }
                let newest = tree.current_version_for_reader();
                assert_eq!(visible(&tree, newest), (keys + 6 * NUM_RECORDS as u64 - 1) as usize);
            }
        }
    }

    #[test]
    #[should_panic(expected = "garbage-collection horizon")]
    fn pinning_a_collected_snapshot_is_refused() {
        let tree = new_tree(VersionIndexType::VANILLA, true);
        tree.dispatch(CRUDOperation::Insert(1, Default::default()));
        let old = tree.current_version_for_reader();
        tree.dispatch(CRUDOperation::Insert(2, Default::default()));
        let _ = tree.gc_horizon();
        let _pin = tree.pin_snapshot_at(old);
    }
}
