use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

use crossbeam_skiplist::SkipMap;
use crate::mv_page_model::BlockRef;
use crate::mv_record_model::version_info::Version;

pub(crate) type DeadPageValue<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload>
= BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>;

type BlockTracerIndex<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload>
= SkipMap<DeadPageKey, DeadPageValue<FAN_OUT, NUM_RECORDS, Key, Payload>>;

/// Ordered by death version first. The sequence number makes keys unique, since a single
/// reorganization (e.g., a merge) kills several nodes at the very same version and a plain
/// `SkipMap::insert` would silently replace (and thereby leak) the earlier one.
pub(crate) type DeadPageKey = (Version, u64);

pub(crate) struct BlockTrace<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static>
(BlockTracerIndex<P_F, P_N, Key, Payload>, AtomicU64);

impl<const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static> Deref for BlockTrace<P_F, P_N, Key, Payload>
{
    type Target = BlockTracerIndex<P_F, P_N, Key, Payload>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default> BlockTrace<P_F, P_N, Key, Payload>
{
    pub(crate) fn new() -> Self {
        Self(SkipMap::new(), AtomicU64::new(0))
    }

    /// Removes and returns the oldest dead block iff `is_reclaimable(death_version)` holds.
    /// The removal is the claim: exactly one thread wins a given block.
    #[inline(always)]
    pub(crate) fn pop_min_if(&self, is_reclaimable: impl FnOnce(Version) -> bool)
        -> Option<BlockRef<P_F, P_N, Key, Payload>>
    {
        let entry = self.front()?;
        if is_reclaimable(entry.key().0) && entry.remove() {
            Some(entry.value().clone())
        } else {
            None
        }
    }

    #[inline(always)]
    pub(crate) fn register_died_page(&self, death_version: Version, page: DeadPageValue<P_F, P_N, Key, Payload>) {
        self.insert((death_version, self.1.fetch_add(1, Relaxed)), page);
    }

    #[inline(always)]
    pub(crate) fn register_died_page_col(&self, dead_pages: [(Version, BlockRef<P_F, P_N, Key, Payload>); 2]) {
        dead_pages
            .into_iter()
            .for_each(|(d_v, d_p)| self.register_died_page(d_v, d_p))
    }
}
