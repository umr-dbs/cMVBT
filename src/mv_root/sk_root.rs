use std::fmt::Display;
use std::hash::Hash;
use crossbeam_skiplist::SkipMap;
use crate::mv_page_model::Height;
use crate::mv_root::root::Root;
use crate::mv_root::tree_root::ValueRootInner;
use crate::mv_tx_model::transaction_result::SnapShot;

pub struct RootSkipList<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash,
    Payload: Default + Clone>(pub(crate) SkipMap<SnapShot, ValueRootInner<FANOUT, NUM_RECORDS, Key, Payload>>);

impl<const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Default + Clone + Display + Sync + 'static> RootSkipList<FANOUT, NUM_RECORDS, Key, Payload>
{
    pub fn new() -> Self {
        Self(SkipMap::new())
    }

    #[inline(always)]
    pub fn height(&self) -> Height {
        self.0.back().unwrap().value().height()
    }

    #[inline(always)]
    pub fn current_root(&self) -> Root<FANOUT, NUM_RECORDS, Key, Payload> {
        let last
            = self.0.back().unwrap();

        Root::new(last.value().block(), *last.key(), last.value().height())
    }

    #[inline(always)]
    pub fn root_for(&self, si: SnapShot) -> Root<FANOUT, NUM_RECORDS, Key, Payload> {
        let root_si
            = self.0.range(..=si).rev().next().unwrap();

        Root::new(root_si.value().block(), *root_si.key(), root_si.value().height())
    }

    #[inline(always)]
    pub fn append_root(&self, root: Root<FANOUT, NUM_RECORDS, Key, Payload>) -> bool {
        self.0.insert(root.version, ValueRootInner(root.block, root.height));
        true
    }
}