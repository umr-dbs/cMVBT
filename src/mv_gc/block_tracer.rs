use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;

use crossbeam_skiplist::SkipMap;
use crate::mv_page_model::BlockRef;
use crate::mv_record_model::version_info::Version;

pub(crate) type DeadPageValue<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload>
= BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>;

type BlockTracerIndex<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload>
= SkipMap<DeadPageKey, DeadPageValue<FAN_OUT, NUM_RECORDS, Key, Payload>>;

pub(crate) type DeadPageKey = Version;

pub(crate) struct BlockTrace<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static>
(BlockTracerIndex<P_F, P_N, Key, Payload>);

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
        Self(SkipMap::new())
    }

    #[inline(always)]
    pub(crate) fn pop_min(&self) -> Option<(DeadPageKey, BlockRef<P_F, P_N, Key, Payload>)> {
        self.pop_front()
            .map(|entry|
                (*entry.key(), entry.value().clone()))
    }

    #[inline(always)]
    pub(crate) fn peek_min(&self) -> Option<Version> {
        self.front()
            .map(|entry| *entry.key())
    }

    #[inline(always)]
    pub(crate) fn register_died_page(&self, page_version: Version, page: DeadPageValue<P_F, P_N, Key, Payload>) {
        let _ = self.insert(page_version, page);
    }

    #[inline(always)]
    pub(crate) fn register_died_page_col(&self, dead_pages: [(Version, BlockRef<P_F, P_N, Key, Payload>); 2]) {
        dead_pages
            .into_iter()
            .for_each(|(d_v, d_p)| self.register_died_page(d_v, d_p))
    }
}