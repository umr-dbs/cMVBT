use std::fmt::Display;
use std::hash::Hash;
use crossbeam_skiplist::SkipMap;
use crate::mv_root::tree_root::ValueRootInner;
use crate::mv_tx_model::transaction::SnapShot;

pub struct RootSkipList<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash,
    Payload: Default + Clone>(pub(crate) SkipMap<SnapShot, ValueRootInner<FANOUT, NUM_RECORDS, Key, Payload>>);