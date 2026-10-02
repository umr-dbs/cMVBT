//! Shared type/constant configuration of the version-list B+-tree variants (formerly `n_test`).
use crate::dexa::mvb_record_model::Version;
use crate::dexa::mvb_tree::bplus_tree::MVBPlusTree;

pub type Payload = u64;
pub type Key = u64;

pub type SnapShot = Version;
pub const FAN_OUT: usize = 255;
pub const NUM_RECORDS: usize = 170;

pub type INDEX = MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

pub fn inc_key(k: Key) -> Key {
    k.checked_add(1).unwrap_or(Key::MAX)
}

pub fn dec_key(k: Key) -> Key {
    k.checked_sub(1).unwrap_or(Key::MIN)
}
