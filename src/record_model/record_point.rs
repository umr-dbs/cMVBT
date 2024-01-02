use std::hash::Hash;
use std::mem;
use std::fmt::{Display, Formatter};
use std::ptr::{addr_of, addr_of_mut};
use crate::record_model::unsafe_clone::UnsafeClone;
use crate::record_model::version_info::VersionInfo;

pub type Payload = Box<u8>;

#[derive(Default)]
#[repr(packed)]
pub struct RecordPoint<Key: Ord + Copy + Hash + Default> {
    pub key: Key,
    pub version: VersionInfo,
    pub payload: Box<u8>
}

impl<Key: Ord + Copy + Hash + Default> RecordPoint<Key> {
    #[inline(always)]
    pub const fn new(key: Key, version: VersionInfo, payload: Payload) -> Self {
        Self {
            key,
            version,
            payload
        }
    }

    #[inline(always)]
    pub const fn key(&self) -> Key {
        unsafe {
            *addr_of!(self.key)
        }
    }

    #[inline(always)]
    pub const fn key_ref(&self) -> &Key {
        unsafe {
            &*addr_of!(self.key)
        }
    }

    #[inline(always)]
    pub fn version(&self) -> &VersionInfo {
        unsafe {
            &*addr_of!(self.version)
        }
    }

    #[inline(always)]
    pub fn payload(&self) -> &Payload {
        unsafe {
            &*addr_of!(self.payload)
        }
    }

    #[inline(always)]
    pub fn version_mut(&mut self) -> &mut VersionInfo {
        unsafe {
            &mut *addr_of_mut!(self.version)
        }
    }
}

impl<Key: Ord + Copy + Hash + Default> UnsafeClone
for RecordPoint<Key> {
    #[inline(always)]
    unsafe fn unsafe_clone(&self) -> Self {
        mem::transmute_copy(self)
    }
}

impl<Key: Display + Ord + Copy + Hash + Default> Display
for RecordPoint<Key> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordPoint(Key: {}, {})", self.key(), self.version())
    }
}