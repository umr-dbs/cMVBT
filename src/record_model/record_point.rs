use std::alloc::Layout;
use std::hash::Hash;
use std::{alloc, mem};
use std::fmt::{Display, Formatter};
use std::mem::ManuallyDrop;
use std::ops::{Add, Deref, DerefMut};
use std::ptr::{addr_of, addr_of_mut, slice_from_raw_parts};
use crate::record_model::unsafe_clone::UnsafeClone;
use crate::record_model::version_info::VersionInfo;

pub type Payload = Box<u8>;

#[derive(Default)]
#[repr(packed)]
pub struct RecordPoint<Key: Ord + Copy + Hash + Default> {
    pub key: Key,
    pub version: VersionInfo,
    pub payload: ManuallyDrop<Payload>,
}

impl<Key: Ord + Copy + Hash + Default> Clone for RecordPoint<Key> {
    fn clone(&self) -> Self {
        Self {
            key: self.key(),
            version: self.version().clone(),
            payload: ManuallyDrop::new(self.payload().clone()),
        }
    }
}

impl<Key: Ord + Copy + Hash + Default> Drop for RecordPoint<Key> {
    fn drop(&mut self) {
        unsafe {
            let size = (self.payload().deref() as *const u8 as *const usize)
                .read();

            let layout = Layout::from_size_align_unchecked(
                size + mem::size_of::<usize>(),
                mem::align_of::<u8>());

            alloc::dealloc(self.payload_mut().deref_mut(), layout);
        }
    }
}

impl<Key: Ord + Copy + Hash + Default> RecordPoint<Key> {
    #[inline(always)]
    pub const fn new(key: Key, version: VersionInfo, payload: Payload) -> Self {
        Self {
            key,
            version,
            payload: ManuallyDrop::new(payload),
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
    pub(crate) fn payload_mut(&mut self) -> &mut Payload {
        unsafe {
            &mut *addr_of_mut!(self.payload)
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

fn read_payload(payload: &Payload) -> &str {
    unsafe {
        let size = (payload.deref() as *const _ as *const usize)
            .read();

        mem::transmute(slice_from_raw_parts(
            (payload.deref() as *const u8).add(mem::size_of::<usize>()),
            size))
    }
}

impl<Key: Display + Ord + Copy + Hash + Default> Display
for RecordPoint<Key> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let payload_str = read_payload(self.payload());
        write!(f, "RecordPoint(Key: {}, Version: {}, payload-bytes(len={}): [{}])",
               self.key(),
               self.version(),
               payload_str.len(),
               payload_str)
    }
}