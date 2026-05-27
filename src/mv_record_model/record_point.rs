use std::alloc::Layout;
use std::hash::Hash;
use std::{alloc, mem, ptr};
use std::fmt::{Display, Formatter};
use std::mem::ManuallyDrop;
use std::ops::{Add, Deref, DerefMut};
use std::ptr::{addr_of, addr_of_mut, slice_from_raw_parts};
use crate::mv_record_model::unsafe_clone::UnsafeClone;
use crate::mv_record_model::version_info::VersionInfo;

// pub type Payload = Box<()>;

#[derive(Default, Clone)]
// #[repr(packed)]
pub struct RecordPoint<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> {
    pub key: Key,
    pub version: VersionInfo,
    pub payload: Payload,
}

pub struct RecordPointResult<Key: Ord + Copy + Hash + Default, Payload: Clone> {
    pub key: Key,
    pub payload: Payload,
}

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> RecordPointResult<Key, Payload> {
    #[inline]
    pub fn from(r: &RecordPoint<Key, Payload>) -> Self {
        Self {
            key: r.key(),
            payload: r.payload.clone()
        }
    }

    #[inline]
    pub const fn new(key: Key, payload: Payload) -> Self {
        Self {
            key,
            payload
        }
    }
}

// impl<Key: Ord + Copy + Hash + Default> Drop for RecordPointResult<Key> {
//     fn drop(&mut self) {
//         unsafe {
//             let _ = Payload::from_raw(self.payload.as_mut());
//
//             // ManuallyDrop::drop(&mut self.payload)
//             // let layout = Layout::from_size_align_unchecked(
//             //     mem::size_of::<usize>(),
//             //     mem::align_of::<u8>());
//
//             // alloc::dealloc(self.payload.deref_mut().deref_mut(), layout);
//         }
//     }
// }

// impl<Key: Ord + Copy + Hash + Default> Clone for RecordPoint<Key> {
//     fn clone(&self) -> Self {
//         Self {
//             key: self.key(),
//             version: self.version().clone(),
//             payload: ManuallyDrop::new(self.payload().clone()),
//         }
//     }
// }

// impl<Key: Ord + Copy + Hash + Default> Drop for RecordPoint<Key> {
//     fn drop(&mut self) {
//         unsafe {
//             let _ = Payload::from_raw(self.payload.as_mut());
//             // ManuallyDrop::drop(&mut self.payload)
//
//             // let layout = Layout::from_size_align_unchecked(
//             //     mem::size_of::<usize>(),
//             //     mem::align_of::<u8>());
//             //
//             // alloc::dealloc(self.payload_mut().deref_mut(), layout);
//         }
//     }
// }

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> RecordPoint<Key, Payload> {
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
        self.key
    }

    #[inline(always)]
    pub const fn key_ref(&self) -> &Key {
        &self.key
    }

    #[inline(always)]
    pub const fn version(&self) -> &VersionInfo {
        &self.version
    }

    #[inline(always)]
    pub const fn payload(&self) -> &Payload { 
        &self.payload
    }

    #[inline(always)]
    pub(crate) fn payload_mut(&mut self) -> &mut Payload {
        &mut self.payload
    }

    #[inline(always)]
    pub fn version_mut(&mut self) -> &mut VersionInfo {
        &mut self.version
    }
}

impl<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> UnsafeClone
for RecordPoint<Key, Payload> {
    #[inline(always)]
    unsafe fn unsafe_clone(&self) -> Self {
        mem::transmute_copy(self)
    }
}

impl<Key: Display + Ord + Copy + Hash + Default, Payload: Clone + Default> Display
for RecordPoint<Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordPoint(Key: {}, Version: {})",
               self.key(),
               self.version())
    }
}

impl<Key: Display + Ord + Copy + Hash + Default, Payload: Clone> Display
for RecordPointResult<Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordPointResult(Key: {})", self.key)
    }
}