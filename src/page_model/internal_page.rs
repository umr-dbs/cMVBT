use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering::{Acquire, Release};
use crate::record_model::version_info::Version;

pub type Pointer = *mut usize;
type Len = AtomicU16;

pub struct IndexEntry<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash>
{
    key: Key,
    version: Version,
    next: Pointer
}

pub struct InternalPage<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> {
    len: Len,
    key_region: [MaybeUninit<Key>; FAN_OUT],
    version_region: [MaybeUninit<Version>; FAN_OUT],
    pointers_region: [MaybeUninit<Pointer>; FAN_OUT],
    _marker: PhantomData<[(Key, Version, Pointer)]>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> Drop for InternalPage<FAN_OUT, NUM_RECORDS, Key>
{
    fn drop(&mut self) {

    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> InternalPage<FAN_OUT, NUM_RECORDS, Key> {
    #[inline(always)]
    pub const fn new() -> Self {
        debug_assert!(FAN_OUT % 3 >= mem::size_of::<Len>(), "FAN_OUT invalid!");

        unsafe {
            InternalPage {
                len: Len::new(0),
                key_region: MaybeUninit::uninit().assume_init(),
                version_region: MaybeUninit::uninit().assume_init(),
                pointers_region: MaybeUninit::uninit().assume_init(),
                _marker: PhantomData,
            }
        }
    }

    #[inline(always)]
    pub fn push(&mut self, key: Key, version: Version, ptr: Pointer) {
        let n_len
            = self.len();

        unsafe {
            self.key_region
                .as_mut_ptr()
                .add(n_len * mem::size_of::<Key>())
                .write(MaybeUninit::new(key));

            self.version_region
                .as_mut_ptr()
                .add(n_len * mem::size_of::<Version>())
                .write(MaybeUninit::new(version));

            self.pointers_region
                .as_mut_ptr()
                .add(n_len * mem::size_of::<Pointer>())
                .write(MaybeUninit::new(ptr));
        }

        self.len.store(n_len as u16 + 1, Release)
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len.load(Acquire) as _
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.len() == FAN_OUT
    }

    #[inline(always)]
    pub fn keys(&self) -> &[Key] {
        unsafe {
            std::slice::from_raw_parts(self.key_region.as_ptr() as _, self.len())
        }
    }

    #[inline(always)]
    pub fn versions(&self) -> &[Version] {
        unsafe {
            std::slice::from_raw_parts(self.version_region.as_ptr() as _, self.len())
        }
    }

    #[inline(always)]
    pub fn children(&self) -> &[Pointer] {
        unsafe {
            std::slice::from_raw_parts(self.pointers_region.as_ptr() as _, self.len())
        }
    }
}