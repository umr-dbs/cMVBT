use std::fmt::Pointer;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering::{Acquire, Release};
use crate::page_model::BlockRef;
use crate::record_model::version_info::Version;
use crate::utils::interval::Interval;

type Len = AtomicU16;

const OBSOLETE_VERSION_MARK: Version = 0x80_00000000000000;

// #[repr(align(16))]
pub struct InternalPage<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> {
    len: Len,
    key_interval_region: [MaybeUninit<Interval<Key>>; FAN_OUT],
    version_region: [MaybeUninit<Version>; FAN_OUT],
    pointer_region: [MaybeUninit<BlockRef<FAN_OUT, NUM_RECORDS, Key>>; FAN_OUT],
    _marker: PhantomData<[(Key, BlockRef<FAN_OUT, NUM_RECORDS, Key>)]>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> Drop for InternalPage<FAN_OUT, NUM_RECORDS, Key>
{
    fn drop(&mut self) {
        unsafe {
            self.children().iter().for_each(|ptr|
                (ptr as *const BlockRef<FAN_OUT, NUM_RECORDS, Key>
                    as *mut BlockRef<FAN_OUT, NUM_RECORDS, Key>)
                    .drop_in_place())
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash
> InternalPage<FAN_OUT, NUM_RECORDS, Key> {
    #[inline(always)]
    pub const fn new() -> Self {
        debug_assert!(mem::size_of::<[Key; FAN_OUT]>() +
                          mem::size_of::<[Version; FAN_OUT]>() +
                          mem::size_of::<[BlockRef<FAN_OUT, NUM_RECORDS, Key>; FAN_OUT]>() +
                          mem::size_of::<Len>()
                          <= 4096, "FAN_OUT Invalid!"
        );
        unsafe {
            InternalPage {
                len: Len::new(0),
                key_interval_region: MaybeUninit::uninit().assume_init(),
                version_region: MaybeUninit::uninit().assume_init(),
                pointer_region: MaybeUninit::uninit().assume_init(),
                _marker: PhantomData,
            }
        }
    }

    #[inline(always)]
    pub fn push(&mut self, key_interval: Interval<Key>, version: Version, ptr: BlockRef<FAN_OUT, NUM_RECORDS, Key>) {
        let n_len
            = self.len();

        unsafe {
            self.key_interval_region
                .as_mut_ptr()
                .add(n_len * mem::size_of::<Interval<Key>>())
                .write(MaybeUninit::new(key_interval));

            self.version_region
                .as_mut_ptr()
                .add(n_len * mem::size_of::<Version>())
                .write(MaybeUninit::new(version));

            self.pointer_region
                .as_mut_ptr()
                .add(n_len * mem::size_of::<BlockRef<FAN_OUT, NUM_RECORDS, Key>>())
                .write(MaybeUninit::new(ptr));
        }

        self.len.store(n_len as u16 + 1, Release)
    }

    #[inline]
    pub fn bulk_push(&mut self, entries: &[((&Interval<Key>, &Version), &BlockRef<FAN_OUT, NUM_RECORDS, Key>)]) {
        let mut len = 0;

        entries.iter().for_each(|((key, version), pointer)| unsafe {
            self.key_interval_region
                .as_mut_ptr()
                .add(len * mem::size_of::<Interval<Key>>())
                .write(MaybeUninit::new((*key).clone()));

            self.version_region
                .as_mut_ptr()
                .add(len * mem::size_of::<Version>())
                .write(MaybeUninit::new(**version));

            self.pointer_region
                .as_mut_ptr()
                .add(len * mem::size_of::<BlockRef<FAN_OUT, NUM_RECORDS, Key>>())
                .write(MaybeUninit::new((*pointer).clone()));
        });

        self.len.store(len as _, Release)
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
    pub fn keys_versions(&self) -> (&[Interval<Key>], &[Version]) {
        let len
            = self.len();

        unsafe {
            (std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, len),
             std::slice::from_raw_parts(self.version_region.as_ptr() as _, len))
        }
    }

    #[inline(always)]
    pub fn keys_versions_pointers(&self) -> (&[Interval<Key>], &[Version], &[BlockRef<FAN_OUT, NUM_RECORDS, Key>]) {
        let len
            = self.len();

        unsafe {
            (std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, len),
             std::slice::from_raw_parts(self.version_region.as_ptr() as _, len),
             std::slice::from_raw_parts(self.pointer_region.as_ptr() as _, len))
        }
    }

    #[inline(always)]
    pub unsafe fn keys(&self) -> &[Interval<Key>] {
        std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, self.len())
    }

    #[inline(always)]
    pub unsafe fn versions(&self) -> &[Version] {
        std::slice::from_raw_parts(self.version_region.as_ptr() as _, self.len())
    }

    #[inline(always)]
    fn children(&self) -> &[BlockRef<FAN_OUT, NUM_RECORDS, Key>] {
        unsafe {
            std::slice::from_raw_parts(self.pointer_region.as_ptr() as _, self.len())
        }
    }

    #[inline(always)]
    pub fn get_pointer(&self, index: usize) -> &BlockRef<FAN_OUT, NUM_RECORDS, Key> {
        unsafe {
            &*(self.pointer_region.as_ptr() as *const BlockRef<FAN_OUT, NUM_RECORDS, Key>)
                .add(index)
        }
    }

    #[inline(always)]
    pub fn active_count(&self) -> usize {
        unsafe {
            self.versions().iter().fold(0, |c, next|
                if *next & OBSOLETE_VERSION_MARK == 0 { c } else { c + 1 })
        }
    }

    #[inline(always)]
    pub fn obsolete_count(&self) -> usize {
        unsafe {
            self.versions().iter().fold(0, |c, next|
                if *next & OBSOLETE_VERSION_MARK != 0 { c } else { c + 1 })
        }
    }

    #[inline(always)]
    pub const fn is_obsolete(version: Version) -> bool {
        version & OBSOLETE_VERSION_MARK != 0
    }

    #[inline(always)]
    pub const fn is_active(version: Version) -> bool {
        version & OBSOLETE_VERSION_MARK == 0
    }

    #[inline(always)]
    pub fn mark_version_obsolete(&mut self, index: usize) {
        unsafe {
            let ptr
                = self.version_region.as_mut_ptr().add(index) as *mut Version;

            debug_assert_eq!(*ptr & OBSOLETE_VERSION_MARK, 0);

            ptr.write(*ptr | OBSOLETE_VERSION_MARK);
        }
    }
}