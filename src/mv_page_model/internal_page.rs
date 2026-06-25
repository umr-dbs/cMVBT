use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ptr;
use std::mem::MaybeUninit;

use crate::mv_page_model::BlockRef;
use crate::mv_query::time_matcher::TimeMatcher;
use crate::mv_record_model::version_info::Version;
use crate::mv_utils::interval::Interval;


const OBSOLETE_VERSION_MARK: Version = 0x80_00000000000000;
pub struct InternalPage<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    key_interval_region: [MaybeUninit<Interval<Key>>; FAN_OUT],
    version_region: [MaybeUninit<Version>; FAN_OUT],
    pointer_region: [MaybeUninit<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>>; FAN_OUT],
    _marker: PhantomData<[(Key, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)]>,
}

// impl<const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Ord + Copy + Hash + Display,
//     Payload: Clone + Default
// > Drop for InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>
// {
//     fn drop(&mut self) {
//         unsafe {
//             self.children().iter().for_each(|ptr|
//                 (ptr as *const BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>
//                     as *mut BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
//                     .drop_in_place())
//         }
//     }
// }

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub const fn new() -> Self {
        unsafe {
            InternalPage {
                key_interval_region: MaybeUninit::uninit().assume_init(),
                version_region: MaybeUninit::uninit().assume_init(),
                pointer_region: MaybeUninit::uninit().assume_init(),
                _marker: PhantomData,
            }
        }
    }

    #[inline]
    pub fn push_uncommitted(&mut self, key_interval: Interval<Key>, version: Version, ptr: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>, index: usize) {
        unsafe {
            self.key_interval_region
                .as_mut_ptr()
                .add(index)
                .write(MaybeUninit::new(key_interval));

            self.version_region
                .as_mut_ptr()
                .add(index)
                .write(MaybeUninit::new(version));

            self.pointer_region
                .as_mut_ptr()
                .add(index)
                .write(MaybeUninit::new(ptr));
        }
    }

    #[inline]
    pub fn on_reuse(&mut self, len: usize) {
        unsafe {
            (0..len).for_each(|index| {
                ptr::drop_in_place(self.pointer_region
                    .as_mut_ptr()
                    .add(index) as *mut BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>);
            });
        }
    }

    #[inline]
    pub fn bulk_push(
        &mut self,
        entries: Vec<((&Interval<Key>, &Version), &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)>)
    -> usize
    {
        let len = entries.len();
        entries.into_iter()
            .enumerate()
            .for_each(|(index, ((key, version), pointer))| unsafe {
                (self.key_interval_region
                    .as_mut_ptr() as *mut Interval<Key>)
                    .add(index)
                    .write(*key);

                (self.version_region
                    .as_mut_ptr() as *mut Version)
                    .add(index)
                    .write(*version);

                (self.pointer_region
                    .as_mut_ptr() as *mut BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
                    .add(index)
                    .write(pointer.clone());
            });
        len
    }

    #[inline]
    pub fn bulk_push_from_slice(
        &mut self,
        entries: &[((&Interval<Key>, &Version), &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)]) -> usize
    {
        let len = entries.len();
        entries.into_iter()
            .enumerate()
            .for_each(|(index, ((key, version), pointer))| unsafe {
                self.key_interval_region
                    .as_mut_ptr()
                    .add(index)
                    .write(MaybeUninit::new((*key).clone()));

                self.version_region
                    .as_mut_ptr()
                    .add(index)
                    .write(MaybeUninit::new(**version));

                self.pointer_region
                    .as_mut_ptr()
                    .add(index)
                    .write(MaybeUninit::new((*pointer).clone()));
            });
        len
    }

    #[inline(always)]
    pub fn keys_versions(&self, len: usize) -> (&[Interval<Key>], &[Version]) {
        unsafe {
            (std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, len),
             std::slice::from_raw_parts(self.version_region.as_ptr() as _, len))
        }
    }

    #[inline(always)]
    pub fn last_child(&self, len: usize) -> &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.get_pointer(len - 1)
    }

    #[inline(always)]
    pub fn keys_versions_pointers(&self, len: usize) -> (&[Interval<Key>], &[Version], &[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>]) {
        unsafe {
            (std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, len),
             std::slice::from_raw_parts(self.version_region.as_ptr() as _, len),
             std::slice::from_raw_parts(self.pointer_region.as_ptr() as _, len))
        }
    }

    #[inline(always)]
    pub fn keys(&self, len: usize) -> &[Interval<Key>] {
        unsafe { std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, len) }
    }

    #[inline(always)]
    pub fn keys_mut(&self, len: usize) -> &mut [Interval<Key>] {
        unsafe { std::slice::from_raw_parts_mut(self.key_interval_region.as_ptr() as _, len) }
    }

    #[inline(always)]
    pub fn get_key(&self, index: usize) -> &Interval<Key> {
        unsafe { &*(self.key_interval_region.as_ptr().add(index) as *const Interval<Key>) }
    }

    #[inline(always)]
    pub fn get_key_mut(&self, index: usize) -> &mut Interval<Key> {
        unsafe { &mut *(self.key_interval_region.as_ptr().add(index) as *mut Interval<Key>) }
    }

    #[inline(always)]
    pub fn versions(&self, len: usize) -> &[Version] {
        unsafe { std::slice::from_raw_parts(self.version_region.as_ptr() as _, len) }
    }

    #[inline(always)]
    pub fn get_version(&self, index: usize) -> Version {
        unsafe { *(self.version_region.as_ptr().add(index) as *const Version) }
    }

    #[inline(always)]
    pub fn get_pointer(&self, index: usize) -> &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        unsafe {
            &*(self.pointer_region.as_ptr() as *const BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
                .add(index)
        }
    }

    #[inline(always)]
    pub fn active_dead(&self, len: usize) -> (usize, usize) {
        self.versions(len)
            .iter()
            .fold((0, 0), |(active, dead), next_version|
                match next_version.is_obsolete() {
                    true => (active, dead + 1),
                    false => (active + 1, dead)
                })
    }

    #[inline(always)]
    pub fn mark_version_obsolete(&mut self, index: usize) {
        unsafe {
            let ptr
                = self.version_region.as_mut_ptr().add(index) as *mut Version;

            ptr.write(*ptr | OBSOLETE_VERSION_MARK);
        }
    }
}