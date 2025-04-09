use std::fmt::{Display, Pointer};
use std::hash::Hash;
use std::marker::PhantomData;
use std::{mem, ptr};
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU16, fence, AtomicU32};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use crate::mv_page_model::BlockRef;
use crate::mv_record_model::version_info::Version;
use crate::mv_utils::interval::Interval;

type Len = AtomicU32;
type LenP = u32;
type Active = u32;
type Dead = u32;

#[inline(always)]
const fn from_len_sum(len: LenP) -> usize {
    (active_len(len) + dead_len(len)) as usize
}
#[inline(always)]
const fn from_len(len: LenP) -> (Active, Dead) {
    (active_len(len), dead_len(len))
}
#[inline(always)]
const fn active_len(len: LenP) -> Active {
    len >> 16
}
#[inline(always)]
const fn dead_len(len: LenP) -> Dead {
    len & 0xFF_FF
}
#[inline(always)]
const fn from_active_dead(active: Active, dead: Dead) -> LenP {
    (active << 16) | dead
}

const OBSOLETE_VERSION_MARK: Version = 0x80_00000000000000;
// pub const OOO_REUSED_VERSION_MARK: Version = 0x40_00000000000000;

pub trait TimeMatcher {
    fn into_cmp(self) -> Self;
    // fn le_other_any(self, other: Version) -> bool;

    fn match_version_active(self, other: Version) -> bool;

    fn lt_self_any(self, other: Version) -> bool;

    fn is_obsolete(&self) -> bool;

    fn is_active(&self) -> bool;

    fn matched(self, other: Version) -> bool;
}

impl TimeMatcher for Version {
    #[inline(always)]
    fn into_cmp(self) -> Self {
        self & !OBSOLETE_VERSION_MARK
    }

    // #[inline(always)]
    // fn le_other_any(self, other: Version) -> bool {
    //     self & !OBSOLETE_VERSION_MARK <= other // & !OBSOLETE_VERSION_MARK
    // }

    #[inline(always)]
    fn match_version_active(self, other: Version) -> bool {
        self <= other
    }

    #[inline(always)]
    fn lt_self_any(self, other: Version) -> bool {
        self & !OBSOLETE_VERSION_MARK < other
    }

    #[inline(always)]
    fn is_obsolete(&self) -> bool {
        *self & OBSOLETE_VERSION_MARK != 0
    }

    #[inline(always)]
    fn is_active(&self) -> bool {
        *self & OBSOLETE_VERSION_MARK == 0
    }

    #[inline(always)]
    fn matched(self, other: Version) -> bool {
        self & !OBSOLETE_VERSION_MARK <= other
    }
}

pub struct InternalPage<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    pub(crate) len: Len,
    key_interval_region: [MaybeUninit<Interval<Key>>; FAN_OUT],
    version_region: [MaybeUninit<Version>; FAN_OUT],
    pointer_region: [MaybeUninit<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>>; FAN_OUT],
    _marker: PhantomData<[(Key, BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)]>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Clone for InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn clone(&self) -> Self {
        Self::from(self)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Drop for InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    fn drop(&mut self) {
        unsafe {
            self.children().iter().for_each(|ptr|
                (ptr as *const BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>
                    as *mut BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
                    .drop_in_place())
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline]
    pub fn from(from: &Self) -> Self {
        let mut new_page
            = Self::new();

        let (keys, versions, pointers)
            = from.keys_versions_pointers();

        keys.iter()
            .zip(versions.iter())
            .zip(pointers.iter())
            .enumerate()
            .for_each(|(index, ((key, version), pointer))| unsafe {
                new_page.key_interval_region
                    .as_mut_ptr()
                    .add(index)
                    .write(MaybeUninit::new(key.clone()));

                new_page.version_region
                    .as_mut_ptr()
                    .add(index)
                    .write(MaybeUninit::new(*version));

                new_page.pointer_region
                    .as_mut_ptr()
                    .add(index)
                    .write(MaybeUninit::new(pointer.clone()));
            });

        fence(Release);

        let (active, dead)
            = from.active_dead_count();

        new_page.len.store(
            from_active_dead(active, dead), Release);

        new_page
    }

    #[inline(always)]
    pub const fn new() -> Self {
        // debug_assert!(mem::size_of::<[Interval<Key>; FAN_OUT]>() +
        //                   mem::size_of::<[Version; FAN_OUT]>() +
        //                   mem::size_of::<[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>; FAN_OUT]>() +
        //                   mem::size_of::<Len>()
        //                   <= 4096, "FAN_OUT Invalid!"
        // );
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

    // #[inline(always)]
    // pub fn push_committed(&mut self, key_interval: Interval<Key>, version: Version, ptr: BlockRef<FAN_OUT, NUM_RECORDS, Key>) {
    //     let len = self.len();
    //     self.push_uncommitted(key_interval, version, ptr, len);
    //     self.commit_until(len);
    // }

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

    #[inline(always)]
    pub fn commit_delta(&self, active_delta: i32, dead_delta: u32) {
        let len= self.len.load(Relaxed);
        let active = active_len(len) as i32 + active_delta;
        let dead = dead_len(len) + dead_delta;

        fence(Release);
        self.len.store(from_active_dead(active as Active, dead as Dead), Release)
    }

    // #[inline]
    // pub fn undo_uncommitted(&self, commit: Version) {
    //     unsafe {
    //         self.pointer_region
    //             .as_ptr()
    //             .add(commit as usize * mem::size_of::<BlockRef<FAN_OUT, NUM_RECORDS, Key>>())
    //             .read()
    //             .assume_init();
    //     }
    // }

    #[inline]
    pub fn on_reuse(&mut self) {
        let len = self.sum_len();
        self.len.store(0, Release);

        unsafe {
            (0..len).for_each(|index| {
                ptr::drop_in_place(self.pointer_region
                    .as_mut_ptr()
                    .add(index) as *mut BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>);
            });
        }
    }

    // #[inline(always)]
    // pub unsafe fn override_clone(&self, entries: Vec<((&Interval<Key>, &Version), &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)>) {
    //     let children = self
    //         .children()
    //         .iter()
    //         .map(|c| ptr::read(c))
    //         .collect_vec();
    //
    //     debug_assert!(children.len() == 1);
    //
    //     self.len.store(0, Release);
    //     self.bulk_push(entries);
    //     mem::drop(children);
    // }

    #[inline]
    pub fn bulk_push(&self, entries: Vec<((&Interval<Key>, &Version), &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)>) {
        let len
            = self.active_len();

        debug_assert_eq!(self.dead_len(), 0);
        let add
            = entries.len();

        entries.into_iter()
            .enumerate()
            .for_each(|(index, ((key, version), pointer))| unsafe {
                (self.key_interval_region
                    .as_ptr() as *mut Interval<Key>)
                    .add(index + len)
                    .write(key.clone());

                (self.version_region
                    .as_ptr() as *mut Version)
                    .add(index + len)
                    .write(*version);

                (self.pointer_region
                    .as_ptr() as *mut BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
                    .add(index + len)
                    .write(pointer.clone());
            });

        fence(Release);
        self.len.store(
            from_active_dead(len as LenP + add as LenP, 0), Release);
    }

    #[inline]
    pub fn bulk_push_from_slice(
        &mut self,
        entries: &[((&Interval<Key>, &Version), &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)])
    {
        let len
            = self.active_len();

        debug_assert_eq!(self.dead_len(), 0);
        let add
            = entries.len();

        entries.into_iter()
            .enumerate()
            .for_each(|(index, ((key, version), pointer))| unsafe {
                self.key_interval_region
                    .as_mut_ptr()
                    .add(index + len)
                    .write(MaybeUninit::new((*key).clone()));

                self.version_region
                    .as_mut_ptr()
                    .add(index + len)
                    .write(MaybeUninit::new(**version));

                self.pointer_region
                    .as_mut_ptr()
                    .add(index + len)
                    .write(MaybeUninit::new((*pointer).clone()));
            });

        fence(Release);
        self.len.store(
            from_active_dead(len as LenP + add as LenP, 0), Release)
    }

    #[inline(always)]
    pub fn active_dead_count(&self) -> (Active, Dead) {
        from_len(self.len.load(Acquire))
    }

    #[inline(always)]
    pub fn active_len(&self) -> usize {
        let len = self.len.load(Acquire);
        // fence(Acquire);

        active_len(len) as _
    }

    #[inline(always)]
    pub fn dead_len(&self) -> usize {
        let len = self.len.load(Acquire);
        // fence(Acquire);

        dead_len(len) as _
    }

    #[inline(always)]
    pub fn sum_len(&self) -> usize {
        let len = self.len.load(Acquire) as _;
        fence(Acquire);

        from_len_sum(len)
    }

    // #[inline(always)]
    // pub fn is_empty(&self) -> bool {
    //     self.sum_len() == 0
    // }

    // #[inline(always)]
    // pub fn is_full(&self) -> bool {
    //     self.sum_len() == FAN_OUT
    // }

    #[inline(always)]
    pub fn keys_versions(&self) -> (&[Interval<Key>], &[Version]) {
        let len
            = self.sum_len();

        unsafe {
            (std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, len),
             std::slice::from_raw_parts(self.version_region.as_ptr() as _, len))
        }
    }

    #[inline(always)]
    pub fn last_child(&self) -> &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.get_pointer(self.sum_len() - 1)
    }

    #[inline(always)]
    pub fn keys_versions_pointers(&self) -> (&[Interval<Key>], &[Version], &[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>]) {
        let len
            = self.sum_len();

        unsafe {
            (std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, len),
             std::slice::from_raw_parts(self.version_region.as_ptr() as _, len),
             std::slice::from_raw_parts(self.pointer_region.as_ptr() as _, len))
        }
    }

    #[inline(always)]
    pub fn keys(&self) -> &[Interval<Key>] {
        unsafe { std::slice::from_raw_parts(self.key_interval_region.as_ptr() as _, self.sum_len()) }
    }

    #[inline(always)]
    pub fn get_key(&self, index: usize) -> &Interval<Key> {
        unsafe { &*(self.key_interval_region.as_ptr().add(index) as *const Interval<Key>) }
    }

    #[inline(always)]
    pub fn versions(&self) -> &[Version] {
        unsafe { std::slice::from_raw_parts(self.version_region.as_ptr() as _, self.sum_len()) }
    }

    #[inline(always)]
    pub unsafe fn versions_mut(&mut self) -> &mut [Version] {
        std::slice::from_raw_parts_mut(self.version_region.as_mut_ptr() as _, self.sum_len())
    }

    #[inline(always)]
    pub unsafe fn versions_byKey_uncommitted_mut(&mut self) -> &mut [Version] {
        std::slice::from_raw_parts_mut(self.version_region.as_mut_ptr() as _, self.sum_len() + 2)
    }

    #[inline(always)]
    pub fn get_version_mut(&mut self, index: usize) -> &mut Version {
        unsafe { &mut *(self.version_region.as_mut_ptr().add(index) as *mut Version) }
    }

    #[inline(always)]
    pub fn get_version(&self, index: usize) -> Version {
        unsafe { *(self.version_region.as_ptr().add(index) as *const Version) }
    }

    #[inline(always)]
    pub fn get_version_ptr(&self, index: usize) -> *mut Version {
        unsafe { (self.version_region.as_ptr().add(index) as *mut Version) }
    }

    #[inline(always)]
    pub fn children(&self) -> &[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts(self.pointer_region.as_ptr() as _, self.sum_len())
        }
    }

    #[inline(always)]
    pub fn get_pointer(&self, index: usize) -> &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        unsafe {
            &*(self.pointer_region.as_ptr() as *const BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
                .add(index)
        }
    }

    // #[inline(always)]
    // pub fn active_dead(&self) -> (usize, usize) {
    //     self.versions()
    //         .iter()
    //         .fold((0, 0), |(active, dead), next_version|
    //             match next_version.is_obsolete() {
    //                 true => (active, dead + 1),
    //                 false => (active + 1, dead)
    //             })
    // }
    //
    // #[inline(always)]
    // pub fn obsolete_count(&self) -> usize {
    //     unsafe {
    //         self.versions().iter().fold(0, |c, next|
    //             if next.is_obsolete() { c + 1 } else { c })
    //     }
    // }

    // #[inline(always)]
    // pub const fn is_obsolete(version: Version) -> bool {
    //     version & OBSOLETE_VERSION_MARK != 0
    // }

    // #[inline(always)]
    // pub const fn is_active(version: Version) -> bool {
    //     version & OBSOLETE_VERSION_MARK == 0
    // }

    #[inline(always)]
    pub fn mark_version_obsolete(&mut self, index: usize) {
        unsafe {
            let ptr
                = self.version_region.as_mut_ptr().add(index) as *mut Version;

            ptr.write(*ptr | OBSOLETE_VERSION_MARK);
        }
    }
}