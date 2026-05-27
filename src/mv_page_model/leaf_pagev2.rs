use std::hash::Hash;
use std::marker::PhantomData;
use std::{mem, ptr, slice};
use std::mem::MaybeUninit;
use std::sync::atomic::{fence, AtomicU32};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use crate::mv_record_model::record_point::{RecordPoint, RecordPointResult};
use crate::mv_record_model::version_info::{Version, VersionInfo};

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

pub struct RecordPointIter<'a, const N: usize, Key: Hash + Ord + Copy + Default, Payload: Clone + Default> {
    page: &'a LeafPage<N, Key, Payload>,
    curr: isize,
    len: isize,
    rev: bool
}

impl<'a, const N: usize, Key: Hash + Ord + Copy + Default, Payload: Clone + Default> RecordPointIter<'a, N, Key, Payload> {
    #[inline(always)]
    pub fn reverse(mut self) -> Self {
        self.rev = true;
        self
    }

    pub fn find_key_version(&self, key: Key, version: Version) -> Option<RecordPointResult<Key, Payload>> {
        let (keys, versions, payload )
            = self.page.keys_versions_payloads();

        if self.rev {
           keys.iter()
                .zip(versions)
                .zip(payload)
                .rfind(|((k, v), _)| **k == key  && v.matches(version))
                .map(|((k, _), payload)| RecordPointResult::new(*k, payload.clone()))
        }
        else {
            keys.iter()
                .zip(versions)
                .zip(payload)
                .find(|((k, v), _)| **k == key  && v.matches(version))
                .map(|((k, _), payload)| RecordPointResult::new(*k, payload.clone()))
        }
    }

    #[inline(always)]
    pub fn key(&self, index: usize) -> Key {
        unsafe { *(self.page.key_region.as_ptr().add(index) as *const _) }
    }

    #[inline(always)]
    pub fn version(&self, index: usize) -> &VersionInfo {
        unsafe { &* (self.page.version_region.as_ptr().add(index) as *const _) }
    }

    #[inline(always)]
    pub fn version_mut(&self, index: usize) -> &mut VersionInfo {
        unsafe { &mut *(self.page.version_region.as_ptr().add(index) as *mut _)  }
    }

    #[inline(always)]
    pub fn delete(&self, index: usize, del_version: Version) -> bool {
        self.version_mut(index).delete(del_version)
    }

    #[inline(always)]
    pub fn commit(&self, index: usize, commit: Version) {
        self.version_mut(index).insert_version = commit
    }

    #[inline(always)]
    pub fn payload(&self, index: usize) -> &Payload {
        unsafe { &*(self.page.payload_region.as_ptr().add(index) as *const _) }
    }
}

impl <'a, const N: usize, Key: Hash + Ord + Copy + Default, Payload: Clone + Default> Iterator
for RecordPointIter<'a, N, Key, Payload>
{
    type Item = RecordPoint<Key, Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rev {
            if self.curr >= 0 {
                let index = self.curr as usize;
                let result = Some(RecordPoint::new(
                    self.key(index),
                    self.version(index).clone(),
                    self.payload(index).clone()));

                self.curr -= 1;
                return result
            }
        } else {
            if self.curr < self.len {
                let index = self.curr as usize;
                let result =  Some(RecordPoint::new(
                    self.key(index),
                    self.version(index).clone(),
                    self.payload(index).clone()));

                self.curr += 1;
                return result
            }
        }

        None
    }
}

pub struct LeafPage<
    const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> {
    pub(crate) len: Len,
    pub(crate) key_region: [MaybeUninit<Key>; NUM_RECORDS],
    pub(crate) version_region: [MaybeUninit<VersionInfo>; NUM_RECORDS],
    pub(crate) payload_region: [MaybeUninit<Payload>; NUM_RECORDS],
    // pub(crate) record_data: [MaybeUninit<RecordPoint<Key, Payload>>; NUM_RECORDS],
    _marker: PhantomData<[RecordPoint<Key, Payload>]>,
}

impl<
    const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> Clone for LeafPage<NUM_RECORDS, Key, Payload> {
    fn clone(&self) -> Self {
        Self::from(self)
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> Default for LeafPage<NUM_RECORDS, Key, Payload> {
    fn default() -> Self {
        LeafPage::new()
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> Drop for LeafPage<NUM_RECORDS, Key, Payload> {
    fn drop(&mut self) {
        self.drop_records(self.len())
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> LeafPage<NUM_RECORDS, Key, Payload> {
    #[inline]
    pub(crate) fn from(leaf_page: &Self) -> Self {
        let mut new_page
            = Self::new();

        unsafe {
            ptr::copy_nonoverlapping(
                leaf_page as *const _ as *const u8,
                (&mut new_page) as *mut Self as *mut u8,
                mem::size_of::<Self>()
            );
        }

        // fence(Release);
        let (active, dead)
            = leaf_page.active_dead_count();

        new_page.len.store(from_active_dead(active, dead), Release);

        new_page
    }

    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            len: Len::new(0),
            key_region: unsafe { MaybeUninit::uninit().assume_init() },
            version_region: unsafe { MaybeUninit::uninit().assume_init() },
            payload_region: unsafe { MaybeUninit::uninit().assume_init() },
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub fn keys_versions_payloads(&self) -> (&[Key], &[VersionInfo], &[Payload]) {
        let len
            = self.len();

        unsafe {
            (slice::from_raw_parts(self.key_region.as_ptr() as _, len),
             slice::from_raw_parts(self.version_region.as_ptr() as _, len),
             slice::from_raw_parts(self.payload_region.as_ptr() as _, len))
        }
    }

    #[inline(always)]
    pub fn keys_versions_payloads_mut(&self) -> (&[Key], &mut [VersionInfo], &mut [Payload]) {
        let len
            = self.len();

        unsafe {
            (slice::from_raw_parts(self.key_region.as_ptr() as _, len),
             slice::from_raw_parts_mut(self.version_region.as_ptr() as _, len),
             slice::from_raw_parts_mut(self.payload_region.as_ptr() as _, len))
        }
    }

    #[inline(always)]
    pub fn keys(&self) -> &[Key] {
        let len
            = self.len();

        unsafe {
            slice::from_raw_parts(self.key_region.as_ptr() as _, len)
        }
    }

    #[inline(always)]
    pub fn versions(&self) -> &[VersionInfo] {
        let len
            = self.len();

        unsafe {
            slice::from_raw_parts(self.version_region.as_ptr() as _, len)
        }
    }

    #[inline(always)]
    pub fn versions_mut(&self) -> &mut [VersionInfo] {
        let len
            = self.len();

        unsafe {
            slice::from_raw_parts_mut(self.version_region.as_ptr() as _, len)
        }
    }

    #[inline(always)]
    pub fn as_records(&self) -> RecordPointIter<NUM_RECORDS, Key, Payload> {
        RecordPointIter {
            page: self,
            curr: 0,
            len: self.len() as _,
            rev: false
        }
    }

    #[inline(always)]
    pub fn as_records_uncommitted(&self) -> RecordPointIter<NUM_RECORDS, Key, Payload> {
        RecordPointIter {
            page: self,
            curr: 0,
            len: (1 + self.len()) as _,
            rev: false
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let len = self.len.load(Acquire) as _;
        // fence(Acquire);

        from_len_sum(len)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.len() == NUM_RECORDS
    }

    #[inline(always)]
    pub fn active_dead_count(&self) -> (Active, Dead) {
        from_len(self.len.load(Acquire))
    }

    #[inline]
    pub fn push_uncommitted(&mut self, record: RecordPoint<Key, Payload>, index: usize) {
        unsafe {
            (self.key_region.as_ptr().add(index) as *mut Key)
                .write(record.key);

            (self.version_region.as_ptr().add(index) as *mut VersionInfo)
                .write(record.version);

            (self.payload_region.as_ptr().add(index) as *mut Payload)
                .write(record.payload);
        }
    }

    #[inline(always)]
    pub fn commit_delta(&self, active_delta: i32, dead_delta: i32) {
        let len= self.len.load(Relaxed);
        let active = active_len(len) as i32 + active_delta;
        let dead = (dead_len(len) as i32 + dead_delta) as u32;

        // fence(Release);
        self.len.store(from_active_dead(active as Active, dead as Dead), Release)
    }

    #[inline]
    pub fn undo_uncommitted(&mut self, index: usize) {
        unsafe {
            if mem::needs_drop::<Key>() {
                (self.key_region.as_ptr().add(index) as *mut Key)
                    .drop_in_place();
            }

            if mem::needs_drop::<VersionInfo>() {
                (self.version_region.as_ptr().add(index) as *mut VersionInfo)
                    .drop_in_place();
            }

            if mem::needs_drop::<Payload>() {
                (self.payload_region.as_ptr().add(index) as *mut Payload)
                    .drop_in_place();
            }
        }
    }

    #[inline]
    pub fn on_reuse(&mut self) {
        let len = self.len();
        self.len.store(0, Release);

        self.drop_records(len)
    }

    #[inline]
    fn drop_records(&mut self, len: usize) {
        unsafe {
            if mem::needs_drop::<Key>() { // is copy!
                slice::from_raw_parts_mut(self.key_region.as_mut_ptr(), len)
                    .iter()
                    .for_each(|k| (k as *const _ as *mut Key).drop_in_place());
            }
            if mem::needs_drop::<VersionInfo>() { // is copy!
                slice::from_raw_parts_mut(self.version_region.as_mut_ptr(), len)
                    .iter()
                    .for_each(|v| (v as *const _ as *mut VersionInfo).drop_in_place());
            }
            if mem::needs_drop::<Payload>() { // only droppable
                slice::from_raw_parts_mut(self.payload_region.as_mut_ptr(), len)
                    .iter()
                    .for_each(|p| (p as *const _ as *mut Payload).drop_in_place());
            }
        }
    }

    #[inline(always)]
    pub(crate) fn bulk_push(&mut self, records: Vec<((&VersionInfo, &Key), &Payload)>) {
        let len
            = self.len();

        debug_assert_eq!(len, 0);
        let n_records_len
            = records.len();

        records.into_iter().enumerate().for_each(|(index, ((version, key), payload))| unsafe {
            (self.key_region.as_ptr().add(len + index) as *mut Key)
                .write(*key);

            (self.version_region.as_ptr().add(len + index) as *mut VersionInfo)
                .write(version.clone());

            (self.payload_region.as_ptr().add(len + index) as *mut Payload)
                .write(payload.clone());
        });

        self.len.store(
            from_active_dead(len as LenP + n_records_len as LenP, 0), Release)
    }

    #[inline(always)]
    pub(crate) fn bulk_push_from_slice_ref(&mut self, records: &[((&VersionInfo, &Key), &Payload)]) {
        let len
            = self.len();

        debug_assert_eq!(len, 0);

        records.into_iter().enumerate().for_each(|(index, ((version, key), payload))| unsafe {
            (self.key_region.as_ptr().add(len + index) as *mut Key)
                .write(**key);

            (self.version_region.as_ptr().add(len + index) as *mut VersionInfo)
                .write((*version).clone());

            (self.payload_region.as_ptr().add(len + index) as *mut Payload)
                .write((*payload).clone());
        });

        self.len.store(
            from_active_dead(len as LenP + records.len() as LenP, 0), Release)
    }

    // #[inline(always)]
    // pub(crate) fn bulk_push_from_slice(&mut self, records: &[RecordPoint<Key, Payload>]) {
    //     let len
    //         = self.len();
    //
    //     unsafe {
    //         records.into_iter().enumerate().for_each(|(index, record)| {
    //             self.record_data
    //                 .as_mut_ptr()
    //                 .add(index + len)
    //                 .write(MaybeUninit::new(record.clone()));
    //         });
    //     }
    //
    //     fence(Release);
    //     self.len.store(
    //         from_active_dead(len as LenP + records.len() as LenP, 0),
    //         Release)
    // }

    #[inline]
    pub(crate) fn delete(&mut self, key: Key, del: Version) -> Result<Option<()>, ()>  {
        let keys
            = self.keys();
        
        let mut index 
            = keys.len() - 1;

        for k in keys {
            index -= 1;
            if key == *k {
                let v = unsafe { self.versions_mut().get_unchecked_mut(index) };
                return if v.delete(del) {
                    Ok(Some(()))
                } else {
                    Err(())
                }
            }
        }

        Ok(None)
    }
}