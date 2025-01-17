use std::hash::Hash;
use std::marker::PhantomData;
use std::{mem, ptr, slice};
use std::mem::MaybeUninit;
use std::sync::atomic::{fence, AtomicU16};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use crate::mv_block::block_manager::BlockManager;
use crate::mv_page_model::BlockRef;
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::version_info::{Version, VersionInfo};
use crate::mv_utils::interval::Interval;

type Len = AtomicU16;

pub struct LeafPage<
    const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> {
    pub(crate) len: Len,
    pub(crate) record_data: [MaybeUninit<RecordPoint<Key, Payload>>; NUM_RECORDS],
    _marker: PhantomData<[RecordPoint<Key, Payload>]>,
}

impl<const NUM_RECORDS: usize,
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
        self.as_records_mut().iter_mut().for_each(|record| unsafe {
            (record as *mut RecordPoint<Key, Payload>)
                .drop_in_place()
        })
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
            leaf_page
                .as_records()
                .iter()
                .enumerate()
                .for_each(|(index, record)| new_page
                    .record_data
                    .as_mut_ptr()
                    .add(index)
                    .write(MaybeUninit::new((*record).clone()))
                );
        }

        fence(Release);
        new_page.len.store(leaf_page.len() as u16, Release);

        new_page
    }

    #[inline(always)]
    pub const fn new() -> Self {
        // debug_assert!(mem::size_of::<Len>() +
        //                   mem::size_of::<[RecordPoint<Key, Payload>; NUM_RECORDS]>()
        //                   <= 4096, "FAN_OUT Invalid!");
        Self {
            len: Len::new(0),
            record_data: unsafe { MaybeUninit::uninit().assume_init() }, // <[MaybeUninit<Entry>; NUM_RECORDS]>::
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub fn as_records(&self) -> &[RecordPoint<Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts(
                self.record_data.as_ptr() as *const RecordPoint<Key, Payload>,
                self.len())
        }
    }

    #[inline(always)]
    pub fn as_records_mut(&mut self) -> &mut [RecordPoint<Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.record_data.as_mut_ptr() as *mut _,
                self.len())
        }
    }

    #[inline(always)]
    pub fn as_records_uncommitted_mut(&mut self) -> &mut [RecordPoint<Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.record_data.as_mut_ptr() as *mut _,
                self.len() + 1)
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let len = self.len.load(Acquire) as _;
        fence(Acquire);
        len
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
    pub fn active_count(&self) -> usize {
        self.as_records()
            .iter()
            .filter(|r| !r.version().is_deleted())
            .count()
    }

    #[inline(always)]
    pub fn active_dead(&self) -> (usize, usize) {
        self.as_records()
            .iter()
            .fold((0, 0), |(active, dead), next_record|
                match next_record.version().is_deleted() {
                    true => (active, dead + 1),
                    false => (active + 1, dead)
                })
    }

    #[inline(always)]
    pub fn dead_count(&self) -> usize {
        self.as_records()
            .iter()
            .filter(|r| r.version().is_deleted())
            .count()
    }

    #[inline]
    pub fn push_uncommitted(&mut self, record: RecordPoint<Key, Payload>, index: usize) {
        unsafe {
            self.record_data
                .as_mut_ptr()
                .add(index)
                .write(MaybeUninit::new(record))
        }
    }

    #[inline(always)]
    pub fn commit_until(&self, index: usize) {
        fence(Release);
        self.len.store(1 + index as u16, Release)
    }

    #[inline]
    pub fn undo_uncommitted(&mut self, index: usize) {
        unsafe {
            ptr::drop_in_place(self.record_data
                .as_mut_ptr()
                .add(index) as *mut RecordPoint<Key, Payload>);
        }
    }

    #[inline]
    pub fn on_reuse(&mut self) {
        let len = self.len();
        self.len.store(0, Release);

        unsafe {
            (0..len).for_each(|index| {
                ptr::drop_in_place(self.record_data
                    .as_mut_ptr()
                    .add(index) as *mut RecordPoint<Key, Payload>);
            });
        }
    }

    // #[inline]
    // pub fn push(&mut self, record: RecordPoint<Key>) {
    //     unsafe {
    //         let n_len
    //             = self.len();
    //
    //         self.record_data
    //             .as_mut_ptr()
    //             .add(n_len)
    //             .write(MaybeUninit::new(record));
    //
    //         self.len.store(n_len as u16 + 1, Release)
    //     }
    // }

    #[inline(always)]
    pub(crate) fn bulk_push(&mut self, records: Vec<&RecordPoint<Key, Payload>>) {
        let len
            = self.len();

        let add
            = records.len();

        unsafe {
            records.into_iter().enumerate().for_each(|(index, record)| {
                self.record_data
                    .as_mut_ptr()
                    .add(index + len)
                    .write(MaybeUninit::new(record.clone()));
            });
        }

        fence(Release);
        self.len.store(len as u16 + add as u16, Release)
    }

    #[inline(always)]
    pub(crate) fn bulk_push_from_slice_ref(&mut self, records: &[&RecordPoint<Key, Payload>]) {
        let len
            = self.len();

        unsafe {
            records.into_iter().enumerate().for_each(|(index, record)| {
                self.record_data
                    .as_mut_ptr()
                    .add(index + len)
                    .write(MaybeUninit::new((*record).clone()));
            });
        }

        fence(Release);
        self.len.store(len as u16 + records.len() as u16, Release)
    }

    #[inline(always)]
    pub(crate) fn bulk_push_from_slice(&mut self, records: &[RecordPoint<Key, Payload>]) {
        let len
            = self.len();

        unsafe {
            records.into_iter().enumerate().for_each(|(index, record)| {
                self.record_data
                    .as_mut_ptr()
                    .add(index + len)
                    .write(MaybeUninit::new(record.clone()));
            });
        }

        fence(Release);
        self.len.store(len as u16 + records.len() as u16, Release)
    }

    #[inline]
    pub(crate) fn delete(&mut self, key: Key, del: Version) -> Result<Option<VersionInfo>, ()>  {
        match self.as_records_mut()
            .iter_mut()
            .rev()
            .find(|record| record.key == key)
        {
            Some(record) => {
                let ver_info = record
                    .version_mut();

                if ver_info.delete(del) {
                    Ok(Some(ver_info.clone()))
                } else {
                    Err(())
                }
            }
            _ => Ok(None)
        }
    }
}