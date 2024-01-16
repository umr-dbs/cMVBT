use std::hash::Hash;
use std::marker::PhantomData;
use std::{mem, ptr, slice};
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering::{Acquire, Release};
use crate::block::block_manager::BlockManager;
use crate::page_model::BlockRef;
use crate::record_model::record_point::RecordPoint;
use crate::record_model::version_info::{Version, VersionInfo};
use crate::utils::interval::Interval;

type Len = AtomicU16;

pub struct LeafPage<
    const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default
> {
    pub(crate) len: Len,
    pub(crate) record_data: [MaybeUninit<RecordPoint<Key>>; NUM_RECORDS],
    _marker: PhantomData<[RecordPoint<Key>]>,
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default
> Default for LeafPage<NUM_RECORDS, Key> {
    fn default() -> Self {
        LeafPage::new()
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default
> Drop for LeafPage<NUM_RECORDS, Key> {
    fn drop(&mut self) {
        self.as_records_mut().iter_mut().for_each(|record| unsafe {
            (record as *mut RecordPoint<Key>)
                .drop_in_place()
        })
    }
}

impl<const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default
> LeafPage<NUM_RECORDS, Key> {
    #[inline(always)]
    pub const fn new() -> Self {
        debug_assert!(mem::size_of::<Len>() +
                          mem::size_of::<[RecordPoint<Key>; NUM_RECORDS]>()
                          <= 4096, "FAN_OUT Invalid!");
        Self {
            len: Len::new(0),
            record_data: unsafe { MaybeUninit::uninit().assume_init() }, // <[MaybeUninit<Entry>; NUM_RECORDS]>::
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub fn as_records(&self) -> &[RecordPoint<Key>] {
        unsafe {
            std::slice::from_raw_parts(
                self.record_data.as_ptr() as *const RecordPoint<Key>,
                self.len())
        }
    }

    #[inline(always)]
    pub fn as_records_mut(&mut self) -> &mut [RecordPoint<Key>] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.record_data.as_mut_ptr() as *mut _,
                self.len())
        }
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
    pub fn dead_count(&self) -> usize {
        self.as_records()
            .iter()
            .filter(|r| r.version().is_deleted())
            .count()
    }

    #[inline]
    pub fn push_uncommitted(&mut self, record: RecordPoint<Key>, index: usize) {
        unsafe {
            self.record_data
                .as_mut_ptr()
                .add(index)
                .write(MaybeUninit::new(record))
        }
    }

    #[inline(always)]
    pub fn commit_until(&self, index: usize) {
        self.len.store(1 + index as u16, Release)
    }

    #[inline]
    pub fn undo_uncommitted(&mut self, index: usize) {
        unsafe {
            self.record_data
                .as_mut_ptr()
                .add(index)
                .read()
                .assume_init();
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
    pub(crate) fn bulk_push(&mut self, records: Vec<&RecordPoint<Key>>) {
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

        self.len.store(len as u16 + add as u16, Release)
    }

    #[inline(always)]
    pub(crate) fn bulk_push_from_slice(&mut self, records: &[&RecordPoint<Key>]) {
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

        self.len.store(len as u16 + records.len() as u16, Release)
    }

    #[inline]
    pub(crate) fn delete(&mut self, key: Key, del: Version) -> Option<VersionInfo> {
        let record_data
            = self.as_records_mut();

        match record_data.binary_search_by_key(
            &key, |record| record.key)
        {
            Ok(index) => unsafe {
                let ver_info = record_data
                    .get_unchecked_mut(index)
                    .version_mut();

                if ver_info.delete(del) {
                    Some(ver_info.clone())
                } else {
                    None
                }
            }
            _ => None
        }
    }
}