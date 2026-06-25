use std::hash::Hash;
use std::marker::PhantomData;
use std::{mem, ptr};
use std::mem::{needs_drop, MaybeUninit};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::version_info::{Version, VersionInfo};

pub struct LeafPage<
    const NUM_RECORDS: usize,
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default
> {
    pub(crate) record_data: [MaybeUninit<RecordPoint<Key, Payload>>; NUM_RECORDS],
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
> LeafPage<NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            record_data: unsafe { MaybeUninit::uninit().assume_init() },
        }
    }

    #[inline(always)]
    pub fn as_records(&self, len: usize) -> &[RecordPoint<Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts(
                self.record_data.as_ptr() as *const RecordPoint<Key, Payload>,
                len)
        }
    }

    #[inline(always)]
    pub fn as_records_mut(&mut self, len: usize) -> &mut [RecordPoint<Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.record_data.as_mut_ptr() as *mut _,
                len)
        }
    }

    #[inline(always)]
    pub fn as_records_uncommitted_mut(&mut self, len: usize) -> &mut [RecordPoint<Key, Payload>] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.record_data.as_mut_ptr() as *mut _,
                len + 1)
        }
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

    #[inline]
    pub fn undo_uncommitted(&mut self, index: usize) {
        unsafe {
            ptr::drop_in_place(self.record_data
                .as_mut_ptr()
                .add(index) as *mut RecordPoint<Key, Payload>);
        }
    }

    #[inline]
    pub fn on_reuse(&mut self, len: usize) {
        unsafe {
            (0..len).for_each(|index| {
                ptr::drop_in_place(self.record_data
                    .as_mut_ptr()
                    .add(index) as *mut RecordPoint<Key, Payload>);
            });
        }
    }

    #[inline(always)]
    pub(crate) fn bulk_push(&mut self, records: Vec<&RecordPoint<Key, Payload>>) -> usize {
        let len = records.len();
        unsafe {
            records.into_iter().enumerate().for_each(|(index, record)| {
                self.record_data
                    .as_mut_ptr()
                    .add(index)
                    .write(MaybeUninit::new(record.clone()));
            });
        }
        len
    }

    #[inline(always)]
    pub(crate) fn bulk_push_from_slice_ref(&mut self, records: &[&RecordPoint<Key, Payload>]) -> usize{
        let len = records.len();
        unsafe {
            records.into_iter().enumerate().for_each(|(index, record)| {
                self.record_data
                    .as_mut_ptr()
                    .add(index)
                    .write(MaybeUninit::new((*record).clone()));
            });
        }
        len
    }

    #[inline]
    pub(crate) fn delete(&mut self, key: Key, del: Version, len: usize) -> Result<Option<VersionInfo>, ()>  {
        match self
            .as_records_mut(len)
            .iter_mut()
            .rfind(|record| record.key == key)
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