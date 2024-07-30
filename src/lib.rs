use std::ffi::{c_void, CString};
use std::{mem, ptr};
use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_record_model::record_point::RecordPointResult;

mod mv_block;
mod mv_crud_model;
mod mv_page_model;
mod mv_record_model;
mod mv_tree;
mod mv_utils;
mod mv_test;
mod mv_tx_model;

const EX_FAN_OUT: usize = 127;
const EX_N: usize = 127;

type EX_KEY = u64;
type EX_VALUE = u64;

type MVBTreeApi = MVBPlusTree<EX_FAN_OUT, EX_N, EX_KEY, EX_VALUE>;

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct tree_options_t {
    key_size: libc::size_t,
    value_size: libc::size_t,
    pool_path: CString,
    pool_size: libc::size_t,
    num_threads: libc::size_t,
}

impl Default for tree_options_t {
    fn default() -> Self {
        Self {
            key_size: 8,
            value_size: 8,
            pool_path: CString::new("").unwrap(),
            pool_size: 0,
            num_threads: 1,
        }
    }
}

struct MVBTreeApiExport(MVBTreeApi);

impl Deref for MVBTreeApiExport {
    type Target = MVBTreeApi;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::query;
use crate::mv_tree::mvbplus_tree::MVBPlusTree;
use crate::mv_tx_model::transaction::AtomicTransaction;
use crate::mv_tx_model::tx_api::IsolatedSnapShot;
use crate::mv_tx_model::tx_manager::TransactionManager;
use crate::mv_utils::interval::Interval;

impl MVBTreeApiExport {
    #[inline(always)]
    fn si(&self) -> IsolatedSnapShot<EX_FAN_OUT, EX_N, EX_KEY, EX_VALUE> {
        self.snapshot_current()
    }

    #[inline(always)]
    fn find(&self, key: *const u8, _sz: usize, value_out: *mut u8) -> bool {
        let querying_v
            = self.current_version();

        match self.dispatch_crud(CRUDOperation::Point(
            unsafe { ptr::read(mem::transmute(key)) }, querying_v))
        {
            CRUDOperationResult::MatchedRecords(result)
            if !result.is_empty() => unsafe {
                ptr::write(mem::transmute(value_out), result.get_unchecked(0).payload);
                true
            },
            _ => false
        }
    }

    #[inline(always)]
    fn insert(&self, key: *const u8, _key_sz: usize, value: *const u8, _value_sz: usize) -> bool {
        match self.dispatch_crud(CRUDOperation::Insert(
            unsafe { ptr::read(mem::transmute(key)) },
            unsafe { ptr::read(mem::transmute(value)) }))
        {
            CRUDOperationResult::Inserted(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    fn update(&self, key: *const u8, _key_sz: usize, value: *const u8, _value_sz: usize) -> bool {
        match self.dispatch_crud(CRUDOperation::Update(
            unsafe { ptr::read(mem::transmute(key)) },
            unsafe { ptr::read(mem::transmute(value)) }))
        {
            CRUDOperationResult::Updated(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    fn remove(&self, key: *const u8, _key_sz: usize) -> bool {
        match self.dispatch_crud(CRUDOperation::Delete(
            unsafe { ptr::read(mem::transmute(key)) }))
        {
            CRUDOperationResult::Deleted(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    fn scan(&self, key: *const u8, _key_sz: usize, mut scan_sz: i32, mut values_out: *mut *mut u8) -> i32 {
        let querying_v
            = self.current_version();

        let mut result
            = Vec::<*mut RecordPointResult<u64, f64>>::with_capacity(scan_sz as _);

        let key_start = unsafe { *(key as *const u64) };
        let key_end = key_start + scan_sz as u64 - 1;

        match self.dispatch_crud(CRUDOperation::Range(Interval::new(key_start, key_end), querying_v)) {
            CRUDOperationResult::MatchedRecords(mut buff) if !buff.is_empty() => unsafe {
                buff.shrink_to_fit();

                buff.iter()
                    .for_each(|r|
                    result.push(r as *const _ as *mut _));

                mem::forget(buff);
            }
            _ => {}
        }

        result.shrink_to_fit();
        unsafe {
            *values_out = result.as_mut_ptr() as _;
        }

        let len = result.len() as _;
        mem::forget(result);

        len
    }
}

// #[no_mangle]
// pub extern "C" fn _create_tree(_options: &tree_options_t) -> *mut c_void { // tree_api.hpp -> create_tree(...)
//     Box::into_raw(Box::new(MVBTreeApiExport(MVBTreeApi::orwc_optimistic_clock()))) as _
// }

#[no_mangle]
pub extern "C" fn init_tree() -> *mut c_void {
    Box::into_raw(Box::new(MVBTreeApiExport(MVBTreeApi::orwc_optimistic_clock()))) as _
}

#[no_mangle]
pub extern "C" fn destroy_tree_api(
    api: *mut c_void)
{
    if !api.is_null() {
        unsafe {
            let _tree = Box::from_raw(api as *mut MVBTreeApiExport);
        }
    }
}

#[no_mangle]
pub extern "C" fn tree_api_find(
    api: *mut c_void,
    key: *const u8,
    sz: usize,
    value_out: *mut u8) -> bool
{
    let api = unsafe { &*(api as *mut MVBTreeApiExport) };
    api.find(key, sz, value_out)
}

#[no_mangle]
pub extern "C" fn tree_api_insert(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize,
    value: *const u8,
    value_sz: usize) -> bool
{
    let api = unsafe { &*(api as *mut MVBTreeApiExport) };
    api.insert(key, key_sz, value, value_sz)
}

#[no_mangle]
pub extern "C" fn tree_api_update(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize,
    value: *const u8,
    value_sz: usize) -> bool
{
    let api = unsafe { &*(api as *mut MVBTreeApiExport) };
    api.update(key, key_sz, value, value_sz)
}

#[no_mangle]
pub extern "C" fn tree_api_remove(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize) -> bool
{
    let api = unsafe { &*(api as *mut MVBTreeApiExport) };
    api.remove(key, key_sz)
}

#[no_mangle]
pub extern "C" fn tree_api_scan(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize,
    scan_sz: i32,
    values_out: *mut *mut u8) -> i32
{
    let api = unsafe { &*(api as *mut MVBTreeApiExport) };
    api.scan(key, key_sz, scan_sz, values_out)
}

/************************************************************************/
/************************************************************************/
/************************************************************************/
// Start for MVBTree with GC handle API Export
/************************************************************************/
/************************************************************************/
/************************************************************************/

struct MVBTreeWithGCApiExport(TransactionManager<EX_FAN_OUT, EX_N, EX_KEY, EX_VALUE>);

impl Deref for MVBTreeWithGCApiExport {
    type Target = TransactionManager<EX_FAN_OUT, EX_N, EX_KEY, EX_VALUE>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[no_mangle]
pub extern "C" fn init_tree_gc() -> *mut c_void {
    const CONSTRUCTOR_THREAD_COUNT: usize = 1;

    Box::into_raw(Box::new(MVBTreeWithGCApiExport(
        TransactionManager::new_with_gc(CONSTRUCTOR_THREAD_COUNT, MVBTreeApi::orwc_optimistic_clock())))) as _
}

#[no_mangle]
pub extern "C" fn destroy_tree_gc_api(
    api: *mut c_void)
{
    if !api.is_null() {
        unsafe {
            let _tree = Box::from_raw(api as *mut MVBTreeWithGCApiExport);
        }
    }
}

#[no_mangle]
pub extern "C" fn tree_gc_api_find(
    api: *mut c_void,
    key: *const u8,
    sz: usize,
    value_out: *mut u8) -> bool
{
    let api = unsafe { &*(api as *mut MVBTreeWithGCApiExport) };
    api.find(key, sz, value_out)
}

#[no_mangle]
pub extern "C" fn tree_gc_api_insert(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize,
    value: *const u8,
    value_sz: usize) -> bool
{
    let api = unsafe { &*(api as *mut MVBTreeWithGCApiExport) };
    api.insert(key, key_sz, value, value_sz)
}

#[no_mangle]
pub extern "C" fn tree_gc_api_update(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize,
    value: *const u8,
    value_sz: usize) -> bool
{
    let api = unsafe { &*(api as *mut MVBTreeWithGCApiExport) };
    api.update(key, key_sz, value, value_sz)
}

#[no_mangle]
pub extern "C" fn tree_gc_api_remove(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize) -> bool
{
    let api = unsafe { &*(api as *mut MVBTreeWithGCApiExport) };
    api.remove(key, key_sz)
}

#[no_mangle]
pub extern "C" fn tree_gc_api_scan(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize,
    scan_sz: i32,
    values_out: *mut *mut u8) -> i32
{
    let api = unsafe { &*(api as *mut MVBTreeWithGCApiExport) };
    api.scan(key, key_sz, scan_sz, values_out)
}

impl MVBTreeWithGCApiExport {
    #[inline(always)]
    fn si(&self) -> IsolatedSnapShot<EX_FAN_OUT, EX_N, EX_KEY, EX_VALUE> {
        self.index().snapshot_current()
    }

    #[inline(always)]
    fn find(&self, key: *const u8, _sz: usize, value_out: *mut u8) -> bool {
        let querying_v
            = self.index().current_version();

        match self.execute_on_caller_thread(AtomicTransaction::new(
            Some(querying_v),
            CRUDOperation::Point(unsafe { ptr::read(mem::transmute(key)) }, querying_v))
        ).unwrap_atomic()
        {
            Ok((.., CRUDOperationResult::MatchedRecords(result)))
            if !result.is_empty() => unsafe {
                ptr::write(mem::transmute(value_out), result.get_unchecked(0).payload);
                true
            },
            _ => false
        }
    }

    #[inline(always)]
    fn insert(&self, key: *const u8, _key_sz: usize, value: *const u8, _value_sz: usize) -> bool {
        match self.execute_on_caller_thread(AtomicTransaction::from_crud(CRUDOperation::Insert(
            unsafe { ptr::read(mem::transmute(key)) },
            unsafe { ptr::read(mem::transmute(value)) }))
        ).unwrap_atomic()
        {
            Ok((.., CRUDOperationResult::Inserted(..))) => true,
            _ => false
        }
    }

    #[inline(always)]
    fn update(&self, key: *const u8, _key_sz: usize, value: *const u8, _value_sz: usize) -> bool {
        match self.execute_on_caller_thread(AtomicTransaction::from_crud(CRUDOperation::Update(
            unsafe { ptr::read(mem::transmute(key)) },
            unsafe { ptr::read(mem::transmute(value)) }))
        ).unwrap_atomic()
        {
            Ok((.., CRUDOperationResult::Updated(..))) => true,
            _ => false
        }
    }

    #[inline(always)]
    fn remove(&self, key: *const u8, _key_sz: usize) -> bool {
        match self.execute_on_caller_thread(AtomicTransaction::from_crud(CRUDOperation::Delete(
            unsafe { ptr::read(mem::transmute(key)) }))
        ).unwrap_atomic()
        {
            Ok((.., CRUDOperationResult::Deleted(..))) => true,
            _ => false
        }
    }

    #[inline(always)]
    fn scan(&self, key: *const u8, _key_sz: usize, mut scan_sz: i32, mut values_out: *mut *mut u8) -> i32 {
        let querying_v
            = self.index().current_version();

        let mut result
            = Vec::<*mut RecordPointResult<u64, f64>>::with_capacity(scan_sz as _);

        let key_start = unsafe { *(key as *const u64) };
        let key_end = key_start + scan_sz as u64 - 1;

        match self.execute_on_caller_thread(AtomicTransaction::new(
                Some(querying_v),
                CRUDOperation::Range(Interval::new(key_start, key_end), querying_v))
        ).unwrap_atomic()
        {
            Ok((.., CRUDOperationResult::MatchedRecords(mut buff))) if !buff.is_empty() => unsafe {
                buff.shrink_to_fit();

                buff.iter()
                    .for_each(|r| result.push(r as *const _ as *mut _));

                mem::forget(buff);
            }
            _ => {}
        }

        result.shrink_to_fit();
        unsafe {
            *values_out = result.as_mut_ptr() as _;
        }

        let len = result.len() as _;
        mem::forget(result);

        len
    }
}