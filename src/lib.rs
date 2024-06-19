use std::ffi::{c_void, CString};
use std::{mem, ptr};
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

struct MVBTreeApi(mv_tree::mvbplus_tree::MVBPlusTree<127, 127, u64, f64>);

use crate::mv_crud_model::crud_api::CRUDDispatcher;

impl MVBTreeApi {
    #[inline(always)]
    fn find(&self, key: *const u8, _sz: usize, value_out: *mut u8) -> bool {
        match self.0.dispatch_crud(CRUDOperation::PointSi(
            unsafe { ptr::read(mem::transmute(key)) }))
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
        match self.0.dispatch_crud(CRUDOperation::Insert(
            unsafe { ptr::read(mem::transmute(key)) },
            unsafe { ptr::read(mem::transmute(value)) }))
        {
            CRUDOperationResult::Inserted(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    fn update(&self, key: *const u8, _key_sz: usize, value: *const u8, _value_sz: usize) -> bool {
        match self.0.dispatch_crud(CRUDOperation::Update(
            unsafe { ptr::read(mem::transmute(key)) },
            unsafe { ptr::read(mem::transmute(value)) }))
        {
            CRUDOperationResult::Updated(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    fn remove(&self, key: *const u8, _key_sz: usize) -> bool {
        match self.0.dispatch_crud(CRUDOperation::Delete(
            unsafe { ptr::read(mem::transmute(key)) }))
        {
            CRUDOperationResult::Deleted(..) => true,
            _ => false
        }
    }

    #[inline(always)]
    fn scan(&self, key: *const u8, _key_sz: usize, mut scan_sz: i32, mut values_out: *mut *mut u8) -> i32 {
        let mut result
            = Vec::<*mut RecordPointResult<u64, f64>>::new();

        let mut count = 0;
        while scan_sz > 0 {
            match self.0.dispatch_crud(CRUDOperation::PointSi(unsafe {
                ptr::read(mem::transmute::<_, *const u64>(key).add(count))
            }))
            {
                CRUDOperationResult::MatchedRecords(mut buff) if !buff.is_empty() => unsafe {
                    buff.shrink_to_fit();
                    result.push(buff.get_unchecked(0) as *const _ as *mut _);

                    mem::forget(buff);
                }
                _ => {}
            }

            scan_sz -= 1;
            count += 1;
        }

        result.shrink_to_fit();
        values_out = result.as_mut_ptr() as _;

        let len = result.len() as _;
        mem::forget(result);

        len
    }
}

#[no_mangle]
pub extern "C" fn _create_tree(_options: &tree_options_t) -> *mut c_void { // tree_api.hpp -> create_tree(...)
    Box::into_raw(Box::new(
            MVBTreeApi(crate::mv_tree::mvbplus_tree::MVBPlusTree::<127, 127, u64, f64>::
            orwc_optimistic_clock()))) as _
}

#[no_mangle]
pub extern "C" fn init_tree() -> *mut c_void {
    Box::into_raw(Box::new(
        MVBTreeApi(crate::mv_tree::mvbplus_tree::MVBPlusTree::<127, 127, u64, f64>::
        orwc_optimistic_clock()))) as _
}

#[no_mangle]
pub extern "C" fn destroy_tree_api(
    api: *mut c_void)
{
    if !api.is_null() {
        unsafe {
            let _tree = Box::from_raw(api as *mut MVBTreeApi);
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
    let api = unsafe { &*(api as *mut MVBTreeApi) };
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
    let api = unsafe { &*(api as *mut MVBTreeApi) };
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
    let api = unsafe { &*(api as *mut MVBTreeApi) };
    api.update(key, key_sz, value, value_sz)
}

#[no_mangle]
pub extern "C" fn tree_api_remove(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize) -> bool
{
    let api = unsafe { &*(api as *mut MVBTreeApi) };
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
    let api = unsafe { &*(api as *mut MVBTreeApi) };
    api.scan(key, key_sz, scan_sz, values_out)
}


