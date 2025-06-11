use std::ffi::c_void;
use std::{mem, ptr};
use std::ops::Deref;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_tree::mvbplus_tree::MVBPlusTree;
use crate::mv_tx_model::transaction::AtomicTransaction;
use mv_gc::tx_manager::TransactionManager;
use crate::mv_utils::interval::Interval;

mod mv_block;
mod mv_crud_model;
mod mv_page_model;
mod mv_record_model;
mod mv_tree;
mod mv_utils;
mod mv_test;
mod mv_tx_model;
mod mv_gc;

const EX_FAN_OUT: usize = 127;
const EX_N: usize = 127;

type EX_KEY = u64;
type EX_VALUE = u64;

type MVBTreeApi = MVBPlusTree<EX_FAN_OUT, EX_N, EX_KEY, EX_VALUE>;

pub const MONO: u8 = 0;
pub const OLC: u8 = 2;

pub const OPT_CLOCK: u8 = 0;
pub const EXCL_CLOCK: u8 = 1;
pub const FREE_CLOCK: u8 = 2;
pub const GC_ENABLED: u8 = 1;

struct MVBTreeWithGCApiExport(TransactionManager<EX_FAN_OUT, EX_N, EX_KEY, EX_VALUE>);

impl Deref for MVBTreeWithGCApiExport {
    type Target = TransactionManager<EX_FAN_OUT, EX_N, EX_KEY, EX_VALUE>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[no_mangle]
pub extern "C" fn init_tree_gc(protocol: u8, clock: u8, gc: u8) -> *mut c_void {
    let index = match (protocol, clock) {
        (OLC, OPT_CLOCK) => MVBTreeApi::olc_optimistic_clock(),
        (OLC, EXCL_CLOCK) => MVBTreeApi::olc_optimistic_clock(),
        _ => MVBTreeApi::default()
    };
    
    Box::into_raw(Box::new(MVBTreeWithGCApiExport(
        TransactionManager::new_unmanaged(index, gc == GC_ENABLED)))) as _
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

        let key_start = unsafe { *(key as *const u64) };
        let key_end = key_start + scan_sz as u64 - 1;

        match self.execute_on_caller_thread(AtomicTransaction::new(
            Some(querying_v),
            CRUDOperation::Range(Interval::new(key_start, key_end), querying_v))
        ).unwrap_atomic()
        {
            Ok((.., CRUDOperationResult::MatchedRecords(mut buff))) => unsafe {
                buff.shrink_to_fit();

                let len = buff.len() as _;
                *values_out = buff.as_mut_ptr() as _;

                mem::forget(buff);
                len
            }
            _ => -1
        }
    }
}