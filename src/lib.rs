use std::ffi::c_void;
use std::{mem, ptr};
use std::ops::Deref;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_tx_model::transaction::AtomicTransaction;
use mv_tx_query::tx_manager::TransactionManager;
use crate::mv_root::index_root::RootIndexType;
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
mod mv_root;
mod mv_query;
mod mv_tx_query;
mod mv_sync;

const EX_FAN_OUT: usize = 127;
const EX_N: usize = 127;

type EX_KEY = u64;
type EX_VALUE = u64;

type MVBTreeApi = MVBTSt<EX_FAN_OUT, EX_N, EX_KEY, EX_VALUE>;

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

#[unsafe(no_mangle)]
pub extern "C" fn init_tree_gc(protocol: u8, clock: u8, gc: u8) -> *mut c_void {
    let index = match (protocol, clock) {
        (OLC, OPT_CLOCK) => MVBTreeApi::make_standard(RootIndexType::default()),
        (OLC, EXCL_CLOCK) => MVBTreeApi::make_standard(RootIndexType::default()),
        _ => MVBTreeApi::default()
    };
    
    Box::into_raw(Box::new(MVBTreeWithGCApiExport(
        TransactionManager::new_unmanaged(index, gc == GC_ENABLED)))) as _
}

#[unsafe(no_mangle)]
pub extern "C" fn destroy_tree_gc_api(
    api: *mut c_void)
{
    if !api.is_null() {
        unsafe {
            let _tree = Box::from_raw(api as *mut MVBTreeWithGCApiExport);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tree_gc_api_find(
    api: *mut c_void,
    key: *const u8,
    sz: usize,
    value_out: *mut u8) -> bool
{
    let api = unsafe { &*(api as *mut MVBTreeWithGCApiExport) };
    api.find(key, sz, value_out)
}

#[unsafe(no_mangle)]
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

#[unsafe(no_mangle)]
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

#[unsafe(no_mangle)]
pub extern "C" fn tree_gc_api_remove(
    api: *mut c_void,
    key: *const u8,
    key_sz: usize) -> bool
{
    let api = unsafe { &*(api as *mut MVBTreeWithGCApiExport) };
    api.remove(key, key_sz)
}

#[unsafe(no_mangle)]
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
        let (querying_v, _pin)
            = self.index().pinned_reader_snapshot();

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
        let (querying_v, _pin)
            = self.index().pinned_reader_snapshot();

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
#[cfg(test)]
mod gc_stress {
    use super::*;
    use crate::mv_record_model::record_point::RecordPointResult;
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
    use std::thread;
    use std::time::{Duration, Instant};

    const STABLE_STRIDE: u64 = 10; // keys divisible by this are inserted once and never touched again
    const KEYS: u64 = 20_000;

    /// Writers churn the non-stable keys (insert/delete -> constant node reorganizations, i.e.,
    /// GC reuse), while scanners check that the committed snapshot stays intact:
    /// every stable key shows up, keys are strictly ascending, and payload == key.
    /// A node reused while a reader still traverses it breaks one of these.
    #[test]
    fn scans_stay_consistent_under_gc_reuse() {
        let _serial = crate::mv_sync::TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let tree = Box::into_raw(Box::new(MVBTreeWithGCApiExport(
            TransactionManager::new_unmanaged(MVBTreeApi::default(), true)))) as usize;

        let api = || unsafe { &*(tree as *mut MVBTreeWithGCApiExport) };

        for k in (0..KEYS).filter(|k| k % STABLE_STRIDE == 0) {
            assert!(api().insert(&k as *const u64 as _, 8, &k as *const u64 as _, 8));
        }

        let stop = AtomicBool::new(false);
        let violations = std::sync::atomic::AtomicUsize::new(0);

        thread::scope(|s| {
            for w in 0..4u64 {
                let (stop, api) = (&stop, &api);
                s.spawn(move || {
                    let mut x = 0x9E3779B97F4A7C15u64 ^ w;
                    while !stop.load(Relaxed) {
                        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
                        let k = x % KEYS;
                        if k % STABLE_STRIDE == 0 { continue }
                        if x & (1 << 40) == 0 {
                            api().insert(&k as *const u64 as _, 8, &k as *const u64 as _, 8);
                        } else {
                            api().remove(&k as *const u64 as _, 8);
                        }
                    }
                });
            }

            for r in 0..4u64 {
                let (stop, api, violations) = (&stop, &api, &violations);
                s.spawn(move || {
                    let mut x = 0xD1B54A32D192ED03u64 ^ r;
                    while !stop.load(Relaxed) {
                        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
                        let start = x % (KEYS - 500);
                        let mut out: *mut u8 = ptr::null_mut();
                        let n = api().scan(&start as *const u64 as _, 8, 500, &mut out);
                        assert!(n >= 0);
                        let got = unsafe {
                            Vec::from_raw_parts(out as *mut RecordPointResult<u64, u64>, n as usize, n as usize)
                        };

                        let ascending = got.windows(2).all(|w| w[0].key < w[1].key);
                        let payloads = got.iter().all(|r| r.payload == r.key);
                        let stable_seen = got.iter().filter(|r| r.key % STABLE_STRIDE == 0).count() as u64;
                        let stable_expected = (start..start + 500).filter(|k| k % STABLE_STRIDE == 0).count() as u64;

                        if !(ascending && payloads && stable_seen == stable_expected) {
                            violations.fetch_add(1, Relaxed);
                        }
                    }
                });
            }

            let t = Instant::now();
            while t.elapsed() < Duration::from_secs(20) && violations.load(Relaxed) == 0 {
                thread::sleep(Duration::from_millis(100));
            }
            stop.store(true, Relaxed);
        });

        assert_eq!(violations.load(Relaxed), 0, "a scan observed a corrupted snapshot");
        unsafe { drop(Box::from_raw(tree as *mut MVBTreeWithGCApiExport)) }
    }
}
