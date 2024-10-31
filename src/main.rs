use std::{env, fs, mem, thread};
use std::alloc::{GlobalAlloc, Layout, System};
use std::fs::OpenOptions;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{fence, AtomicUsize};
use std::sync::atomic::Ordering::SeqCst;
use std::time::{Duration, SystemTime};
use CCBPlusTree::test;
// use cc_bplustree::mv_tree::bplus_tree::BPlusTree;
use chrono::{DateTime, Local};
use itertools::{Either, Itertools};
use rand::prelude::{SliceRandom, StdRng};
use rand::{SeedableRng, thread_rng};
// use rayon::iter::{IntoParallelIterator, ParallelIterator};
use crate::mv_block::block::Block;
use crate::mv_tree::mvbplus_tree;
use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation::{CRUDOperation, TxAtomicOperation};
use crate::mv_crud_model::crud_operation::CRUDOperation::Point;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_crud_model::query::RangeQueryIter;
use crate::mv_page_model::internal_page::{InternalPage, TimeMatcher};
use crate::mv_page_model::leaf_page::LeafPage;
use crate::mv_page_model::node::Node;
use crate::mv_record_model::version_info::Version;
use crate::mv_test::{alloc_memory_force, allocate_free, format_insertions, INDEX, Key, MAKE_INDEX, Payload, experiment, gen_data_exp, IndexHandler};
use crate::mv_tree::mvbplus_tree::{ClockType, MVBPlusTree};
use crate::mv_tree::locking_strategy::{CRUDProtocol, LHL_read, LockingStrategy, OLC, orwc};
use crate::mv_tx_model::transaction::{AtomicTransaction, SnapShot};
use crate::mv_tx_model::tx_manager::TransactionManager;
use crate::mv_utils::interval::Interval;
use crate::mv_utils::smart_cell::ENABLE_YIELD;

mod mv_block;
mod mv_crud_model;
mod mv_page_model;
mod mv_record_model;
mod mv_tree;
mod mv_utils;
mod mv_test;
mod mv_tx_model;

pub const TREE: fn(CRUDProtocol) -> Tree = |crud| {
    Arc::new(MAKE_INDEX(crud))
};

fn mk_payload() -> Box<()> {
    unsafe {
        mem::transmute(Box::into_raw(Box::new(())))
    }
}

const FAN_OUT: usize = mv_test::FAN_OUT;
const NUM_RECORDS: usize = mv_test::NUM_RECORDS;

pub type MVTree = MVBPlusTree::<FAN_OUT, NUM_RECORDS, u64, f64>;

// struct NoCacheAllocator;
// unsafe impl GlobalAlloc for NoCacheAllocator {
//     unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
//         System.alloc(layout)
//     }
//     unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
//         System.dealloc(ptr, layout)
//     }
// }
//
// #[global_allocator]
// static GLOBAL: NoCacheAllocator = NoCacheAllocator;

static TOTAL_TX_COUNTER: AtomicUsize = AtomicUsize::new(0);


fn main() {
    make_splash();

    let protocol = LockingStrategy::OLC;
    let clock = ClockType::OPTIMISTIC;

    println!("---------------------------------------------------------------------------------");
    println!("[Configuration] - Protocol = {protocol}, Clock = {clock}");
    println!("---------------------------------------------------------------------------------");

    let range_start = 0;
    let range_end = u64::MAX;
    let lambda = 0.1;
    let gc_enable = true;
    let threads = num_cpus::get();
    let total_tx = 10_000_000;

    let insert_ratio = 0;
    let update_ratio = 0;
    let point_reads_ratio = 50;
    let range_reads_ratio = 50;

    let range_size = 100;

    let index_handler =  run_experiment_with_params(
        threads,
        Either::Right((protocol, clock)),
        gc_enable,
        lambda,
        range_start,
        range_end,
        100,
        0,
        0,
        0,
        0,
        total_tx,
    );

    // Start Experiment
    let index_handler = run_experiment_with_params(
        threads,
        index_handler,
        gc_enable,
        lambda,
        range_start,
        range_end,
        insert_ratio,
        update_ratio,
        point_reads_ratio,
        range_reads_ratio,
        range_size,
        total_tx,
    );
    // End Experiment
}

fn run_experiment_with_params(threads: usize,
                              index: IndexHandler,
                              gc_enable: bool,
                              lambda: f64,
                              range_start: u64,
                              range_end: u64,
                              insert_ratio: usize,
                              update_ratio: usize,
                              point_reads_ratio: usize,
                              range_reads_ratio: usize,
                              range_size: u64,
                              total_tx: usize
) -> IndexHandler {
    let (index_handler, handles) = experiment(
        threads,
        index,
        gc_enable,
        lambda,
        range_start,
        range_end,
        insert_ratio,
        update_ratio,
        point_reads_ratio,
        range_reads_ratio,
        range_size,
        &TOTAL_TX_COUNTER,
    );

    while TOTAL_TX_COUNTER.load(SeqCst) < total_tx {
        thread::yield_now();
    }

    let bulk_killer = handles
        .into_iter()
        .map(|(handle, killer)| {
            drop(killer);
            handle
        }).collect_vec();

    let result = bulk_killer
        .into_iter()
        .map(|handle|
            handle.join().unwrap())
        .collect_vec();

    let mut total_executed_tx = 0;
    let mut total_time = 0;
    for (index, (tx_success, tx_error, time)) in result.iter().enumerate() {
        println!("\t[tid_{index}]: tx_success = {tx_success}, tx_error = {tx_error}, time = {time}");
        total_executed_tx += tx_success + tx_error;
        total_time = total_time.max(*time);
    }
    println!("---------------------------------------------------------------------------------");
    println!("[Summary] - Tx Executed = {total_executed_tx}, Target Tx = {total_tx}, Total Time = {total_time}");
    println!("---------------------------------------------------------------------------------");

    TOTAL_TX_COUNTER.store(0, SeqCst);
    index_handler
}

/// Essential function.
fn make_splash() {
    let datetime: DateTime<Local> = fs::metadata(std::env::current_exe().unwrap())
        .unwrap().modified().unwrap().into();

    println!("                         _________________________");
    println!("                 _______/                         \\_______");
    println!("                /                                         \\");
    println!(" +-------------+                                           +-------------+");
    println!(" |                                                                       |");
    println!(" |               ------------------------------                          |");
    println!(" |               # Build:   {}                          |", datetime.format("%d-%m-%Y %T"));
    println!(" |               # Current version: {}                               |", env!("CARGO_PKG_VERSION"));
    println!(" |               -------------------------                               |");
    println!(" |               # OLC-HLE:   {}                                     |", hle());
    println!(" |               # RW-HLE:    AUTO                                       |");
    println!(" |               # SYS-YIELD: {}                                       |",
             if ENABLE_YIELD { "ON  " } else { "OFF " });
    println!(" |               -----------------                                       |");
    println!(" |                                                                       |");
    println!(" |               --------------------------------------------            |");
    println!(" |               # E-Mail: elshaikh@mathematik.uni-marburg.de            |");
    println!(" |               # Written by: Amir El-Shaikh                            |");
    println!(" |               # First released: 02-01-2024                            |");
    println!(" |               ----------------------------                            |");
    println!(" |                                                                       |");
    println!(" |               ...MV-B+Tree Application Launching...                   |");
    println!(" +-------------+                                           +-------------+");
    println!("                \\_______                           _______/");
    println!("                        \\_________________________/");

    println!();
    println!("--> System Log:");
}

pub type Tree = Arc<INDEX>;

pub fn hle() -> &'static str {
    if cfg!(feature = "hardware-lock-elision") {
        if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
            "ON    "
        } else {
            "NO HLE"
        }
    } else {
        "OFF   "
    }
}