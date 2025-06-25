use std::{env, fs, mem};
use std::sync::Arc;
use chrono::{DateTime, Local};
use itertools::{Either, Itertools};
use libc::exit;
use rand::prelude::SliceRandom;
use rand_distr::Zipf;
use crate::mv_block::block::Block;
use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_gc::tx_manager::TransactionManager;
use crate::mv_record_model::record_point::RecordPointResult;
use crate::mv_record_model::version_info::Version;
use crate::mv_test::{GroupConfig, Key, Payload, VERBOSE, FAN_OUT, NUM_RECORDS, height_root, F_MUL, FILLED_BLOCK, F_OFF, F_ABS_OFF, N_MUL, N_OFF, N_ABS_OFF};
use crate::mv_tree::mvbplus_tree::MVBPlusTree;
use crate::mv_utils::safe_cell::SafeCell;

mod mv_block;
mod mv_crud_model;
mod mv_page_model;
mod mv_record_model;
mod mv_tree;
mod mv_utils;
mod mv_test;
mod mv_tx_model;
mod mv_gc;

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

// static TOTAL_TX_COUNTER: AtomicUsize = AtomicUsize::new(0);

const MANUEL_MAIN: bool = false;

fn main() {
    if MANUEL_MAIN {
        manuel_main()
    }
    else {
        println!(">>HLE: \t\t\t{}", hle());

        let block_size = size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>();
        let kb = block_size as f32 / 1024f32;
        println!("\
           >>FAN_OUT: \t\t{FAN_OUT}\n\
           >>NUM_RECORDS: \t\t{NUM_RECORDS}\n\
           >>size_of(BLOCK): \t{} bytes; {kb} kb\n\
           >>size_of(PTR): \t{} bytes",
                 size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>(),
                 mem::size_of::<*const ()>());
        println!();

        make_splash();
        mv_test::execute_experiments();
    }
}

/// Essential function.
fn make_splash() {
    let datetime: DateTime<Local> = fs::metadata(env::current_exe().unwrap())
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
    println!(" |               # HLE:   {}                                         |", hle());
    // println!(" |               # RW-HLE:    AUTO                                       |");
    println!(" |               -----------------                                       |");
    println!(" |                                                                       |");
    println!(" |               --------------------------------------------            |");
    println!(" |               # E-Mail: elshaikh@mathematik.uni-marburg.de            |");
    println!(" |               # Written by: Amir El-Shaikh                            |");
    println!(" |               # First released: 02-01-2024                            |");
    println!(" |               # Repository: https://github.com/umr-dbs/MV-BPlusTree   |");
    println!(" |               -----------------------------------------------------   |");
    println!(" |                                                                       |");
    println!(" |               ...MV-B⁺Tree Application Launching...                   |");
    println!(" +-------------+                                           +-------------+");
    println!("                \\_______                           _______/");
    println!("                        \\_________________________/");

    println!();
    println!("--> System Log:");
}

fn manuel_main() {
    type MVTree = MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

    let mv_tree = MVTree::default();
    let n = 9990;

    let inserts = vec![75, 91, 78, 24, 82, 3, 10, 38, 57, 81, 51, 67, 73,
                       14, 37, 87, 26, 33, 66, 12, 99, 61, 29, 20, 45, 27,
                       32, 21, 6, 52, 4, 35, 16, 58, 8, 28, 23, 97, 63, 9,
                       92, 22, 17, 30, 79, 42, 84, 59, 31];
    
    let max = inserts.iter().max().unwrap().clone();

    let updates = vec![27, 63, 57, 45, 61, 59, 16, 8, 9,
                       78, 6, 23, 4, 17, 67, 79, 87, 66, 97, 75, 20, 22, 12,
                       29];

    // let updates = vec![];

    let deletes = vec![14, 87, 37, 59, 97, 31, 30, 21, 73,
                       4, 29, 78, 66, 35, 99, 32, 8, 10, 6, 81, 51, 45, 42,
                       79, 82, 22, 23, 33, 75, 26, 3, 61];

    let logged_inserts = Arc::new(SafeCell::new(vec![]));
    
    let check_integrity = || for key in 0..=max*2 {
        // println!("Query: {:?}", (key, snapshot));
        if let CRUDOperationResult::MatchedRecords(record)
            = mv_tree.dispatch_crud(CRUDOperation::Point(key, Version::MAX - 1))
        {
            if record.is_empty() && inserts.contains(&key){
                panic!("No point record found");
            }
        }
        else {
            panic!("Error Point key: {}", key);
        }
    };

    // Inserts
    for key in inserts.clone() {
        let crud
            = mv_tree.dispatch_crud(CRUDOperation::Insert(key, key));

        logged_inserts.get_mut().push(if let CRUDOperationResult::Inserted(v) = crud {
            (key, v)
        } else {
            println!("Error Insert key = {key}");
            unsafe {
                exit(1);
            }
        })
    }

    check_integrity();
    println!("Finish insert");
    // Updates
    // inserts.shuffle(&mut rand::rng());

    // println!("Updates: {:?}", updates);
    for key in updates.iter() {
        if *key == 29 {
            let s = "adasd".to_string();
        }
        let update
            = mv_tree.dispatch_crud(CRUDOperation::Update(*key, *key));

        check_integrity();

        logged_inserts.get_mut().push(if let CRUDOperationResult::Updated(v) = update {
            (*key, v)
        } else {
            println!("Update error key: {key}");
            unsafe {
                exit(1);
            }
        });
    }
    println!("Finish update");
    
    // inserts.shuffle(&mut rand::rng());

    // println!("Deletes: {:?}", deletes);
    // Deletes
    for key in deletes.iter() {
        if *key == 61{
            let s = "adasd".to_string();
        }
        let crud
            = mv_tree.dispatch_crud(CRUDOperation::Delete(*key));

        if let CRUDOperationResult::Deleted(d) = crud {
            logged_inserts.get_mut().push((*key, d));
            println!("Delete key: {key}");
        }
        else {
            println!("Delete error key: {key}");
            // unsafe {
            //     exit(1);
            // }
        }
    }
}

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