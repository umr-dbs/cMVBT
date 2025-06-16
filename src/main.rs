use std::{env, fs, mem};
use std::sync::Arc;
use chrono::{DateTime, Local};
use itertools::{Either, Itertools};
use rand_distr::Zipf;
use crate::mv_block::block::Block;
use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_gc::tx_manager::TransactionManager;
use crate::mv_record_model::version_info::Version;
use crate::mv_test::{GroupConfig, Key, Payload, VERBOSE, FAN_OUT, NUM_RECORDS, height_root, F_MUL, FILLED_BLOCK, F_OFF, F_ABS_OFF, N_MUL, N_OFF, N_ABS_OFF};
use crate::mv_tree::mvbplus_tree::MVBPlusTree;

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
    let n = 999;

    let mut inserts = vec![];
    for key in 1..=n {
        let crud = mv_tree.dispatch_crud(CRUDOperation::Insert(key, key));
        if let CRUDOperationResult::Error = crud {
            panic!("Error insert")
        }
        inserts.push(if let CRUDOperationResult::Inserted(v) = crud {
            (key, v)
        } else {
            panic!()
        })
        // println!("{crud}");
    }

    for (key, v) in inserts.iter() {
        let crud = mv_tree.dispatch_crud(CRUDOperation::Point(*key, *v));
        if let CRUDOperationResult::MatchedRecords(records) = crud {
            println!("find {}", records.iter().join(","))
        } else {
            panic!("find error for key: {key}, version: {v}")
        }
    }

    for (key, v) in inserts.iter() {
        let crud
            = mv_tree.dispatch_crud(CRUDOperation::Delete(*key));

        if let CRUDOperationResult::Deleted(d) = crud {
            println!("Deleted {}, delete version: {d}", key);

            if let CRUDOperationResult::MatchedRecords(found) =
                mv_tree.dispatch_crud(CRUDOperation::Point(*key, d))
            {
                if !found.is_empty() {
                    println!("root version: {}", mv_tree.root.0.version);
                    panic!("Matched wrong version: {}", found.iter().join(","));
                }
            }

            for (key, v) in inserts.iter() {
                let crud = mv_tree.dispatch_crud(CRUDOperation::Point(*key, *v));
                if let CRUDOperationResult::MatchedRecords(records) = crud {
                    if records.is_empty() {
                        panic!("No record found of key: {key}, version: {v}");
                    } else {}
                } else {
                    panic!("find error for key after delete: {key}, version: {v}")
                }
            }
        }
        else {
            panic!("Delete error key: {key}, version: {v}")
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