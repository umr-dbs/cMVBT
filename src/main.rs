use std::{env, fs, mem, thread};
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use chrono::{DateTime, Local};
use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::thread_rng;
// use rayon::iter::{IntoParallelIterator, ParallelIterator};
use crate::block::block::Block;
use crate::tree::mvbplus_tree;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::{CRUDOperation, TxAtomicOperation};
use crate::crud_model::crud_operation::CRUDOperation::Point;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::crud_model::query::RangeQueryIter;
use crate::page_model::internal_page::{InternalPage, TimeMatcher};
use crate::page_model::leaf_page::LeafPage;
use crate::page_model::node::Node;
use crate::record_model::version_info::Version;
use crate::test::{format_insertions, INDEX, Key, MAKE_INDEX, test01, test02};
use crate::tree::mvbplus_tree::MVBPlusTree;
use crate::tree::locking_strategy::{CRUDProtocol, LHL_read, LockingStrategy, OLC, orwc};
use crate::tx_model::transaction::AtomicTransaction;
use crate::tx_model::tx_manager::TransactionManager;
use crate::utils::interval::Interval;
use crate::utils::smart_cell::ENABLE_YIELD;

mod block;
mod crud_model;
mod page_model;
mod record_model;
mod tree;
mod utils;
mod test;
mod tx_model;

pub const TREE: fn(CRUDProtocol) -> Tree = |crud| {
    Arc::new(MAKE_INDEX(crud))
};

fn mk_payload() -> Box<u8> {
    unsafe {
        mem::transmute(Box::leak(Box::new(0_usize)))
    }
}

const FAN_OUT: usize = test::FAN_OUT;
const NUM_RECORDS: usize = test::NUM_RECORDS;

pub type MVTree = MVBPlusTree::<FAN_OUT, NUM_RECORDS, u64>;

fn main() {
    make_splash();

    // println!("{}", mem::align_of::<Block<FAN_OUT, NUM_RECORDS, Key>>());
    // println!("InternalPage Align = {}, Size = {}",
    //          mem::align_of::<InternalPage<FAN_OUT, NUM_RECORDS, Key>>(),
    //          mem::size_of::<InternalPage<FAN_OUT, NUM_RECORDS, Key>>(),
    // );
    //
    // println!("LeafPage Align = {}, Size = {}",
    //          mem::align_of::<LeafPage<NUM_RECORDS, Key>>(),
    //          mem::size_of::<LeafPage<NUM_RECORDS, Key>>(),
    // );

    // let trees = vec![
    //     Arc::new(MVTree::orwc_optimistic_clock()),
    //     Arc::new(MVTree::lhl_optimistic_clock()),
    //     Arc::new(MVTree::olc_optimistic_clock()),
    // ];

    // println!("Records,Threads,Protocol,Errors,Time,Inserts,Reads");
    // for tree in trees.into_iter() {
    //     test01(tree.clone());
    //     test02(tree.clone());
    // }

    assert!(mem::size_of::<Block<FAN_OUT, NUM_RECORDS, u64>>() <= 4096);

    let tree
        = Arc::new(MVTree::olc_optimistic_clock());

    let insertions = 10_000_000_u64;
    // let mut last_insert_version = Version::MIN;
    // let mut version_inserts = vec![];
    //
    // for key in 0u64..insertions {
    //     match tree.dispatch(CRUDOperation::Insert(key, mk_payload())) {
    //         CRUDOperationResult::Inserted(ver) => {
    //             last_insert_version = ver;
    //             version_inserts.push(ver);
    //             // println!("Inserted at version {}", ver);
    //             match tree.dispatch(CRUDOperation::Point(key, ver)) {
    //                 CRUDOperationResult::MatchedRecords(found)
    //                 if found.last().unwrap().key == key => {}
    //                     // println!("Record(s) found ({}): {}", found.len(), found.into_iter().join(",")),
    //                 err => println!("Err at insertion {}", err),
    //             }
    //         }
    //         err => println!("Err at insertion {}", err),
    //     }
    // }

    // match tree.dispatch(CRUDOperation::Range(Interval::new(0, 255), last_insert_version)) {
    //     CRUDOperationResult::MatchedRecords(v) if v.len() == 256.min(insertions as usize) =>{}
    //         // println!("Range Query:\n\t{}", v.iter().join("\n\t")),
    //     _ => println!("Error Range")
    // }
    //
    // let lazy_range = RangeQueryIter::new(
    //     &tree,
    //     last_insert_version,
    //     Interval::new(0, insertions));
    //
    // println!("Height = {}", tree.root.unsafe_borrow().height());
    // println!("Lazy Range = {}, all = {insertions}", lazy_range.count());
    //
    // println!("Before Delete Height = {}", tree.root.unsafe_borrow().height);
    // for key in 0u64..insertions{
    //     match tree.dispatch(CRUDOperation::Delete(key)) {
    //         CRUDOperationResult::Deleted(v) => {}
    //             // println!("Key = {}, v = {} deleted", key, v),
    //         _ => println!("Error delete key = {}", key)
    //     }
    // }
    // for key in 0u64..insertions {
    //     // println!("Verified key = {key}");
    //     let r = tree
    //         .dispatch(CRUDOperation::Point(key, *version_inserts.get(key as usize).unwrap()));
    //     if let CRUDOperationResult::MatchedRecords(v) = r {
    //         if v.last().unwrap().key != key {
    //             println!("ERR")
    //         }
    //     }
    // }
    //
    // for key in 0u64..insertions as u64 {
    //     match tree.dispatch(CRUDOperation::Point(key, last_insert_version)) {
    //         CRUDOperationResult::MatchedRecords(mut v) if v.last().unwrap().key == key => {}
    //             // println!("Found Point  {}", v.pop().unwrap()),
    //         err => panic!("Point failed: {}, key = {}", err, key)
    //     }
    // }
    //
    // let (keys, versions) = tree.root.unsafe_borrow()
    //     .root.block.unsafe_borrow().as_internal_page_ref().keys_versions();
    //
    // println!("Keys Root\n{}", keys
    //     .iter()
    //     .zip(versions)
    //     .filter(|(.., v)| v.is_active())
    //     .map(|(k, v)|
    //         format!("{k}, v: {v}")).into_iter().join("\n"));


    // let mut insertions_vec = (0..insertions)
    //     .map(|k| CRUDOperation::Insert(k, mk_payload()))
    //     .collect_vec();
    //
    // insertions_vec.extend((0..insertions).map(|k| Point(k, k)));
    // let (time, ..) = test::bulk_crud(
    //     num_cpus::get(),
    //     tree.clone(),
    //     insertions_vec.as_slice());
    // //
    // println!("Insertions = {}, Time = {time}ms", format_insertions(insertions_vec.len()));
    println!("Insertions,Threads,Protocol,Clock,Time");
    // let insertions = 2_000_000_u64;

    let mut all_tx = (0u64..insertions)
        .map(|key| AtomicTransaction::new_latest_si(TxAtomicOperation::Insert(key, mk_payload())))
        .collect_vec();

    all_tx.extend((0..insertions).map(|key|
        AtomicTransaction::new_latest_si(TxAtomicOperation::PointSi(key))));

    all_tx.shuffle(&mut thread_rng());
    let mut tx_manager = TransactionManager::new_with_gc(
        num_cpus::get(), MVTree::olc_optimistic_clock());

    // tx_manager.disable_gc();

    let start = SystemTime::now();

    all_tx
        .into_iter()
        .for_each(|tx|
            tx_manager.execute_tx_non_reader(tx));

    tx_manager.join();

    let end = SystemTime::now().duration_since(start).unwrap().as_millis();

    println!("{insertions},{},{},{},{end}",
             tx_manager.threads(),
             tx_manager.locking_protocol(),
             tx_manager.clock_type());

    // let mut insertions_vec = (0u64..insertions)
    //     .map(|key| CRUDOperation::Insert(key, mk_payload()).into())
    //     .collect_vec();
    //
    // insertions_vec.shuffle(&mut thread_rng());


    // println!("Insertions,Threads,Protocol,Clock,Time");
    // for btree in [
    //     MVTree::olc_optimistic_clock(),
    //     MVTree::lhl_optimistic_clock(),
    //     MVTree::orwc_optimistic_clock()]
    // {
    //     for threads in [1, 2, 4, 8, 16, 32, 64] {
    //         let tree = Arc::new(btree.make_empty_copy());
    //
    //         let (time, ..)
    //             = test::bulk_atomic_tx(threads, tree.clone(), insertions_vec.as_slice());
    //
    //         println!("{insertions},{threads},{},{},{time}",
    //                  tree.locking_strategy(),
    //                  tree.clock_type());
    //     }
    // }
    // let snapshot =
    //     tree.current_version();
    //
    // let start = SystemTime::now();
    // (0..insertions).into_par_iter().for_each(|key|
    //     if tree.dispatch_crud(CRUDOperation::Point(key, snapshot))
    //         .is_err()
    //     {
    //         println!("ERROR Point dispatch crud.")
    //     });
    // let end = SystemTime::now().duration_since(start).unwrap().as_millis();
    //
    // println!("\
    // All Keys Point Search = {}\n\
    // Threads = {}\n\
    // Time = {}ms\n",
    //          format_insertions(insertions as _),
    //          rayon::current_num_threads(),
    //          end);
    //
    // let range
    //     = Interval::new(tree.min_key, tree.max_key);
    //
    // let start = SystemTime::now();
    // match tree.dispatch_crud(CRUDOperation::RangeIter(range, snapshot)) {
    //     CRUDOperationResult::MatchedRecordIter(iter) => if iter.count() != insertions as _ {
    //         println!("ERROR Range Iter")
    //     }
    //     _ => println!("Range Iter Failed")
    // }
    //
    // let end = SystemTime::now().duration_since(start).unwrap().as_millis();
    // println!("Scan = Key-Space\nTime = {end}ms");
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