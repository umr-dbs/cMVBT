use std::{env, fs, mem, thread};
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::SeqCst;
use std::time::{Duration, SystemTime};
// use cc_bplustree::mv_tree::bplus_tree::BPlusTree;
use chrono::{DateTime, Local};
use itertools::Itertools;
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
use crate::mv_test::{alloc_memory_force, allocate_free, format_insertions, INDEX, Key, MAKE_INDEX, Payload, test01, test02};
use crate::mv_tree::mvbplus_tree::MVBPlusTree;
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

fn main() {
    make_splash();


    // const F: usize = 250;
    // const R: usize = 499;
    // let internal_cc
    //     = mem::size_of::<cc_bplustree::mv_page_model::internal_page::InternalPage<F, R, SnapShot, ()>>();
    //
    // let leaf_cc
    //     = mem::size_of::<cc_bplustree::mv_page_model::leaf_page::LeafPage<R, SnapShot, ()>>();
    //
    // let block_cc
    //     = mem::size_of::<cc_bplustree::mv_block::mv_block::Block<F, R, SnapShot, ()>>();
    // println!("Fanout = {F}, Records = {R}, Internal = {internal_cc}, Leaf = {leaf_cc}, Block = {block_cc}");

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
    // for mv_tree in trees.into_iter() {
    //     test01(mv_tree.clone());
    //     test02(mv_tree.clone());
    // }

    assert!(mem::size_of::<Block<FAN_OUT, NUM_RECORDS, u64, f64>>() <= 4096);

    let tree
        = Arc::new(MVTree::olc_optimistic_clock());
    //
    let insertions = 50_000_u64;
    // let mut last_insert_version = Version::MIN;
    // let mut version_inserts = vec![];
    //
    // for key in 0u64..insertions {
    //     match mv_tree.dispatch_crud(CRUDOperation::Insert(key, mk_payload())) {
    //         CRUDOperationResult::Inserted(ver) => {
    //             last_insert_version = ver;
    //             version_inserts.push(ver);
    //             // println!("Inserted at version {}", ver);
    //             match mv_tree.dispatch_crud(CRUDOperation::Point(key, ver)) {
    //                 CRUDOperationResult::MatchedRecords(found)
    //                 if found.last().unwrap().key == key => {}
    //                     // println!("Record(s) found ({}): {}", found.len(), found.into_iter().join(",")),
    //                 err => println!("Err at insertion {}", err),
    //             }
    //         }
    //         err => println!("Err at insertion {}", err),
    //     }
    // }
    //
    // match mv_tree.dispatch_crud(CRUDOperation::Range(Interval::new(0, 255), last_insert_version)) {
    //     CRUDOperationResult::MatchedRecords(v) if v.len() == 256.min(insertions as usize) =>{}
    //         // println!("Range Query:\n\t{}", v.iter().join("\n\t")),
    //     x => println!("Error Range: {x}")
    // }
    //
    // let lazy_range = RangeQueryIter::new(
    //     &mv_tree,
    //     last_insert_version,
    //     Interval::new(0, insertions));
    //
    // println!("Height = {}", mv_tree.root.unsafe_borrow().height());
    // println!("Lazy Range = {}, all = {insertions}", lazy_range.count());
    //
    // println!("Before Delete Height = {}", mv_tree.root.unsafe_borrow().height);
    // for key in 0u64..insertions{
    //     if key == insertions - 1 {
    //         let s = "asdas".to_string();
    //     }
    //     match mv_tree.dispatch_crud(CRUDOperation::Delete(key)) {
    //         CRUDOperationResult::Deleted(v) => {}
    //             // println!("Key = {}, v = {} deleted", key, v),
    //         _ => println!("Error delete key = {}", key)
    //     }
    // }
    // for key in 0u64..insertions {
    //     // println!("Verified key = {key}");
    //     let r = mv_tree
    //         .dispatch_crud(CRUDOperation::Point(key, *version_inserts.get(key as usize).unwrap()));
    //     if let CRUDOperationResult::MatchedRecords(v) = r {
    //         if v.last().unwrap().key != key {
    //             println!("ERR expected = {key}, found = {}", v.last().unwrap().key)
    //         }
    //     }
    // }
    //
    // for key in 0u64..insertions as u64 {
    //     match mv_tree.dispatch_crud(CRUDOperation::Point(key, last_insert_version)) {
    //         CRUDOperationResult::MatchedRecords(mut v) if v.last().unwrap().key == key => {}
    //             // println!("Found Point  {}", v.pop().unwrap()),
    //         err => panic!("Point failed: {}, key = {}", err, key)
    //     }
    // }

    // let (keys, versions) = mv_tree.root.unsafe_borrow()
    //     .root.mv_block.unsafe_borrow().as_internal_page_ref().keys_versions();
    //
    // println!("Keys Root\n{}", keys
    //     .iter()
    //     .zip(versions)
    //     .filter(|(.., v)| v.is_active())
    //     .map(|(k, v)|
    //         format!("{k}, v: {v}")).into_iter().join("\n"));

    // let mut insertions_vec = (0..insertions)
    //     .map(|k| CRUDOperation::<Key, Payload>::Insert(k, k as _))
    //     .collect_vec();
    //
    // let mut rnd = StdRng::seed_from_u64(90501960);
    // let mut insertions_vec: Vec<_> = test::gen_data_exp(
    //     insertions, 0.01, &mut rnd
    // )
    //     .into_iter()
    //     .map(|key| CRUDOperation::Insert(key, key as _).into())
    //     .collect::<Vec<_>>();
    //
    // // insertions_vec.extend((0..insertions).map(|k| Point(k, k)));
    // let (time, ..) = test::bulk_crud(
    //     num_cpus::get(),
    //     mv_tree.clone(),
    //     insertions_vec.as_slice());
    //
    //
    // println!("Insertions = {}, Time = {time}ms", format_insertions(insertions_vec.len()));
    // let insertions = 40_000_u64;
    let gigs = 100;
    let ptr = alloc_memory_force(gigs);

    let insertions = 10_000_000_u64;
    println!("> Generating {insertions} keys..");
    let mut rnd = StdRng::seed_from_u64(90501960);
    let mut all_tx: Vec<AtomicTransaction<Key, Payload>> = mv_test::gen_data_exp(insertions, 0.01, &mut rnd)
        .into_iter()
        .map(|key| CRUDOperation::Insert(key, key as _).into())
        .collect::<Vec<_>>();

    let points = insertions;
    // all_tx.extend((0..points).map(|key|
    //     AtomicTransaction::new_latest_si(TxAtomicOperation::PointSi(
    //         test::gen_rand_key(key, Key::MIN, Key::MAX, 0.01, &mut rnd)))));



    // all_tx.shuffle(&mut thread_rng());
    println!("> Finished generating {insertions} keys!");
    println!("Inserts,Points,Threads,Protocol,Clock,Time,GC,Alloc,Reuse");

    for threads in [1, 2, 4, 8, 16, 24,  32, 64, 72, 96, 128] {
        for gc in [true, false] {
            for tree in [
                MVTree::standard(),
                MVTree::orwc(),
                MVTree::orwc_optimistic_clock(),
                // MVTree::lc(),
                // MVTree::lc_optimistic_clock(),
                MVTree::olc(),
                MVTree::olc_optimistic_clock()
            ] {
                if tree.locking_strategy().is_mono_writer() && threads > 1 {
                    continue;
                }

                let (end, tx_manager) = mv_test::bulk_tx_manager(
                    threads,
                    tree,
                    gc,
                    all_tx.as_slice()
                );

                fence(SeqCst);
                println!("{insertions},{points},{},{},{},{end},{},{},{}",
                         tx_manager.threads(),
                         tx_manager.locking_protocol(),
                         tx_manager.clock_type(),
                         tx_manager.is_gc_enabled(),
                         tx_manager.index().block_manager.alloc_count.load(SeqCst),
                         tx_manager.index().block_manager.reuse_count.load(SeqCst));
            }
        }
    }

    allocate_free(ptr, gigs);
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