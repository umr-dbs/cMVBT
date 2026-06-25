use crate::mv_block::block::Block;
use crate::mv_test::{main_append, main_generate, main_load, main_load_cc_new, main_load_ycsb, main_sorted_insert, Key, MVBT, Payload, FAN_OUT, NUM_RECORDS, };
use chrono::{DateTime, Local};
use itertools::Itertools;
use std::{env, fs, mem};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::Ordering::SeqCst;
use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;

mod mv_block;
mod mv_crud_model;
mod mv_gc;
mod mv_page_model;
mod mv_query;
mod mv_record_model;
mod mv_test;
mod mv_tree;
mod mv_root;
mod mv_tx_model;
mod mv_tx_query;
mod mv_sync;
mod mv_utils;
mod cmvbt_tree;

use jemallocator::Jemalloc;
use crate::mv_page_model::internal_page::InternalPage;
use crate::mv_page_model::leaf_page::LeafPage;
use crate::mv_page_model::node::Node;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const TEST: bool = false;

fn main() {
    if TEST {
        println!("Block = {}", mem::size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>());
        println!("Node = {}", mem::size_of::<Node<FAN_OUT, NUM_RECORDS, Key, Payload>>());
        println!("InternalPage = {}", mem::size_of::<InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>>());
        println!("LeafPage = {}", mem::size_of::<LeafPage<NUM_RECORDS, Key, Payload>>());
        return;
    }
   
    startup();

    let args = env::args();
    let parms = args.collect_vec();

    if parms.len() > 1  {
        match parms[1].as_str() {
            "" | "test" => test(),
            "generate" => main_generate(parms),
            "append" => main_append(parms),
            "load" => main_load(parms),
            "load2" => main_load_ycsb(parms),
            // "load_cc_new" => main_load_cc_new(parms),
            // "sorted_insert" => main_sorted_insert(parms),
            s => println!("Unknown Command '{s}'")
        }
    }
    else {
        println!("*********** Use a Command ***********")
    }

    // fs::write("restarts.csv", "\n").unwrap();
    //
    // let mut f = OpenOptions::new()
    //     .append(true)
    //     .create(true)
    //     .open("restarts.csv")
    //     .unwrap();
    //
    // f.write_all( unsafe { RESTARTS_COUNTER.as_ref() }
    //     .iter()
    //     .map(|a| a.load(SeqCst))
    //     .join(",")
    //     .as_bytes())
    //     .unwrap();
    //
    // println!("Restarts: {}", unsafe { RESTARTS_COUNTER.as_ref() }
    //     .iter()
    //     .enumerate()
    //     .map(|(i, count)| format!("{i}: {}", count.load(SeqCst)))
    //     .join("\n"))
}

fn test() {
    let tree = MVBT::default();

    for key in 0..200 {
        let res
            = tree.dispatch_crud(CRUDOperation::Insert(key, 0));
    }

    for v in 1..2000 {
        let range
            = tree.dispatch_crud(CRUDOperation::Range((0..Key::MAX).into(), v));

        match range {
            CRUDOperationResult::MatchedRecords(records) =>{
                let len = records.len();
                let str_re = records.iter().join("\n");

                println!("Len= {}", len);
            }
            s => println!("ERROR = {s}")
        }

    }

}
/// Essential function.
fn make_splash() {
    let datetime: DateTime<Local> = fs::metadata(env::current_exe().unwrap())
        .unwrap()
        .modified()
        .unwrap()
        .into();

    println!("                         _________________________");
    println!("                 _______/                         \\_______");
    println!("                /                                         \\");
    println!(" +-------------+                                           +-------------+");
    println!(" |                                                                       |");
    println!(" |               ------------------------------                          |");
    println!(
        " |               # Build:   {}                          |",
        datetime.format("%d-%m-%Y %T")
    );
    println!(
        " |               # Current version: {}                              |",
        env!("CARGO_PKG_VERSION")
    );
    println!(" |               -------------------------                               |");
    println!(
        " |               # HLE:   {}                                         |",
        hle()
    );
    // println!(" |               # RW-HLE:    AUTO                                       |");
    println!(" |               -----------------                                       |");
    println!(" |                                                                       |");
    println!(" |               ----------------------------------------------          |");
    println!(" |               # E-Mail: amir.tonta@mathematik.uni-marburg.de          |");
    println!(" |               # Written by: Amir Tonta                                |");
    println!(" |               # First released: 02-01-2024                            |");
    println!(" |               # Repository: https://github.com/umr-dbs/cMVBT          |");
    println!(" |               -----------------------------------------------------   |");
    println!(" |                                                                       |");
    println!(" |               ...cMVBT Application Launching...                       |");
    println!(" +-------------+                                           +-------------+");
    println!("                \\_______                           _______/");
    println!("                        \\_________________________/");

    println!();
    println!("--> System Log:");
}

fn startup() {
    make_splash();

    println!(">>HLE: \t\t\t{}", hle());

    let block_size = size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>();

    let kb = block_size as f32 / 1024f32;

    println!(
        "\
           >>FAN_OUT: \t\t{FAN_OUT}\n\
           >>NUM_RECORDS: \t\t{NUM_RECORDS}\n\
           >>size_of(BLOCK): \t{} bytes; {kb} kb\n\
           >>size_of(PTR): \t{} bytes",
        size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>(),
        mem::size_of::<*const ()>()
    );
    println!("*****************************************************");
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