use crate::mv_block::block::Block;
use crate::mv_test::{main_append, main_generate, main_load, main_load_cc_new, main_sorted_insert, Key, MVTree, Payload, FAN_OUT, NUM_RECORDS};
use chrono::{DateTime, Local};
use itertools::Itertools;
use std::{env, fs, mem};
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

fn main() {
    startup();

    let args = env::args();
    let parms = args.collect_vec();

    if parms.len() > 1  {
        match parms[1].as_str() {
            "" | "test" => test(),
            "generate" => main_generate(parms),
            "append" => main_append(parms),
            "load" => main_load(parms),
            // "load_cc_new" => main_load_cc_new(parms),
            // "sorted_insert" => main_sorted_insert(parms),
            s => println!("Unknown Command '{s}'")
        }
    }
    else {
        println!("*********** Use a Command ***********")
    }
}

fn test() {
    let tree = MVTree::default();

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
        " |               # Current version: {}                               |",
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
    println!(" |               # Repository: https://github.com/umr-dbs/MVTree         |");
    println!(" |               -----------------------------------------------------   |");
    println!(" |                                                                       |");
    println!(" |               ...MVTree Application Launching...                      |");
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