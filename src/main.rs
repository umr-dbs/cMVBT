use std::{env, fs, mem};
use std::io::Read;
use std::sync::Arc;
use chrono::{DateTime, Local};
use itertools::Itertools;
use parking_lot::RwLock;
use crate::block::block::Block;
use crate::tree::bplus_tree;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::record_model::version_info::Version;
use crate::test::{INDEX, Key, MAKE_INDEX};
use crate::tree::bplus_tree::BPlusTree;
use crate::tree::locking_strategy::{CRUDProtocol, LockingStrategy};
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
    Arc::new(if let LockingStrategy::MonoWriter = crud {
        TreeDispatcher::Wrapper(RwLock::new(MAKE_INDEX(crud)))
    } else {
        TreeDispatcher::Ref(MAKE_INDEX(crud))
    })
};
fn mk_payload() -> Box<u8> {
    unsafe {
        mem::transmute(Box::leak(Box::new(0_usize)))
    }
}

fn main() {
    make_splash();
    const FAN_OUT: usize = 127; // const FAN_OUT: usize = 70;
    const NUMBER_RECORDS: usize = 127;
    type MVTree = BPlusTree::<FAN_OUT, NUMBER_RECORDS, u64>;

    assert!(mem::size_of::<Block<FAN_OUT, NUMBER_RECORDS, u64>>() <= 4096);


    let tree
        = MVTree::orwc();

    let insertions = 10_000_u64;
    let mut last_insert_version = Version::MIN;

    for key in 0u64..insertions {
        match tree.dispatch(CRUDOperation::Insert(key, mk_payload())) {
            CRUDOperationResult::Inserted(ver) => {
                last_insert_version = ver;
                // println!("Inserted at version {}", ver);
                match tree.dispatch(CRUDOperation::Point(key, ver)) {
                    CRUDOperationResult::MatchedRecords(found)
                    if found.last().unwrap().key ==  key => {}
                        // println!("Record(s) found ({}): {}", found.len(), found.into_iter().join(",")),
                    err => println!("Err at insertion {}", err),
                }
            }
            err => println!("Err at insertion {}", err),
        }
    }


    for key in 0u64..1000 {
        match tree.dispatch(CRUDOperation::Delete(key)) {
            CRUDOperationResult::Deleted(v) => {}
                // println!("Key = {}, v = {} deleted", key, v),
            _ => println!("Error delete key = {}", key)
        }
    }

    for key in 0u64..insertions as u64 {
        match tree.dispatch(CRUDOperation::Point(key, last_insert_version)) {
            CRUDOperationResult::MatchedRecords(mut v) if v.last().unwrap().key == key => {}
                // println!("Found Point  {}", v.pop().unwrap()),
            err => panic!("Point failed: {}, key = {}", err, key)
        }
    }
    //
    // let end_time = SystemTime::now().duration_since(start_time).unwrap().as_millis();
    // println!("Insertions = {}, Time = {}", format_insertions(insertions as _), end_time);
    //
    // let insertions = (0u64..insertions)
    //     .map(|key| CRUDOperation::Insert(key, mk_payload()))
    //     .collect_vec();
    //
    // let (time, errors) = test::bulk_crud(
    //     num_cpus::get(),
    //     Tree::new(TreeDispatcher::Ref(MVTree::orwc_optimistic_clock())),
    //     insertions.as_slice());
    //
    // println!("Insertions = {}, Threads = {}, Time = {}, Errors = {}",
    //          format_insertions(insertions.len()),
    //          num_cpus::get(),
    //          time,
    //          errors);
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
    println!(" |               # Current version: {}                                |", env!("CARGO_PKG_VERSION"));
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

pub type Tree = Arc<TreeDispatcher>;

pub enum TreeDispatcher {
    Wrapper(RwLock<INDEX>),
    Ref(INDEX),
}

impl CRUDDispatcher<Key> for TreeDispatcher {
    #[inline(always)]
    fn dispatch(&self, crud: CRUDOperation<Key>) -> CRUDOperationResult<Key> {
        match self {
            TreeDispatcher::Ref(inner) => inner.dispatch(crud),
            TreeDispatcher::Wrapper(sync) => if crud.is_read() {
                sync.read().dispatch(crud)
            } else {
                sync.write().dispatch(crud)
            }
        }
    }
}

// unsafe impl Send for TreeDispatcher {}
// unsafe impl Sync for TreeDispatcher {}

impl TreeDispatcher {
    pub fn as_index(&self) -> &INDEX {
        match self {
            TreeDispatcher::Wrapper(inner) => unsafe { &*inner.data_ptr() },
            TreeDispatcher::Ref(inner) => inner
        }
    }
}

pub fn hle() -> &'static str {
    if cfg!(feature = "hardware-lock-elision") {
        if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
            "ON    "
        } else {
            "NO HTL"
        }
    } else {
        "OFF   "
    }
}