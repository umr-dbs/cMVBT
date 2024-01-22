use std::{env, fs, mem};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::io::Read;
use std::ptr::NonNull;
use std::sync::Arc;
use std::time::SystemTime;
use chrono::{DateTime, Local};
use itertools::Itertools;
use parking_lot::RwLock;
use crate::block::block::Block;
use crate::tree::bplus_tree;
use crate::crud_model::crud_api::{CRUDDispatcher, NodeVisits};
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::page_model::internal_page::InternalPage;
use crate::page_model::leaf_page::LeafPage;
use crate::page_model::node::Node;
use crate::record_model::record_point::RecordPoint;
use crate::record_model::version_info::{Version, VersionInfo};
use crate::test::{dec_key, format_insertions, inc_key, INDEX, Key, MAKE_INDEX};
use crate::tree::bplus_tree::BPlusTree;
use crate::tree::locking_strategy::{CRUDProtocol, LockingStrategy};
use crate::utils::interval::Interval;
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::{ENABLE_YIELD, OBSOLETE_FLAG_VERSION};

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

fn main() {
    make_splash();
    const FAN_OUT: usize = 127; // const FAN_OUT: usize = 70;
    const NUMBER_RECORDS: usize = 127;
    type MVTree = BPlusTree::<FAN_OUT, NUMBER_RECORDS, u64>;

    assert!(mem::size_of::<Block<FAN_OUT, NUMBER_RECORDS, u64>>() <= 4096);

    // let tree
    //     = MVTree::orwc();

    let insertions = 1_000_000_u64;
    // let start_time = SystemTime::now();
    // for key in 0u64..insertions as u64 {
    //     match tree.dispatch(CRUDOperation::Insert(key, Box::new(0))) {
    //         CRUDOperationResult::Inserted(ver) => {
    //             // println!("Inserted at version {}", ver);
    //             // match tree.dispatch(CRUDOperation::Point(key, ver)) {
    //             //     CRUDOperationResult::MatchedRecords(found) =>
    //             //         println!("Record(s) found ({}): {}", found.len(), found.into_iter().join(",")),
    //             //     err => println!("Err at insertion {}", err),
    //             // }
    //         }
    //         err => println!("Err at insertion {}", err),
    //     }
    // }

    // let end_time = SystemTime::now().duration_since(start_time).unwrap().as_millis();
    // println!("Insertions = {}, Time = {}", format_insertions(insertions), end_time);


    let insertions = (0u64..insertions)
        .map(|key| CRUDOperation::Insert(key, Box::new(0)))
        .collect_vec();

    let (time, errors) = test::bulk_crud(
        24,
        Tree::new(TreeDispatcher::Ref(MVTree::lc_optimistic_clock())),
        insertions.as_slice());
    println!("Insertions = {}, Time = {}, Errors = {}", format_insertions(insertions.len()), time, errors);
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