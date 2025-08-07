use crate::mv_block::block::Block;
use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_gc::tx_manager::TransactionManager;
use crate::mv_record_model::record_point::RecordPointResult;
use crate::mv_record_model::version_info::Version;
use crate::mv_test::Sampler::Uniform;
use crate::mv_test::{format_insertions, height_root, GroupConfig, Key, Payload, Sampler, FAN_OUT, FILLED_BLOCK, F_ABS_OFF, F_MUL, F_OFF, LOG_REORG, NUM_RECORDS, N_ABS_OFF, N_MUL, N_OFF, VERBOSE};
use crate::mv_tree::mvbplus_tree::MVBPlusTree;
use crate::mv_tree::version_manager::VersionManager;
use crate::mv_utils::safe_cell::SafeCell;
use chrono::{DateTime, Local};
use itertools::{Either, Itertools};
use libc::{exit, rand};
use rand::prelude::SliceRandom;
use rand::thread_rng;
use rand_distr::Zipf;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::thread::spawn;
use std::time::SystemTime;
use std::{env, fs, mem};
use std::collections::HashSet;

mod mv_block;
mod mv_crud_model;
mod mv_gc;
mod mv_page_model;
mod mv_record_model;
mod mv_test;
mod mv_tree;
mod mv_tx_model;
mod mv_utils;
mod mv_paper_tests;
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
const BERNHARD_TESTS: bool = false;

const BERNHARD_TESTS_NEW: bool = true;

type MVTree = MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

fn main() {
    startup();

    if MANUEL_MAIN {
        manuel_main()
    } else if BERNHARD_TESTS {
        bernhard_tests()
    } else if BERNHARD_TESTS_NEW {
        bernhard_tests_new()
    } else {
        mv_test::execute_experiments();
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

fn startup() {
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
    println!();

    make_splash();
}


fn bernhard_tests_new() {
    const INITIAL_POPULATION: usize = 10_000_000;
    const INSERTS: usize = 200; // Insert and Update seem to create errors
    const UPDATES: usize = 600;
    const DELETES: usize = 200; // works fine
    const TOTAL_BLOCKS: usize = 1000;

    const NUMBER_OLAPS: usize = 12;
    const OLAP_TX_PER_WORKER: usize = 20;

    println!("\
    Initial Population = {}\n\
    Total Operations   = {}\n\t -Blocks   = {}\n\
    \t -Inserts  = {}\n\
    \t -Updates  = {}\n\
    \t -Deletes  = {}",
             format_insertions(INITIAL_POPULATION),
             format_insertions(TOTAL_BLOCKS * (INSERTS + UPDATES + DELETES)),
             format_insertions(TOTAL_BLOCKS),
             format_insertions(INSERTS),
             format_insertions(UPDATES),
             format_insertions(DELETES));

    let mv_tree
        = Arc::new(MVTree::default());

    let mut map
        = HashSet::with_capacity(INITIAL_POPULATION);

    for _ in 0..INITIAL_POPULATION {
        'l: loop {
            let key = rand::random_range(0..Key::MAX);
            if !map.contains(&key) {
                mv_tree.dispatch_crud(CRUDOperation::Insert(key, Payload::default()));
                map.insert(key);
                break 'l
            }
        }
    }
    mem::drop(map);

    let block = {
        let mut crud
            = Vec::with_capacity(INSERTS + UPDATES + DELETES);

        crud.extend((0..INSERTS).map(|_| CRUDOperation::<Key, Payload>::InsertRand));
        crud.extend((0..UPDATES).map(|_| CRUDOperation::<Key, Payload>::UpdateRand));
        crud.extend((0..DELETES).map(|_| CRUDOperation::<Key, Payload>::DeleteRand));
        crud
    };

    let gen_block = || {
        let mut crud = block.clone();
        crud.shuffle(&mut rand::rng());
        crud
    };

    for block in 0..TOTAL_BLOCKS {
        // println!(">> Dispatching Block {block}");
        for op in gen_block() {
            mv_tree.dispatch_crud(op);
        }
    }
    let skew = 0;
    let _nc = fs::remove_file(format!("mv_olap_skew_{skew}.csv"));
    let mut olap_file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .write(true)
        .open(format!("mv_olap_skew_{skew}.csv"))
        .unwrap();

    olap_file
        .write_all(
            b"target_snapshot,\
            current_snapshot,\
            sleep_time,\
            range_start,\
            range_end,\
            count_results,\
            latency\n",
        )
        .unwrap();
    let mut olaps = vec![];
    for _ in 0..NUMBER_OLAPS {
        let index = mv_tree.clone();
        olaps.push(spawn(move || {
            let mut results = vec![];
            for _ in 1..OLAP_TX_PER_WORKER {
                let key_max = rand::random_range(0..Key::MAX);

                let key_min = 0;

                let current_si = index.current_version();

                let si = rand::random_range(VersionManager::START_VERSION..=current_si);

                let time_start = SystemTime::now();

                let crud =
                    index.dispatch_crud(CRUDOperation::Range((key_min, key_max).into(), si));

                let time_spent = SystemTime::now()
                    .duration_since(time_start)
                    .unwrap()
                    .as_nanos();

                let count_results = match crud {
                    CRUDOperationResult::MatchedRecords(data) => data.len(),
                    _ => 0,
                };
                results.push((
                    si,
                    current_si,
                    0u128,
                    key_min,
                    key_max,
                    count_results,
                    time_spent,
                ))
            }
            results
        }))
    }

    let olaps = olaps
        .into_iter()
        .map(|j| j.join().unwrap())
        .flatten()
        .collect::<Vec<_>>();

    olaps.into_iter().for_each(
        |(target_si, current_si, sleep_time, key_min, key_max, count_results, time_spent)| {
            olap_file
                .write_all(
                    format!(
                        "\
                            {target_si},\
                            {current_si},\
                            {sleep_time},\
                            {key_min},\
                            {key_max},\
                            {count_results},\
                            {time_spent}\n"
                    )
                        .as_bytes(),
                )
                .unwrap();
        },
    );

    println!(">> Finished dispatching...");
}

fn bernhard_tests() {
    const INSERTIONS: Key = 10_000;
    const UPDATES: Key = 100_000_000 as Key;
    const DELETIONS: f64 = 0.9_f64;
    const NUMBER_OLAPS: usize = 12;
    const NUMBER_UPDATERS: usize = 1;
    const OLAP_TX_PER_WORKER: usize = 2000;
    const RANGE_SIZE: Key = 1_000;
    const SKEWs: [f64; 3] = [0f64, 0.4, 1.4];

    let deletions_number = (DELETIONS * INSERTIONS as f64) as usize;
    println!(
        "\t- Inserts = {}\n\t- Updates = {}\n\t- Deletions = {} ({}% of keys)",
        format_insertions(INSERTIONS as _),
        format_insertions(UPDATES as _),
        format_insertions(deletions_number),
        DELETIONS * 100.0
    );

    for skew in SKEWs {
        println!(
            "\t- Skew = {}\n\t- ####################################################",
            skew
        );
        let mv_tree = MVTree::default();

        let mut data_inserts = (0..INSERTIONS).collect_vec();

        data_inserts.shuffle(&mut rand::rng());

        data_inserts.iter().for_each(|key| {
            let crud_ins = mv_tree.dispatch_crud(CRUDOperation::Insert(*key, *key));

            match crud_ins {
                CRUDOperationResult::Inserted(_) => {}
                _ => panic!("Error in Inserted crud"),
            }
        });

        let mut sampler = Sampler::new(skew, INSERTIONS - 1);

        (0..UPDATES).for_each(|_| {
            let crud = CRUDOperation::Update(sampler.sample(), Payload::default());
            let crud_update = mv_tree.dispatch_crud(crud.clone());

            match crud_update {
                CRUDOperationResult::Updated(_) => {}
                _ => panic!("Error in Updated crud = {crud}"),
            }
        });

        let mut deletes = data_inserts.clone();
        deletes.shuffle(&mut rand::rng());
        deletes.truncate(deletions_number);

        deletes.into_iter().for_each(|key| {
            let crud_ins = mv_tree.dispatch_crud(CRUDOperation::Delete(key));

            match crud_ins {
                CRUDOperationResult::Deleted(_) => {}
                _ => panic!("Error in Deleted crud"),
            }
        });

        mem::drop(data_inserts);

        println!(
            "\t- MVTree Init. \n\t- \
    [{NUMBER_OLAPS}] OLAPs starting with [{OLAP_TX_PER_WORKER}] transactions per worker."
        );

        // Start OLAPs here
        let index = Arc::new(mv_tree);
        let mut olaps = vec![];

        let _nc = fs::remove_file(format!("mv_olap_skew_{skew}.csv"));
        let mut olap_file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .write(true)
            .open(format!("mv_olap_skew_{skew}.csv"))
            .unwrap();

        olap_file
            .write_all(
                b"target_snapshot,\
            current_snapshot,\
            sleep_time,\
            range_start,\
            range_end,\
            count_results,\
            latency\n",
            )
            .unwrap();

        // splits, merges, root_splits, root_merges

        if LOG_REORG {
            unsafe {
                for (file_name, counter) in [
                    (format!("skew_{skew}_splits.csv"), mv_test::SPLITS_COUNTER.lock()),
                    (format!("skew_{skew}_merges.csv"), mv_test::MERGES_COUNTER.lock()),
                    (format!("skew_{skew}_root_splits.csv"), mv_test::SPLITS_ROOT_COUNTER.lock()),
                    (format!("skew_{skew}_root_merges.csv"), mv_test::MERGE_ROOT_COUNTER.lock()),
                ] {
                    let _ = fs::remove_file(file_name.as_str());
                    let mut file_io = BufWriter::new(
                        OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(file_name.as_str())
                            .unwrap(),
                    );

                    file_io.write_all("current_snapshot\n".as_bytes()).unwrap();
                    counter
                        .iter()
                        .for_each(|s| file_io.write_all(format!("{s}\n").as_bytes()).unwrap());

                    file_io.flush().unwrap();
                    println!(">> {file_name} written.");
                }
        }
    }

    let mut updaters = vec![];
        for _ in 0..NUMBER_UPDATERS {
            let index = index.clone();

            let (sender, receiver) = std::sync::mpsc::channel::<()>();

            updaters.push((
                sender,
                spawn(move || {
                    let mut sampler = Sampler::new(skew, INSERTIONS - 1);

                    loop {
                        match receiver.try_recv() {
                            Err(..) => break,
                            _ => {
                                index.dispatch_crud(CRUDOperation::Update(
                                    sampler.sample(),
                                    Payload::default(),
                                ));
                            }
                        }
                    }
                }),
            ))
        }
        for _ in 0..NUMBER_OLAPS {
            let index = index.clone();
            olaps.push(spawn(move || {
                let mut results = vec![];
                for _ in 1..OLAP_TX_PER_WORKER {
                    let mut key_min = rand::random_range(0..INSERTIONS);

                    let mut key_max = key_min + RANGE_SIZE;

                    if key_max >= INSERTIONS {
                        key_max = key_min;
                        key_min -= RANGE_SIZE;
                    }

                    let current_si = index.current_version();

                    let si = rand::random_range(VersionManager::START_VERSION..=current_si);

                    let time_start = SystemTime::now();

                    let crud =
                        index.dispatch_crud(CRUDOperation::Range((key_min, key_max).into(), si));

                    let time_spent = SystemTime::now()
                        .duration_since(time_start)
                        .unwrap()
                        .as_nanos();

                    let count_results = match crud {
                        CRUDOperationResult::MatchedRecords(data) => data.len(),
                        _ => 0,
                    };
                    results.push((
                        si,
                        current_si,
                        0u128,
                        key_min,
                        key_max,
                        count_results,
                        time_spent,
                    ))
                }
                results
            }))
        }

        let olaps = olaps
            .into_iter()
            .map(|j| j.join().unwrap())
            .flatten()
            .collect::<Vec<_>>();

        mem::drop(updaters);

        olaps.into_iter().for_each(
            |(target_si, current_si, sleep_time, key_min, key_max, count_results, time_spent)| {
                olap_file
                    .write_all(
                        format!(
                            "\
                            {target_si},\
                            {current_si},\
                            {sleep_time},\
                            {key_min},\
                            {key_max},\
                            {count_results},\
                            {time_spent}\n"
                        )
                        .as_bytes(),
                    )
                    .unwrap();
            },
        )
    }
}

fn manuel_main() {
    let mv_tree = MVTree::default();
    let n = 999000;

    let inserts = vec![
        75, 91, 78, 24, 82, 3, 10, 38, 57, 81, 51, 67, 73, 14, 37, 87, 26, 33, 66, 12, 99, 61, 29,
        20, 45, 27, 32, 21, 6, 52, 4, 35, 16, 58, 8, 28, 23, 97, 63, 9, 92, 22, 17, 30, 79, 42, 84,
        59, 31,
    ];

    let mut inserts = (0..n).collect_vec();

    inserts.shuffle(&mut rand::rng());
    let max = inserts.iter().max().unwrap().clone();

    let updates = vec![
        27, 63, 57, 45, 61, 59, 16, 8, 9, 78, 6, 23, 4, 17, 67, 79, 87, 66, 97, 75, 20, 22, 12, 29,
    ];

    // let updates = vec![];

    let deletes = vec![
        14, 87, 37, 59, 97, 31, 30, 21, 73, 4, 29, 78, 66, 35, 99, 32, 8, 10, 6, 81, 51, 45, 42,
        79, 82, 22, 23, 33, 75, 26, 3, 61,
    ];

    let logged_inserts = Arc::new(SafeCell::new(vec![]));

    let check_integrity = || {
        for key in 0..=max * 2 {
            // println!("Query: {:?}", (key, snapshot));
            if let CRUDOperationResult::MatchedRecords(record) =
                mv_tree.dispatch_crud(CRUDOperation::Point(key, Version::MAX - 1))
            {
                if record.is_empty() && inserts.contains(&key) {
                    panic!("No point record found");
                }
            } else {
                panic!("Error Point key: {}", key);
            }
        }
    };

    let check_integrity = || {};
    // Inserts
    for key in inserts.clone() {
        let crud = mv_tree.dispatch_crud(CRUDOperation::Insert(key, key));

        logged_inserts
            .get_mut()
            .push(if let CRUDOperationResult::Inserted(v) = crud {
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
        let update = mv_tree.dispatch_crud(CRUDOperation::Update(*key, *key));

        check_integrity();

        logged_inserts
            .get_mut()
            .push(if let CRUDOperationResult::Updated(v) = update {
                (*key, v)
            } else {
                println!("Update error key: {key}");
                unsafe {
                    exit(1);
                }
            });
    }
    println!("Finish update");

    inserts.shuffle(&mut rand::rng());

    // println!("Deletes: {:?}", deletes);
    // Deletes
    for key in inserts.iter() {
        if *key == 61 {
            let s = "adasd".to_string();
        }
        let crud = mv_tree.dispatch_crud(CRUDOperation::Delete(*key));

        if let CRUDOperationResult::Deleted(d) = crud {
            logged_inserts.get_mut().push((*key, d));
            println!("Delete key: {key}");
        } else {
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
