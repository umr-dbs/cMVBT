use crate::mv_block::block::Block;
use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_record_model::version_info::Version;
use crate::mv_test::{format_insertions, Key, Payload, Sampler, FAN_OUT, LOG_REORG, NUM_RECORDS};
use crate::mv_tree::mvbplus_tree::MVBPlusTree;
use crate::mv_tree::version_manager::VersionManager;
use crate::mv_sync::safe_cell::SafeCell;
use chrono::{DateTime, Local};
use itertools::Itertools;
use libc::exit;
use rand::prelude::SliceRandom;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::Arc;
use std::thread::spawn;
use std::time::SystemTime;
use std::{env, fs, mem};
use std::collections::{HashMap, HashSet};
use crate::mv_root::index_root::RootIndexType;
use crate::mv_sync::smart_cell::LatchType;

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
    let args = env::args();
    let mut parms = args.collect_vec();

    if parms.len() == 1 {
        parms.extend(["test", "20000", "1", "10", "0", "MAX"].map(String::from));
    }
    if parms.len() > 1  {
        match parms[1].as_str() {
            "test" => {
                let n = parms[2].parse().unwrap();
                let num_olaps = parms[3].parse::<usize>().unwrap();
                let olaps_per_worker = parms[4].parse::<usize>().unwrap();
                let skew = parms[5].parse::<f32>().unwrap();
                let key_range = parms[6].parse().unwrap_or(Key::MAX);

                let tree = Arc::new(MVTree::default());
                let mut check = HashMap::new();
                let mut errors = 0;

                while check.len() < n {
                    let key
                        = rand::random_range(0..Key::MAX);

                    if !check.contains_key(&key) {
                        match tree.dispatch_crud(CRUDOperation::Insert(key, Payload::default())) {
                            CRUDOperationResult::Inserted(v) => {
                                check.insert(key, v);
                            }
                            _ => {
                                println!("Error insert key={key}");
                                errors += 1
                            }
                        };

                    }
                }
                for (k, _) in check.iter() {
                    (0..rand::random_range(1..100)).for_each(|_| {
                        tree.dispatch_crud(CRUDOperation::Update(*k, Payload::default()));
                    });
                }

                //
                // for (k, v) in check.iter() {
                //     match tree.dispatch_crud(CRUDOperation::Point(*k, *v)) {
                //         CRUDOperationResult::MatchedRecords(vec)
                //         if !vec.is_empty() => {}
                //         CRUDOperationResult::MatchedRecords(_) => {
                //             println!("Empty result of point: key={k}, version={v}");
                //         }
                //         _ => {
                //             println!("Error crud point: key={k}, version={v}");
                //             errors += 1
                //         }
                //     }
                // }

                mem::drop(check);

                let start_time_iter = SystemTime::now();
                let iter_range = tree
                    .dispatch_crud(CRUDOperation::RangeIter((0..=Key::MAX).into(), Version::MAX));

                let iter_res = match iter_range {
                    CRUDOperationResult::MatchedRecordIter(iter) => iter,
                    _ => panic!()
                };

                let mut data_from_iter = iter_res.collect_vec();

                let end_time_iter = SystemTime::now().duration_since(start_time_iter).unwrap();
                println!("Time elapsed Iter: {:?}", end_time_iter);
                data_from_iter.sort_by_key(|r| r.key);

                let start_time_range = SystemTime::now();
                let res_all = tree
                    .dispatch_crud(CRUDOperation::Range((0..=Key::MAX).into(), Version::MAX));

                let all_res = match res_all {
                    CRUDOperationResult::MatchedRecords(vec) => vec,
                    _ => panic!()
                };

                let end_time_range = SystemTime::now().duration_since(start_time_range).unwrap();
                println!("Time elapsed Range: {:?}", end_time_range);
                let mut data_from_all = all_res;
                data_from_all.sort_by_key(|r| r.key);

                println!("Results Iter = {}, Results All = {}",
                         data_from_iter.len(),data_from_all.len());

                for (k1, k2) in data_from_iter.iter().zip(data_from_all.iter()) {
                    if k1.key != k2.key {
                        panic!("Key mismatch");
                    }

                }
                // olap_tests(tree, num_olaps, olaps_per_worker, skew, key_range, false)
            }
            "generate" => {
                let query_file_name= parms[2].as_str();
                let init_population: usize = parms[3].parse().unwrap();
                let total_blocks: usize = parms[4].parse().unwrap();
                let block_inserts: usize = parms[5].parse().unwrap();
                let block_updates: usize = parms[6].parse().unwrap();
                let block_deletes: usize = parms[7].parse().unwrap();

                println!("Generating init_pop = {init_population}\n\
                total_blocks = {total_blocks}\n\
                block_inserts = {block_inserts}\n\
                block_updates = {block_updates}\n\
                block_deletes = {block_deletes}");
                generate_query(
                    query_file_name,
                    init_population,
                    total_blocks,
                    block_inserts,
                    block_updates,
                    block_deletes,
                );
                println!("Finished generate.")
            }
            "load" => {
                let query_file_name= parms[2].as_str();

                let num_olaps = parms[3].parse().unwrap();
                let workers_per_thread = parms[4].parse().unwrap();
                let skew = parms[5].parse().unwrap();
                let range = parms[6].parse().unwrap_or(Key::MAX);
                let root_star_index = match parms[7].as_str() {
                    "sk" => RootIndexType::SkipList(LatchType::Optimistic),
                    "ll" => RootIndexType::LinkedList(LatchType::Optimistic),
                    "fg" => RootIndexType::FrugalList(LatchType::Optimistic),
                    "bt" => RootIndexType::BTree(LatchType::Optimistic),
                    _ => RootIndexType::default()
                };
                let index
                    = Arc::new(MVTree::olc_optimistic_clock(root_star_index));

                println!("root_start_index = {}", root_star_index);

                let num = load_query(query_file_name, index.clone());

                println!("Finished executing {} CRUD operations from {query_file_name},\
                 starting OLAP testings...", format_insertions(num));
                olap_tests(index, num_olaps, workers_per_thread, skew, range, false);
            }
            s => println!("unknown command '{s}'-")
        }
    }
    else {
        startup();
    }

    // let index = Arc::new(MVTree::default());
    //
    // let cruds = load_query("query_0", index.clone());
    //
    // println!("query_0 -> {cruds}");
    //
    // generate_query(
    //     "query_1",
    //     10_000_000,
    //     10_000,
    //     200,
    //     800,
    //     200
    // );

    // let index = Arc::new(MVTree::default());
    //
    // let cruds = load_query("query_1", index.clone());
    //
    // println!("query_1 -> {cruds}");
    //
    //
    // unsafe {
    //     exit(0);
    // }
    // if MANUEL_MAIN {
    //     manuel_main()
    // } else if BERNHARD_TESTS {
    //     bernhard_tests()
    // } else if BERNHARD_TESTS_NEW {
    //     bernhard_tests_new()
    // } else {
    //     mv_test::execute_experiments();
    // }
}


fn olap_tests(index: Arc<MVTree>,
              num_olaps: usize,
              workers_per_thread: usize,
              skew: f32,
              range: u64,
              fixed_si: bool)
{
    println!("Starting OLAPs...");

    let mut olaps = vec![];
    let v_index = format!("mv_{}",
                          match index.root_star_index() {
                              RootIndexType::FrugalList(_) => "fg",
                              RootIndexType::SkipList(_) => "sk",
                              RootIndexType::BTree(_) => "bt",
                              RootIndexType::LinkedList(_) => "ll"
                          });

    let _nc = fs::remove_file(format!("{v_index}_olap_skew_{skew}.csv"));
    let mut olap_file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .write(true)
        .open(format!("{v_index}_olap_skew_{skew}.csv"))
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

    for _ in 0..num_olaps {
        let index = index.clone();
        olaps.push(spawn(move || {
            let mut results = vec![];
            for _ in 1..workers_per_thread {
                let mut key_min
                    = rand::random_range(0..Key::MAX);

                let mut key_max
                    = key_min.checked_add(range).unwrap_or(Key::MAX);

                if range == Key::MAX {
                    key_min = 0;
                    key_max = Key::MAX - 1;
                }
                else if key_max >= Key::MAX {
                    key_max = key_min;
                    key_min -= range;
                }

                let current_si
                    = index.current_version();

                let si = if fixed_si  {
                    current_si+1
                }
                else {
                    rand::random_range(1..=current_si)
                };

                // println!("Min = {key_min}, max = {key_max}");
                let time_start
                    = SystemTime::now();

                let crud =
                    index.dispatch_crud(CRUDOperation::Range((key_min, key_max).into(), si));

                let time_spent
                    = SystemTime::now().duration_since(time_start).unwrap().as_nanos();

                let count_results =  match crud {
                    CRUDOperationResult::MatchedRecords(data) =>  data.len(),
                    _ => panic!()
                };
                results.push(
                    (si, current_si, 0u128, key_min, key_max, count_results, time_spent)
                )
            }
            results
        }))
    }

    let olaps = olaps.into_iter().map(|j| j.join().unwrap())
        .flatten()
        .collect::<Vec<_>>();

    // mem::drop(updaters);

    olaps.into_iter()
        .for_each(|(target_si,
                       current_si,
                       sleep_time,
                       key_min,
                       key_max,
                       count_results,
                       time_spent)|
            {
                olap_file.write_all(format!("\
                            {target_si},\
                            {current_si},\
                            {sleep_time},\
                            {key_min},\
                            {key_max},\
                            {count_results},\
                            {time_spent}\n").as_bytes()).unwrap();
            })
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


const INSERT: u8 = 0;
const UPDATE: u8 = 1;
const DELETE: u8 = 2;

fn generate_query(
    query_file_name: &str,
    init_population: usize,
    total_blocks: usize,
    block_inserts: usize,
    block_updates: usize,
    block_deletes: usize)
{
    let mv_tree
        = Arc::new(MVTree::default());

    let mut map
        = HashSet::with_capacity(init_population);

    let mut init_pop_order =
        Vec::with_capacity(init_population);

    for _ in 0..init_population {
        'l: loop {
            let key = rand::random_range(0..Key::MAX);
            if !map.contains(&key) {
                mv_tree.dispatch_crud(CRUDOperation::Insert(key, Payload::default()));
                map.insert(key);
                init_pop_order.push(CRUDOperation::Insert(key, Payload::default()));

                break 'l
            }
        }
    }
    mem::drop(map);

    let _nc = fs::remove_file("{query_file_name}");
    let mut query_file = BufWriter::new(OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("{query_file_name}"))
        .unwrap());

    let mut querys = 0_usize;

    let mut io_handle = |crud: CRUDOperation<Key, Payload>| {
        let mut buff = [0, 0, 0, 0, 0, 0, 0, 0, 0];
        match crud {
            CRUDOperation::Insert(key, ..) => {
                buff[0] = INSERT;
                buff[1..].copy_from_slice(key.to_le_bytes().as_slice());
            }
            CRUDOperation::Update(key, ..) => {
                buff[0] = UPDATE;
                buff[1..].copy_from_slice(key.to_le_bytes().as_slice());
            }
            CRUDOperation::Delete(key, ..) => {
                buff[0] = DELETE;
                buff[1..].copy_from_slice(key.to_le_bytes().as_slice());
            }
            _ => panic!("Unknown CRUD Operation for blocks"),
        }

        querys += 1;
        query_file.write_all(buff.as_slice()).unwrap()
    };

    init_pop_order.into_iter().for_each(|op| io_handle(op));

    let block = {
        let mut crud
            = Vec::with_capacity(block_inserts + block_updates + block_deletes);

        crud.extend((0..block_inserts).map(|_| CRUDOperation::<Key, Payload>::InsertRand));
        crud.extend((0..block_updates).map(|_| CRUDOperation::<Key, Payload>::UpdateRand));
        crud.extend((0..block_deletes).map(|_| CRUDOperation::<Key, Payload>::DeleteRand));
        crud
    };

    let gen_block = || {
        let mut crud = block.clone();
        crud.shuffle(&mut rand::rng());
        crud
    };

    for _ in 0..total_blocks {
        for op in gen_block() {
            match mv_tree.dispatch_crud(op) {
                CRUDOperationResult::InsertedRand(key, _) => io_handle(
                    CRUDOperation::Insert(key, 0)),
                CRUDOperationResult::UpdatedRand(key, _) => io_handle(
                    CRUDOperation::Update(key, 0)),
                CRUDOperationResult::DeletedRand(key, _) => io_handle(
                   CRUDOperation::Delete::<_, Payload>(key)),
                _ => panic!("Error on rand query; generate_query()")
            }
        }
    }

    query_file.flush().unwrap();
    println!("Generated: {} CRUD Ops", format_insertions(querys))
}

fn load_query(query_file: &str, index: Arc<MVTree>) -> usize {
    let mut query_file = BufReader::new(OpenOptions::new()
        .read(true)
        .open(format!("{query_file}"))
        .unwrap());

    let mut query_count = 0;
    let payload = Payload::default();

    loop {
        let mut buff = [0, 0, 0, 0, 0, 0, 0, 0, 0];
        match query_file.read_exact(buff.as_mut_slice()) {
            Ok(..) => match buff[0] {
                INSERT => {
                    let crud = CRUDOperation::Insert(
                        Key::from_le_bytes((&buff[1..]).try_into().unwrap()),
                        payload
                    );

                    if let CRUDOperationResult::Inserted(..) = index.dispatch_crud(crud) {
                    } else {
                        panic!("Error loading query insert number = {}", query_count)
                    }
                }
                UPDATE => {
                    let crud = CRUDOperation::Update(
                        Key::from_le_bytes(buff[1..].try_into().unwrap()),
                        payload
                    );

                    if let CRUDOperationResult::Updated(..) = index.dispatch_crud(crud) {
                    }
                    else {
                        panic!("Error loading query update number = {}", query_count)
                    }
                }
                DELETE => {
                    let crud = CRUDOperation::Delete(
                        Key::from_le_bytes(buff[1..].try_into().unwrap()));

                    if let CRUDOperationResult::Deleted(..) = index.dispatch_crud(crud) {
                    }
                    else {
                        panic!("Error loading query delete number = {}", query_count)
                    }
                }
                _ => panic!("Unknown CRUD Operation for blocks in load query!"),
            }
            Err(..) => break
        }

        query_count += 1
    }

    assert!(query_file.read_exact([0].as_mut_slice()).is_err());
    query_count
}


fn bernhard_tests_new() {
    const NUMBER_OLAPS: usize = 12;
    const OLAP_TX_PER_WORKER: usize = 20;
    const QUERY_NAME: &str = "query_0";

    println!("[Starting] - \
    Loading query {QUERY_NAME}...");

    let mv_tree
        = Arc::new(MVTree::default());

    let num_cruds = load_query(QUERY_NAME, mv_tree.clone());

    println!("[Loaded] - \
    Query with {} CRUD instructions dispatched to MVTree.", format_insertions(num_cruds));

    println!("[OLAP Start] - \
    Starting {NUMBER_OLAPS} OLAP workers with {OLAP_TX_PER_WORKER} CRUD instructions per worker...");

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

    println!(">> Finished dispatching olaps...");
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
