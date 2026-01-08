use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_sync::latch_protocol::CRUDProtocol;
use crate::mv_tree::mvtree::MVTreeSt;
use crate::mv_tx_model::transaction::{AtomicTransaction};
use crossbeam_channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use itertools::{Either, Itertools};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};
use std::sync::Arc;
use std::{fs, mem, thread};
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, BufWriter, Read, Write};
use std::thread::{spawn, yield_now, JoinHandle};
use std::time::{Duration, Instant, SystemTime};
use parking_lot::Mutex;
use rand::distr::{Alphanumeric, Distribution, Uniform};
use rand::prelude::SliceRandom;
use rand::rngs::ThreadRng;
use rand_distr::Zipf;
use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_tx_query::tx_manager::TransactionManager;
use crate::mv_page_model::node::PageType;
use crate::mv_query::dispatch::RANGE_DISPATCH_LAZY;
use crate::mv_root::index_root::RootIndexType;
use crate::mv_sync::smart_cell::LatchType;
use crate::mv_sync::version_handle;
use crate::mv_tx_model::transaction_result::SnapShot;
use crate::mv_utils::crud_rate_control::{ThreadWorker, ThreadWorkerInfo};
use crate::mv_utils::interval::Interval;

pub const VERBOSE: bool = false;
pub const LOG_REORG: bool = false;
const SYSTEM_STR: &str = "MVTree";
pub static mut MERGES_COUNTER: Mutex<Vec<SnapShot>> = Mutex::new(vec![]);
pub static mut SPLITS_COUNTER: Mutex<Vec<SnapShot>> = Mutex::new(vec![]);
pub static mut MERGE_ROOT_COUNTER: Mutex<Vec<SnapShot>> = Mutex::new(vec![]);
pub static mut SPLITS_ROOT_COUNTER: Mutex<Vec<SnapShot>> = Mutex::new(vec![]);

fn olap_tests(index: Arc<MVTree>,
              num_olaps: usize,
              tx_per_thread: usize,
              skew: f32,
              range: Either<Key, Arc<AtomicU64>>,
              fixed_si: bool,
              control_signal: Option<Receiver<ThreadWorkerInfo>>) -> usize
{
    if control_signal.is_none() {
        println!("> Starting OLAPs...{num_olaps} threads, \
        {tx_per_thread} scans per thread.");
    } else {
        println!("> Starting OLAPs...{num_olaps} threads, \
         with control signal for continuous scans per thread");
    }

    if range.is_left() {
        println!("> Scan key-range is fixed to 0..={}", range.as_ref().left().unwrap())
    } else {
        println!("> Scan key-range is dynamic to 0..=LastKey")
    }

    let mut olaps = vec![];
    let v_index = format!("mv_{}",
                          match index.root_star_index() {
                              RootIndexType::FrugalList(_) => "fg",
                              RootIndexType::SkipList(_) => "sk",
                              RootIndexType::BTree(_) => "bt",
                              RootIndexType::LinkedList(_) => "ll"
                          });

    let lazy = if RANGE_DISPATCH_LAZY {
        "_lazy"
    } else { "" };

    let file_log = format!("{v_index}{lazy}_olap_skew_{skew}.csv");
    let _nc = fs::remove_file(file_log.as_str());
    let mut olap_file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .write(true)
        .open(file_log.as_str())
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

    let g_counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..num_olaps {
        let index
            = index.clone();

        let signal
            = control_signal.clone();

        let range
            = range.clone();

        let count_olaps
            = g_counter.clone();

        olaps.push(spawn(move || {
            let mut results = vec![];
            let mut tx_c = 0;
            while tx_c < tx_per_thread {
                let mut key_max = 1000;
                let mut key_min = Key::MIN;
                if let Either::Left(range) = range {
                    key_min = 0;
                    key_max = range;
                } else if let Either::Right(ref range) = range {
                    key_max = range.load(Acquire);
                    key_min = key_max.checked_sub(1000).unwrap_or(0);
                }

                let mut current_si = index.current_version_for_reader();

                while current_si == version_handle::START_VERSION {
                    yield_now();
                    current_si = index.current_version_for_reader();
                }

                let si = if fixed_si {
                    current_si
                } else {
                    rand::random_range(version_handle::START_VERSION..=current_si)
                };

                // println!("Min = {key_min}, max = {key_max}");
                let time_start
                    = SystemTime::now();

                let crud =
                    index.dispatch_crud(CRUDOperation::Range((key_min, key_max).into(), si));

                let time_spent
                    = SystemTime::now().duration_since(time_start).unwrap().as_nanos();

                let count_results = match crud {
                    CRUDOperationResult::MatchedRecords(data) => data.len(),
                    _ => panic!()
                };

                let _ = count_olaps.fetch_add(1, Relaxed);

                results.push(
                    (si, current_si, 0u128, key_min, key_max, count_results, time_spent));

                if let Some(signal) = signal.as_ref() {
                    match signal.try_recv() {
                        Err(TryRecvError::Disconnected) => break,
                        _ => continue
                    }
                }

                tx_c += 1;
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
            });

    g_counter.load(SeqCst)
}

const INSERT: u8 = 0;
const UPDATE: u8 = 1;
const DELETE: u8 = 2;

pub(crate) fn main_insert_rate_limiter(parms: Vec<String>) {
    let log = parms[2].parse::<bool>().unwrap_or(false);
    let runtime_sec = parms[3].parse::<u64>().unwrap_or(10);
    let num_workers = parms[4].parse::<usize>().unwrap_or(10);
    let fps = parms[5].parse::<usize>().unwrap_or(100);
    let crud = CRUDOperation::InsertRand;
    let index = Arc::new(MVTree::default());
    let olap_workers = parms[6].parse::<usize>().unwrap_or(10);
    let olaps_per_worker = parms[7].parse::<usize>().unwrap_or(10);
    let olap_skew_workers = parms[8].parse::<f32>().unwrap_or(0f32);
    let olaps_key_range = parms[9].parse::<Key>().unwrap_or(Key::MAX);
    let olaps_si_freshest = parms[10].parse::<bool>().unwrap_or(false);
    let (info_sender, info_receiver)
        = unbounded();

    let file_name
        = format!("mv_runtime_{runtime_sec}_workers_{num_workers}_fps_{fps}_crud_{crud}.csv");

    let _ = fs::remove_file(file_name.as_str());
    let mut log_file = BufWriter::new(OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(file_name.as_str()).unwrap());

    log_file.write_all(b"tid,crud,fps,load,tick_ops,total_ops\n").unwrap();

    let start_time = Instant::now();
    let workers = (0..num_workers)
        .map(|_| ThreadWorker::new(
            index.clone(),
            fps,
            crud.clone(),
            log,
            info_sender.clone()))
        .collect_vec();

    let signal = info_receiver.clone();
    spawn(move || olap_tests(
        index,
        olap_workers,
        olaps_per_worker,
        olap_skew_workers,
        Either::Left(olaps_key_range),
        olaps_si_freshest,
        Some(signal)));

    while start_time.elapsed().as_secs() < runtime_sec {
        match info_receiver.try_recv() {
            Ok(info) =>
                log_file.write_all(format!("{}\n", info).as_bytes()).unwrap(),
            _ => thread::yield_now()
        }
    }

    println!("Total Ops = {}", workers
        .into_iter()
        .map(|t| t.stop())
        .collect_vec()
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .sum::<usize>());

    mem::drop(info_receiver);
}
pub(crate) fn main_test(parms: Vec<String>) {
    let n = parms[2].parse().unwrap();
    let num_olaps = parms[3].parse::<usize>().unwrap();
    let olaps_per_worker = parms[4].parse::<usize>().unwrap();
    let skew = parms[5].parse::<f32>().unwrap();
    let key_range = parms[6].parse().unwrap_or(Key::MAX);
    let root_star_index = match parms[7].as_str() {
        "sk" => RootIndexType::SkipList(LatchType::Optimistic),
        "ll" => RootIndexType::LinkedList(LatchType::Optimistic),
        "fg" => RootIndexType::FrugalList(LatchType::Optimistic),
        "bt" => RootIndexType::BTree(LatchType::Optimistic),
        _ => RootIndexType::default()
    };
    println!("RootStar = {}", root_star_index);

    let tree
        = Arc::new(MVTree::olc_optimistic_clock(root_star_index));
    let mut check = HashMap::new();
    let mut errors = 0;

    let p = AtomicU64::new(0);
    while check.len() < n {
        let key
            = rand::random_range(0..100_000_000);

        if !check.contains_key(&key) {
            match tree.dispatch_crud(CRUDOperation::Insert(key, p.fetch_add(1, SeqCst))) {
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
        (0..1_00).for_each(|_| {
            match tree.dispatch_crud(CRUDOperation::Update(*k, p.fetch_add(1, SeqCst))) {
                CRUDOperationResult::Updated(_) => {}
                _ => panic!()
            }
        });
    }

    for (k, v) in check.iter() {
        (0..1_00).for_each(|o| {
            match tree.dispatch_crud(CRUDOperation::Point(*k, *v + o)) {
                CRUDOperationResult::MatchedRecords(r) =>
                    if r.len() == 1 && r[0].payload <= *v + o {} else {
                        println!("Found Version = {}\nQuery Version = {}",
                                 r[0].payload,
                                 *v + o);
                    }
                _ => panic!()
            }
        });
    }

    // test root retrival time.
    mem::drop(check);

    thread::sleep(Duration::from_millis(100));

    println!("Roots present = {}", tree.count_roots());
    let start_root = SystemTime::now();
    tree.retrieve_root_for(1);
    let end_root = SystemTime::now().duration_since(start_root).unwrap();
    println!("{root_star_index} -> Root access: {end_root:?}");

    olap_tests(
        tree,
        num_olaps,
        olaps_per_worker,
        0f32,
        Either::Left(key_range),
        false,
        None,
    );
    return;
    let start_time_iter = SystemTime::now();
    let iter_range = tree
        .dispatch_crud(CRUDOperation::RangeIter((0..=Key::MAX).into(), 10));

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
        .dispatch_crud(CRUDOperation::Range((0..=Key::MAX).into(), 10));

    let all_res = match res_all {
        CRUDOperationResult::MatchedRecords(vec) => vec,
        _ => panic!()
    };

    let end_time_range = SystemTime::now().duration_since(start_time_range).unwrap();
    println!("Time elapsed Range: {:?}", end_time_range);
    let mut data_from_all = all_res;
    data_from_all.sort_by_key(|r| r.key);

    println!("Results Iter = {}, Results All = {}",
             data_from_iter.len(), data_from_all.len());

    for (k1, k2) in data_from_iter.iter().zip(data_from_all.iter()) {
        if k1.key != k2.key {
            panic!("Key mismatch");
        }
    }
    // olap_tests(tree, num_olaps, olaps_per_worker, skew, key_range, false, None)
}
pub(crate) fn main_sorted_insert(parms: Vec<String>) {
    let query_file_name = parms[2].clone();
    let n: usize = parms[3].parse().unwrap();
    let _nc = fs::remove_file(query_file_name.as_str());

    let mut query_file = BufWriter::new(OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("{query_file_name}"))
        .unwrap());

    let mut querys = 0_usize;

    let mut io_handle = |key: Key| {
        let mut buff = [INSERT, 0, 0, 0, 0, 0, 0, 0, 0];
        buff[1..].copy_from_slice(key.to_le_bytes().as_slice());

        querys += 1;
        query_file.write_all(buff.as_slice()).unwrap()
    };

    (0..n as Key).into_iter().for_each(|op| io_handle(op));
    query_file.flush().unwrap();

    println!("Generated {querys} / {n} keys in sorted order in {query_file_name}!")
}
pub(crate) fn main_load(parms: Vec<String>) {
    println!("###### Command: {} ######", parms.iter().skip(1).join(" "));

    let query_file_name = parms[2].to_string();
    let concurrent = parms[3].parse::<bool>().unwrap();
    let num_olaps = parms[4].parse().unwrap();

    let scans_per_thread = parms[5].parse().unwrap();

    let skew = parms[6].parse().unwrap();
    let range = parms[7].parse().unwrap_or(Key::MAX);
    let root_star_index = match parms[8].as_str() {
        "sk" => RootIndexType::SkipList(LatchType::Optimistic),
        "ll" => RootIndexType::LinkedList(LatchType::Optimistic),
        "fg" => RootIndexType::FrugalList(LatchType::Optimistic),
        "bt" => RootIndexType::BTree(LatchType::Optimistic),
        _ => RootIndexType::default()
    };
    let index
        = Arc::new(MVTreeSt::olc_optimistic_clock(root_star_index));

    println!("- QueryFile = {query_file_name}\n\
                - Concurrent = {concurrent}\n\
                - OLAP Threads = {num_olaps} (Cores = {}, Threads = {})\n\
                - Scans/Thread = {}\n\
                - Skew = {skew}\n\
                - Range = {range}\n\
                - Root* = {root_star_index}",
             num_cpus::get_physical(),
             num_cpus::get(),
             if concurrent { format!("Continuous\n- OLTP Threads = {scans_per_thread}") } else { format!("{scans_per_thread}") });


    if concurrent {
        let query_file_name_clone = query_file_name.clone();
        let mut oltp = load_query_into_memory(
            query_file_name_clone.as_str());

        let oltp_threads = scans_per_thread;
        let slice = oltp.len() / oltp_threads;

        let work_oltp = (0..oltp_threads)
            .map(|_| oltp.drain(..slice).collect_vec())
            .collect_vec();

        let oltp_joins = work_oltp
            .into_iter()
            .map(|work| {
                let index = index.clone();
                spawn(move || {
                    let mut count_crud = 0;
                    work.into_iter().for_each(|crud| {
                        let _ = index.dispatch_crud(crud);
                        count_crud += 1;
                    });
                    count_crud
                })
            }).collect_vec();

        let (olap_signal, olap_sink)
            = unbounded();

        let olaps = spawn(move || olap_tests(
            index,
            num_olaps,
            1,
            skew,
            Either::Left(range),
            false,
            Some(olap_sink)));

        let oltp_executed = oltp_joins
            .into_iter()
            .map(|j| j.join().unwrap())
            .sum::<usize>();

        drop(olap_signal);
        let num_scans_executed = olaps.join().unwrap();

        println!("- Executed {} OLTPs from {query_file_name}\n\
        - Executed = {} OLAPs", format_insertions(oltp_executed),
                 format_insertions(num_scans_executed));

        println!("###### End Command: {} ######", parms.iter().skip(1).join(" "));
    } else {
        let num = load_query(query_file_name.as_str(), index.clone(), None);

        println!("- Executed {} CRUD operations from {query_file_name}, \
                 starting OLAPs...", format_insertions(num));

        let num_scans_executed = olap_tests(index,
                   num_olaps,
                   scans_per_thread,
                   skew,
                   Either::Left(range),
                   false,
                   None);

        println!("- Executed = {} OLAPs", format_insertions(num_scans_executed));
        println!("###### End Command: {} ######", parms.iter().skip(1).join(" "));
    }
}
pub(crate) fn main_load_cc_new(parms: Vec<String>) {
    let query_file_name = parms[2].to_string();

    let num_olaps = parms[3].parse().unwrap();
    let workers_per_thread = parms[4].parse().unwrap();
    let skew = parms[5].parse().unwrap();
    let root_star_index = match parms[6].as_str() {
        "sk" => RootIndexType::SkipList(LatchType::Optimistic),
        "ll" => RootIndexType::LinkedList(LatchType::Optimistic),
        "fg" => RootIndexType::FrugalList(LatchType::Optimistic),
        "bt" => RootIndexType::BTree(LatchType::Optimistic),
        _ => RootIndexType::default()
    };
    let index
        = Arc::new(MVTreeSt::olc_optimistic_clock(root_star_index));

    println!("root_start_index = {}", root_star_index);

    let atomic_key
        = Arc::new(AtomicU64::new(0));

    let index_c = index.clone();
    let (olap_signal, olap_sink)
        = unbounded();

    let atomic_key_clone = atomic_key.clone();
    let query_file_name_clone = query_file_name.clone();
    let num = spawn(move ||
        load_query(query_file_name_clone.as_str(), index_c, Some(atomic_key_clone)));

    let olaps = spawn(move || olap_tests(
        index,
        num_olaps,
        workers_per_thread,
        skew,
        Either::Right(atomic_key),
        true,
        Some(olap_sink)));

    let num = num.join().unwrap();
    mem::drop(olap_signal);

    olaps.join().unwrap();

    println!("Finished executing {} CRUD operations from {query_file_name}", format_insertions(num));
}
pub(crate) fn main_generate(parms: Vec<String>) {
    let query_file_name = parms[2].as_str();
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
pub(crate) fn main_append(parms: Vec<String>) {
    let query_file_name = parms[2].as_str();
    let total_blocks: usize = parms[4].parse().unwrap();
    let block_inserts: usize = parms[5].parse().unwrap();
    let block_updates: usize = parms[6].parse().unwrap();
    let block_deletes: usize = parms[7].parse().unwrap();

    println!("Appending-Mode\n\
                total_blocks = {total_blocks}\n\
                block_inserts = {block_inserts}\n\
                block_updates = {block_updates}\n\
                block_deletes = {block_deletes}");
    generate_query(
        query_file_name,
        0,
        total_blocks,
        block_inserts,
        block_updates,
        block_deletes,
    );
    println!("Finished generate.")
}


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

                break 'l;
            }
        }
    }
    mem::drop(map);

    if init_population > 0 {
        let _nc = fs::remove_file(format!("{query_file_name}"));
    } else {
        load_query(query_file_name, mv_tree.clone(), None);
    }

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
    if init_population > 0 {
        println!("Generated: {} CRUD Ops", format_insertions(querys))
    } else {
        let total_crud = query_file.into_inner().unwrap().metadata().unwrap().len() / 9;
        println!("Appended: {} CRUD Ops. Total: {} CRUD Ops", format_insertions(querys), format_insertions(total_crud as _))
    }
}

fn load_query_into_memory(query_file: &str) -> Vec<CRUDOperation<Key, Payload>> {
    let mut query_file = BufReader::new(OpenOptions::new()
        .read(true)
        .open(format!("{query_file}"))
        .unwrap());

    let payload = Payload::default();
    let mut loaded = vec![];

    loop {
        let mut buff = [0, 0, 0, 0, 0, 0, 0, 0, 0];
        match query_file.read_exact(buff.as_mut_slice()) {
            Ok(..) => match buff[0] {
                INSERT => {
                    let key = Key::from_le_bytes((&buff[1..]).try_into().unwrap());
                    let crud = CRUDOperation::Insert(key, payload);
                    loaded.push(crud);
                }
                UPDATE => {
                    let crud = CRUDOperation::Update(
                        Key::from_le_bytes(buff[1..].try_into().unwrap()), payload);

                    loaded.push(crud);
                }
                DELETE => {
                    let crud = CRUDOperation::Delete(
                        Key::from_le_bytes(buff[1..].try_into().unwrap()));

                    loaded.push(crud);
                }
                _ => panic!("Unknown CRUD Operation for blocks in load query into memory!"),
            }
            Err(..) => break
        }
    }

    assert!(query_file.read_exact([0].as_mut_slice()).is_err());

    loaded
}
fn load_query(query_file: &str, index: Arc<MVTree>,
              report_signal: Option<Arc<AtomicU64>>) -> usize
{
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
                    let key = Key::from_le_bytes((&buff[1..]).try_into().unwrap());
                    let crud = CRUDOperation::Insert(
                        key,
                        payload,
                    );

                    let r = index.dispatch_crud(crud);
                    if let CRUDOperationResult::Inserted(..) = r {
                        if let Some(ref sender) = report_signal {
                            sender.store(key, Ordering::Release);
                        }
                    } else {
                        panic!("Error loading query insert number = {}: {r}", query_count)
                    }
                }
                UPDATE => {
                    let crud = CRUDOperation::Update(
                        Key::from_le_bytes(buff[1..].try_into().unwrap()),
                        payload,
                    );

                    let r = index.dispatch_crud(crud);
                    if let CRUDOperationResult::Updated(..) = r {} else {
                        panic!("Error loading query update number = {}: {r}", query_count)
                    }
                }
                DELETE => {
                    let crud = CRUDOperation::Delete(
                        Key::from_le_bytes(buff[1..].try_into().unwrap()));

                    let r = index.dispatch_crud(crud);
                    if let CRUDOperationResult::Deleted(..) = r {} else {
                        panic!("Error loading query delete number = {}: {r}", query_count)
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

pub enum Sampler {
    Uniform(Uniform<u64>, ThreadRng),
    Zipf(Zipf<f64>, ThreadRng),
}

impl Sampler {
    pub fn new(skew: f64, n: Key) -> Self {
        if skew == 0_f64 {
            Sampler::Uniform(Uniform::new(0, n).unwrap(), rand::rng())
        } else {
            Sampler::Zipf(Zipf::new(n as f64, skew).unwrap(), rand::rng())
        }
    }
    #[inline(always)]
    pub fn sample(&mut self) -> Key {
        match self {
            Sampler::Uniform(dist, rng) =>
                dist.sample(rng) as Key,
            Sampler::Zipf(dist, rng) =>
                dist.sample(rng) as Key,
        }
    }
}

impl Display for Sampler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Sampler::Uniform(..) => write!(f, "Uniform"),
            Sampler::Zipf(..) => write!(f, "Zipf"),
        }
    }
}

type CurrentVersionSI = u64;
type RangeMax = Key;
type OlapTime = u128;
type SleepTime = u64;
type ResultsCount = usize;

const FIXED_RANGE_VAR_SI: bool = false;
const FIXED_RANGE_INTERVAL: u64 = 10_000;

pub fn run_olaps(handler: IndexHandler,
                 number_workers: usize,
                 number_olaps_per_worker: usize,
                 n: usize,
) -> Vec<JoinHandle<Vec<(SnapShot, Interval<Key>, OlapTime, CurrentVersionSI, SleepTime, ResultsCount)>>>
{
    let mut handles
        = Vec::with_capacity(number_workers);

    for i in 1..=number_workers as u64 {
        handles.push(olap(i, handler.clone(), number_olaps_per_worker, n));
    }

    handles
}

pub fn olap(olap_id: u64, handler: IndexHandler, number_olaps: usize, n: usize)
            -> JoinHandle<Vec<(SnapShot, Interval<Key>, OlapTime, CurrentVersionSI, SleepTime, ResultsCount)>> {
    let manager = handler
        .left()
        .expect("OLAP init failed! Provide an initialized TxManager!");

    spawn(move || {
        let uni_form
            = Uniform::new(0_usize, n).unwrap();

        let mut olap_res
            = Vec::with_capacity(number_olaps);

        let index
            = manager.tx_dispatcher();

        let mut current_version
            = index.current_version_for_reader();

        let si_steps = current_version / number_olaps as u64;
        let limit = if FIXED_RANGE_VAR_SI {
            match current_version % number_olaps as u64 == 0 {
                true => number_olaps as u64,
                false => number_olaps as u64 + 1,
            }
        } else {
            number_olaps as u64 - 1
        };

        for olap_id in 0..=limit {
            let mut target_si;
            let mut key_range = Interval::blank();
            let mut sleep_time = 0;

            if FIXED_RANGE_VAR_SI {
                target_si = si_steps * olap_id;
            } else {
                current_version = index.current_version_for_reader();
                target_si = rand::random_range(1..=current_version);

                key_range.lower = uni_form.sample(&mut rand::rng()) as RangeMax;
                key_range.upper = key_range.lower + 1_000;
            }

            // println!("---> Start OLAP");
            let time_start = SystemTime::now();
            let crud_res = index.dispatch_crud(CRUDOperation::Range(
                key_range.clone(),
                target_si));

            let time_spent = SystemTime::now().duration_since(time_start).unwrap().as_nanos();
            let results_count = if let CRUDOperationResult::MatchedRecords(records) = crud_res {
                records.len()
            } else {
                0
            };

            // println!("---> End OLAP");
            olap_res.push(
                (target_si,
                 key_range,
                 time_spent,
                 current_version,
                 sleep_time,
                 results_count
                )
            );
        }

        olap_res
    })
}

const CONFIG_PARAMETERS: &'static str = "config.json";
#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum VersionIndexType {
    VANILLA,
    SkipList,
    SkipListSynced,
    BTree,
}
impl Display for VersionIndexType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MV")
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub struct GroupConfig {
    olap_joint_workload: bool,
    olap_workers: usize,
    olaps_tx_per_worker: usize,
    protocol: CRUDProtocol,
    v_index_type: VersionIndexType,
    skew: f64,
    skew_n: usize,
    gc_enable: bool,
    threads: usize,
    total_tx: usize,
    insert_ratio: usize,
    update_ratio: usize,
    delete_ratio: usize,
    point_reads_ratio: usize,
    range_reads_ratio: usize,
    range_size: u64,
    chain_groups: Vec<SubGroupConfig>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SubGroupConfig {
    olap_joint_workload: bool,
    olap_workers: usize,
    olaps_tx_per_worker: usize,
    skew: f64,
    skew_n: usize,
    gc_enable: bool,
    threads: usize,
    total_tx: usize,
    insert_ratio: usize,
    update_ratio: usize,
    delete_ratio: usize,
    point_reads_ratio: usize,
    range_reads_ratio: usize,
    range_size: u64,
}

impl GroupConfig {
    fn is_valid(&self) -> bool {
        100 == self.insert_ratio
            + self.update_ratio
            + self.delete_ratio
            + self.point_reads_ratio
            + self.range_reads_ratio
            && self.threads > 1
            && self.protocol.is_mono_writer()
            && self.is_read_only()
            || self.threads == 1 && self.protocol.is_mono_writer()
            || !self.protocol.is_mono_writer()
    }

    fn index_handler(&self) -> IndexHandler {
        Either::Right(self.protocol.clone())
    }

    fn is_read_only(&self) -> bool {
        self.insert_ratio == 0 && self.update_ratio == 0 && self.delete_ratio == 0
    }

    fn is_write_only(&self) -> bool {
        self.point_reads_ratio == 0 && self.range_reads_ratio == 0
    }

    fn is_mix_read_write(&self) -> bool {
        !self.is_read_only() && !self.is_write_only()
    }

    fn num_chains(&self) -> usize {
        self.chain_groups.len()
    }
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            olap_joint_workload: false,
            olap_workers: 0,
            olaps_tx_per_worker: 0,
            chain_groups: vec![],
            protocol: Default::default(),
            v_index_type: VersionIndexType::VANILLA,
            skew: 0.1,
            skew_n: 10000,
            gc_enable: false,
            threads: 1,
            total_tx: 10_0000,
            insert_ratio: 100,
            update_ratio: 0,
            delete_ratio: 0,
            point_reads_ratio: 0,
            range_reads_ratio: 0,
            range_size: 0,
        }
    }
}

impl Display for GroupConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{},{}",
            self.protocol,
            self.v_index_type,
            self.skew,
            self.skew_n,
            self.gc_enable,
            self.threads,
            self.insert_ratio,
            self.update_ratio,
            self.delete_ratio,
            self.point_reads_ratio,
            self.range_reads_ratio,
            self.range_size
        )
    }
}

impl Display for SubGroupConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{},{},{},{},{},{},{}",
            self.skew,
            self.skew_n,
            self.gc_enable,
            self.threads,
            self.insert_ratio,
            self.update_ratio,
            self.delete_ratio,
            self.point_reads_ratio,
            self.range_reads_ratio,
            self.range_size,
        )
    }
}

pub type IndexHandler =
Either<Arc<TransactionManager<FAN_OUT, NUM_RECORDS, Key, Payload>>, CRUDProtocol>;

fn load_config_experiments() -> Vec<GroupConfig> {
    match OpenOptions::new().read(true).open(CONFIG_PARAMETERS) {
        Ok(file) => serde_json::from_reader(file).unwrap_or_else(|error| {
            println!("JSON Error: {}", error);
            println!("Using default ConfigParameters");
            vec![GroupConfig::default()]
        }),
        Err(error) => {
            println!("File Error: {}", error);
            println!("Using default ConfigParameters");
            vec![GroupConfig::default()]
        }
    }
}

pub fn execute_experiments() {
    let groups
        = load_config_experiments();

    let total_exps = groups
        .iter()
        .fold(groups.len(), |acc, group| acc + group.num_chains());

    println!("[Loaded] - Experiments loaded #{total_exps}");
    println!("main_index,\
    experiment_id,\
    chain_id,\
    tx_target,\
    tx_executed,\
    tx_success,\
    tx_fail,\
    time,\
    protocol,\
    version_index,\
    skew,\
    skew_n,\
    gc_enable,\
    threads,\
    insert_ratio,\
    update_ratio,\
    delete_ratio,\
    point_reads_ratio,\
    range_reads_ratio,\
    range_size,\
    log_height,\
    actual_height,\
    blocks_allocated,\
    blocks_reused,\
    olaps_total_time,\
    olaps_workers,\
    olaps_per_worker,\
    olaps_avg_sleep_time,\
    olaps_joint_workload,\
    total_running_time");

    groups
        .into_iter()
        .enumerate()
        .for_each(|(experiment_id, experiment)| {
            let mut olap_handle = None;
            let mut index_handler = None;
            let init_target_tx = experiment.total_tx;
            let mut total_running_time = 0u128;

            if experiment.olap_workers > 0 {
                if let Either::Right(protocol) = experiment.index_handler() {
                    print!("{SYSTEM_STR},{experiment_id},INIT,{init_target_tx}");
                    index_handler = Some(Either::Left(Arc::new(TransactionManager::new_unmanaged(
                        MVTreeSt::make_standard(protocol, RootIndexType::default()),
                        experiment.gc_enable,
                    ))));
                    olap_handle = Some(run_olaps(index_handler.clone().unwrap(),
                                                 experiment.olap_workers,
                                                 experiment.olaps_tx_per_worker,
                                                 init_target_tx));
                }
            } else {
                print!("{SYSTEM_STR},{experiment_id},INIT,{init_target_tx}");
            }

            let terminate_workload = match olap_handle {
                Some(..) => Some(Arc::new(AtomicBool::new(false))),
                _ => None
            };
            let terminate_clone
                = terminate_workload.clone();

            let handler_clone
                = index_handler.clone();

            let exp_clone
                = experiment.clone();

            let mut start_time = SystemTime::now();
            let sp_index_handler
                = spawn(move || start_experiment_by_config(&exp_clone, handler_clone, terminate_clone));

            let mut total_olap_time = 0;
            let mut avg_olap_sleep_time = 0;
            if let Some(olap_handle) = olap_handle {
                let olap_data_result = olap_handle
                    .into_iter()
                    .flat_map(|jh| jh.join().unwrap())
                    .map(|t @ (.., olap_time, _, sleep_time, _)| {
                        total_olap_time += olap_time;
                        avg_olap_sleep_time += sleep_time;
                        t
                    }).collect_vec();

                terminate_workload.map(|shutdown| shutdown.store(true, SeqCst));
                index_handler = Some(sp_index_handler.join().unwrap());

                total_running_time = SystemTime::now()
                    .duration_since(start_time)
                    .unwrap()
                    .as_millis();

                total_olap_time /= 1_000_000;
                avg_olap_sleep_time /= experiment.olap_workers as CurrentVersionSI;
                avg_olap_sleep_time /= experiment.olaps_tx_per_worker as CurrentVersionSI;

                let _nc = fs::remove_file(format!("mv_olap_{experiment_id}_INIT.csv"));
                let mut olap_file = fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .write(true)
                    .open(format!("mv_olap_{experiment_id}_INIT.csv"))
                    .unwrap();

                olap_file.write_all(b"target_snapshot,current_snapshot,sleep_time,range_start,range_end,count_results,latency\n").unwrap();
                for (si, key_range, olap_latency, curr_si, t_sleep, count) in olap_data_result {
                    olap_file.write_all(format!("\
                                      {si},\
                                      {curr_si},\
                                      {t_sleep},\
                                      {},\
                                      {},\
                                      {count},\
                                      {olap_latency}\n",
                                                key_range.lower, key_range.upper).as_bytes())
                        .unwrap();
                }
            } else {
                terminate_workload.map(|shutdown| shutdown.store(true, SeqCst));
                index_handler = Some(sp_index_handler.join().unwrap());
                total_running_time = SystemTime::now()
                    .duration_since(start_time)
                    .unwrap()
                    .as_millis();
            }

            let mut index_handler
                = index_handler.unwrap();

            let (h, r) = height_root(&index_handler);
            let (alloc, reuse) = block_alloc_reuses(&index_handler);
            let (olap_w, olaps_per_w, olaps_joint_workload)
                = (experiment.olap_workers, experiment.olaps_tx_per_worker, experiment.olap_joint_workload);

            println!(",{experiment},{h},{r},{alloc},{reuse},\
            {total_olap_time},{olap_w},{olaps_per_w},{avg_olap_sleep_time},{olaps_joint_workload},{total_running_time}");

            experiment
                .chain_groups
                .into_iter()
                .enumerate()
                .for_each(|(num, inner_group)| {
                    let subgroup = num + 1;
                    let target_tx = inner_group.total_tx;
                    let mut olap_handle = None;

                    if inner_group.olap_workers > 0 {
                        print!("{SYSTEM_STR},{experiment_id},{subgroup},{target_tx}");
                        olap_handle = Some(run_olaps(index_handler.clone(),
                                                     inner_group.olap_workers,
                                                     inner_group.olaps_tx_per_worker,
                                                     init_target_tx));
                    } else {
                        print!("{SYSTEM_STR},{experiment_id},{subgroup},{target_tx}");
                    }

                    if let Either::Left(ref m_manager) = index_handler {
                        if inner_group.gc_enable && !m_manager.is_gc_enabled() {
                            m_manager.enable_gc();
                        } else if !inner_group.gc_enable && m_manager.is_gc_enabled() {
                            m_manager.disable_gc();
                        }

                        m_manager.index().block_manager.reset_alloc_reuse_counts();
                    }

                    let terminate_workload = match olap_handle {
                        Some(..) => Some(Arc::new(AtomicBool::new(false))),
                        _ => None
                    };
                    let terminate_clone
                        = terminate_workload.clone();

                    let exp_clone
                        = inner_group.clone();

                    let handle_clone
                        = index_handler.clone();

                    start_time = SystemTime::now();
                    let sp_index_handler
                        = spawn(move || chain_experiment_by_config(&exp_clone, handle_clone, terminate_clone));

                    let mut total_olap_time = 0;
                    let mut avg_olap_sleep_time = 0;
                    if let Some(olap_handle) = olap_handle {
                        let olap_data_result = olap_handle.into_iter()
                            .flat_map(|jh| jh.join().unwrap())
                            .map(|t @ (.., olap_time, _, olap_sleep_time, _)| {
                                total_olap_time += olap_time;
                                avg_olap_sleep_time += olap_sleep_time;
                                t
                            }).collect_vec();

                        terminate_workload.map(|shutdown| shutdown.store(true, SeqCst));
                        index_handler = sp_index_handler.join().unwrap();

                        total_running_time
                            = SystemTime::now().duration_since(start_time).unwrap().as_millis();

                        total_olap_time /= 1_000_000;
                        avg_olap_sleep_time /= inner_group.olap_workers as CurrentVersionSI;
                        avg_olap_sleep_time /= inner_group.olaps_tx_per_worker as CurrentVersionSI;

                        let _nc = fs::remove_file(format!("mv_olap_{experiment_id}_{subgroup}.csv"));
                        let mut olap_file = fs::OpenOptions::new()
                            .append(true)
                            .create(true)
                            .write(true)
                            .open(format!("mv_olap_{experiment_id}_{subgroup}.csv"))
                            .unwrap();

                        olap_file.write_all(b"target_snapshot,current_snapshot,sleep_time,range_start,range_end,latency\n").unwrap();
                        for (si, key_range, olap_latency, curr_si, sleep_time, count) in olap_data_result {
                            olap_file.write_all(format!("\
                            {si},\
                            {curr_si},\
                            {sleep_time},\
                            {},\
                            {},\
                            {count},\
                            {olap_latency}\n", key_range.lower, key_range.upper).as_bytes()).unwrap();
                        }
                    } else {
                        terminate_workload.map(|shutdown| shutdown.store(true, SeqCst));
                        index_handler = sp_index_handler.join().unwrap();
                        total_running_time = SystemTime::now()
                            .duration_since(start_time)
                            .unwrap()
                            .as_millis();
                    }

                    // drop(olap_handle.take());

                    let (h, r) = height_root(&index_handler);
                    let (alloc, reuse) = block_alloc_reuses(&index_handler);
                    let (olap_w, olaps_per_w, olaps_joint_workload)
                        = (inner_group.olap_workers, inner_group.olaps_tx_per_worker, inner_group.olap_joint_workload);

                    println!(",{},{},{},{h},{r},{alloc},{reuse},\
                    {total_olap_time},{olap_w},{olaps_per_w},{avg_olap_sleep_time},{olaps_joint_workload},{total_running_time}",
                             experiment.protocol,
                             experiment.v_index_type,
                             inner_group);
                });
        })
}

fn start_experiment_by_config(
    config: &GroupConfig,
    index_handler: Option<IndexHandler>,
    terminate_workload: Option<Arc<AtomicBool>>) -> IndexHandler
{
    if terminate_workload.is_some() {
        run_experiment_with_params_until(
            config.threads,
            index_handler.unwrap_or(config.index_handler()),
            config.gc_enable,
            config.skew,
            config.skew_n,
            config.insert_ratio,
            config.update_ratio,
            config.delete_ratio,
            config.point_reads_ratio,
            config.range_reads_ratio,
            config.range_size,
            terminate_workload.unwrap(),
        )
    } else {
        run_experiment_with_params(
            config.threads,
            index_handler.unwrap_or(config.index_handler()),
            config.gc_enable,
            config.skew,
            config.skew_n,
            config.insert_ratio,
            config.update_ratio,
            config.delete_ratio,
            config.point_reads_ratio,
            config.range_reads_ratio,
            config.range_size,
            config.total_tx,
        )
    }
}

fn chain_experiment_by_config(
    config: &SubGroupConfig,
    index_handler: IndexHandler,
    terminate_workload: Option<Arc<AtomicBool>>) -> IndexHandler
{
    if terminate_workload.is_some() {
        run_experiment_with_params_until(
            config.threads,
            index_handler,
            config.gc_enable,
            config.skew,
            config.skew_n,
            config.insert_ratio,
            config.update_ratio,
            config.delete_ratio,
            config.point_reads_ratio,
            config.range_reads_ratio,
            config.range_size,
            terminate_workload.unwrap(),
        )
    } else {
        run_experiment_with_params(
            config.threads,
            index_handler,
            config.gc_enable,
            config.skew,
            config.skew_n,
            config.insert_ratio,
            config.update_ratio,
            config.delete_ratio,
            config.point_reads_ratio,
            config.range_reads_ratio,
            config.range_size,
            config.total_tx,
        )
    }
}

fn run_experiment_with_params_until(
    threads: usize,
    index: IndexHandler,
    gc_enable: bool,
    skew: f64,
    skew_n: usize,
    insert_ratio: usize,
    update_ratio: usize,
    delete_ratio: usize,
    point_reads_ratio: usize,
    range_reads_ratio: usize,
    range_size: u64,
    terminate: Arc<AtomicBool>,
) -> IndexHandler {
    let total_tx_counter
        = Arc::new(AtomicUsize::new(0));

    let (index_handler, handles) = experiment(
        threads,
        index,
        gc_enable,
        skew,
        skew_n,
        insert_ratio,
        update_ratio,
        delete_ratio,
        point_reads_ratio,
        range_reads_ratio,
        range_size,
        total_tx_counter.clone(),
    );

    while !terminate.load(SeqCst) {
        thread::yield_now();
    }

    let bulk_killer = handles
        .into_iter()
        .map(|(handle, killer)| {
            drop(killer);
            handle
        })
        .collect_vec();

    let result = bulk_killer
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect_vec();

    let mut total_time = 0;
    let mut total_success = 0;
    let mut total_error = 0;
    for (_index, (tx_success, tx_error, time)) in result.iter().enumerate() {
        // println!("\t[tid_{index}]: tx_success = {tx_success}, tx_error = {tx_error}, time = {time}");
        total_success += tx_success;
        total_error += tx_error;
        total_time = total_time.max(*time);
    }

    let total_executed_tx = total_success + total_error;

    print!(",{total_executed_tx},{total_success},{total_error},{total_time}");
    // println!("\t---------------------------------------------------------------------------------");
    // println!("\t[Summary] - Tx Executed = {total_executed_tx}, Target Tx = {total_tx}, Total Time = {total_time}");
    // println!("\t---------------------------------------------------------------------------------");

    index_handler
}

fn run_experiment_with_params(
    threads: usize,
    index: IndexHandler,
    gc_enable: bool,
    skew: f64,
    skew_n: usize,
    insert_ratio: usize,
    update_ratio: usize,
    delete_ratio: usize,
    point_reads_ratio: usize,
    range_reads_ratio: usize,
    range_size: u64,
    limit_tx: usize,
) -> IndexHandler {
    let total_tx_counter
        = Arc::new(AtomicUsize::new(0));

    let (index_handler, handles) = experiment(
        threads,
        index,
        gc_enable,
        skew,
        skew_n,
        insert_ratio,
        update_ratio,
        delete_ratio,
        point_reads_ratio,
        range_reads_ratio,
        range_size,
        total_tx_counter.clone(),
    );

    while total_tx_counter.load(SeqCst) < limit_tx {
        thread::yield_now();
    }

    let bulk_killer = handles
        .into_iter()
        .map(|(handle, killer)| {
            drop(killer);
            handle
        })
        .collect_vec();

    let result = bulk_killer
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect_vec();

    let mut total_time = 0;
    let mut total_success = 0;
    let mut total_error = 0;
    for (_index, (tx_success, tx_error, time)) in result.iter().enumerate() {
        // println!("\t[tid_{index}]: tx_success = {tx_success}, tx_error = {tx_error}, time = {time}");
        total_success += tx_success;
        total_error += tx_error;
        total_time = total_time.max(*time);
    }

    let total_executed_tx = total_success + total_error;

    print!(",{total_executed_tx},{total_success},{total_error},{total_time}");
    // println!("\t---------------------------------------------------------------------------------");
    // println!("\t[Summary] - Tx Executed = {total_executed_tx}, Target Tx = {total_tx}, Total Time = {total_time}");
    // println!("\t---------------------------------------------------------------------------------");

    index_handler
}

pub const MEM_SZ_KB: usize = 5; // 1 = 1KB, 2 = 2KB, 3 = 3KB, 4= 4KB
pub const FILLED_BLOCK: usize = (127 / 4) * MEM_SZ_KB;
pub const F_MUL: usize = 1;
pub const N_MUL: usize = 1;
pub const N_OFF: usize = 0;
pub const F_OFF: usize = 0;
pub const N_ABS_OFF: usize = 28;
pub const F_ABS_OFF: usize = 28;

// pub const FAN_OUT: usize = F_MUL * (FILLED_BLOCK - F_OFF) - F_ABS_OFF;
// pub const NUM_RECORDS: usize = N_MUL * (FILLED_BLOCK - N_OFF) - N_ABS_OFF;

pub const FAN_OUT: usize = 127;
pub const NUM_RECORDS: usize = 127;

pub type MVTree = MVTreeSt<FAN_OUT, NUM_RECORDS, Key, Payload>;

pub type Key = u64;
// pub type Payload = PayloadIndirection;
pub type Payload = u64;

pub const PAYLOAD_STR_LEN_MIN: usize = 704;
pub const PAYLOAD_STR_LEN_MAX: usize = 7078;
pub const PAYLOAD_ATTR_STR_COUNT: usize = 67;

fn rnd_str(len_min: usize, len_max: usize) -> String {
    let len = rand::rng().random_range(len_min..=len_max);
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn rnd_str_vec(items: usize, str_len_min: usize, str_len_max: usize) -> Vec<String> {
    (0..items)
        .map(|i| rnd_str(str_len_min, str_len_max))
        .collect()
}
#[derive(Clone)]
pub struct PayloadIndirection(Box<PayloadData>);

#[derive(Clone)]
pub struct PayloadData {
    attributes: Vec<String>,
}

impl PayloadData {
    pub fn attr(&self, i: usize) -> &str {
        self.attributes.get(i).unwrap()
    }
}

impl Default for PayloadIndirection {
    fn default() -> Self {
        Self(Box::new(PayloadData {
            attributes: rnd_str_vec(
                PAYLOAD_ATTR_STR_COUNT,
                PAYLOAD_STR_LEN_MIN,
                PAYLOAD_STR_LEN_MAX),
        }))
    }
}

pub fn inc_key(k: Key) -> Key {
    k.checked_add(1).unwrap_or(Key::MAX)
}

pub fn dec_key(k: Key) -> Key {
    k.checked_sub(1).unwrap_or(Key::MIN)
}


fn experiment(
    num_threads: usize,
    index_handler: IndexHandler,
    gc_enable: bool,
    skew: f64,
    skew_n: usize,
    insert_ratio: usize,
    update_ratio: usize,
    delete_ratio: usize,
    points_reads_ratio: usize,
    range_reads_ratio: usize,
    range_size: u64,
    total_tx: Arc<AtomicUsize>,
) -> (IndexHandler, Vec<(JoinHandle<(usize, usize, u128)>, Sender<()>)>)
{
    debug_assert_eq!(
        insert_ratio + update_ratio + delete_ratio + points_reads_ratio + range_reads_ratio,
        100,
        "Ratios must add to 100%"
    );

    let manager = match index_handler {
        Either::Left(m_manager) => m_manager,
        Either::Right(protocol) => Arc::new(TransactionManager::new_unmanaged(
            MVTreeSt::make_standard(protocol, RootIndexType::default()),
            gc_enable,
        )),
    };

    type WorkerSignal = ();

    let is_nop =
        insert_ratio == 0 &&
            delete_ratio == 0 &&
            update_ratio == 0 &&
            points_reads_ratio == 0 &&
            range_reads_ratio == 0;

    let handles = (0..num_threads)
        .map(|_| {
            let manager = manager.clone();

            let (thread_killer, thread_control)
                = bounded::<WorkerSignal>(0);

            let total_tx = total_tx.clone();

            // tx_success, tx_error, time_spent
            let handle = spawn(move || {
                let mut sampler
                    = Sampler::new(skew, skew_n as Key);

                let (mut tx_success, mut tx_error, start_execution_time) =
                    (0usize, 0usize, SystemTime::now());

                let random_number
                    = rand::rng().random_range(0..100);

                let local_tx = move |key: Key| -> AtomicTransaction<Key, Payload> {
                    if random_number < insert_ratio {
                        AtomicTransaction::from_crud(CRUDOperation::Insert(key, Payload::default()))
                    } else if random_number < insert_ratio + points_reads_ratio {
                        AtomicTransaction::from_crud(CRUDOperation::PointSi(key))
                    } else if random_number < insert_ratio + points_reads_ratio + range_reads_ratio
                    {
                        if u64::MAX - range_size <= key {
                            AtomicTransaction::from_crud(CRUDOperation::RangeSi(
                                (key..=u64::MAX).into(),
                            ))
                        } else {
                            AtomicTransaction::from_crud(CRUDOperation::RangeSi(
                                (key..key + range_size).into(),
                            ))
                        }
                    } else if random_number
                        < insert_ratio + points_reads_ratio + range_reads_ratio + delete_ratio
                    {
                        AtomicTransaction::from_crud(CRUDOperation::Delete(key))
                    } else {
                        AtomicTransaction::from_crud(CRUDOperation::Update(key, Payload::default()))
                    }
                };

                loop {
                    match thread_control.try_recv() {
                        Err(TryRecvError::Disconnected) => break,
                        _ if is_nop => thread::sleep(Duration::from_millis(1)),
                        _ => {
                            let next
                                = local_tx(sampler.sample());

                            match manager.execute_on_caller_thread(next).unwrap_atomic() {
                                Ok(_) => tx_success += 1,
                                Err(_) => tx_error += 1,
                            }

                            total_tx.fetch_add(1, Relaxed);
                        }
                    }
                }

                (
                    tx_success,
                    tx_error,
                    SystemTime::now()
                        .duration_since(start_execution_time)
                        .unwrap()
                        .as_millis(),
                )
            });

            (handle, thread_killer)
        })
        .collect_vec();

    (IndexHandler::Left(manager), handles)
}

pub fn format_insertions(mut i: usize) -> String {
    let mut parts = Vec::new();

    let units = [
        (1_000_000_000, "B"),
        (1_000_000, "Mio"),
        (1_000, "K"),
    ];

    for &(value, suffix) in &units {
        if i >= value {
            let count = i / value;
            parts.push(format!("{} {}", count, suffix));
            i %= value;
        }
    }

    if i > 0 {
        parts.push(i.to_string());
    }

    if parts.is_empty() {
        "0".to_string()
    } else {
        parts.join(" + ")
    }
}

fn block_alloc_reuses(index_handler: &IndexHandler) -> (usize, usize) {
    if let Either::Left(manager) = index_handler {
        (manager.index().block_manager.alloc_count.load(SeqCst) as _,
         manager.index().block_manager.reuse_count.load(SeqCst) as _)
    } else {
        unreachable!()
    }
}

pub fn height_root(index_handler: &IndexHandler) -> (usize, usize) {
    if let Either::Left(m_manager) = index_handler {
        let index = m_manager.index();
        let log_height = index.root.height() as usize;
        let mut real_height = 1usize;

        let mut curr_block = index.root.borrow_read().block();
        let mut curr_guard = curr_block.borrow_read();
        loop {
            match curr_guard.deref().unwrap().as_page_ref() {
                PageType::IndexRef(page) => {
                    curr_block = page.get_pointer(0).clone();
                    curr_guard = curr_block.borrow_read();
                }
                _ => return (log_height, real_height),
            }
            real_height += 1;
        }
    }
    unreachable!()
}
