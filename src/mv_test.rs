use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_tree::mvbt::MVBTSt;
use crate::mv_tx_model::transaction::{AtomicTransaction};
use crossbeam_channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use itertools::{Either, Itertools};
use rand::{Rng, RngExt};
use std::fmt::{Display, Formatter};
use std::fs::OpenOptions;
use std::sync::atomic::{fence, AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, SeqCst};
use std::sync::Arc;
use std::{fs, hint, mem, thread};
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::io::{BufReader, BufWriter, Read, Write};
use std::thread::{spawn, yield_now, JoinHandle};
use std::time::{Duration, Instant, SystemTime};
use parking_lot::Mutex;
use rand::distr::{Alphanumeric, Distribution, Uniform};
use rand::prelude::SliceRandom;
use rand::rngs::ThreadRng;
use rand_distr::Zipf;
// use crate::mv_block::block_handle::NODES_REQUEST;
use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
// use crate::mv_tx_query::tx_manager::TransactionManager;
use crate::mv_page_model::node::PageType;
use crate::mv_query::dispatch::RANGE_DISPATCH_LAZY;
use crate::mv_root::index_root::RootIndexType;
use crate::mv_sync::smart_cell::sched_yield;
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

// pub static mut RESTARTS_COUNTER: [AtomicUsize; 100] = [
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
//     AtomicUsize::new(0), AtomicUsize::new(0),
// ];

fn olap_tests(index: Arc<MVBT>,
              num_olaps: usize,
              tx_per_thread: usize,
              skew: f32,
              range: Either<Key, Arc<AtomicU64>>,
              fixed_si: bool,
              control_signal: Option<Receiver<ThreadWorkerInfo>>) -> (usize, u128)
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

    let v_index = format!("mv_{}",
                          match index.root_star_index() {
                              RootIndexType::FrugalList => "fg",
                              RootIndexType::SkipList => "sk",
                              RootIndexType::BTree => "bt",
                              RootIndexType::LinkedList => "ll"
                          });

    let lazy = if RANGE_DISPATCH_LAZY {
        "_lazy"
    } else { "" };

    let mut olaps = vec![];

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
            target_root_number,\
            current_roots_count,\
            sleep_time,\
            range_start,\
            range_end,\
            count_results,\
            latency\n",
        )
        .unwrap();

    let g_counter = Arc::new(AtomicUsize::new(0));

    let start_olap_time = Instant::now();
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
            // let range = range.left().unwrap_or(0);
            while tx_c < tx_per_thread {
                let key_min = 0;
                let key_max = Key::MAX;

                let current_si = index.current_version_for_reader();
                let si = if fixed_si {
                    current_si
                } else {
                    rand::random_range(version_handle::START_VERSION..=current_si)
                };

                let (current_root_position, roots_count)
                = (0,0);
                    // = index.retrieve_root_number_for(si);
                // println!("Min = {key_min}, max = {key_max}");

                let op = CRUDOperation::Range((key_min..key_max).into(), si);
                // let op = CRUDOperation::Point(key_min, si);
                let time_start
                    = SystemTime::now();

                let crud =
                    index.dispatch_crud(op);

                let time_spent
                    = SystemTime::now().duration_since(time_start).unwrap().as_nanos();

                let count_results = match crud {
                    CRUDOperationResult::MatchedRecords(data) => data.len(),
                    _ => panic!()
                };

                let _ = count_olaps.fetch_add(1, Relaxed);

                results.push(
                    (si, current_si, 0u128, key_min, key_min, count_results, time_spent,
                    current_root_position, roots_count));

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

    let time_olap = start_olap_time.elapsed().as_nanos();
    // mem::drop(updaters);

    olaps.into_iter()
        .for_each(|(target_si,
                       current_si,
                       sleep_time,
                       key_min,
                       key_max,
                       count_results,
                       time_spent,
                       current_root_psotion,
                       c_roots_count)|
            {
                olap_file.write_all(format!("\
                            {target_si},\
                            {current_si},\
                            {current_root_psotion},\
                            {c_roots_count},\
                            {sleep_time},\
                            {key_min},\
                            {key_max},\
                            {count_results},\
                            {time_spent}\n").as_bytes()).unwrap();
            });

    (g_counter.load(SeqCst), time_olap)
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
    let index = Arc::new(MVBT::default());
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
        "sk" => RootIndexType::SkipList,
        "ll" => RootIndexType::LinkedList,
        "fg" => RootIndexType::FrugalList,
        "bt" => RootIndexType::BTree,
        _ => RootIndexType::default()
    };
    println!("RootStar = {}", root_star_index);

    let tree
        = Arc::new(MVBT::make_standard(root_star_index));
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

pub fn main_load_ycsb(parms: Vec<String>) {
    println!("###### Command: {} ######", parms.iter().skip(1).join(" "));

    let query_file_name = parms[2].to_string();
    let concurrent = true;
    let num_olaps: usize = parms[4].parse().unwrap();

    let scans_per_thread = parms[5].parse().unwrap();

    let skew: f64 = parms[6].parse().unwrap();
    let range = parms[7].parse().unwrap_or(Key::MAX);
    let root_star_index = match parms[8].as_str() {
        "sk" => RootIndexType::SkipList,
        "ll" => RootIndexType::LinkedList,
        "fg" => RootIndexType::FrugalList,
        "bt" => RootIndexType::BTree,
        _ => RootIndexType::default()
    };

    let gc = parms[9].parse::<bool>().unwrap_or(false);
    let update_in_place = if gc {
        parms[10].parse::<bool>().unwrap_or(false)
    } else { false };

    let index
        = Arc::new(MVBTSt::<FAN_OUT, NUM_RECORDS, Key, Payload>::make_standard(root_star_index));

    let mut gc_str = "Off".to_string();
    if gc {
        index.enable_gc(update_in_place);
        gc_str = format!("On (UIP = {})", update_in_place);
    }

    let oltp_threads = if concurrent {
        scans_per_thread
    }
    else {
        1
    };
    println!("- QueryFile = {query_file_name}\n\
                - Concurrent = {concurrent}\n\
                - OLTP Threads = {oltp_threads}\n\
                - OLAP Threads = {num_olaps} (Cores = {}, Threads = {})\n\
                - Scans/Thread = {}\n\
                - Skew = {skew}\n\
                - Range = {range}\n\
                - Root* = {root_star_index}\n\
                - GC = {gc_str}",
             num_cpus::get_physical(),
             num_cpus::get(),
             if concurrent { format!("Continuous\n- OLTP Threads = {scans_per_thread}") } else { format!("{scans_per_thread}") });

    let oltp_there = fs::exists("oltp.csv").unwrap();
    let mut oltp_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("oltp.csv")
        .unwrap();

    if !oltp_there {
        oltp_file.write_all(b"\
            is_concurrent,\
            oltp_threads,\
            olap_threads,\
            v_index,\
            skew,\
            gc,\
            update_in_place,\
            slice_per_thread,\
            rest_slice,\
            blocks_allocated,\
            blocks_reused,\
            total_num_scan_tx,\
            total_num_oltp_tx,\
            total_oltp_time,\
            total_olap_time\n"
        ).unwrap();
    }
    if concurrent {
        (0..15_000_000).for_each(|i| {
            let _ = index.dispatch_crud(
                CRUDOperation::Insert(rand::random_range(0..Key::MAX), Payload::default()));
        });
        // index.block_manager.alloc_count.store(0, Ordering::SeqCst);
        // index.block_manager.reuse_count.store(0, Ordering::SeqCst);

        // TODO: End experimental setting
        let counter_inserts
            = Arc::new(AtomicU64::new(0));

        oltp_file.write_all(format!("\
            true,\
            {oltp_threads},\
            {num_olaps},\
            cMVBT({root_star_index}),\
            {skew},\
            {gc},\
            {update_in_place},\
            dynamic,\
            0").as_bytes()).unwrap();

        let start_time_oltp = Instant::now();
        let oltp_joins = (0..oltp_threads)
            .into_iter()
            .map(|_| {
                let index = index.clone();
                let counter_inserts = counter_inserts.clone();
                spawn(move || {
                    let mut count_crud = 0;
                    while counter_inserts.fetch_add(1, Relaxed) < 10_000_000 {
                        let _ = index.dispatch_crud(
                            CRUDOperation::Insert(
                                rand::random_range(0..Key::MAX), Payload::default()));

                        count_crud += 1;
                    }

                    count_crud
                })
            }).collect_vec();

        let oltp_executed = oltp_joins
            .into_iter()
            .map(|j| j.join().unwrap())
            .sum::<usize>();

        let oltp_total_time = start_time_oltp.elapsed().as_nanos();

        let (num_scans_executed, olap_total_time) = (0,0);

        // let reuse_blocks
        //     = index.block_manager.reuse_count.load(SeqCst);
        // let alloc_blocks
        //     = index.block_manager.alloc_count.load(SeqCst);

        let reuse_blocks
            = 0;
        let alloc_blocks
            = 0;

        let oltp_executed = counter_inserts.load(SeqCst) as _;
        oltp_file.write_all(format!(",\
        {alloc_blocks},\
        {reuse_blocks},\
        {num_scans_executed},\
        {oltp_executed},\
        {oltp_total_time},\
        {olap_total_time}\n").as_bytes()).unwrap();

        println!("- Executed {} OLTPs from {query_file_name}\n\
        - Executed = {} OLAPs", format_insertions(oltp_executed),
                 format_insertions(num_scans_executed));

        println!("###### End Command: {} ######", parms.iter().skip(1).join(" "));
    }

    oltp_file.flush().unwrap();
    // println!("{}", NODES_REQUEST.load(SeqCst));
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
        "sk" => RootIndexType::SkipList,
        "ll" => RootIndexType::LinkedList,
        "fg" => RootIndexType::FrugalList,
        "bt" => RootIndexType::BTree,
        _ => RootIndexType::default()
    };

    let gc = parms[9].parse::<bool>().unwrap_or(false);
    let update_in_place = if gc {
        parms[10].parse::<bool>().unwrap_or(false)
    } else { false };

    let init_keys = parms[11].parse::<usize>().unwrap_or(100_000);

    let index
        = Arc::new(MVBTSt::make_standard(root_star_index));

    let mut gc_str = "Off".to_string();
    if gc {
        index.enable_gc(update_in_place);
        gc_str = format!("On (UIP = {})", update_in_place);
    }

    let oltp_threads = if concurrent {
        scans_per_thread
    }
    else {
        1
    };
    println!("- QueryFile = {query_file_name}\n\
                - Concurrent = {concurrent}\n\
                - OLTP Threads = {oltp_threads}\n\
                - OLAP Threads = {num_olaps} (Cores = {}, Threads = {})\n\
                - Scans/Thread = {}\n\
                - Skew = {skew}\n\
                - Range = {range}\n\
                - Root* = {root_star_index}\n\
                - GC = {gc_str}",
             num_cpus::get_physical(),
             num_cpus::get(),
             if concurrent { format!("Continuous\n- OLTP Threads = {scans_per_thread}") } else { format!("{scans_per_thread}") });

    let oltp_there = fs::exists("oltp.csv").unwrap();
    let mut oltp_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("oltp.csv")
        .unwrap();

    if !oltp_there {
        oltp_file.write_all(b"\
            is_concurrent,\
            oltp_threads,\
            olap_threads,\
            v_index,\
            skew,\
            gc,\
            update_in_place,\
            slice_per_thread,\
            rest_slice,\
            blocks_allocated,\
            blocks_reused,\
            total_num_scan_tx,\
            total_num_oltp_tx,\
            total_oltp_time,\
            total_olap_time\n"
        ).unwrap();
    }

    if concurrent {
        let query_file_name_clone = query_file_name.clone();
        let mut oltp = load_query_into_memory(
            query_file_name_clone.as_str());

        // TODO: Explicit for Experiment
        oltp.drain(0..init_keys).for_each(|i| {
            let _ = index.dispatch_crud(i);
        });
        // index.block_manager.alloc_count.store(0, Ordering::SeqCst);
        // index.block_manager.reuse_count.store(0, Ordering::SeqCst);

        // TODO: End experimental setting
        let oltp_threads = scans_per_thread;
        let slice = oltp.len() / oltp_threads;

        let mut work_oltp = (0..oltp_threads)
            .map(|_| oltp.drain(..slice).collect_vec())
            .collect_vec();

        let rest_slice = oltp.len();
        work_oltp.first_mut().unwrap().extend(oltp);
        oltp_file.write_all(format!("\
            true,\
            {oltp_threads},\
            {num_olaps},\
            cMVBT({root_star_index}),\
            {skew},\
            {gc},\
            {update_in_place},\
            {slice},\
            {rest_slice}").as_bytes()).unwrap();

        let start_time_oltp = Instant::now();
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

        let index_olaps = index.clone();
        let olaps = spawn(move || olap_tests(
            index_olaps,
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

        let oltp_total_time = start_time_oltp.elapsed().as_nanos();
        drop(olap_signal);
        let (num_scans_executed, olap_total_time) = olaps.join().unwrap();

        // let reuse_blocks
        //     = index.block_manager.reuse_count.load(SeqCst);
        // let alloc_blocks
        //     = index.block_manager.alloc_count.load(SeqCst);

        let reuse_blocks
            = 0;
        let alloc_blocks
            = 0;

        oltp_file.write_all(format!(",\
        {alloc_blocks},\
        {reuse_blocks},\
        {num_scans_executed},\
        {oltp_executed},\
        {oltp_total_time},\
        {olap_total_time}\n").as_bytes()).unwrap();

        println!("- Executed {} OLTPs from {query_file_name}\n\
        - Executed = {} OLAPs", format_insertions(oltp_executed),
                 format_insertions(num_scans_executed));

        println!("###### End Command: {} ######", parms.iter().skip(1).join(" "));
    } else {
        let mut oltp_tx_buff = load_query_into_memory(
            query_file_name.as_str());

        // TODO: Explicit for Experiment
        oltp_tx_buff.drain(0..init_keys).for_each(|i| {
            let _ = index.dispatch_crud(i);
        });

        let num = oltp_tx_buff.len();
        let start_oltp_time = Instant::now();

        oltp_tx_buff.into_iter().for_each(|crud| {
            let _ = index.dispatch_crud(crud);
        });

        let oltp_total_time = start_oltp_time.elapsed().as_nanos();

        println!("- Executed {} CRUD operations from {query_file_name}, \
                 starting OLAPs...", format_insertions(num));

        let (num_scans_executed, olap_total_time) = olap_tests(
            index.clone(),
            num_olaps,
            scans_per_thread,
            skew,
            Either::Left(range),
            false,
            None);

        // let reuse_blocks
        //     = index.block_manager.reuse_count.load(SeqCst);
        // let alloc_blocks
        //     = index.block_manager.alloc_count.load(SeqCst);

        let reuse_blocks
            = 0;
        let alloc_blocks
            = 0;

        oltp_file.write_all(format!("\
            false,\
            1,\
            {num_olaps},\
            cMVBT({root_star_index}),\
            {skew},\
            {gc},\
            {update_in_place},\
            {num},\
            0,\
            {alloc_blocks},\
            {reuse_blocks},\
            {num_scans_executed},\
            {num},\
            {oltp_total_time},\
            {olap_total_time}\n").as_bytes()).unwrap();

        println!("- Executed = {} OLAPs", format_insertions(num_scans_executed));
        println!("###### End Command: {} ######", parms.iter().skip(1).join(" "));
    }

    oltp_file.flush().unwrap();
    // println!("{}", NODES_REQUEST.load(SeqCst));
}
pub(crate) fn main_load_cc_new(parms: Vec<String>) {
    let query_file_name = parms[2].to_string();

    let num_olaps = parms[3].parse().unwrap();
    let workers_per_thread = parms[4].parse().unwrap();
    let skew = parms[5].parse().unwrap();
    let root_star_index = match parms[6].as_str() {
        "sk" => RootIndexType::SkipList,
        "ll" => RootIndexType::LinkedList,
        "fg" => RootIndexType::FrugalList,
        "bt" => RootIndexType::BTree,
        _ => RootIndexType::default()
    };
    let index
        = Arc::new(MVBTSt::make_standard(root_star_index));

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

    let skew = parms[8].parse::<f64>().unwrap();

    println!("Generating init_pop = {init_population}\n\
                total_blocks = {total_blocks}\n\
                block_inserts = {block_inserts}\n\
                block_updates = {block_updates}\n\
                block_deletes = {block_deletes}\n\
                skew = {skew}\n");
    generate_query(
        query_file_name,
        init_population,
        total_blocks,
        block_inserts,
        block_updates,
        block_deletes,
        skew
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
        0f64
    );
    println!("Finished generate.")
}


fn generate_query(
    query_file_name: &str,
    init_population: usize,
    total_blocks: usize,
    block_inserts: usize,
    block_updates: usize,
    block_deletes: usize,
    skew: f64)
{
    let mv_tree
        = Arc::new(MVBT::default());

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

    println!("Finished generating {} init keys", init_population);
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

    init_pop_order.into_iter().for_each(|op| {
        // println!("Executing {}", op);
        io_handle(op)
    });

    let block = {
        let mut crud
            = Vec::with_capacity(block_inserts + block_updates + block_deletes);

        crud.extend((0..block_inserts).map(|_| CRUDOperation::<Key, Payload>::InsertRand));
        crud.extend((0..block_updates).map(|_| CRUDOperation::<Key, Payload>::UpdateRand));
        crud.extend((0..block_deletes).map(|_| CRUDOperation::<Key, Payload>::DeleteRand));
        crud
    };

    let zipf = Zipf::new(Key::MAX as f64, skew);
    let payload = Payload::default();

    let gen_block = || {
        let mut crud = block.clone();
        crud.shuffle(&mut rand::rng());

        if skew == 0_f64 {
            crud
        }
        else {
            let key = zipf.as_ref().unwrap().sample(&mut rand::rng()) as Key;
            crud.iter_mut().for_each(|c| {
                match c {
                    CRUDOperation::UpdateRand =>
                        *c = CRUDOperation::Update(key, payload),
                    CRUDOperation::DeleteRand =>
                        *c = CRUDOperation::Delete(key),
                    CRUDOperation::InsertRand =>
                        *c = CRUDOperation::Insert(key, payload),
                    _ => panic!("Unknown CRUD Operation for blocks"),
                }
            });
            crud
        }
    };

    for _ in 0..total_blocks {
        for op in gen_block() {
            match mv_tree.dispatch_crud(op.clone()) {
                CRUDOperationResult::InsertedRand(key, _) => io_handle(
                    CRUDOperation::Insert(key, 0)),
                CRUDOperationResult::UpdatedRand(key, _) => io_handle(
                    CRUDOperation::Update(key, 0)),
                CRUDOperationResult::DeletedRand(key, _) => io_handle(
                    CRUDOperation::Delete::<_, Payload>(key)),
                CRUDOperationResult::Error =>
                    panic!("Error on rand query; generate_query(): CRUD({op}) ---> Result(Error)"),
                _ => io_handle(op),
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
fn load_query(query_file: &str, index: Arc<MVBT>,
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




pub const FAN_OUT: usize = 125;
pub const NUM_RECORDS: usize = 125;

pub type MVBT = MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>;

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