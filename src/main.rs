use std::{env, fs, thread};
use std::arch::x86_64::_mm_mfence;
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use chrono::{DateTime, Local};
use itertools::{Either, Itertools};
use serde::{Deserialize, Serialize};
use crate::mv_test::{experiment, format_insertions, IndexHandler};
use crate::mv_tree::mvbplus_tree::ClockType;
use crate::mv_tree::locking_strategy::{CRUDProtocol, LockingStrategy};
use crate::mv_utils::smart_cell::ENABLE_YIELD;

mod mv_block;
mod mv_crud_model;
mod mv_page_model;
mod mv_record_model;
mod mv_tree;
mod mv_utils;
mod mv_test;
mod mv_tx_model;

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

const CONFIG_PARAMETERS: &'static str = "config.json";

#[derive(Clone, Serialize, Deserialize)]
struct GroupConfig {
    group_id: usize,
    sub_group_execute_order: usize,
    protocol: CRUDProtocol,
    clock: ClockType,
    range_start: u64,
    range_end: u64,
    lambda: f64,
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
        100 == self.insert_ratio +
            self.update_ratio +
            self.delete_ratio +
            self.point_reads_ratio +
            self.range_reads_ratio &&
            self.threads > 1 && self.protocol.is_mono_writer() && self.is_read_only() ||
            self.threads == 1 && self.protocol.is_mono_writer() ||
            !self.protocol.is_mono_writer()
    }

    fn index_handler(&self) -> IndexHandler {
        Either::Right((self.protocol.clone(), self.clock.clone()))
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
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            group_id: 0,
            sub_group_execute_order: 0,
            protocol: Default::default(),
            clock: ClockType::FREE,
            range_start: 0,
            range_end: u64::MAX,
            lambda: 0.1,
            gc_enable: false,
            threads: 1,
            total_tx: 10_000_000,
            insert_ratio: 100,
            update_ratio: 0,
            delete_ratio: 0,
            point_reads_ratio: 0,
            range_reads_ratio: 0,
            range_size: 0,
        }
    }
}

fn main() {
    make_splash();

    // println!("{}", serde_json::to_string(&orwc()).unwrap());
    let configs: Vec<GroupConfig> = match OpenOptions::new().read(true).open(CONFIG_PARAMETERS) {
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
    };

    println!("[Info] - Total Loaded #{} Experiments", configs.len());
    execute_experiments(configs);
    // 
    // let mut groups = configs
    //     .into_iter()
    //     .into_group_map_by(|c| c.group_id)
    //     .into_iter()
    //     .sorted_by_key(|(group, _)| *group)
    //     .map(|(_, groups)| groups)
    //     .collect_vec();
    // 
    // groups
    //     .iter_mut()
    //     .for_each(|group|
    //         group.sort_by_key(|c| c.sub_group_execute_order));
    // 
    // println!("[Info] - Number of Experiment-Groups #{}", groups.len());
    // 
    // for (group_id, group) in groups.into_iter().enumerate() { // E x p e r i m e n t
    //     let mut index_handler
    //         = group[0].index_handler();
    // 
    //     for (config_id, config) in group.into_iter().enumerate() {
    //         println!("------------------------------------- # {group_id}.{config_id} # -------------------------------------");
    //         println!("[Configuration] - Protocol \t\t= {}", config.protocol);
    //         println!("[Configuration] - Clock \t\t= {}", config.clock);
    //         println!("[Configuration] - Lambda \t\t= {}", config.lambda);
    //         println!("[Configuration] - GC \t\t\t= {}", config.gc_enable);
    //         println!("[Configuration] - Threads \t\t= {}", config.threads);
    //         println!("[Configuration] - Total \t\t= {}", format_insertions(config.total_tx));
    //         println!("[Configuration] - InsertRatio \t\t= {}%", config.insert_ratio);
    //         println!("[Configuration] - UpdateRatio \t\t= {}%", config.update_ratio);
    //         println!("[Configuration] - DeleteRatio \t\t= {}%", config.delete_ratio);
    //         println!("[Configuration] - PointReadsRatio \t= {}%", config.point_reads_ratio);
    //         println!("[Configuration] - RangeReadsRatio \t= {}%", config.range_reads_ratio);
    //         println!("[Configuration] - RangeSize \t\t= {}", config.range_size);
    //         if !config.is_valid() {
    //             println!("***[Configuration] - Invalid Configuration!");
    //             continue;
    //         }
    // 
    //         if let Either::Left(m_manager) = &mut index_handler {
    //             if config.gc_enable && !m_manager.is_gc_enabled() {
    //                 println!("[Note]\t\t- Enabling Garbage Collector...");
    //                 m_manager.enable_gc();
    // 
    //                 assert!(m_manager.is_gc_enabled())
    //             } else if !config.gc_enable && m_manager.is_gc_enabled() {
    //                 println!("[Note]\t\t- Disabling Garbage Collector...");
    //                 m_manager.disable_gc();
    // 
    //                 assert!(!m_manager.is_gc_enabled())
    //             }
    //         }
    // 
    //         println!("----------------------------------------------------------------------------------------");
    //         println!("----------------------------------------------------------------------------------------");
    // 
    //         index_handler = run_experiment_with_params(
    //             config.threads,
    //             index_handler,
    //             config.gc_enable,
    //             config.lambda,
    //             config.range_start,
    //             config.range_end,
    //             config.insert_ratio,
    //             config.update_ratio,
    //             config.delete_ratio,
    //             config.point_reads_ratio,
    //             config.range_reads_ratio,
    //             config.range_size,
    //             config.total_tx,
    //         );
    //     }
    // }
}

fn execute_experiments(groups: Vec<GroupConfig>) {
    println!("group_id,subgroup_id,tx_target,tx_executed,tx_success,tx_fail,time");
    groups.into_iter()
        .into_group_map_by(|c| c.group_id)
        .into_iter()
        .sorted_by_key(|(group, _)| *group)
        .map(|(_, mut groups)| {
            groups.sort_by_key(|group| group.sub_group_execute_order);
            VecDeque::from(groups)
        })
        .into_iter()
        .for_each(|mut group| {
            let init_group = group.front().unwrap();
            let curr_group_id = init_group.group_id;
            let subgroup = init_group.sub_group_execute_order;
            let target_tx = init_group.total_tx;
            print!("{curr_group_id},{subgroup},{target_tx}");
            
            // println!("[Starting Experiment] - [Group: {curr_group_id}]");
            // println!("\t[Initialize Experiment] - [Sub-Group: {}]",
            //          group.front().unwrap().sub_group_execute_order);
            
            let mut index_handler
                = start_experiment_by_config(group.pop_front().unwrap());

            group.into_iter().for_each(|inner_group| {
                // println!("\t[Chain Experiment] - [Sub-Group: {}]", inner_group.sub_group_execute_order);

                let subgroup = inner_group.sub_group_execute_order;
                let target_tx = inner_group.total_tx;
                print!("{curr_group_id},{subgroup},{target_tx}");
                
                index_handler = chain_experiment_by_config(
                    inner_group,
                    index_handler.clone());
            });
            // println!("[Ending Experiment] - [Group: {}]", curr_group_id);
        })
}

fn start_experiment_by_config(config: GroupConfig) -> IndexHandler {
    run_experiment_with_params(
        config.threads,
        config.index_handler(),
        config.gc_enable,
        config.lambda,
        config.range_start,
        config.range_end,
        config.insert_ratio,
        config.update_ratio,
        config.delete_ratio,
        config.point_reads_ratio,
        config.range_reads_ratio,
        config.range_size,
        config.total_tx)
}

fn chain_experiment_by_config(config: GroupConfig, index_handler: IndexHandler) -> IndexHandler {
    run_experiment_with_params(
        config.threads,
        index_handler,
        config.gc_enable,
        config.lambda,
        config.range_start,
        config.range_end,
        config.insert_ratio,
        config.update_ratio,
        config.delete_ratio,
        config.point_reads_ratio,
        config.range_reads_ratio,
        config.range_size,
        config.total_tx)
}

fn run_experiment_with_params(threads: usize,
                              index: IndexHandler,
                              gc_enable: bool,
                              lambda: f64,
                              range_start: u64,
                              range_end: u64,
                              insert_ratio: usize,
                              update_ratio: usize,
                              delete_ratio: usize,
                              point_reads_ratio: usize,
                              range_reads_ratio: usize,
                              range_size: u64,
                              total_tx: usize,
) -> IndexHandler {
    let total_tx_counter
        = Arc::new(AtomicUsize::new(0));

    let (index_handler, handles) = experiment(
        threads,
        index,
        gc_enable,
        lambda,
        range_start,
        range_end,
        insert_ratio,
        update_ratio,
        delete_ratio,
        point_reads_ratio,
        range_reads_ratio,
        range_size,
        total_tx_counter.clone(),
    );

    while total_tx_counter.load(SeqCst) < total_tx {
        thread::yield_now();
    }

    let bulk_killer = handles
        .into_iter()
        .map(|(handle, killer)| {
            drop(killer);
            handle
        }).collect_vec();

    let result = bulk_killer
        .into_iter()
        .map(|handle|
            handle.join().unwrap())
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

    println!(",{total_executed_tx},{total_success},{total_error},{total_time}");
    // println!("\t---------------------------------------------------------------------------------");
    // println!("\t[Summary] - Tx Executed = {total_executed_tx}, Target Tx = {total_tx}, Total Time = {total_time}");
    // println!("\t---------------------------------------------------------------------------------");

    index_handler
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