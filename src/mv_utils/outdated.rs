use std::collections::VecDeque;
use std::ffi::c_void;
use std::fs::OpenOptions;
use std::{fs, mem, ptr, thread};
use std::fmt::{Display, Formatter};
use std::io::{BufWriter, Write};
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::{fence, AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::thread::{spawn, yield_now, JoinHandle};
use std::time::{Duration, SystemTime};

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
const FAN_OUT: usize = mv_test::FAN_OUT;
const NUM_RECORDS: usize = mv_test::NUM_RECORDS;
pub const VALIDATE_OPERATION_RESULT: bool = false;
pub const EXE_LOOK_UPS: bool = false;
pub const EXE_RANGE_LOOK_UPS: bool = false;
pub const BSZ_BASE: usize = _4KB;
pub const BSZ: usize = BSZ_BASE - 0; // bsz_alignment::<Key, Payload>();
// pub const FAN_OUT: usize = BSZ / 8 / 2;
// pub const NUM_RECORDS: usize = (BSZ - 2) / (8 + 8);


pub type MVTree = MVBPlusTree::<FAN_OUT, NUM_RECORDS, u64, f64>;
pub type Tree = Arc<INDEX>;

pub type INDEX = MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

pub const MAKE_INDEX: fn(LockingStrategy) -> INDEX
= |ls| INDEX::new_with(ls, inc_key, dec_key, Key::MIN, Key::MAX);

pub type Tree = Arc<INDEX>;
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

fn bernhard_tests_new() {
    const NUMBER_OLAPS: usize = 12;
    const OLAP_TX_PER_WORKER: usize = 20;
    const QUERY_NAME: &str = "query_0";

    println!("[Starting] - \
    Loading query {QUERY_NAME}...");

    let mv_tree
        = Arc::new(MVTreeSt::default());

    let num_cruds = load_query(QUERY_NAME, mv_tree.clone(), None);

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

                let mut current_si = index.current_version_for_reader();

                while current_si == version_handle::START_VERSION {
                    yield_now();
                    current_si = index.current_version_for_reader();
                }
                let si = rand::random_range(version_handle::START_VERSION..=current_si);

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

                    let mut current_si = index.current_version_for_reader();

                    while current_si == version_handle::START_VERSION {
                        yield_now();
                        current_si = index.current_version_for_reader();
                    }

                    let si = rand::random_range(version_handle::START_VERSION..=current_si);

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

pub const TREE: fn(CRUDProtocol) -> Tree = |crud| {
    Arc::new(MAKE_INDEX(crud))
};

fn mk_payload() -> Box<()> {
    unsafe {
        mem::transmute(Box::into_raw(Box::new(())))
    }
}

pub fn alloc_memory_force(gigs: usize) -> *mut c_void {
    let size = gigs * 1024 * 1024 * 1024;

    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            size,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANON,
            -1,
            0)
    };

    if ptr == MAP_FAILED {
        println!("***********Failed to allocate memory");
        return null_mut();
    }

    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    for offset in (0..size).step_by(page_size) {
        unsafe {
            ptr::write_bytes(ptr.add(offset) as *mut u8, 0, mem::size_of::<u8>() * page_size);
        }
    }

    // for offset in (0..size).step_by(mem::size_of::<u8>()) {
    //     unsafe {
    //         let p = (ptr as *mut u8).offset(offset as isize);
    //         *p = 0;
    //     }
    // }

    println!("> Memory allocated successfully");
    ptr
}

pub fn allocate_free(ptr: *mut c_void, gigs: size_t) {
    let size = gigs * 1024 * 1024 * 1024;
    let ret = unsafe { libc::munmap(ptr, size) };

    if ret != 0 {
        println!("> Failed to free memory");
    }
}

pub fn gen_data_exp(limit: u64, lambda: f64, rnd: &mut StdRng) -> Vec<u64> {
    (1..=limit)
        .map(|i|
            gen_rand_key(i, 0, i, lambda, rnd))
        .collect()
}

pub fn gen_rand_key(i: u64, range_start: u64, range_end: u64, lambda: f64, rnd: &mut StdRng) -> u64 {
    #[inline(always)]
    fn sample_next(lambda: f64, rnd: &mut StdRng) -> f64 {
        let num
            = rnd.gen_range(0_f64..1_f64);

        (1_f64 - num)
            .ln()
            .div(-lambda)
    }

    let range = range_end - range_start;

    (((loop {
        let key = i as f64 * (1_f64 - sample_next(lambda, rnd));
        if key >= 0_f64 {
            break key;
        }
    }) / range as f64) * u64::MAX as f64) as _
}

#[inline(always)]
pub fn bulk_atomic_tx(worker_threads: usize, tree: Tree, operations_queue: &[AtomicTransaction<Key, Payload>]) -> (u128, u64) {
    let mut data_buff = operations_queue
        .iter()
        .chunks(operations_queue.len() / worker_threads)
        .into_iter()
        .map(|s| s.into_iter().cloned().collect::<Vec<_>>())
        .collect::<VecDeque<_>>();

    if data_buff.len() > worker_threads {
        let back = data_buff.pop_back().unwrap();
        data_buff.front_mut().unwrap().extend(back);
    }

    let mut handles
        = Vec::with_capacity(worker_threads);

    let start = SystemTime::now();
    for _ in 1..=worker_threads {
        let current_chunk
            = data_buff.pop_front().unwrap();

        let index = tree.clone();
        handles.push(spawn(move || {
            let mut counter_errs = 0;
            current_chunk
                .into_iter()
                .for_each(|next_query| match index.dispatch_atomic_transaction(next_query) { // mv_tree.execute(operation),
                    Err(..) => counter_errs += 1,
                    _ => {}
                });
            counter_errs
        }));
    }

    let errs = handles
        .into_iter()
        .map(|handle| handle
            .join()
            .unwrap()
        ).fold(0, |errors, n_e| errors + n_e);

    let time_elapsed
        = SystemTime::now().duration_since(start).unwrap();

    (time_elapsed.as_millis(), errs)
}

#[inline(always)]
pub fn bulk_crud(worker_threads: usize, tree: Tree, operations_queue: &[CRUDOperation<Key, Payload>]) -> (u128, u64) {
    let mut data_buff = operations_queue
        .iter()
        .chunks(operations_queue.len() / worker_threads)
        .into_iter()
        .map(|s| s.into_iter().cloned().collect::<Vec<_>>())
        .collect::<VecDeque<_>>();

    if data_buff.len() > worker_threads {
        let back = data_buff.pop_back().unwrap();
        data_buff.front_mut().unwrap().extend(back);
    }

    let mut handles
        = Vec::with_capacity(worker_threads);

    let start = SystemTime::now();
    for _ in 1..=worker_threads {
        let current_chunk
            = data_buff.pop_front().unwrap();

        let index = tree.clone();
        handles.push(spawn(move || {
            let mut counter_errs = 0;
            current_chunk
                .into_iter()
                .for_each(|next_query| match index.dispatch_crud(next_query) { // mv_tree.execute(operation),
                    CRUDOperationResult::Error => counter_errs += 1,
                    _ => {}
                });
            counter_errs
        }));
    }

    let errs = handles
        .into_iter()
        .map(|handle| handle
            .join()
            .unwrap()
        ).fold(0, |errors, n_e| errors + n_e);

    let time_elapsed
        = SystemTime::now().duration_since(start).unwrap();

    (time_elapsed.as_millis(), errs)
}


pub fn test01(mut tree: Tree) {
    let protocol = tree.locking_strategy().clone();
    const EVENT_COUNT: u64
    = 10_000_000;

    let insertions = (1u64..=EVENT_COUNT)
        .map(|key| CRUDOperation::Insert(key, key as _))
        .collect_vec();

    for threads in 1..=num_cpus::get() {
        let (time, errors) = bulk_crud(
            threads,
            tree.clone(),
            insertions.as_slice());

        println!("{EVENT_COUNT},{threads},{protocol},{errors},{time},{EVENT_COUNT},0");

        tree = Tree::new(tree.make_empty_copy());
    }
}

pub fn test02(mut tree: Tree) {
    const EVENT_COUNT: u64
    = 3_000_000;

    const READER_COUNT: u64
    = 7_000_000;
    let protocol = tree.locking_strategy().clone();
    let total = EVENT_COUNT + READER_COUNT;
    let mut crud = (1u64..=EVENT_COUNT)
        .map(|key| CRUDOperation::Insert(key, key as _))
        .collect_vec();

    crud.extend((1u64..=READER_COUNT)
        .map(|key| CRUDOperation::Point(key, Version::MAX)));

    crud.shuffle(&mut thread_rng());

    for threads in 1..=num_cpus::get() {
        let (time, errors) = bulk_crud(
            threads,
            tree.clone(),
            crud.as_slice());

        println!("{total},{threads},{protocol},{errors},{time},{EVENT_COUNT},{READER_COUNT}");

        tree = Tree::new(tree.make_empty_copy());
    }
}

pub fn dump_to_json(tree: &INDEX) {
    const VERSION_STAR: Version = Version::MAX - 1;
    let data
        = tree.dispatch_crud(CRUDOperation::Range((Key::MIN..=Key::MAX).into(), VERSION_STAR));

    if let CRUDOperationResult::MatchedRecords(all_data) = data {
        println!("Records: {}", format_insertions(all_data.len()));

        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open("mv.json")
            .unwrap();

        let data = all_data
            .iter()
            .map(|r| r.key)
            .collect_vec();

        serde_json::to_writer(file, data.as_slice()).unwrap();
    }
}

#[inline(always)]
pub fn bulk_tx_manager(
    worker_threads: usize,
    tree: MVBPlusTree::<FAN_OUT, NUM_RECORDS, u64, u64>,
    gc: bool,
    operations_queue: &[AtomicTransaction<Key, Payload>]) -> (u128, TransactionManager<FAN_OUT, NUM_RECORDS, u64, u64>)
{
    let mut data_buff = operations_queue
        .iter()
        .chunks(operations_queue.len() / worker_threads)
        .into_iter()
        .map(|s| s.into_iter()
            .cloned()
            .collect::<Vec<_>>())
        .map(|col| SafeCell::new(col))
        .collect::<VecDeque<_>>();

    if data_buff.len() > worker_threads {
        let back = data_buff.pop_back().unwrap();
        data_buff.front_mut().unwrap().extend(back.into_inner());
    }

    let m_manager = TransactionManager::new_with(
        worker_threads,
        tree,
        gc);

    let start = SystemTime::now();
    for _ in 1..=worker_threads {
        m_manager.execute_tx_non_reader_batch(data_buff.pop_front().unwrap());
    }

    m_manager.join();
    let time_elapsed
        = SystemTime::now().duration_since(start).unwrap();

    (time_elapsed.as_millis(), m_manager)
}

fn main_old()  {

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
        = MVTree::orwc_optimistic_clock();
    //
    let insertions = 50_000_u64;

    (0..insertions).for_each(|i| {
        let cr
            = tree.dispatch_crud(CRUDOperation::Insert(i, i as _));
        let s = "asfdad".to_string();
    });

    let range
        = tree.dispatch_crud(CRUDOperation::Range((0..300).into(), Version::MAX));

    if let CRUDOperationResult::MatchedRecords(data) = range {
        let hase = "hase".to_string();
        let old = "olaf".to_string();
    }


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
    // );
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
    // let gigs = 100;
    // let ptr = alloc_memory_force(gigs);

    // let insertions = 10_000_000_u64;
    // println!("> Generating {insertions} keys..");
    // let mut rnd = StdRng::seed_from_u64(90501960);
    // let mut all_tx: Vec<AtomicTransaction<Key, Payload>> = mv_test::gen_data_exp(insertions, 0.1, &mut rnd)
    //     .into_iter()
    //     .map(|key| CRUDOperation::Insert(key, key as _).into())
    //     .collect::<Vec<_>>();
    //
    // let points = 0;
    // all_tx.extend((0..points).map(|key|
    //     AtomicTransaction::new_latest_si(TxAtomicOperation::PointSi(
    //         test::gen_rand_key(key, Key::MIN, Key::MAX, 0.01, &mut rnd)))));

    //
    // let file = OpenOptions::new()
    //     .read(true)
    //     .open("/home/amir/Schreibtisch/100k.json")
    //     .unwrap();
    //
    // let data_lambdas: Vec<u64>
    //     = serde_json::from_reader(file).unwrap();

    // let data_lambdas = all_tx
    //     .into_iter()
    //     .map(|v| AtomicTransaction::from_crud(CRUDOperation::Insert(v, 0f64)))
    //     .collect_vec();



    // all_tx.shuffle(&mut thread_rng());
    println!("> Finished generating {insertions} keys!");
    println!("Inserts,Points,Threads,Protocol,Clock,Time,GC,Alloc,Reuse");

    for threads in [1, 2, 4, 8, 16, 24,  32, 64, 72, 96, 128] {
        for gc in [false] {
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


                dump_to_json(tx_manager.index());
                return;

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

    // allocate_free(ptr, gigs);
}

pub fn start_paper_tests_old() {
    println!("Number Records,Number Threads,Locking Strategy,Create Time,Duplicates Count,Lambda,Run");

    let number_records
        = 1_000_000;

    let repeats
        = 10_usize;

    let threads
        = [1, 2, 4, 6, 8, 16, 20, 24, 32, 40, 56, 64, 72, 80, 96, 128];

    let lambdas
        = [0.1_f64, 16_f64, 32_f64, 64_f64, 128_f64, 256_f64, 512_f64, 1024_f64];

    let locking_protocols = [
        // MonoWriter,
        // LockCoupling,
        // orwc_attempts(0),
        // orwc_attempts(1),
        // orwc_attempts(4),
        // orwc_attempts(16),
        // orwc_attempts(64),
        // orwc_attempts(128),
        olc(),
        // lightweight_hybrid_lock_unlimited(),
        // lightweight_hybrid_lock_write_attempts(0),
        // lightweight_hybrid_lock_write_attempts(1),
        // lightweight_hybrid_lock_write_attempts(4),
        // lightweight_hybrid_lock_write_attempts(16),
        // lightweight_hybrid_lock_write_attempts(64),
        // lightweight_hybrid_lock_write_attempts(128),
        // lightweight_hybrid_lock_write_attempts(0),
        // hybrid_lock(),
    ];

    let data_lambdas = lambdas.iter().map(|lambda| {
        let mut rnd = StdRng::seed_from_u64(90501960);
        gen_data_exp(number_records, *lambda, &mut rnd)
            .into_iter()
            .map(|key|
                CRUDOperation::Insert(key, Payload::default()))
            .collect::<Vec<_>>()
    }).collect::<Vec<_>>();

    for protocol in locking_protocols {
        for create_threads in threads.iter() {
            for lambda in 0..lambdas.len() {
                for run in 1..=repeats {
                    print!("{}", number_records);
                    print!(",{}", create_threads);
                    print!(",{}", protocol);

                    let (time, dups) = beast_test2(
                        *create_threads,
                        TREE(protocol.clone()),
                        data_lambdas[lambda].as_slice());

                    println!(",{},{},{},{}", time, dups, lambdas[lambda], run);
                }
            }
        }
    }
}

pub fn simple_test2() {
    let singled_versioned_index = MAKE_INDEX(LockingStrategy::MonoWriter);

    for key in 1..=10_000 as Key {
        singled_versioned_index.dispatch(CRUDOperation::Insert(key, key as f64));
    }

    log_debug_ln(format!(""));
    log_debug_ln(format!(""));
    log_debug_ln(format!(""));
}

pub fn format_insertions(i: Key) -> String {
    if i % 1_000_000_000 == 0 {
        format!("{} B", i / 1_000_000_000)
    } else if i % 1_000_000 == 0 {
        format!("{} Mio", i / 1_000_000)
    } else if i % 1_000 == 0 {
        format!("{} K", i / 1_000)
    } else {
        i.to_string()
    }
}

pub trait ToGigs {
    fn gigs(self) -> u64;
}

/// Implements the converter method.
impl ToGigs for u64 {
    fn gigs(self) -> u64 {
        self / 1024 / 1024 / 1024
    }
}

pub fn get_system_info() -> String {
    use sysinfo::{NetworkExt, NetworksExt, ProcessExt, SystemExt};

    let mut sys = System::new_all();
    sys.refresh_all();

    let mut system_info = String::new();
    system_info.push_str("# Components temperature:\n");
    let components = sys.components();
    if components.is_empty() {
        system_info.push_str("\t- Error: Couldn't retrieve components information!\n");
    }

    for component in components {
        system_info.push_str(format!("\t- {:?}\n", component).as_str());
    }

    system_info.push_str("\n# System information\n");
    let boot_time = sys.boot_time();
    system_info.push_str(format!("\t- System booted at {} seconds\n", boot_time).as_str());
    let up_time = sys.uptime();
    system_info.push_str(format!("\t- System running since {} seconds\n", up_time).as_str());

    let load_avg = sys.load_average();
    system_info.push_str(format!("\t- System load_avg one minute = {}\n", load_avg.one).as_str());
    system_info.push_str(format!("\t- System load_avg five minutes = {}\n", load_avg.five).as_str());
    system_info.push_str(format!("\t- System load_avg fifteen minutes = {}\n", load_avg.fifteen).as_str());

    system_info.push_str(format!("\t- System name = {:?}\n", sys.name().unwrap_or_default()).as_str());
    system_info.push_str(format!("\t- System kernel version = {:?}\n", sys.kernel_version().unwrap_or_default()).as_str());
    system_info.push_str(format!("\t- System OS version = {:?}\n", sys.os_version().unwrap_or_default()).as_str());
    system_info.push_str(format!("\t- System host name = {:?}\n", sys.host_name().unwrap_or_default()).as_str());

    for user in sys.users() {
        system_info.push_str(format!("\t- User name = {}, groups = {:?}\n", user.name(), user.groups()).as_str());
    }

    let cpuid = raw_cpuid::CpuId::new();
    system_info.push_str("\n# CPU information:\n");
    system_info.push_str(
        format!("\t- Vendor: {}\n",
                cpuid.get_vendor_info()
                    .as_ref()
                    .map_or_else(|| "\t- unknown", |vf| vf.as_str())
        ).as_str());

    system_info.push_str(
        format!("\t- Cores/threads: {}/{}\n", num_cpus::get_physical(), num_cpus::get()).as_str());
    system_info.push_str(
        format!("\t- APIC ID: {}\n",
                cpuid.get_feature_info()
                    .as_ref()
                    .map_or_else(|| String::from("\t- n/a"), |finfo|
                        format!("{}", finfo.initial_local_apic_id()))
        ).as_str());

    // 10.12.8.1 Consistency of APIC IDs and CPUID:
    // "Initial APIC ID (CPUID.01H:EBX[31:24]) is always equal to CPUID.0BH:EDX[7:0]."
    system_info.push_str(
        format!("\t- x2APIC ID: {}\n",
                cpuid.get_extended_topology_info()
                    .map_or_else(|| String::from("n/a"), |mut topiter|
                        format!("{}", match topiter.next() {
                            None => "n/a".to_string(),
                            Some(ref etl) => etl.x2apic_id().to_string()
                        }),
                    )
        ).as_str());

    system_info.push_str(cpuid.get_feature_info().as_ref().map_or_else(
        || format!("\t- Family: {}\n\t- Extended Family: {}\n\t- Model: {}\n\t- Extended Model: {}\n\t- Stepping: {}\n\t- Brand Index: {}\n", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a"),
        |finfo|
            format!("\t- Family: {}\n\t- Extended Family: {}\n\t- Model: {}\n\t- Extended Model: {}\n\t- Stepping: {}\n\t- Brand Index: {}\n",
                    finfo.family_id(),
                    finfo.extended_family_id(),
                    finfo.model_id(),
                    finfo.extended_model_id(),
                    finfo.stepping_id(),
                    finfo.brand_index()),
    ).as_str());

    system_info.push_str(format!(
        "\t- Serial#: {}\n",
        cpuid.get_processor_serial().as_ref().map_or_else(
            || String::from("n/a"),
            |serial_info| format!("{}", serial_info.serial()),
        )
    ).as_str());

    let mut features = Vec::with_capacity(80);
    cpuid.get_feature_info().map(|finfo| {
        if finfo.has_sse3() {
            features.push("sse3")
        }
        if finfo.has_pclmulqdq() {
            features.push("pclmulqdq")
        }
        if finfo.has_ds_area() {
            features.push("ds_area")
        }
        if finfo.has_monitor_mwait() {
            features.push("monitor_mwait")
        }
        if finfo.has_cpl() {
            features.push("cpl")
        }
        if finfo.has_vmx() {
            features.push("vmx")
        }
        if finfo.has_smx() {
            features.push("smx")
        }
        if finfo.has_eist() {
            features.push("eist")
        }
        if finfo.has_tm2() {
            features.push("tm2")
        }
        if finfo.has_ssse3() {
            features.push("ssse3")
        }
        if finfo.has_cnxtid() {
            features.push("cnxtid")
        }
        if finfo.has_fma() {
            features.push("fma")
        }
        if finfo.has_cmpxchg16b() {
            features.push("cmpxchg16b")
        }
        if finfo.has_pdcm() {
            features.push("pdcm")
        }
        if finfo.has_pcid() {
            features.push("pcid")
        }
        if finfo.has_dca() {
            features.push("dca")
        }
        if finfo.has_sse41() {
            features.push("sse41")
        }
        if finfo.has_sse42() {
            features.push("sse42")
        }
        if finfo.has_x2apic() {
            features.push("x2apic")
        }
        if finfo.has_movbe() {
            features.push("movbe")
        }
        if finfo.has_popcnt() {
            features.push("popcnt")
        }
        if finfo.has_tsc_deadline() {
            features.push("tsc_deadline")
        }
        if finfo.has_aesni() {
            features.push("aesni")
        }
        if finfo.has_xsave() {
            features.push("xsave")
        }
        if finfo.has_oxsave() {
            features.push("oxsave")
        }
        if finfo.has_avx() {
            features.push("avx")
        }
        if finfo.has_f16c() {
            features.push("f16c")
        }
        if finfo.has_rdrand() {
            features.push("rdrand")
        }
        if finfo.has_fpu() {
            features.push("fpu")
        }
        if finfo.has_vme() {
            features.push("vme")
        }
        if finfo.has_de() {
            features.push("de")
        }
        if finfo.has_pse() {
            features.push("pse")
        }
        if finfo.has_tsc() {
            features.push("tsc")
        }
        if finfo.has_msr() {
            features.push("msr")
        }
        if finfo.has_pae() {
            features.push("pae")
        }
        if finfo.has_mce() {
            features.push("mce")
        }
        if finfo.has_cmpxchg8b() {
            features.push("cmpxchg8b")
        }
        if finfo.has_apic() {
            features.push("apic")
        }
        if finfo.has_sysenter_sysexit() {
            features.push("sysenter_sysexit")
        }
        if finfo.has_mtrr() {
            features.push("mtrr")
        }
        if finfo.has_pge() {
            features.push("pge")
        }
        if finfo.has_mca() {
            features.push("mca")
        }
        if finfo.has_cmov() {
            features.push("cmov")
        }
        if finfo.has_pat() {
            features.push("pat")
        }
        if finfo.has_pse36() {
            features.push("pse36")
        }
        if finfo.has_psn() {
            features.push("psn")
        }
        if finfo.has_clflush() {
            features.push("clflush")
        }
        if finfo.has_ds() {
            features.push("ds")
        }
        if finfo.has_acpi() {
            features.push("acpi")
        }
        if finfo.has_mmx() {
            features.push("mmx")
        }
        if finfo.has_fxsave_fxstor() {
            features.push("fxsave_fxstor")
        }
        if finfo.has_sse() {
            features.push("sse")
        }
        if finfo.has_sse2() {
            features.push("sse2")
        }
        if finfo.has_ss() {
            features.push("ss")
        }
        if finfo.has_htt() {
            features.push("htt")
        }
        if finfo.has_tm() {
            features.push("tm")
        }
        if finfo.has_pbe() {
            features.push("pbe")
        }
    });
    cpuid.get_extended_feature_info().map(|finfo| {
        if finfo.has_bmi1() {
            features.push("bmi1")
        }
        if finfo.has_hle() {
            features.push("hle")
        }
        if finfo.has_avx2() {
            features.push("avx2")
        }
        if finfo.has_fdp() {
            features.push("fdp")
        }
        if finfo.has_smep() {
            features.push("smep")
        }
        if finfo.has_bmi2() {
            features.push("bmi2")
        }
        if finfo.has_rep_movsb_stosb() {
            features.push("rep_movsb_stosb")
        }
        if finfo.has_invpcid() {
            features.push("invpcid")
        }
        if finfo.has_rtm() {
            features.push("rtm")
        }
        if finfo.has_rdtm() {
            features.push("rdtm")
        }
        if finfo.has_fpu_cs_ds_deprecated() {
            features.push("fpu_cs_ds_deprecated")
        }
        if finfo.has_mpx() {
            features.push("mpx")
        }
        if finfo.has_rdta() {
            features.push("rdta")
        }
        if finfo.has_rdseed() {
            features.push("rdseed")
        }
        if finfo.has_adx() {
            features.push("adx")
        }
        if finfo.has_smap() {
            features.push("smap")
        }
        if finfo.has_clflushopt() {
            features.push("clflushopt")
        }
        if finfo.has_processor_trace() {
            features.push("processor_trace")
        }
        if finfo.has_sha() {
            features.push("sha")
        }
        if finfo.has_sgx() {
            features.push("sgx")
        }
        if finfo.has_avx512f() {
            features.push("avx512f")
        }
        if finfo.has_avx512dq() {
            features.push("avx512dq")
        }
        if finfo.has_avx512_ifma() {
            features.push("avx512_ifma")
        }
        if finfo.has_avx512pf() {
            features.push("avx512pf")
        }
        if finfo.has_avx512er() {
            features.push("avx512er")
        }
        if finfo.has_avx512cd() {
            features.push("avx512cd")
        }
        if finfo.has_avx512bw() {
            features.push("avx512bw")
        }
        if finfo.has_avx512vl() {
            features.push("avx512vl")
        }
        if finfo.has_clwb() {
            features.push("clwb")
        }
        if finfo.has_prefetchwt1() {
            features.push("prefetchwt1")
        }
        if finfo.has_umip() {
            features.push("umip")
        }
        if finfo.has_pku() {
            features.push("pku")
        }
        if finfo.has_ospke() {
            features.push("ospke")
        }
        if finfo.has_rdpid() {
            features.push("rdpid")
        }
        if finfo.has_sgx_lc() {
            features.push("sgx_lc")
        }
    });
    system_info.push_str("\t- ");
    system_info.push_str(features.join(" ").as_str());
    system_info.push_str("\n");

    system_info.push_str("\n# System memory:\n");
    system_info.push_str(format!("\t- Used memory : {} KB\n", sys.used_memory()).as_str());
    system_info.push_str(format!("\t- Total memory: {} KB\n", sys.total_memory()).as_str());
    system_info.push_str(format!("\t- Used swap   : {} KB\n", sys.used_swap()).as_str());
    system_info.push_str(format!("\t- Total swap  : {} KB\n", sys.total_swap()).as_str());

    let mut disks = sys.disks();

    system_info.push_str(format!("\n# System Disks: {} disks installed\n", disks.len()).as_str());
    for (index, disk) in disks.iter().enumerate() {
        system_info.push_str(format!("# [{}] - Disk name: {:?}\n\t\
        - type = {:?}\n\t\
        - file system = {}\n\t\
        - total space = {} GB\n\t\
        - free space = {} GB\n\t\
        - mount point = {:?}\n\t\
        - removable = {}\n",
                                     index,
                                     disk.name(),
                                     disk.kind(),
                                     disk.file_system().into_iter().map(|b| char::from(*b)).collect::<String>(),
                                     disk.total_space().gigs(),
                                     disk.available_space().gigs(),
                                     disk.mount_point().as_os_str(),
                                     disk.is_removable()
        ).as_str());
    }

    let networks = sys.networks();
    system_info.push_str(format!("\n# System Networks: {} networks installed\n", networks.iter().count()).as_str());
    for (index, (interface_name, data)) in networks.iter().enumerate() {
        system_info.push_str(format!("# [{}] - Interface name: {}\n\t\
        - received = {}\n\t\
        - errors_on_received = {}\n\t\
        - total_received = {}\n\t\
        - packets_received = {}\n\t\
        - total_packets_received = {}\n\t\
        - total_errors_on_received = {}\n\t\
        - transmitted = {}\n\t\
        - errors_on_transmitted = {}\n\t\
        - total_transmitted = {}\n\t\
        - packets_transmitted = {}\n\t\
        - total_packets_transmitted = {}\n\t\
        - total_errors_on_transmitted = {}\n",
                                     index,
                                     interface_name,
                                     data.received(),
                                     data.errors_on_received(),
                                     data.total_received(),
                                     data.packets_received(),
                                     data.total_packets_received(),
                                     data.total_errors_on_received(),
                                     data.transmitted(),
                                     data.errors_on_transmitted(),
                                     data.total_transmitted(),
                                     data.packets_transmitted(),
                                     data.total_packets_transmitted(),
                                     data.total_errors_on_transmitted()).as_str());
    }

    system_info
}

pub fn create_filter_params(params: &str) -> (Vec<usize>, Vec<Key>, Vec<CRUDProtocol>) {
    let mut p = params.split("+");
    let inserts = serde_json::from_str::<Vec<Key>>(p.next().unwrap_or(""))
        .unwrap_or(S_INSERTIONS.to_vec());

    let mut crud_str
        = p.next().unwrap_or_default().to_string();

    if crud_str.contains("MonoWriter") && !crud_str.contains("\"MonoWriter\"") {
        crud_str = crud_str.replace("MonoWriter", "\"MonoWriter\"");
    }
    if crud_str.contains("LockCoupling") && !crud_str.contains("\"LockCoupling\"") {
        crud_str = crud_str.replace("LockCoupling", "\"LockCoupling\"");
    }

    let threads
        = serde_json::from_str::<Vec<usize>>(p.next().unwrap_or_default())
        .unwrap_or(S_THREADS_CPU.to_vec());

    let crud = serde_json::from_str::<Vec<CRUDProtocol>>(crud_str.as_str())
        .unwrap_or(S_STRATEGIES.to_vec());

    (threads, inserts, crud)
}

pub fn do_tests() {
    let mut args: Vec<String>
        = env::args().collect();

    if args.len() > 1 {
        let raw = args
            .pop()
            .unwrap()
            .split_whitespace()
            .collect::<String>();

        let (params, command) = if raw.contains("=") {
            let mut command_salad = raw.split("=").collect::<Vec<_>>();
            (command_salad.pop().unwrap(), command_salad.pop().unwrap())
        } else {
            ("", raw.as_str())
        };

        match command {
            "all" => experiment(S_THREADS_CPU.to_vec(),
                                S_INSERTIONS.as_slice(),
                                S_STRATEGIES.as_slice()),
            "t1" => println!("Time = {}ms",
                             beast_test(24, TREE(MonoWriter), gen_rand_data(200_000).as_slice(), true)),
            "t2" => println!("Time = {}ms",
                             beast_test(24, TREE(olc()), gen_rand_data(20_000_000).as_slice(), true)),
            "crud_protocol" | "crud_protocols" | "crud" | "cruds" | "protocol" | "protocols" =>
                println!("{}", S_STRATEGIES
                    .as_slice()
                    .iter()
                    .map(|s| format!("Name: \t`{}`\nObject: `{}`",
                                     s,
                                     serde_json::to_string(s).unwrap()))
                    .join("\n******************************************************************\n")),
            "info" | "system" | "sys" => println!("{}", get_system_info()),
            "make_splash" | "splash" =>
                make_splash(),
            "yield_enabled" | "yield" =>
                println!("yield_enabled: {}", ENABLE_YIELD),
            "cpu_cores" | "cpu_threads" | "cpu" =>
                println!("Cores/Threads: {}/{}", num_cpus::get_physical(), num_cpus::get()),
            "simple_test" | "st" =>
                simple_test(),
            "create" | "c" => {
                let (threads, inserts, crud)
                    = create_filter_params(params);

                experiment(threads,
                           inserts.as_slice(),
                           crud.as_slice())
            }
            "update_read" | "ur" => { //update=
                // tree_records+
                // update_records+
                // [CRUD,..]+
                // [t0,..]

                log_debug_ln(format!("Running `{}={}`", command, params));

                let mut params
                    = params.split("+");

                let tree_records
                    = params.next().unwrap().parse::<usize>().unwrap();

                let update_records
                    = params.next().unwrap().parse::<f32>().unwrap();

                let mut crud_str
                    = params.next().unwrap_or_default().to_string();

                let threads
                    = serde_json::from_str::<Vec<usize>>(params.next().unwrap_or_default())
                    .unwrap_or(S_THREADS_CPU.to_vec());

                if crud_str.contains("MonoWriter") && !crud_str.contains("\"MonoWriter\"") {
                    crud_str = crud_str.replace("MonoWriter", "\"MonoWriter\"");
                }

                if crud_str.contains("LockCoupling") && !crud_str.contains("\"LockCoupling\"") {
                    crud_str = crud_str.replace("LockCoupling", "\"LockCoupling\"");
                }

                let crud = serde_json::from_str::<Vec<CRUDProtocol>>(crud_str.as_str())
                    .unwrap_or(S_STRATEGIES.to_vec());

                log_debug_ln(format!("CRUD = `{}` ", crud.as_slice().iter().join(",")));
                log_debug_ln(format!("Threads = `{}` ", threads.as_slice().iter().join(",")));

                let update_records
                    = (update_records * tree_records as f32) as usize;

                log_debug_ln(format!("Records = `{}`, Updates = `{}` ",
                                     format_insertions(tree_records as _),
                                     format_insertions(update_records as _)));

                let data_file
                    = data_file_name(tree_records);

                let read_file
                    = read_data_file_name(tree_records, update_records);

                let from_file = path::Path::new(data_file.as_str())
                    .exists() && path::Path::new(read_file.as_str())
                    .exists();

                let (create_data, read_data) = if from_file {
                    log_debug_ln(format!("Using `{}` for data, `{}` for reads", data_file, read_file));

                    (serde_json::from_str::<Vec<Key>>(fs::read_to_string(data_file).unwrap()
                        .as_str()
                    ).unwrap(), serde_json::from_str::<Vec<Key>>(fs::read_to_string(read_file).unwrap()
                        .as_str()
                    ).unwrap())
                } else {
                    log_debug_ln(format!("Generating `{}` for data", data_file));

                    let c_data = gen_rand_data(tree_records);

                    let mut read_data
                        = (0 as Key..tree_records as Key).collect::<Vec<_>>();

                    read_data.shuffle(&mut rand::thread_rng());
                    read_data.truncate(update_records);

                    read_data
                        .iter_mut()
                        .for_each(|index| *index = c_data[(*index) as usize]);

                    fs::write(data_file, serde_json::to_string(c_data.as_slice()).unwrap())
                        .unwrap();

                    fs::write(read_file, serde_json::to_string(read_data.as_slice()).unwrap())
                        .unwrap();

                    (c_data, read_data)
                };

                crud.into_iter().for_each(|crud| unsafe {
                    log_debug_ln("Creating index...".to_string());
                    let mut index = TREE(crud.clone());

                    let create_time = if crud.is_mono_writer() {
                        beast_test(1, index.clone(), create_data.as_slice(), false)
                    } else {
                        beast_test(4, index.clone(), create_data.as_slice(), false)
                    };

                    log_debug_ln(format!("Created index in `{}` ms", create_time));

                    let read_data: &'static [_] = unsafe { mem::transmute(read_data.as_slice()) };

                    log_debug_ln("UPDATE + READ BENCHMARK; Each Thread = [Updater Thread + Reader Thread]".to_string());
                    println!("Locking Strategy,Threads,Time");
                    threads.iter().for_each(|spawns| unsafe {
                        if crud.is_mono_writer() {
                            let start = SystemTime::now();
                            (0..=*spawns).map(|_| {
                                let i1 = index.clone();
                                let i2 = index.clone();
                                [thread::spawn(move || {
                                    read_data
                                        .iter()
                                        .for_each(|read_key| if let CRUDOperationResult::Error =
                                            i1.dispatch(CRUDOperation::Point(*read_key))
                                        {
                                            log_debug_ln(format!("Error reading key = {}", read_key));
                                        });
                                }), thread::spawn(move || {
                                    read_data
                                        .iter()
                                        .for_each(|read_key| if let CRUDOperationResult::Error
                                            = i2.dispatch(
                                            CRUDOperation::Update(*read_key, Payload::default()))
                                        {
                                            log_debug_ln(format!("Error reading key = {}", read_key));
                                        });
                                })]
                            }).collect::<Vec<_>>()
                                .into_iter()
                                .for_each(|h| h.into_iter().for_each(|sh| sh.join().unwrap()));

                            println!("{},{},{}", crud, *spawns,
                                     SystemTime::now().duration_since(start).unwrap().as_millis());
                        } else {
                            let read_data = read_data.clone();
                            let index_r: &'static INDEX = mem::transmute(&index);
                            let start = SystemTime::now();
                            (0..=*spawns).map(|_| {
                                [thread::spawn(move || {
                                    read_data
                                        .iter()
                                        .for_each(|read_key| if let CRUDOperationResult::Error =
                                            index_r.dispatch(CRUDOperation::Point(*read_key))
                                        {
                                            log_debug_ln(format!("Error reading key = {}", read_key));
                                        });
                                }), thread::spawn(move || {
                                    read_data
                                        .iter()
                                        .for_each(|read_key| if let CRUDOperationResult::Error
                                            = index_r.dispatch(
                                            CRUDOperation::Update(*read_key, Payload::default()))
                                        {
                                            log_debug_ln(format!("Error reading key = {}", read_key));
                                        });
                                })]
                            }).collect::<Vec<_>>()
                                .into_iter()
                                .for_each(|h| h.into_iter().for_each(|sh| sh.join().unwrap()));

                            println!("{},{},{}", crud, *spawns,
                                     SystemTime::now().duration_since(start).unwrap().as_millis());
                        }
                    });
                });
            }
            "generate" | "gen" => fs::write(
                data_file_name(params.parse::<usize>().unwrap()),
                serde_json::to_string(
                    gen_rand_data(params.parse::<usize>().unwrap()).as_slice()).unwrap(),
            ).unwrap(),
            "block_alignment" | "bsz_aln" | "alignment" | "aln" | "mv_block" | "bsz" =>
                show_alignment_bsz(),
            "hardware_lock_elision" | "hle" =>
                println!("OLC hardware_lock_elision: {}", hle()),
            "x86_64" | "x86" =>
                println!("x86_64 or x86: {}", cfg!(any(target_arch = "x86", target_arch = "x86_64"))),
            _ => make_splash(),
        }
    } else {
        make_splash()
    }
}

pub fn data_file_name(n_records: usize) -> String {
    format!("create_{}.create", format_insertions(n_records as _))
}

pub fn read_data_file_name(n_records: usize, read_records: usize) -> String {
    format!("{}__read_{}.read",
            data_file_name(n_records).replace(".create", ""),
            format_insertions(read_records as _))
}


fn longest_runner_test(timeout: Duration, number_u: usize, number_r: usize, ls: LockingStrategy, n: usize) {
    let key_range = 1..=n as Key;

    print!("{}", n);
    print!(",{}", number_u);
    print!(",{}", number_r);
    print!(",{}", timeout.as_millis());
    print!(",{}", ls);

    let is_mono = ls.is_mono_writer();
    let tree = TREE(ls);

    if is_mono {
        beast_test2(1, tree.clone(), gen_rand_data(*key_range.end() as usize).as_slice());
    } else {
        beast_test2(16, tree.clone(), gen_rand_data(*key_range.end() as usize).as_slice());
    }

    let (send_u, rec_u)
        = crossbeam::channel::unbounded::<()>();

    let updater = || {
        let u_tree = tree.clone();
        let rec_u = rec_u.clone();
        let key_range = key_range.clone();

        spawn(move || {
            let mut rng
                = rand::thread_rng();

            let mut longest_time = 0;

            loop {
                let updater_time = SystemTime::now();

                let key
                    = rng.gen_range(key_range.clone());

                u_tree.dispatch(CRUDOperation::Update(key, Payload::default()));

                longest_time = longest_time.max(SystemTime::now().duration_since(updater_time).unwrap().as_nanos());

                match rec_u.try_recv() {
                    Ok(..) | Err(TryRecvError::Disconnected) => return longest_time,
                    _ => {}
                }
            }
        })
    };

    let (send_r, rec_r)
        = crossbeam::channel::unbounded::<()>();

    let reader = || {
        let r_tree = tree.clone();
        let rec_r = rec_r.clone();
        let key_range = key_range.clone();

        spawn(move || {
            let mut rng
                = rand::thread_rng();

            let mut longest_time = 0;
            loop {
                let reader_time = SystemTime::now();
                let key
                    = rng.gen_range(key_range.clone());

                r_tree.dispatch(CRUDOperation::Point(key));

                longest_time = longest_time.max(SystemTime::now().duration_since(reader_time).unwrap().as_nanos());

                match rec_r.try_recv() {
                    Ok(..) | Err(TryRecvError::Disconnected) => return longest_time,
                    _ => {}
                }
            }
        })
    };

    let start = SystemTime::now();
    let mut u_handle
        = (0..number_u).map(|_| (updater)()).collect::<Vec<_>>();

    let mut r_handle
        = (0..number_r).map(|_| (reader)()).collect::<Vec<_>>();

    while SystemTime::now().duration_since(start).unwrap().lt(&timeout) {
        thread::yield_now()
    }

    mem::drop(send_u);
    mem::drop(send_r);

    let u = u_handle
        .drain(..)
        .map(|h| h.join().unwrap())
        .sum::<u128>();

    let r = r_handle
        .drain(..)
        .map(|h| h.join().unwrap())
        .sum::<u128>();

    println!(",{},{}", u, r);
}

fn real_contention_test(timeout: Duration, number_u: usize, number_r: usize, ls: LockingStrategy, n: usize) {
    let key_range = 1..=n as Key;

    print!("{}", n);
    print!(",{}", number_u);
    print!(",{}", number_r);
    print!(",{}", timeout.as_millis());
    print!(",{}", ls);

    let is_mono = ls.is_mono_writer();
    let tree = TREE(ls);

    if is_mono {
        beast_test2(1, tree.clone(), gen_rand_data(*key_range.end() as usize).as_slice());
    } else {
        beast_test2(16, tree.clone(), gen_rand_data(*key_range.end() as usize).as_slice());
    }

    let (send_u, rec_u)
        = crossbeam::channel::unbounded::<()>();

    let updater = || {
        let u_tree = tree.clone();
        let rec_u = rec_u.clone();
        let key_range = key_range.clone();

        spawn(move || {
            let mut rng
                = rand::thread_rng();

            let mut u_counter = 0_usize;
            loop {
                u_counter += 1;
                let key
                    = rng.gen_range(key_range.clone());

                u_tree.dispatch(CRUDOperation::Update(key, Payload::default()));

                match rec_u.try_recv() {
                    Ok(..) | Err(TryRecvError::Disconnected) => return u_counter,
                    _ => {}
                }
            }
        })
    };

    let (send_r, rec_r)
        = crossbeam::channel::unbounded::<()>();

    let reader = || {
        let r_tree = tree.clone();
        let rec_r = rec_r.clone();
        let key_range = key_range.clone();

        spawn(move || {
            let mut rng
                = rand::thread_rng();

            let mut r_counter = 0_usize;
            loop {
                r_counter += 1;
                let key
                    = rng.gen_range(key_range.clone());

                r_tree.dispatch(CRUDOperation::Point(key));

                match rec_r.try_recv() {
                    Ok(..) | Err(TryRecvError::Disconnected) => return r_counter,
                    _ => {}
                }
            }
        })
    };

    let start = SystemTime::now();
    let mut u_handle
        = (0..number_u).map(|_| (updater)()).collect::<Vec<_>>();

    let mut r_handle
        = (0..number_r).map(|_| (reader)()).collect::<Vec<_>>();

    while SystemTime::now().duration_since(start).unwrap().lt(&timeout) {
        thread::yield_now()
    }

    mem::drop(send_u);
    mem::drop(send_r);

    let u = u_handle
        .drain(..)
        .map(|h| h.join().unwrap())
        .sum::<usize>();

    let r = r_handle
        .drain(..)
        .map(|h| h.join().unwrap())
        .sum::<usize>();

    println!(",{},{}", u, r);
}

fn mixed_test(create: &[Key], updates: &[Key], reads: &[Key], ratio_update: f64, ratio_read: f64) {
    let threads_cpu
        = [10, 20, 30, 60, 70, 80, 90, 100, 120];

    let strategies = vec![
        MonoWriter,
        LockCoupling,
        orwc_attempts(0),
        orwc_attempts(1),
        orwc_attempts(4),
        orwc_attempts(16),
        orwc_attempts(64),
        olc(),
        lightweight_hybrid_lock_read_attempts(0),
        lightweight_hybrid_lock_read_attempts(1),
        lightweight_hybrid_lock_read_attempts(4),
        lightweight_hybrid_lock_read_attempts(16),
        lightweight_hybrid_lock_read_attempts(64),

        // lightweight_hybrid_lock_write_attempts(0),
        // lightweight_hybrid_lock_write_attempts(1),
        // lightweight_hybrid_lock_write_attempts(4),
        // lightweight_hybrid_lock_write_attempts(16),
        // lightweight_hybrid_lock_write_attempts(64),
        //
        // lightweight_hybrid_lock_write_read_attempts(0, 0),
        // lightweight_hybrid_lock_write_read_attempts(1, 1),
        // lightweight_hybrid_lock_write_read_attempts(4, 4),
        // lightweight_hybrid_lock_write_read_attempts(16, 16),
        // lightweight_hybrid_lock_write_read_attempts(64, 64),

        // hybrid_lock()
    ];

    for num_threads in threads_cpu.iter() {
        let reader_threads
            = (ratio_read * *num_threads as f64) as usize;

        let updater_threads
            = (ratio_update * *num_threads as f64) as usize;
// Number Records,Update Records,Read Records,Update Threads,Read Threads,Locking Strategy,Mixed Time,Fan Out
        for ls in strategies.iter() {
            print!("{}", create.len());
            print!(",{}", (create.len() as f64 * ratio_update) as usize);
            print!(",{}", (create.len() as f64 * ratio_read) as usize);
            print!(",{}", updater_threads);
            print!(",{}", reader_threads);

            let index = TREE(ls.clone());
            let _create_time = beast_test(
                *num_threads,
                index.clone(),
                create, true);

            let mut update_chunks = updates
                .chunks(updates.len() / updater_threads)
                .map(|c| c.to_vec())
                .collect::<VecDeque<_>>();

            if update_chunks.len() > updater_threads {
                let back = update_chunks.pop_back().unwrap();
                update_chunks.front_mut().unwrap().extend(back);
            }

            let mut read_chunks = reads
                .chunks(reads.len() / reader_threads)
                .map(|c| c.to_vec())
                .collect::<VecDeque<_>>();

            if read_chunks.len() > reader_threads {
                let back = read_chunks.pop_back().unwrap();
                read_chunks.front_mut().unwrap().extend(back);
            }

            let mut handles
                = Vec::with_capacity(*num_threads);

            let start = SystemTime::now();

            handles.extend((0..updater_threads).map(|_| {
                let u_chunk
                    = update_chunks.pop_front().unwrap();

                let u_index
                    = index.clone();

                thread::spawn(move ||
                    for key in u_chunk {
                        match u_index.dispatch(CRUDOperation::Update(key, Payload::default())) {
                            CRUDOperationResult::Updated(..) => {}
                            CRUDOperationResult::Error => {
                                log_debug_ln(format!("Not found key = {}", key));
                                log_debug_ln(format!("Point = {}", u_index.dispatch(CRUDOperation::Point(key))));
                            }
                            cor =>
                                log_debug(format!("sleepy joe hit me -> {}", cor))
                        }
                    })
            }));

            handles.extend((0..reader_threads).map(|_| {
                let r_index
                    = index.clone();

                let r_chunk
                    = read_chunks.pop_front().unwrap();

                thread::spawn(move ||
                    for key in r_chunk {
                        match r_index.dispatch(CRUDOperation::Point(key)) {
                            CRUDOperationResult::MatchedRecord(..) => {}
                            CRUDOperationResult::Error => {
                                log_debug_ln(format!("Not found key = {}", key));
                                log_debug_ln(format!("Point = {}", r_index.dispatch(CRUDOperation::Point(key))));
                            }
                            cor =>
                                log_debug(format!("sleepy joe hit me -> {}", cor))
                        }
                    })
            }));

            handles
                .into_iter()
                .for_each(|handle| handle.join().unwrap());

            let mixed_time
                = SystemTime::now().duration_since(start).unwrap().as_millis();

            print!(",{}", mixed_time);
            print!(",{}", FAN_OUT);
            print!(",{}", NUM_RECORDS);
            println!(",{}", BSZ_BASE);
        }
    }
}

fn update_test(t1s: &[Key], updates: &[Key]) {
    let threads_cpu = [
        1,
        2,
        3,
        4,
        8,
        10,
        12,
        16,
        24,
        32,
        64,
        128
    ];

    let strategies = [
        MonoWriter,
        LockCoupling,
        orwc_attempts(0),
        orwc_attempts(1),
        orwc_attempts(4),
        orwc_attempts(16),
        orwc_attempts(64),
        orwc_attempts(1024),
        olc(),
        lightweight_hybrid_lock_write_attempts(0),
        lightweight_hybrid_lock_write_attempts(1),
        lightweight_hybrid_lock_write_attempts(4),
        lightweight_hybrid_lock_write_attempts(16),
        lightweight_hybrid_lock_write_attempts(64),
        lightweight_hybrid_lock_write_attempts(1024),
        hybrid_lock()
    ];

    for num_threads in threads_cpu.iter() {
        for ls in strategies.iter() {
            print!("{}", t1s.len());
            print!(",{}", *num_threads);

            let index = TREE(ls.clone());
            let _create_time = beast_test(
                *num_threads,
                index.clone(),
                t1s, true);

            let mut slices = updates
                .chunks(updates.len() / *num_threads)
                .map(|c| c.to_vec())
                .collect::<VecDeque<_>>();

            if slices.len() > *num_threads {
                let back = slices.pop_back().unwrap();
                slices.front_mut().unwrap().extend(back);
            }

            let start = SystemTime::now();
            let update_handles = (0..slices.len()).map(|_| {
                let chunk
                    = slices.pop_front().unwrap();

                let index
                    = index.clone();

                thread::spawn(move ||
                    for key in chunk {
                        match index.dispatch(CRUDOperation::Update(key, Payload::default())) {
                            CRUDOperationResult::Updated(..) => {}
                            CRUDOperationResult::Error => {
                                log_debug_ln(format!("Not found key = {}", key));
                                log_debug_ln(format!("Point = {}", index.dispatch(CRUDOperation::Point(key))));
                            }
                            cor =>
                                log_debug(format!("sleepy joe hit me -> {}", cor))
                        }
                    })
            }).collect::<Vec<_>>();
            update_handles
                .into_iter()
                .for_each(|handle|
                    handle.join().unwrap());

            let update_time
                = SystemTime::now().duration_since(start).unwrap().as_millis();

            print!(",{}", update_time);
            print!(",{}", FAN_OUT);
            print!(",{}", NUM_RECORDS);
            println!(",{}", BSZ_BASE);
        }
    }
}

fn create_scan_test(t1s: &[Key], scans: &[Key]) {
    let threads_cpu = [
        1,
        2,
        3,
        4,
        8,
        10,
        12,
        16,
        24,
        32,
        64,
        128
    ];

    let strategies = vec![
        // MonoWriter,
        // LockCoupling,
        // orwc_attempts(0),
        // orwc_attempts(1),
        // orwc_attempts(4),
        // orwc_attempts(16),
        // orwc_attempts(64),
        //
        // olc(),

        lightweight_hybrid_lock_write_attempts(0),
        lightweight_hybrid_lock_write_attempts(1),
        lightweight_hybrid_lock_write_attempts(4),
        lightweight_hybrid_lock_write_attempts(16),
        lightweight_hybrid_lock_write_attempts(64),
        hybrid_lock(),
    ];

    for num_threads in threads_cpu.iter() {
        for ls in strategies.iter() {
            print!("{}", t1s.len());
            print!(",{}", *num_threads);

            let index = TREE(ls.clone());
            let create_time = beast_test(
                *num_threads,
                index.clone(),
                t1s, true);

            print!(",{}", create_time);
            print!(",{}", FAN_OUT);
            print!(",{}", NUM_RECORDS);
            print!(",{}", BSZ_BASE);

            let mut slices = scans
                .chunks(scans.len() / *num_threads)
                .map(|c| c.to_vec())
                .collect::<VecDeque<_>>();

            if slices.len() > *num_threads {
                let back = slices.pop_back().unwrap();
                slices.front_mut().unwrap().extend(back);
            }

            let start = SystemTime::now();
            let read_handles = (0..*num_threads).map(|_| {
                let chunk
                    = slices.pop_front().unwrap();

                let index
                    = index.clone();

                thread::spawn(move ||
                    for key in chunk {
                        match index.dispatch(CRUDOperation::Point(key)) {
                            CRUDOperationResult::MatchedRecord(..) => {}
                            CRUDOperationResult::Error => log_debug_ln(format!("Not found key = {}", key)),
                            cor =>
                                log_debug_ln(format!("sleepy joe hit me -> {}", cor))
                        }
                    })
            }).collect::<Vec<_>>();
            read_handles
                .into_iter()
                .for_each(|handle|
                    handle.join().unwrap());


            let read_time
                = SystemTime::now().duration_since(start).unwrap().as_millis();

            println!(",{}", read_time);
        }
    }
}

// pub fn experiment(threads_cpu: Vec<usize>,
//                   insertions: &[Key],
//                   strategies: &[LockingStrategy])
// {
//     // if CPU_THREADS {
//     //     let cpu = num_cpus::get();
//     //     threads_cpu.truncate(threads_cpu
//     //         .iter()
//     //         .enumerate()
//     //         .find(|(_, t)| **t > cpu)
//     //         .unwrap()
//     //         .0)
//     // }
//
//     println!("Number Insertions,Number Threads,Locking Strategy,Time,Fan Out,Leaf Records,Block Size");
//
//     for insertion_n in insertions {
//         let t1s = gen_rand_data(*insertion_n as usize);
//         for num_threads in threads_cpu.iter() {
//             // if *num_threads > t1s.len() {
//             //     continue;
//             // }
//
//             for ls in strategies {
//                 print!("{}", t1s.len());
//                 print!(",{}", *num_threads);
//
//                 let (time, _dups) = beast_test2(
//                     *num_threads,
//                     TREE(ls.clone()),
//                     t1s.as_slice());
//
//                 print!(",{}", time);
//                 print!(",{}", FAN_OUT);
//                 print!(",{}", NUM_RECORDS);
//                 println!(",{}", BSZ_BASE);
//             }
//         }
//     }
// }

// pub fn start_paper_solo() {
//     println!("Number Records,Update Threads,Read Threads,Timeout,Locking Strategy,Updates Performed,Reads Performed");
//
//     let n
//         = 100_000_000;
//
//     let time_out
//         = Duration::from_secs(10);
//
//     let thread_cases = [1, 2, 4, 8, 16, 32, 64, 128]
//         .into_iter()
//         .flat_map(|u| vec![(u, 0), (0, u)]).collect::<Vec<_>>();
//
//     let locking_protocols = vec![
//         MonoWriter,
//         LockCoupling,
//         orwc_attempts(0),
//         orwc_attempts(1),
//         orwc_attempts(4),
//         orwc_attempts(16),
//         orwc_attempts(64),
//         olc(),
//         // lightweight_hybrid_lock_unlimited(),
//         lightweight_hybrid_lock_read_attempts(0),
//         lightweight_hybrid_lock_read_attempts(1),
//         lightweight_hybrid_lock_read_attempts(4),
//         lightweight_hybrid_lock_read_attempts(16),
//         lightweight_hybrid_lock_read_attempts(64),
//         lightweight_hybrid_lock_write_attempts(0),
//         lightweight_hybrid_lock_write_attempts(1),
//         lightweight_hybrid_lock_write_attempts(4),
//         lightweight_hybrid_lock_write_attempts(16),
//         lightweight_hybrid_lock_write_attempts(64),
//         lightweight_hybrid_lock_write_read_attempts(0, 0),
//         hybrid_lock()
//     ];
//
//     for (u, r) in thread_cases {
//         for ls in locking_protocols.iter() {
//             real_contention_test(
//                 time_out,
//                 u,
//                 r,
//                 ls.clone(),
//                 n);
//         }
//     }
// }
#[inline(always)]
pub fn beast_test(num_thread: usize, index: Tree, t1s: &[Key], log: bool) -> u128 {
    let ls = index.as_index().locking_strategy.clone();
    let (time, _dups) = beast_test2(num_thread, index, t1s);
    if log {
        print!(",{}", ls);
    }

    time
}
pub fn simple_test() {
    const INSERT: fn(u64) -> CRUDOperation<Key, Payload> = |k: Key|
        CRUDOperation::Insert(k, k as _);

    const UPDATE: fn(Key) -> CRUDOperation<Key, Payload> = |k: Key|
        CRUDOperation::Update(k, k as _);

    let _keys_insert_org = vec![
        1, 5, 6, 7, 3, 4, 10, 30, 11, 12, 14, 17, 18, 13, 16, 15, 36, 20, 21, 22, 23, 37, 2, 0,
    ];

    let keys_insert_org: Vec<Key> = vec![
        8, 11, 19, 33, 24, 36, 34, 25, 12, 37, 14, 10, 45, 31, 18, ];
    //  3, 9, 5, 2, 13, 40, 38, 41, 27, 16, 28, 42, 1, 43, 23, 26,
    // 44, 17, 29, 39, 20, 6, 4, 7, 30, 21, 35, 8];

    // let mut rand = rand::thread_rng();
    // let mut keys_insert = gen_rand_data(1_000);
    //
    // let dups = rand.next_u32().min(keys_insert.len() as _) as usize;
    // keys_insert.extend(keys_insert.get(..dups).unwrap().to_vec());
    // let mut rng = thread_rng();
    // keys_insert.shuffle(&mut rng);

    let mut already_used: Vec<Key> = vec![];
    let keys_insert = keys_insert_org
        .iter()
        .map(|key| if already_used.contains(key) {
            UPDATE(*key)
        } else {
            already_used.push(*key);
            INSERT(*key)
        }).collect::<Vec<_>>();


    let tree = MAKE_INDEX(
        LockingStrategy::MonoWriter);
    let mut search_queries = vec![];

    for (i, tx) in keys_insert.into_iter().enumerate() {
        log_debug_ln(format!("# {}", i + 1));
        log_debug_ln(format!("############################################\
        ###########################################################"));

        let key = match tree.dispatch(tx) {
            CRUDOperationResult::Inserted(key) => {
                log_debug_ln(format!("Ingest: {}", CRUDOperationResult::<Key, Payload>::Inserted(key)));
                key
            }
            CRUDOperationResult::Updated(key, payload) => {
                log_debug_ln(format!("Ingest: {}", CRUDOperationResult::<Key, Payload>::Updated(key, payload)));
                key
            }
            joe => panic!("Sleepy Joe -> TransactionResult::{}", joe)
        };

        let search = vec![
            CRUDOperation::Point(key),
            CRUDOperation::Point(key),
        ];

        search_queries.push(search.clone());
        search.into_iter().for_each(|query| match tree.dispatch(query.clone()) {
            CRUDOperationResult::Error =>
                panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n",
                       query,
                       CRUDOperationResult::<Key, Payload>::Error),
            CRUDOperationResult::MatchedRecords(records) if records.len() != 1 =>
                panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n",
                       query,
                       CRUDOperationResult::<Key, Payload>::Error),
            CRUDOperationResult::MatchedRecord(None) =>
                panic!("\n\t- Query: {}\n\t- Result: {}\n\t\n",
                       query,
                       CRUDOperationResult::<Key, Payload>::MatchedRecord(None)),
            result =>
                log_debug_ln(format!("\t- Query:  {}\n\t- Result: {}", query, result)),
        });
        log_debug_ln(format!("##################################################################################\
        ######################\n"));
    }

    log_debug_ln(format!("--------------------------------\
    ------------------------------------------------------------------------"));
    log_debug_ln(format!("----------------------------------\
    ----------------------------------------------------------------------"));
    log_debug_ln(format!("\n############ Query All via Searches ############\n"));
    for (s, chunk) in search_queries.into_iter().enumerate() {
        log_debug_ln(format!("----------------------------------\
        ----------------------------------------------------------------------"));
        log_debug_ln(format!("\t# [{}]", s));
        // if s == 42 {
        //     let x = 31;
        // }
        for query in chunk {
            // if let Transaction::ExactSearchLatest(..) = operation {
            //     continue
            // }
            match tree.dispatch(query.clone()) {
                CRUDOperationResult::Error =>
                    panic!("\n\t- Query: {}\n\t- Result: {}", query, CRUDOperationResult::<Key, Payload>::Error),
                CRUDOperationResult::MatchedRecords(records) if records.len() != 1 =>
                    panic!("\n\t#- Query: {}\n\t- Result: {}", query, CRUDOperationResult::<Key, Payload>::Error),
                CRUDOperationResult::MatchedRecord(None) =>
                    panic!("\n\t#- Query: {}\n\t- Result: {}", query, CRUDOperationResult::<Key, Payload>::MatchedRecord(None)),
                result =>
                    log_debug_ln(format!("\t- Query:  {}\n\t- Result: {}", query, result)),
            }
        }
        log_debug_ln(format!("----------------------------------------------------------\
        ----------------------------------------------\n"));
    }

    show_alignment_bsz();

    let range = Interval::new(
        18,
        36,
    );

    let matches = keys_insert_org
        .into_iter()
        .filter(|k| range.contains(*k))
        .unique();

    let results
        = tree.dispatch(CRUDOperation::Range(range.clone()));

    log_debug_ln(format!("Results of Range Query:\n{}\n\nExpected: \t{}\nFound: \t\t{}\nRange: {}", results, matches.count(), match results {
        CRUDOperationResult::MatchedRecords(ref records) => records.len(),
        _ => 0
    }, range));

    log_debug_ln(format!("Printing Tree:\n"));
    level_order(tree.root.block.clone());
    // json_index(&mv_tree, "simple_tree.json");
}

pub fn gen_rand_data(n: usize) -> Vec<Key> {
    // let mut nums = HashSet::new();

    let mut rand = rand::thread_rng();
    let mut nums = (1..=n as Key).collect::<Vec<Key>>();
    nums.shuffle(&mut rand);
    nums

    // loop {
    //     let next = rand.next_u64() as Key;
    //     if !nums.contains(&next) {
    //         nums.insert(next);
    //     }
    //
    //     if nums.len() == n as usize {
    //         break;
    //     }
    // }
    // nums.into_iter().collect::<Vec<_>>()
}
// #[derive(Copy, Clone, Default)]
// pub struct KeyWrap(f64);
//
// unsafe impl Sync for KeyWrap { }
// impl Into<KeyWrap> for u64 {
//     fn into(self) -> KeyWrap {
//         (self as f64).into()
//     }
// }
//
// impl Into<usize> for KeyWrap {
//     fn into(self) -> usize {
//         self.0 as usize
//     }
// }
//
// impl Into<KeyWrap> for f64 {
//     fn into(self) -> KeyWrap {
//         KeyWrap(self)
//     }
// }
//
// impl Into<Key> for usize {
//     fn into(self) -> KeyWrap {
//         (self as f64).into()
//     }
// }
//
// impl Into<KeyWrap> for i32 {
//     fn into(self) -> KeyWrap {
//         (self as f64).into()
//     }
// }
//
// impl Into<KeyWrap> for u32 {
//     fn into(self) -> KeyWrap {
//         (self as f64).into()
//     }
// }
//
// impl KeyWrap {
//     pub const MIN: Self = Self(f64::MIN);
//     pub const MAX: Self = Self(f64::MAX);
//
//     pub fn checked_sub(&self, other: &Self) -> Option<Self> {
//         if self.to_bits() == Self::MIN.to_bits() {
//             None
//         }
//         else {
//             Some(Self(self.0.sub(other.0)))
//         }
//     }
//
//     pub fn checked_add(&self, other: &Self) -> Option<Self> {
//         if self.to_bits() == Self::MAX.to_bits() {
//             None
//         }
//         else {
//             Some(Self(self.0.add(other.0)))
//         }
//     }
// }
//
// impl Deref for Key {
//     type Target = f64;
//
//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }
//
// impl Display for Key {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{}", self)
//     }
// }
//
// impl Eq for Key {}
//
// impl PartialEq<Self> for Key {
//     fn eq(&self, other: &Self) -> bool {
//         self.cmp(other).is_eq()
//     }
// }
//
// impl PartialOrd<Self> for Key {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }
//
// impl Ord for Key {
//     fn cmp(&self, other: &Self) -> Ordering {
//         self.total_cmp(other)
//     }
// }
//
// impl Hash for KeyWrap {
//     fn hash<H: Hasher>(&self, state: &mut H) {
//         state.write_u64(self.0.to_bits())
//     }
// }
pub fn level_order<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display,
    Payload: Default + Clone + Sync + Display>(root: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
{
    let mut queue = VecDeque::new();
    queue.push_back(root);

    while !queue.is_empty() {
        let next = queue.pop_front().unwrap();

        match next.unsafe_borrow().as_ref() {
            Node::Index(index_page) =>
                println!("id: {}, Index(keys: {}, children: {})",
                         next.unsafe_borrow().block_id(),
                         index_page.keys()
                             .iter()
                             .join(","),
                         index_page.children()
                             .iter()
                             .map(|b| {
                                 queue.push_back(b.clone());
                                 b.unsafe_borrow().block_id()
                             })
                             .join(",")),
            Node::Leaf(leaf_page) =>
                println!("id: {}, Leaf({})",
                         next.unsafe_borrow().block_id(),
                         leaf_page.as_records().iter().join(","))
        }
    }
}

pub fn show_alignment_bsz() {
    log_debug_ln(format!("\t- Block Size: \t\t{} bytes\n\t\
        - Block Align-Size: \t{} bytes\n\t\
        - Block/Delta: \t\t{}/{} bytes\n\t\
        - Num Keys: \t\t{}\n\t\
        - Fan Out: \t\t{}\n\t\
        - Num Records: \t\t{}\n",
                         BSZ_BASE,
                         bsz_alignment::<Key, Payload>(),
                         mem::size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>(),
                         BSZ_BASE - mem::size_of::<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>(),
                         FAN_OUT - 1,
                         FAN_OUT,
                         NUM_RECORDS)
    );
}


pub(crate) const S_THREADS_CPU: [usize; 12] = [
    1,
    2,
    3,
    4,
    8,
    10,
    12,
    16,
    24,
    32,
    64,
    128,
    // 256,
    // 512,
    // 1024,
    // usize::MAX
];

pub(crate) const S_INSERTIONS: [Key; 1] = [
    // 10,
    // 100,
    // 1_000,
    // 10_000,
    // 100_000,
    // 1_000_000,
    // 2_000_000,
    // 5_000_000,
    // 10_000_000,
    // 20_000_000,
    // 50_000_000,
    100_000_000,
];

pub(crate) const S_STRATEGIES: [CRUDProtocol; 17] = [
    MonoWriter,
    LockCoupling,
    orwc_attempts(0),
    orwc_attempts(1),
    orwc_attempts(4),
    orwc_attempts(16),
    orwc_attempts(64),
    orwc_attempts(1024),

    // lightweight_hybrid_lock_read_attempts(0), // only relevant in contented workloads, i.e. WRITE+READ
    // lightweight_hybrid_lock_read_attempts(1),
    // lightweight_hybrid_lock_read_attempts(4),
    // lightweight_hybrid_lock_read_attempts(16),
    // lightweight_hybrid_lock_read_attempts(64),
    // lightweight_hybrid_lock_read_attempts(1024),

    olc(),
    lightweight_hybrid_lock_unlimited(),
    lightweight_hybrid_lock_write_attempts(0),
    lightweight_hybrid_lock_write_attempts(1),
    lightweight_hybrid_lock_write_attempts(4),
    lightweight_hybrid_lock_write_attempts(16),
    lightweight_hybrid_lock_write_attempts(64),
    lightweight_hybrid_lock_write_attempts(1024),
    hybrid_lock()
];



pub fn log_debug_ln(s: String) {
    println!("> {}", s.replace("\n", "\n>"))
}

pub fn log_debug(s: String) {
    print!("> {}", s.replace("\n", "\n>"))
}