//! Online (non-trace-replay) implementation of the workloads used by Section 8 of the paper.

use super::reader_perf::{ReaderPerf, ReaderPerfValue};
use super::systems::{PaperIndex, SYSTEMS, make_system};
use super::{FileOp, Key};
use crate::ycsb::{Dist, KeyChooser, Rng};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const USAGE: &str = "\
usage: paper-ycsb [--key value]...
  --system <cmvbt|chain|frugal|vweaver|skiplist|mdbx>                    [cmvbt]
  --experiment <label>                                                   [paper]
  --repeat <n>                                                           [1]
  --records <n>             initial insertions                         [2000000]
  --operations <n>          measured online writes                       [1000000]
  --update-rate <0..100>    inserts/deletes split the remainder equally  [60]
  --distribution <uniform|zipf> update-key distribution                [uniform]
  --theta <x>               Zipfian skew (any finite x > 0)               [0.99]
  --scramble <bool>         spread hot ranks over the live key range       [true]
  --writers <n>                                                          [32]
  --readers <n>             concurrent fresh-snapshot scanners           [16]
  --historical-scans <n>    scans after writes, uniformly over versions  [0]
  --scan-range <n>          consecutive keys selected by each scan      [100000]
  --scan-mode <range|full>  bounded ranges or the entire version          [range]
  --key-mode <sliding|random> dense FIFO keys or paper-style random keys [sliding]
  --scan-threads <n>        threads used for historical scans             [1]
  --gc <bool>                                                            [false]
  --root-index <fg|ll|sk|bt>                                             [fg]
  --seed <n>                                                             [42]
  --csv <file>                                                           [paper.csv]

The write mix is generated online in exact blocks of 1000 operations. Concurrent readers query
the freshest visible snapshot. Full scans and random keys reproduce the Figure 5 protocol;
historical scans run after the writes and sample uniformly from versions created by the measured workload.";

#[derive(Clone)]
struct Config {
    system: String,
    experiment: String,
    repeat: u64,
    records: u64,
    operations: u64,
    update_rate: u64,
    distribution: String,
    theta: f64,
    scramble: bool,
    writers: usize,
    readers: usize,
    historical_scans: u64,
    scan_range: u64,
    scan_mode: String,
    key_mode: String,
    scan_threads: usize,
    gc: bool,
    root_index: String,
    seed: u64,
    csv: String,
}

fn parse(parms: &[String]) -> Result<Config, String> {
    let mut values = std::collections::HashMap::new();
    let mut args = parms.iter();
    while let Some(flag) = args.next() {
        let name = flag
            .strip_prefix("--")
            .ok_or_else(|| format!("expected --flag, got '{flag}'"))?;
        let value = args
            .next()
            .ok_or_else(|| format!("--{name} needs a value"))?;
        values.insert(name.to_string(), value.clone());
    }
    fn get<T: std::str::FromStr>(
        values: &std::collections::HashMap<String, String>,
        name: &str,
        default: T,
    ) -> Result<T, String> {
        values.get(name).map_or(Ok(default), |value| {
            value
                .parse()
                .map_err(|_| format!("bad value '{value}' for --{name}"))
        })
    }
    const FLAGS: [&str; 20] = [
        "system",
        "experiment",
        "repeat",
        "records",
        "operations",
        "update-rate",
        "distribution",
        "theta",
        "scramble",
        "writers",
        "readers",
        "historical-scans",
        "scan-range",
        "scan-mode",
        "key-mode",
        "scan-threads",
        "gc",
        "root-index",
        "seed",
        "csv",
    ];
    if let Some(unknown) = values.keys().find(|name| !FLAGS.contains(&name.as_str())) {
        return Err(format!("unknown option '--{unknown}'"));
    }
    let cfg = Config {
        system: get(&values, "system", "cmvbt".to_string())?,
        experiment: get(&values, "experiment", "paper".to_string())?,
        repeat: get(&values, "repeat", 1)?,
        records: get(&values, "records", 2_000_000)?,
        operations: get(&values, "operations", 1_000_000)?,
        update_rate: get(&values, "update-rate", 60)?,
        distribution: get(&values, "distribution", "uniform".to_string())?,
        theta: get(&values, "theta", 0.99)?,
        scramble: get(&values, "scramble", true)?,
        writers: get(&values, "writers", 32)?,
        readers: get(&values, "readers", 16)?,
        historical_scans: get(&values, "historical-scans", 0)?,
        scan_range: get(&values, "scan-range", 100_000)?,
        scan_mode: get(&values, "scan-mode", "range".to_string())?,
        key_mode: get(&values, "key-mode", "sliding".to_string())?,
        scan_threads: get(&values, "scan-threads", 1)?,
        gc: get(&values, "gc", false)?,
        root_index: get(&values, "root-index", "fg".to_string())?,
        seed: get(&values, "seed", 42)?,
        csv: get(&values, "csv", "paper.csv".to_string())?,
    };
    if cfg.records == 0
        || cfg.operations == 0
        || cfg.writers == 0
        || cfg.scan_threads == 0
        || cfg.scan_range == 0
    {
        return Err(
            "records, operations, writers, scan-threads and scan-range must be positive".into(),
        );
    }
    if cfg.scan_mode == "range" && cfg.scan_range > cfg.records {
        return Err("scan-range must not exceed records in range mode".into());
    }
    if cfg.update_rate > 100 {
        return Err("update-rate must be in 0..=100".into());
    }
    if !matches!(cfg.scan_mode.as_str(), "range" | "full") {
        return Err("scan-mode must be 'range' or 'full'".into());
    }
    if !matches!(cfg.key_mode.as_str(), "sliding" | "random") {
        return Err("key-mode must be 'sliding' or 'random'".into());
    }
    if cfg.key_mode == "random" && cfg.writers != 1 {
        return Err("paper-style random keys require exactly one writer".into());
    }
    Dist::parse(&cfg.distribution, cfg.theta, 0.01, 0.9).and_then(|dist| match dist {
        Dist::Uniform | Dist::Zipfian(_) => Ok(()),
        _ => Err("paper-ycsb supports only uniform and zipf distributions".into()),
    })?;
    if cfg.historical_scans > 0 && cfg.readers > 0 {
        return Err(
            "historical scans and concurrent readers are separate paper protocols; choose one"
                .into(),
        );
    }
    if cfg.historical_scans > 0 && cfg.writers != 1 {
        return Err(
            "historical scans require one writer so versions map exactly to workload positions"
                .into(),
        );
    }
    Ok(cfg)
}

#[derive(Clone)]
struct Latency {
    buckets: Vec<u64>,
    count: u64,
    total_ns: u128,
    max_ns: u64,
}

/// Optional synchronization with `perf stat --control`. The FIFOs are opened
/// only after the initial load, so hardware counters cover the concurrent
/// Figure 6 phase rather than database construction.
struct PerfControl {
    control: std::fs::File,
    ack: BufReader<std::fs::File>,
}

impl PerfControl {
    fn from_env() -> Result<Option<Self>, String> {
        let control_path = std::env::var_os("CMVBT_PERF_CONTROL_FIFO");
        let ack_path = std::env::var_os("CMVBT_PERF_ACK_FIFO");
        match (control_path, ack_path) {
            (None, None) => Ok(None),
            (Some(control_path), Some(ack_path)) => {
                let control = OpenOptions::new()
                    .write(true)
                    .open(&control_path)
                    .map_err(|e| {
                        format!(
                            "open perf control FIFO {}: {e}",
                            std::path::Path::new(&control_path).display()
                        )
                    })?;
                let ack = OpenOptions::new().read(true).open(&ack_path).map_err(|e| {
                    format!(
                        "open perf acknowledgement FIFO {}: {e}",
                        std::path::Path::new(&ack_path).display()
                    )
                })?;
                Ok(Some(Self {
                    control,
                    ack: BufReader::new(ack),
                }))
            }
            _ => Err("CMVBT_PERF_CONTROL_FIFO and CMVBT_PERF_ACK_FIFO must be set together".into()),
        }
    }

    fn command(&mut self, command: &str) -> Result<(), String> {
        writeln!(self.control, "{command}")
            .and_then(|_| self.control.flush())
            .map_err(|e| format!("send perf command '{command}': {e}"))?;
        let mut acknowledgement = String::new();
        self.ack
            .read_line(&mut acknowledgement)
            .map_err(|e| format!("read perf acknowledgement for '{command}': {e}"))?;
        let acknowledgement =
            acknowledgement.trim_matches(|c: char| c == '\0' || c.is_whitespace());
        if acknowledgement != "ack" {
            return Err(format!(
                "unexpected perf acknowledgement for '{command}': {:?}",
                acknowledgement
            ));
        }
        Ok(())
    }
}

impl Latency {
    fn new() -> Self {
        Self {
            buckets: vec![0; 256],
            count: 0,
            total_ns: 0,
            max_ns: 0,
        }
    }

    fn record(&mut self, ns: u64) {
        let idx = if ns < 4 {
            ns as usize
        } else {
            let exponent = 63 - ns.leading_zeros() as usize;
            exponent * 4 + ((ns >> (exponent - 2)) & 3) as usize
        };
        self.buckets[idx] += 1;
        self.count += 1;
        self.total_ns += ns as u128;
        self.max_ns = self.max_ns.max(ns);
    }

    fn merge(&mut self, other: &Latency) {
        self.buckets
            .iter_mut()
            .zip(&other.buckets)
            .for_each(|(a, b)| *a += b);
        self.count += other.count;
        self.total_ns += other.total_ns;
        self.max_ns = self.max_ns.max(other.max_ns);
    }

    fn average(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            (self.total_ns / self.count as u128) as u64
        }
    }

    fn quantile(&self, q: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = ((self.count as f64 * q).ceil() as u64).max(1);
        let mut seen = 0;
        for (idx, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= target {
                return if idx < 4 {
                    idx as u64
                } else {
                    (4 + (idx % 4) as u64) << (idx / 4 - 2)
                };
            }
        }
        self.max_ns
    }

    fn csv(&self) -> String {
        format!(
            "{},{},{},{},{},{},{}",
            self.count,
            self.average(),
            self.quantile(0.50),
            self.quantile(0.95),
            self.quantile(0.99),
            self.quantile(0.999),
            self.max_ns
        )
    }
}

struct WriterStats {
    update: Latency,
    insert: Latency,
    delete: Latency,
    internal_retries: u64,
}

impl WriterStats {
    fn new() -> Self {
        Self {
            update: Latency::new(),
            insert: Latency::new(),
            delete: Latency::new(),
            internal_retries: 0,
        }
    }
    fn merge(&mut self, other: &WriterStats) {
        self.update.merge(&other.update);
        self.insert.merge(&other.insert);
        self.delete.merge(&other.delete);
        self.internal_retries += other.internal_retries;
    }
}

#[derive(Clone, Copy)]
enum WriteKind {
    Update,
    Insert,
    Delete,
}

/// A permutation of each 1000-operation block provides an exact, well-mixed operation ratio without a trace.
fn write_kind(operation: u64, update_rate: u64, seed: u64) -> WriteKind {
    let updates = update_rate * 10;
    let inserts = (1000 - updates) / 2;
    let block = operation / 1000;
    let offset = seed.wrapping_add(block.wrapping_mul(0x9E37_79B9)) % 1000;
    let slot = ((operation % 1000) * 791 + offset) % 1000; // gcd(791, 1000) = 1
    if slot < updates {
        WriteKind::Update
    } else if slot < updates + inserts {
        WriteKind::Insert
    } else {
        WriteKind::Delete
    }
}

/// Insert/delete counts in the exact prefix of the deterministic online mix.
fn structural_counts_before(operations: u64, update_rate: u64, seed: u64) -> (u64, u64) {
    let per_kind = (100 - update_rate) * 5;
    let full_blocks = operations / 1000;
    let (mut inserts, mut deletes) = (full_blocks * per_kind, full_blocks * per_kind);
    for operation in full_blocks * 1000..operations {
        match write_kind(operation, update_rate, seed) {
            WriteKind::Insert => inserts += 1,
            WriteKind::Delete => deletes += 1,
            WriteKind::Update => {}
        }
    }
    (inserts, deletes)
}

struct OnlineKeys {
    initial: u64,
    reserved: AtomicU64,
    published: AtomicU64,
    expired: AtomicU64,
    inserted: Box<[AtomicBool]>,
}

/// Exact workload commit versions for the historical snapshots that will actually be measured.
/// Structural modifications may consume extra clock versions, so operation number cannot be
/// reconstructed from the raw clock value.
struct HistoricalPlan {
    completed: Vec<u64>,
    versions: Vec<AtomicU64>,
    ordered: Vec<(u64, usize)>,
}

impl HistoricalPlan {
    fn new(scans: u64, operations: u64, seed: u64) -> Self {
        let mut rng = Rng::new(seed.wrapping_add(0x5CA9));
        let completed: Vec<u64> = (0..scans).map(|_| 1 + rng.below(operations)).collect();
        let mut ordered: Vec<(u64, usize)> = completed
            .iter()
            .copied()
            .enumerate()
            .map(|(scan, operation)| (operation, scan))
            .collect();
        ordered.sort_unstable();
        let versions = (0..scans).map(|_| AtomicU64::new(0)).collect();
        Self {
            completed,
            versions,
            ordered,
        }
    }

    fn record(&self, cursor: &mut usize, completed: u64, version: u64) {
        while let Some(&(target, scan)) = self.ordered.get(*cursor) {
            if target != completed {
                break;
            }
            self.versions[scan].store(version, Release);
            *cursor += 1;
        }
    }
}

impl OnlineKeys {
    fn new(initial: u64, operations: u64) -> Self {
        Self {
            initial,
            reserved: AtomicU64::new(initial),
            published: AtomicU64::new(initial),
            expired: AtomicU64::new(0),
            inserted: (0..operations)
                .map(|_| AtomicBool::new(false))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    fn reserve_insert(&self) -> u64 {
        self.reserved.fetch_add(1, Relaxed)
    }

    fn publish_insert(&self, key: u64) {
        self.inserted[(key - self.initial) as usize].store(true, Release);
        loop {
            let published = self.published.load(Acquire);
            let offset = published - self.initial;
            if offset as usize >= self.inserted.len()
                || !self.inserted[offset as usize].load(Acquire)
            {
                break;
            }
            let _ =
                self.published
                    .compare_exchange_weak(published, published + 1, Release, Relaxed);
        }
    }

    fn reserve_delete(&self) -> u64 {
        let key = self.expired.fetch_add(1, Relaxed);
        while key >= self.published.load(Acquire) {
            thread::yield_now()
        }
        key
    }

    fn sample_live(&self, chooser: &KeyChooser, rng: &mut Rng) -> u64 {
        let lo = self.expired.load(Acquire);
        let hi = self.published.load(Acquire);
        chooser.next(rng, lo, hi.saturating_sub(1).max(lo))
    }

    fn sample_scan_start(&self, range: u64, rng: &mut Rng) -> u64 {
        let lo = self.expired.load(Acquire);
        let hi = self.published.load(Acquire);
        lo + rng.below(
            hi.saturating_sub(lo)
                .saturating_sub(range)
                .saturating_add(1)
                .max(1),
        )
    }
}

/// A seeded permutation of the u64 key space. Consecutive logical record IDs
/// become unique, randomly distributed physical keys like the paper's trace generator.
fn paper_key(id: u64, seed: u64) -> u64 {
    let mut key = id.wrapping_add(seed).wrapping_add(0x9E37_79B9_7F4A_7C15);
    key = (key ^ (key >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    key = (key ^ (key >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    key ^ (key >> 31)
}

fn load(index: &dyn PaperIndex, cfg: &Config) -> Result<(), String> {
    if cfg.key_mode == "sliding" {
        if let Some(result) = index.bulk_load(cfg.records) {
            return result;
        }
    }
    for id in 0..cfg.records {
        let key = if cfg.key_mode == "random" {
            paper_key(id, cfg.seed)
        } else {
            id
        };
        if !index.apply(FileOp::Insert(key)) {
            return Err(format!("initial insert({key}) failed"));
        }
    }
    index.finish_thread();
    Ok(())
}

fn random_key_writer(
    index: Arc<dyn PaperIndex>,
    cfg: Config,
    historical: Option<Arc<HistoricalPlan>>,
    barrier: Arc<Barrier>,
    seed: u64,
) -> WriterStats {
    let mut rng = Rng::new(seed);
    let dist =
        Dist::parse(&cfg.distribution, cfg.theta, 0.01, 0.9).expect("validated paper distribution");
    let chooser = KeyChooser::new(dist, cfg.records, cfg.scramble);
    let mut live: Vec<Key> = (0..cfg.records).map(|id| paper_key(id, cfg.seed)).collect();
    let mut next_id = cfg.records;
    let mut stats = WriterStats::new();
    let mut historical_cursor = 0;
    barrier.wait();

    for operation in 0..cfg.operations {
        let kind = write_kind(operation, cfg.update_rate, cfg.seed);
        let started = Instant::now();
        let version = match kind {
            WriteKind::Insert => {
                let key = paper_key(next_id, cfg.seed);
                next_id += 1;
                let version = index
                    .apply_versioned(FileOp::Insert(key))
                    .unwrap_or_else(|| panic!("fresh random insert({key}) failed"));
                live.push(key);
                stats.insert.record(started.elapsed().as_nanos() as u64);
                version
            }
            WriteKind::Delete => {
                let rank = chooser.next(&mut rng, 0, live.len() as u64 - 1) as usize;
                let key = live.swap_remove(rank);
                let version = index
                    .apply_versioned(FileOp::Delete(key))
                    .unwrap_or_else(|| panic!("random live delete({key}) failed"));
                stats.delete.record(started.elapsed().as_nanos() as u64);
                version
            }
            WriteKind::Update => loop {
                let rank = chooser.next(&mut rng, 0, live.len() as u64 - 1) as usize;
                let key = live[rank];
                if let Some(version) = index.apply_versioned(FileOp::Update(key)) {
                    stats.update.record(started.elapsed().as_nanos() as u64);
                    break version;
                }
                stats.internal_retries += 1;
            },
        };
        if let Some(plan) = &historical {
            plan.record(&mut historical_cursor, operation + 1, version);
        }
    }
    index.finish_thread();
    stats
}

fn writer(
    index: Arc<dyn PaperIndex>,
    cfg: Config,
    next: Arc<AtomicU64>,
    keys: Arc<OnlineKeys>,
    historical: Option<Arc<HistoricalPlan>>,
    barrier: Arc<Barrier>,
    seed: u64,
) -> WriterStats {
    if cfg.key_mode == "random" {
        return random_key_writer(index, cfg, historical, barrier, seed);
    }
    let mut rng = Rng::new(seed);
    let dist =
        Dist::parse(&cfg.distribution, cfg.theta, 0.01, 0.9).expect("validated paper distribution");
    let chooser = KeyChooser::new(dist, cfg.records, cfg.scramble);
    let mut stats = WriterStats::new();
    let mut historical_cursor = 0;
    barrier.wait();
    loop {
        let operation = next.fetch_add(1, Relaxed);
        if operation >= cfg.operations {
            break;
        }
        let kind = write_kind(operation, cfg.update_rate, cfg.seed);
        let started = Instant::now();
        let version = match kind {
            WriteKind::Insert => {
                let key = keys.reserve_insert();
                let version = index
                    .apply_versioned(FileOp::Insert(key))
                    .unwrap_or_else(|| panic!("fresh insert({key}) failed"));
                keys.publish_insert(key);
                stats.insert.record(started.elapsed().as_nanos() as u64);
                version
            }
            WriteKind::Delete => {
                let key = keys.reserve_delete();
                let version = index
                    .apply_versioned(FileOp::Delete(key))
                    .unwrap_or_else(|| panic!("live delete({key}) failed"));
                stats.delete.record(started.elapsed().as_nanos() as u64);
                version
            }
            WriteKind::Update => loop {
                let key = keys.sample_live(&chooser, &mut rng);
                if let Some(version) = index.apply_versioned(FileOp::Update(key)) {
                    stats.update.record(started.elapsed().as_nanos() as u64);
                    break version;
                }
                stats.internal_retries += 1;
            },
        };
        if let Some(plan) = &historical {
            plan.record(&mut historical_cursor, operation + 1, version);
        }
    }
    index.finish_thread();
    stats
}

fn fresh_reader(
    index: Arc<dyn PaperIndex>,
    keys: Arc<OnlineKeys>,
    scan_range: u64,
    full_scan: bool,
    seed: u64,
    done: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) -> (Latency, u64, Vec<ReaderPerfValue>) {
    let mut rng = Rng::new(seed);
    let mut latency = Latency::new();
    let mut records = 0;
    let mut perf = ReaderPerf::prepare();
    barrier.wait();
    perf.start();
    while !done.load(Acquire) {
        let started = Instant::now();
        records += if full_scan {
            index.scan_fresh() as u64
        } else {
            let start = keys.sample_scan_start(scan_range, &mut rng);
            index.scan_fresh_range(start, scan_range) as u64
        };
        latency.record(started.elapsed().as_nanos() as u64);
    }
    (latency, records, perf.finish())
}

fn historical_scans(
    index: &Arc<dyn PaperIndex>,
    cfg: &Config,
    plan: &Arc<HistoricalPlan>,
) -> (Latency, u64, u128) {
    if cfg.historical_scans == 0 {
        return (Latency::new(), 0, 0);
    }
    let next = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let workers: Vec<_> = (0..cfg.scan_threads)
        .map(|worker| {
            let (index, next, cfg, plan) = (index.clone(), next.clone(), cfg.clone(), plan.clone());
            thread::spawn(move || {
                let mut rng = Rng::new(cfg.seed.wrapping_add(0x5CA9 + worker as u64));
                let mut latency = Latency::new();
                let mut records = 0;
                loop {
                    let scan = next.fetch_add(1, Relaxed);
                    if scan >= cfg.historical_scans {
                        break;
                    }
                    let completed = plan.completed[scan as usize];
                    let version = plan.versions[scan as usize].load(Acquire);
                    assert!(
                        version > 0,
                        "historical version for scan {scan} was not recorded"
                    );
                    let t = Instant::now();
                    records += if cfg.scan_mode == "full" {
                        index.scan_at(version) as u64
                    } else {
                        let (lo, hi) = if index.historical_scan_mode() == "pinned_initial" {
                            // libmdbx can preserve the beginning snapshot only by
                            // retaining its read transaction. Keep every range in
                            // that snapshot's original key domain.
                            (0, cfg.records)
                        } else {
                            let (inserts, deletes) =
                                structural_counts_before(completed, cfg.update_rate, cfg.seed);
                            (deletes, cfg.records + inserts)
                        };
                        let start = lo
                            + rng.below(
                                hi.saturating_sub(lo)
                                    .saturating_sub(cfg.scan_range)
                                    .saturating_add(1)
                                    .max(1),
                            );
                        index.scan_at_range(start, cfg.scan_range, version) as u64
                    };
                    latency.record(t.elapsed().as_nanos() as u64);
                }
                (latency, records)
            })
        })
        .collect();
    let mut latency = Latency::new();
    let mut records = 0;
    for worker in workers {
        let (part, part_records) = worker.join().expect("historical scan worker panicked");
        latency.merge(&part);
        records += part_records;
    }
    (latency, records, started.elapsed().as_nanos())
}

const LATENCY_COLUMNS: &str = "count,avg_ns,p50_ns,p95_ns,p99_ns,p999_ns,max_ns";

fn append_reader_perf(
    cfg: &Config,
    reader: usize,
    scan_count: u64,
    scan_records: u64,
    values: &[ReaderPerfValue],
) -> Result<(), String> {
    let Some(path) = std::env::var_os("CMVBT_READER_PERF_CSV") else {
        return Ok(());
    };
    if values.is_empty() {
        return Ok(());
    }
    let group = std::env::var("CMVBT_READER_PERF_GROUP").unwrap_or_default();
    let path = std::path::Path::new(&path);
    let existed = path.exists();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("open {}: {e}", path.display()))?;
    if !existed {
        writeln!(
            file,
            "experiment,repeat,system,update_rate,event_group,reader,event,value,raw_value,time_enabled_ns,time_running_ns,enabled_fraction,scan_count,scan_records,error"
        )
        .map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    let clean = |text: &str| text.replace([',', '\n', '\r'], ";");
    for value in values {
        let display = |number: Option<u64>| number.map(|n| n.to_string()).unwrap_or_default();
        let enabled_fraction = match (value.time_enabled_ns, value.time_running_ns) {
            (Some(enabled), Some(running)) if enabled > 0 => {
                format!("{:.6}", running as f64 / enabled as f64)
            }
            _ => String::new(),
        };
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            clean(&cfg.experiment),
            cfg.repeat,
            clean(&cfg.system),
            cfg.update_rate,
            clean(&group),
            reader,
            value.event,
            display(value.value),
            display(value.raw_value),
            display(value.time_enabled_ns),
            display(value.time_running_ns),
            enabled_fraction,
            scan_count,
            scan_records,
            clean(value.error.as_deref().unwrap_or_default()),
        )
        .map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    Ok(())
}

fn append_csv(
    cfg: &Config,
    oltp_ns: u128,
    stats: &WriterStats,
    scan: &Latency,
    scan_records: u64,
    scan_ns: u128,
    allocated: u64,
    reused: u64,
    scan_mode: &str,
) -> Result<(), String> {
    let latency = |name: &str| {
        LATENCY_COLUMNS
            .split(',')
            .map(|field| format!("{name}_{field}"))
            .collect::<Vec<_>>()
            .join(",")
    };
    let header = format!(
        "experiment,repeat,system,root_index,seed,distribution,theta,scramble,update_rate,gc,records,operations,writers,readers,scan_mode,historical_scans,scan_range,scan_threads,\
oltp_time_ns,oltp_ops_per_s,scan_time_ns,scan_ops_per_s,scan_records,internal_write_retries,blocks_allocated,blocks_reused,{},{},{},{}",
        latency("update"),
        latency("insert"),
        latency("delete"),
        latency("scan")
    );
    let existed = std::path::Path::new(&cfg.csv).exists();
    if existed {
        let existing =
            std::fs::read_to_string(&cfg.csv).map_err(|e| format!("read {}: {e}", cfg.csv))?;
        if existing.lines().next() != Some(header.as_str()) {
            return Err(format!(
                "{} has an incompatible CSV header; choose a new output directory",
                cfg.csv
            ));
        }
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cfg.csv)
        .map_err(|e| format!("open {}: {e}", cfg.csv))?;
    if !existed {
        writeln!(file, "{header}").map_err(|e| format!("write {}: {e}", cfg.csv))?;
    }
    let throughput = cfg.operations as f64 / (oltp_ns as f64 / 1e9);
    let scan_throughput = if scan_ns == 0 {
        0.0
    } else {
        scan.count as f64 / (scan_ns as f64 / 1e9)
    };
    writeln!(
        file,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.3},{},{:.3},{},{},{},{},{},{},{},{}",
        cfg.experiment,
        cfg.repeat,
        cfg.system,
        cfg.root_index,
        cfg.seed,
        cfg.distribution,
        if cfg.distribution == "uniform" { 0.0 } else { cfg.theta },
        cfg.scramble,
        cfg.update_rate,
        cfg.gc,
        cfg.records,
        cfg.operations,
        cfg.writers,
        cfg.readers,
        if cfg.historical_scans > 0 {
            scan_mode
        } else if cfg.readers > 0 {
            "fresh"
        } else {
            "none"
        },
        cfg.historical_scans,
        cfg.scan_range,
        cfg.scan_threads,
        oltp_ns,
        throughput,
        scan_ns,
        scan_throughput,
        scan_records,
        stats.internal_retries,
        allocated,
        reused,
        stats.update.csv(),
        stats.insert.csv(),
        stats.delete.csv(),
        scan.csv()
    )
    .map_err(|e| format!("write {}: {e}", cfg.csv))?;
    Ok(())
}

fn run(cfg: Config) -> Result<(), String> {
    let index = make_system(&cfg.system, &cfg.root_index, cfg.gc)
        .map_err(|e| format!("{e} (systems: {SYSTEMS})"))?;
    println!(
        "# online paper workload: system={} distribution={} theta={} update={}%, records={}, operations={}, writers={}, readers={}, historical_scans={}, scan_mode={}, scan_range={}, key_mode={}, gc={}",
        cfg.system,
        cfg.distribution,
        if cfg.distribution == "uniform" {
            0.0
        } else {
            cfg.theta
        },
        cfg.update_rate,
        cfg.records,
        cfg.operations,
        cfg.writers,
        cfg.readers,
        cfg.historical_scans,
        cfg.scan_mode,
        cfg.scan_range,
        cfg.key_mode,
        cfg.gc
    );
    // Keep loading separate from the measurement workers; `load` explicitly releases its commit
    // slot before this join boundary.
    let loader = index.clone();
    let load_cfg = cfg.clone();
    thread::spawn(move || load(loader.as_ref(), &load_cfg))
        .join()
        .map_err(|_| "initial-load thread panicked".to_string())??;
    index.reset_alloc_counts();

    let next = Arc::new(AtomicU64::new(0));
    let keys = Arc::new(OnlineKeys::new(cfg.records, cfg.operations));
    let historical = (cfg.historical_scans > 0).then(|| {
        Arc::new(HistoricalPlan::new(
            cfg.historical_scans,
            cfg.operations,
            cfg.seed,
        ))
    });
    if historical.is_some() {
        index.prepare_historical_snapshot()?;
    }
    let done = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(cfg.writers + cfg.readers + 1));
    let readers: Vec<_> = (0..cfg.readers)
        .map(|worker| {
            let (index, keys, done, barrier) =
                (index.clone(), keys.clone(), done.clone(), barrier.clone());
            let scan_range = cfg.scan_range;
            let full_scan = cfg.scan_mode == "full";
            let seed = cfg.seed.wrapping_add(0x0A1A + worker as u64);
            thread::spawn(move || {
                fresh_reader(index, keys, scan_range, full_scan, seed, done, barrier)
            })
        })
        .collect();
    let writers: Vec<_> = (0..cfg.writers)
        .map(|worker| {
            let (index, cfg, next, keys, historical, barrier) = (
                index.clone(),
                cfg.clone(),
                next.clone(),
                keys.clone(),
                historical.clone(),
                barrier.clone(),
            );
            thread::spawn(move || {
                writer(
                    index,
                    cfg.clone(),
                    next,
                    keys,
                    historical,
                    barrier,
                    cfg.seed.wrapping_add(worker as u64 * 7919 + 1),
                )
            })
        })
        .collect();

    let mut perf_control = PerfControl::from_env()?;
    if let Some(control) = perf_control.as_mut() {
        control.command("enable")?;
    }
    let started = Instant::now();
    barrier.wait();
    let mut stats = WriterStats::new();
    for worker in writers {
        stats.merge(&worker.join().expect("paper writer panicked"));
    }
    let oltp_ns = started.elapsed().as_nanos();
    done.store(true, Release);

    let mut scan = Latency::new();
    let mut scan_records = 0;
    for (reader_id, reader) in readers.into_iter().enumerate() {
        let (part, records, perf) = reader.join().expect("paper reader panicked");
        append_reader_perf(&cfg, reader_id, part.count, records, &perf)?;
        scan.merge(&part);
        scan_records += records;
    }
    if let Some(control) = perf_control.as_mut() {
        control.command("disable")?;
    }
    let mut scan_ns = if cfg.readers > 0 { oltp_ns } else { 0 };
    let (historical_latency, historical_records, historical_ns) = match &historical {
        Some(plan) => historical_scans(&index, &cfg, plan),
        None => (Latency::new(), 0, 0),
    };
    scan.merge(&historical_latency);
    scan_records += historical_records;
    if historical_ns > 0 {
        scan_ns = historical_ns;
    }

    let (allocated, reused) = index.alloc_counts();
    let scan_mode = if cfg.historical_scans > 0 && cfg.scan_mode == "full" {
        format!("{}_full", index.historical_scan_mode())
    } else {
        index.historical_scan_mode().to_string()
    };
    append_csv(
        &cfg,
        oltp_ns,
        &stats,
        &scan,
        scan_records,
        scan_ns,
        allocated,
        reused,
        &scan_mode,
    )?;
    println!(
        "# OLTP {:.0} ops/s; scans {:.1}/s; scan p50 {:.3} ms p99 {:.3} ms; internal write retries {}",
        cfg.operations as f64 / (oltp_ns as f64 / 1e9),
        if scan_ns == 0 {
            0.0
        } else {
            scan.count as f64 / (scan_ns as f64 / 1e9)
        },
        scan.quantile(0.5) as f64 / 1e6,
        scan.quantile(0.99) as f64 / 1e6,
        stats.internal_retries
    );
    Ok(())
}

pub fn main_online(parms: Vec<String>) {
    if parms.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("{USAGE}");
        return;
    }
    if let Err(error) = parse(&parms[2..]).and_then(run) {
        eprintln!("error: {error}\n\n{USAGE}");
        std::process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_online_block_has_the_exact_paper_mix() {
        for update_rate in [10, 20, 50, 60, 75, 90, 100] {
            for block in 0..3 {
                let mut counts = [0; 3];
                for operation in block * 1000..(block + 1) * 1000 {
                    counts[match write_kind(operation, update_rate, 42) {
                        WriteKind::Update => 0,
                        WriteKind::Insert => 1,
                        WriteKind::Delete => 2,
                    }] += 1;
                }
                assert_eq!(
                    counts,
                    [
                        update_rate * 10,
                        (100 - update_rate) * 5,
                        (100 - update_rate) * 5
                    ]
                );
            }
        }
    }

    #[test]
    fn paper_defaults_to_two_million_uniform_records() {
        let cfg = parse(&[]).unwrap();
        assert_eq!(cfg.records, 2_000_000);
        assert_eq!(cfg.scan_range, 100_000);
        assert_eq!(cfg.scan_mode, "range");
        assert_eq!(cfg.key_mode, "sliding");
        assert_eq!(cfg.distribution, "uniform");
        assert_eq!(cfg.theta, 0.99);
        assert!(cfg.scramble);
    }

    #[test]
    fn full_scan_random_key_mode_accepts_the_figure5_shape() {
        let cfg = parse(&[
            "--records".into(),
            "10000".into(),
            "--scan-range".into(),
            "10000".into(),
            "--scan-mode".into(),
            "full".into(),
            "--key-mode".into(),
            "random".into(),
            "--writers".into(),
            "1".into(),
        ])
        .unwrap();
        assert_eq!(cfg.scan_mode, "full");
        assert_eq!(cfg.key_mode, "random");
        assert_eq!(cfg.records, 10_000);
    }

    #[test]
    fn paper_key_permutation_has_no_duplicates() {
        let mut keys: Vec<_> = (0..100_000).map(|id| paper_key(id, 42)).collect();
        keys.sort_unstable();
        assert!(keys.windows(2).all(|pair| pair[0] != pair[1]));
    }

    #[test]
    fn paper_update_rates_allow_the_75_percent_figure() {
        let cfg = parse(&["--update-rate".into(), "75".into()]).unwrap();
        assert_eq!(cfg.update_rate, 75);
    }

    #[test]
    fn paper_accepts_requested_zipfian_skews() {
        for theta in [0.1, 0.4, 0.8, 0.99, 1.4] {
            let cfg = parse(&[
                "--distribution".into(),
                "zipf".into(),
                "--theta".into(),
                theta.to_string(),
            ])
            .unwrap();
            assert_eq!(cfg.distribution, "zipf");
            assert_eq!(cfg.theta, theta);
        }
    }

    #[test]
    fn structural_prefix_counts_match_the_generated_mix() {
        for rate in [10, 60, 75, 100] {
            for operations in [0, 1, 17, 999, 1000, 1731, 10_000] {
                let mut expected = (0, 0);
                for operation in 0..operations {
                    match write_kind(operation, rate, 42) {
                        WriteKind::Insert => expected.0 += 1,
                        WriteKind::Delete => expected.1 += 1,
                        WriteKind::Update => {}
                    }
                }
                assert_eq!(structural_counts_before(operations, rate, 42), expected);
            }
        }
    }

    #[test]
    fn historical_plan_records_exact_versions_for_sampled_operations() {
        let plan = HistoricalPlan::new(500, 10_000, 42);
        let mut cursor = 0;
        for completed in 1..=10_000 {
            plan.record(&mut cursor, completed, 1_000_000 + completed);
        }
        assert_eq!(cursor, plan.ordered.len());
        for scan in 0..plan.completed.len() {
            assert_eq!(
                plan.versions[scan].load(Relaxed),
                1_000_000 + plan.completed[scan]
            );
        }
    }
}
