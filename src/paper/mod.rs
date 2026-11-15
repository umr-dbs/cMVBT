//! The experiment drivers of the paper, for the cMVBT and the version-list baselines alike.
//!
//! `load` replays a workload file written by `generate` (initial insertions, then blocks of mixed
//! insertions/updates/deletions) with the paper's protocol; `retry-exp` measures the retries of
//! optimistic write traversals (Figure 12). Both append one row per run to a CSV file.
//!
//! ```text
//! cMVBT generate 60.dat 10000 1000 200 600 200 0 [seed]
//! cMVBT load 60.dat true 16 32 0 max fg false false 10000 [system]
//! ```
//! `system` is `cmvbt` (default), `chain`, `frugal`, `vweaver`, `skiplist` or `mdbx` (no historical scans:
//! libmdbx has no time-travel queries). The environment variables `RESULTS_CSV` (default `oltp.csv`),
//! `EXPERIMENT` and `REPEAT` label the row.
//!
//! Concurrent mode partitions the measured operations by key over the writers (`partition_by_key`); readers scan
//! the freshest snapshot. Sequential mode applies the workload with one writer, then scans versions drawn uniformly
//! from all versions of the tree.

mod systems;
mod online;
mod reader_perf;
#[cfg(test)]
mod tests;

pub use online::main_online;

use std::fs::OpenOptions;
use std::io::{BufReader, Read, Write};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crate::mv_utils::retry_stats;
use systems::{make_system, PaperIndex, SYSTEMS};

pub type Key = u64;

const INSERT: u8 = 0;
const UPDATE: u8 = 1;
const DELETE: u8 = 2;

#[derive(Clone, Copy)]
pub enum FileOp {
    Insert(Key),
    Update(Key),
    Delete(Key),
}

/// Reads a workload file: 9 bytes per operation (kind, little-endian key).
pub(crate) fn load_workload(path: &str) -> Vec<FileOp> {
    let mut file = BufReader::new(OpenOptions::new().read(true).open(path)
        .unwrap_or_else(|e| panic!("cannot open workload file '{path}': {e}")));
    let mut ops = vec![];
    let mut buff = [0u8; 9];
    while file.read_exact(&mut buff).is_ok() {
        let key = Key::from_le_bytes(buff[1..].try_into().unwrap());
        ops.push(match buff[0] {
            INSERT => FileOp::Insert(key),
            UPDATE => FileOp::Update(key),
            DELETE => FileOp::Delete(key),
            other => panic!("unknown operation kind {other} in '{path}'"),
        });
    }
    ops
}

/// Operations attempted by the OLTP threads, in total and per kind, and how many of them did not take effect
/// (e.g. an update or delete of an absent key). `executed - failed` are the successful operations.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OltpCounts {
    pub(crate) executed: u64,
    pub(crate) failed: u64,
    pub(crate) inserts: u64,
    pub(crate) updates: u64,
    pub(crate) deletes: u64,
    pub(crate) failed_inserts: u64,
    pub(crate) failed_updates: u64,
    pub(crate) failed_deletes: u64,
}

impl OltpCounts {
    fn add(&mut self, op: FileOp, ok: bool) {
        let failed = !ok as u64;
        self.executed += 1;
        self.failed += failed;
        match op {
            FileOp::Insert(_) => { self.inserts += 1; self.failed_inserts += failed; }
            FileOp::Update(_) => { self.updates += 1; self.failed_updates += failed; }
            FileOp::Delete(_) => { self.deletes += 1; self.failed_deletes += failed; }
        }
    }

    fn merge(self, o: OltpCounts) -> OltpCounts {
        OltpCounts {
            executed: self.executed + o.executed,
            failed: self.failed + o.failed,
            inserts: self.inserts + o.inserts,
            updates: self.updates + o.updates,
            deletes: self.deletes + o.deletes,
            failed_inserts: self.failed_inserts + o.failed_inserts,
            failed_updates: self.failed_updates + o.failed_updates,
            failed_deletes: self.failed_deletes + o.failed_deletes,
        }
    }

    pub(crate) fn succeeded(&self) -> u64 {
        self.executed - self.failed
    }
}

pub(crate) fn apply_all(index: &dyn PaperIndex, ops: &[FileOp]) -> OltpCounts {
    let mut counts = OltpCounts::default();
    for op in ops {
        counts.add(*op, index.apply(*op));
    }
    index.finish_thread();
    counts
}

/// Splits the measured workload among `writers` OLTP threads by key: all operations on one key go to the same
/// writer, in their original trace order. Operations on different keys are independent (an insert, update or delete
/// of key k only depends on earlier operations on k), so every operation of a serially valid trace still finds its
/// key in the state the trace expects -- no update/delete overtakes the insert of its key in another thread -- and
/// the live data set stays at the size the trace implies. Contiguous slices of the trace, as replayed before,
/// let a delete/update in one slice run before the insert of its key in an earlier slice, which failed ~20% of the
/// operations and let the live set drift far above the initial size.
///
/// Keys are spread by a 64-bit mixer (the splitmix64 finalizer), so the partitions are balanced in expectation.
pub(crate) fn partition_by_key(ops: Vec<FileOp>, writers: usize) -> Vec<Vec<FileOp>> {
    assert!(writers > 0, "at least one OLTP thread");
    fn mix(mut z: u64) -> u64 {
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
    let mut work: Vec<Vec<FileOp>> = (0..writers).map(|_| Vec::with_capacity(ops.len() / writers + 1)).collect();
    for op in ops {
        let (FileOp::Insert(key) | FileOp::Update(key) | FileOp::Delete(key)) = op;
        work[(mix(key) % writers as u64) as usize].push(op);
    }
    work
}

#[derive(Default)]
pub(crate) struct ScanStats {
    scans: u64,
    records: u64,
    total_ns: u128,
    latencies_ns: Vec<u64>,
}

impl ScanStats {
    fn record(&mut self, records: usize, ns: u64) {
        self.scans += 1;
        self.records += records as u64;
        self.total_ns += ns as u128;
        self.latencies_ns.push(ns);
    }

    fn merge(mut self, other: ScanStats) -> ScanStats {
        self.scans += other.scans;
        self.records += other.records;
        self.total_ns += other.total_ns;
        self.latencies_ns.extend(other.latencies_ns);
        self
    }

    fn avg_ns(&self) -> f64 {
        if self.scans == 0 { 0.0 } else { self.total_ns as f64 / self.scans as f64 }
    }

    fn quantile_ns(&mut self, q: f64) -> u64 {
        if self.latencies_ns.is_empty() {
            return 0
        }
        self.latencies_ns.sort_unstable();
        self.latencies_ns[((self.latencies_ns.len() - 1) as f64 * q) as usize]
    }
}

fn append_row(header: &str, row: &str) {
    let path = std::env::var("RESULTS_CSV").unwrap_or_else(|_| "oltp.csv".into());
    let existed = std::path::Path::new(&path).exists();
    let mut f = OpenOptions::new().create(true).append(true).open(&path).unwrap();
    if !existed {
        writeln!(f, "{header}").unwrap();
    }
    writeln!(f, "{row}").unwrap();
}

fn label() -> (String, String) {
    (std::env::var("EXPERIMENT").unwrap_or_default(), std::env::var("REPEAT").unwrap_or_else(|_| "0".into()))
}

/// `oltp_ops` counts every attempted operation and `oltp_failed` those that did not take effect; plots use
/// `oltp_ops - oltp_failed` (successful operations). The per-kind columns and `final_records` (records of the
/// freshest snapshot after the run) were appended after the original columns.
const LOAD_HEADER: &str = "experiment,repeat,system,workload,concurrent,oltp_threads,olap_threads,gc,init_keys,\
oltp_ops,oltp_failed,oltp_time_ns,scans,scanned_records,olap_time_ns,avg_scan_ns,p50_scan_ns,p99_scan_ns,\
blocks_allocated,blocks_reused,oltp_inserts,oltp_updates,oltp_deletes,oltp_failed_inserts,oltp_failed_updates,\
oltp_failed_deletes,records_per_scan,final_records";

/// `load <file> <concurrent> <olap-threads> <oltp-threads|scans> <olap-skew> <range> <root*> <gc> <gc-uip> <init-keys> [system]`
pub fn main_load(parms: Vec<String>) {
    println!("###### Command: {} ######", parms[1..].join(" "));
    if parms.len() < 12 {
        return eprintln!("usage: load <file> <concurrent> <olap-threads> <oltp-threads|scans-per-olap-thread> <olap-skew> \
<key-range-max> <root*-index> <gc> <gc-uip> <init-keys> [{SYSTEMS}]")
    }

    let file = parms[2].as_str();
    let concurrent: bool = parms[3].parse().expect("concurrent: true|false");
    let olap_threads: usize = parms[4].parse().expect("olap-threads");
    let oltp_threads_or_scans: usize = parms[5].parse().expect("oltp-threads / scans");
    // parms[6] (olap skew) and parms[7] (key range) are accepted for command compatibility; the paper's
    // OLAP queries return the entire data set of a version.
    let root_index = parms[8].as_str();
    let gc: bool = parms[9].parse().unwrap_or(false);
    let init_keys: usize = parms[11].parse().expect("init-keys");
    let system = parms.get(12).map_or("cmvbt", String::as_str);

    let index: Arc<dyn PaperIndex> = make_system(system, root_index, gc)
        .unwrap_or_else(|e| panic!("{e} (systems: {SYSTEMS})"));

    let mut ops = load_workload(file);
    assert!(init_keys <= ops.len(), "init-keys exceeds the workload size");
    let init: Vec<FileOp> = ops.drain(..init_keys).collect();
    // The initial insertions run in a thread of their own: a thread that committed and then stays idle (here: the
    // main thread, waiting for the workers) would hold back the snapshots of all readers and the GC at its last
    // commit version, since the visible snapshot is the minimum over the threads' latest commits.
    // The commit version of the first initial insertion is the oldest version that historical scans may query.
    let (init_counts, first_version) = {
        let index = index.clone();
        thread::spawn(move || {
            let (mut counts, mut first) = (OltpCounts::default(), u64::MAX);
            for op in &init {
                let version = index.apply_versioned(*op);
                if let Some(v) = version {
                    first = first.min(v);
                }
                counts.add(*op, version.is_some());
            }
            index.finish_thread();
            (counts, first)
        }).join().unwrap()
    };
    assert_eq!(init_counts.failed, 0, "initial insertions must all succeed");
    index.reset_alloc_counts();
    if !concurrent {
        index
            .prepare_historical_snapshot()
            .unwrap_or_else(|e| panic!("prepare historical snapshot: {e}"));
    }

    let (oltp_threads, olap_threads_effective) = if concurrent { (oltp_threads_or_scans, olap_threads) } else { (1, olap_threads) };
    println!("- system={system} workload={file} concurrent={concurrent} oltp_threads={oltp_threads} \
olap_threads={olap_threads_effective} gc={gc} ({} cores)", num_cpus::get_physical());

    let (oltp, oltp_ns, mut scans, olap_ns) = if concurrent {
        run_concurrent(&index, ops, oltp_threads, olap_threads)
    } else {
        run_sequential(&index, ops, first_version, olap_threads, oltp_threads_or_scans)
    };

    let (alloc, reuse) = index.alloc_counts();
    // The live data set after the run; with a key-partitioned replay it is exactly what the trace implies.
    let final_records = index.scan_fresh();
    let records_per_scan = if scans.scans == 0 { 0.0 } else { scans.records as f64 / scans.scans as f64 };
    let (experiment, repeat) = label();
    let (p50, p99) = (scans.quantile_ns(0.5), scans.quantile_ns(0.99));
    append_row(LOAD_HEADER, &format!(
        "{experiment},{repeat},{system},{},{concurrent},{oltp_threads},{olap_threads_effective},{gc},{init_keys},\
{},{},{oltp_ns},{},{},{olap_ns},{:.0},{p50},{p99},{alloc},{reuse},{},{},{},{},{},{},{records_per_scan:.1},{final_records}",
        std::path::Path::new(file).file_stem().unwrap().to_string_lossy(),
        oltp.executed, oltp.failed, scans.scans, scans.records, scans.avg_ns(),
        oltp.inserts, oltp.updates, oltp.deletes, oltp.failed_inserts, oltp.failed_updates, oltp.failed_deletes));

    let seconds = oltp_ns as f64 / 1e9;
    println!("- OLTP: {} ops attempted ({} inserts, {} updates, {} deletes) in {:.3}s; {} successful = {:.0} ops/s",
             oltp.executed, oltp.inserts, oltp.updates, oltp.deletes, seconds, oltp.succeeded(),
             oltp.succeeded() as f64 / seconds);
    if oltp.failed > 0 {
        let warning = format!("WARNING: {} of {} OLTP operations failed (inserts {}, updates {}, deletes {}); \
the replay did not follow the workload's per-key order", oltp.failed, oltp.executed,
                              oltp.failed_inserts, oltp.failed_updates, oltp.failed_deletes);
        println!("- {warning}");
        eprintln!("{warning}");
    }
    println!("- OLAP: {} scans, avg {:.3} ms, p99 {:.3} ms, {records_per_scan:.1} records/scan; \
{final_records} records after the run (init {init_keys})", scans.scans, scans.avg_ns() / 1e6, p99 as f64 / 1e6);
    println!("- Nodes: {alloc} allocated, {reuse} reused");
    println!("###### End Command ######");
}

/// OLTP threads replay key partitions of the workload (see `partition_by_key`) while OLAP threads run full scans
/// of the freshest visible snapshot; the readers stop as soon as all writers are done.
pub(crate) fn run_concurrent(index: &Arc<dyn PaperIndex>, ops: Vec<FileOp>, oltp_threads: usize, olap_threads: usize)
                  -> (OltpCounts, u128, ScanStats, u128) {
    let work = partition_by_key(ops, oltp_threads);
    let (min, max) = work.iter().map(Vec::len).fold((usize::MAX, 0), |(lo, hi), n| (lo.min(n), hi.max(n)));
    println!("- {oltp_threads} OLTP threads replay key partitions of {min}..{max} operations");

    let done = Arc::new(AtomicBool::new(false));
    let start = Instant::now();

    let readers: Vec<_> = (0..olap_threads).map(|_| {
        let (index, done) = (index.clone(), done.clone());
        thread::spawn(move || {
            let mut stats = ScanStats::default();
            while !done.load(Acquire) {
                let t = Instant::now();
                let records = index.scan_fresh();
                stats.record(records, t.elapsed().as_nanos() as u64);
            }
            stats
        })
    }).collect();

    let writers: Vec<_> = work.into_iter().map(|work| {
        let index = index.clone();
        thread::spawn(move || apply_all(index.as_ref(), &work))
    }).collect();

    let oltp = writers.into_iter().map(|w| w.join().unwrap()).fold(OltpCounts::default(), OltpCounts::merge);
    let oltp_ns = start.elapsed().as_nanos();

    done.store(true, Release);
    let scans = readers.into_iter().map(|r| r.join().unwrap()).fold(ScanStats::default(), ScanStats::merge);
    (oltp, oltp_ns, scans, start.elapsed().as_nanos())
}

/// The workload is applied by one thread; afterwards `scans_per_thread` scans per OLAP thread are run, each on
/// a version drawn uniformly from all versions of the tree, those of the initial insertions included (Section 8:
/// "uniformly distributed across all versions"); `first` is the version of the first initial insertion. The drawn
/// versions differ per repetition (`REPEAT`).
fn run_sequential(index: &Arc<dyn PaperIndex>, ops: Vec<FileOp>, first: u64, olap_threads: usize, scans_per_thread: usize)
                  -> (OltpCounts, u128, ScanStats, u128) {
    let repeat: u64 = label().1.parse().unwrap_or(0);
    let start = Instant::now();
    // The commit version of the last successful operation is the newest version.
    let (mut oltp, mut last) = (OltpCounts::default(), 0);
    for op in &ops {
        let version = index.apply_versioned(*op);
        if let Some(v) = version {
            last = last.max(v);
        }
        oltp.add(*op, version.is_some());
    }
    index.finish_thread();
    let oltp_ns = start.elapsed().as_nanos();
    println!("- applied {} operations, starting {} scans on {olap_threads} threads", oltp.executed, scans_per_thread * olap_threads);

    assert!(first <= last, "the workload created no version");
    let start = Instant::now();
    let readers: Vec<_> = (0..olap_threads).map(|t| {
        let index = index.clone();
        thread::spawn(move || {
            let mut rng = crate::ycsb::Rng::new(0x5CA9 + t as u64 + repeat.wrapping_mul(1_000_003));
            let mut stats = ScanStats::default();
            for _ in 0..scans_per_thread {
                let version = first + rng.below(last - first + 1);
                let t = Instant::now();
                let records = index.scan_at(version);
                stats.record(records, t.elapsed().as_nanos() as u64);
            }
            stats
        })
    }).collect();
    let scans = readers.into_iter().map(|r| r.join().unwrap()).fold(ScanStats::default(), ScanStats::merge);
    (oltp, oltp_ns, scans, start.elapsed().as_nanos())
}

/// `retry-exp <threads> <insertions> <zipf-alpha>`: Figure 12. The threads insert keys drawn from a Zipf
/// distribution over the whole key domain (alpha 0 = uniform); the retries of the optimistic write
/// traversals are reported in the paper's five groups.
pub fn main_retry_exp(parms: Vec<String>) {
    use rand_distr::{Distribution, Zipf};
    use std::sync::atomic::AtomicU64;

    if parms.len() < 5 {
        return eprintln!("usage: retry-exp <threads> <insertions> <zipf-alpha>")
    }
    let threads: usize = parms[2].parse().unwrap();
    let insertions: u64 = parms[3].parse().unwrap();
    let alpha: f64 = parms[4].parse().unwrap();

    let index = make_system("cmvbt", "fg", false).unwrap();
    let counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let workers: Vec<_> = (0..threads).map(|_| {
        let (index, counter) = (index.clone(), counter.clone());
        thread::spawn(move || {
            let zipf = (alpha > 0.0).then(|| Zipf::new(Key::MAX as f64, alpha).unwrap());
            let mut rng = rand::rng();
            while counter.fetch_add(1, Relaxed) < insertions {
                let key = match &zipf {
                    Some(z) => z.sample(&mut rng) as Key,
                    None => rand::RngExt::random_range(&mut rng, 0..Key::MAX),
                };
                index.apply(FileOp::Insert(key));
            }
            index.finish_thread();
            retry_stats::flush_current_thread();
        })
    }).collect();
    workers.into_iter().for_each(|w| w.join().unwrap());
    let elapsed = start.elapsed();

    let groups = retry_stats::take();
    let total: u64 = groups.iter().sum();
    let (experiment, repeat) = label();
    append_row("experiment,repeat,alpha,threads,insertions,time_ns,g0,g1_5,g6_9,g10_19,g20p,total",
               &format!("{experiment},{repeat},{alpha},{threads},{insertions},{},{},{}", elapsed.as_nanos(),
                        groups.iter().map(u64::to_string).collect::<Vec<_>>().join(","), total));
    println!("alpha={alpha}: {} ops in {:.2}s; retries {}", total, elapsed.as_secs_f64(),
             retry_stats::GROUP_NAMES.iter().zip(groups).map(|(n, g)| format!("{n}: {:.4}%", 100.0 * g as f64 / total as f64))
                 .collect::<Vec<_>>().join(", "));
}
