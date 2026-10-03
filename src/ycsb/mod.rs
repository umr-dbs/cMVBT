//! YCSB-style benchmark driving the cMVBT and the version-list baselines (Version Chains,
//! Frugal Lists, vWeaver, skip lists) in one binary with one workload generator.
//!
//! ```text
//! cMVBT ycsb --system cmvbt --workload a --records 10000000 --threads 32 --secs 20
//! cMVBT ycsb --system vweaver --workload churn --theta 0.99 --olap-threads 16 --olap-range 10000
//! ```

mod stats;
mod systems;
mod value;
mod workload;
#[cfg(test)]
mod tests;

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU64, AtomicU8};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use stats::{Histogram, ThreadStats};
pub(crate) use systems::parse_root_index;
use value::ValueKind;
use systems::{make_system, ReadOutcome, SystemOptions, YcsbIndex, SYSTEMS};
pub(crate) use workload::{Dist, KeyChooser, Rng};
use workload::{preset, DeleteKeys, Mix, Op, OPS};

const USAGE: &str = "\
usage: ycsb [--key value]...
  --system <cmvbt|chain|frugal|vweaver|skiplist>   system under test            [cmvbt]
  --workload <a|b|c|d|e|f|churn|update-heavy|custom> operation mix preset       [a]
  --mix r:u:i:d:s:rmw      percentages (sum 100); overrides the preset's mix
  --records <n>            initially loaded keys (dense ids 0..n)                [1000000]
  --value-size <8|1024>    8 = the value stored inline; 1024 = YCSB's 1 KB record behind a   [1024]
                           pointer (triomphe::Arc: copies are an atomic increment)
  --threads <n>            OLTP threads                                          [8]
  --load-threads <n>       threads of the initial load (disjoint ascending key chunks)  [4]
  --secs <s> --warmup <s>  measured / warm-up duration                           [10 / 2]
  --dist <uniform|zipf|latest|hotspot>  key distribution (preset default)
  --theta <x>              zipfian skew (alpha), 0 = uniform, any x > 0, also > 1      [0.99]
  --hot-frac <x> --hot-prob <x>  hotspot parameters                              [0.01 / 0.9]
  --scramble <bool>        spread hot ranks over the key range                   [true]
  --scan-len <n>           max keys per YCSB scan, uniform in 1..=n              [100]
  --delete <oldest|dist>   key choice of deletes: FIFO expiry of the oldest key or the
                           key distribution (preset default). Inserts always use fresh keys.
  --olap-threads <n>       dedicated long-range scan threads                     [0]
  --olap-range <n>         keys per OLAP scan                                    [10000]
  --root-index <fg|ll|sk|bt>  cMVBT root* index                                  [fg]
  --gc <bool>              garbage collection (cMVBT only, see notes)            [false]
  --seed <n>               base seed                                             [42]
  --csv <file>             append one result row                                 [ycsb.csv]";

struct Config {
    system: String,
    workload: String,
    mix: Mix,
    dist: Dist,
    scramble: bool,
    deletes: DeleteKeys,
    records: u64,
    threads: usize,
    load_threads: usize,
    secs: f64,
    warmup: f64,
    scan_len: u64,
    olap_threads: usize,
    olap_range: u64,
    root_index: String,
    value: ValueKind,
    gc: bool,
    seed: u64,
    csv: String,
}

fn parse_args(parms: &[String]) -> Result<Config, String> {
    const FLAGS: [&str; 20] = [
        "system", "workload", "mix", "records", "value-size", "threads", "load-threads",
        "secs", "warmup", "dist", "theta", "hot-frac", "hot-prob", "scramble", "scan-len",
        "delete", "olap-threads", "olap-range", "root-index", "gc",
    ];
    const EXTRA_FLAGS: [&str; 2] = ["seed", "csv"];
    let mut kv = std::collections::HashMap::new();
    let mut it = parms.iter();
    while let Some(flag) = it.next() {
        let name = flag.strip_prefix("--").ok_or_else(|| format!("expected --flag, got '{flag}'"))?;
        if !FLAGS.contains(&name) && !EXTRA_FLAGS.contains(&name) {
            return Err(format!("unknown option '--{name}'"));
        }
        let value = it.next().ok_or_else(|| format!("--{name} needs a value"))?;
        kv.insert(name.to_string(), value.clone());
    }

    fn get<T: std::str::FromStr>(kv: &std::collections::HashMap<String, String>, k: &str, default: T) -> Result<T, String> {
        kv.get(k).map_or(Ok(default), |v| v.parse().map_err(|_| format!("bad value '{v}' for --{k}")))
    }

    let theta: f64 = get(&kv, "theta", 0.99)?;
    let workload = get(&kv, "workload", "a".to_string())?;
    let base = preset(&workload, theta).or_else(|| (workload == "custom").then(|| workload::Preset {
        mix: Mix([100., 0., 0., 0., 0., 0.]),
        dist: if theta <= 0.0 { Dist::Uniform } else { Dist::Zipfian(theta) },
        deletes: DeleteKeys::FromDistribution,
    })).ok_or_else(|| format!("unknown workload '{workload}'"))?;

    let mix = match kv.get("mix") { Some(m) => Mix::parse(m)?, None => base.mix };
    let dist = match kv.get("dist") {
        Some(d) => Dist::parse(d, theta, get(&kv, "hot-frac", 0.01)?, get(&kv, "hot-prob", 0.9)?)?,
        None => base.dist,
    };
    let deletes = match kv.get("delete").map(String::as_str) {
        Some("oldest") => DeleteKeys::Oldest,
        Some("dist") => DeleteKeys::FromDistribution,
        Some(o) => return Err(format!("bad --delete '{o}' (oldest|dist)")),
        None => base.deletes,
    };

    let cfg = Config {
        system: get(&kv, "system", "cmvbt".to_string())?,
        workload,
        mix,
        dist,
        scramble: get(&kv, "scramble", true)?,
        deletes,
        records: get(&kv, "records", 1_000_000)?,
        threads: get(&kv, "threads", 8)?,
        load_threads: get(&kv, "load-threads", 4)?,
        secs: get(&kv, "secs", 10.0)?,
        warmup: get(&kv, "warmup", 2.0)?,
        scan_len: get(&kv, "scan-len", 100)?,
        olap_threads: get(&kv, "olap-threads", 0)?,
        olap_range: get(&kv, "olap-range", 10_000)?,
        root_index: get(&kv, "root-index", "fg".to_string())?,
        value: ValueKind::parse(&get(&kv, "value-size", "1024".to_string())?)?,
        gc: get(&kv, "gc", false)?,
        seed: get(&kv, "seed", 42)?,
        csv: get(&kv, "csv", "ycsb.csv".to_string())?,
    };

    if cfg.records < 2 || cfg.scan_len == 0 || cfg.olap_range == 0 || cfg.load_threads == 0
        || cfg.threads + cfg.olap_threads == 0 {
        return Err("records >= 2, load-threads >= 1, scan-len >= 1, olap-range >= 1 and at least one worker thread are required".into());
    }
    if !cfg.secs.is_finite() || cfg.secs <= 0.0 || !cfg.warmup.is_finite() || cfg.warmup < 0.0 {
        return Err("secs must be finite and > 0; warmup must be finite and >= 0".into());
    }
    Ok(cfg)
}

pub fn main_ycsb(parms: Vec<String>) {
    if parms.iter().any(|p| p == "--help" || p == "-h") {
        return println!("{USAGE}");
    }

    match parse_args(&parms[2..]).and_then(|cfg| run(cfg)) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("error: {e}\n\n{USAGE}\n(systems: {SYSTEMS})");
            std::process::exit(2);
        }
    }
}

const WARMUP: u8 = 0;
const MEASURE: u8 = 1;
const STOP: u8 = 2;

fn load(index: &Arc<dyn YcsbIndex>, records: u64, threads: usize) -> Duration {
    let start = Instant::now();
    let threads = threads.max(1).min(32) as u64;
    let chunk = records.div_ceil(threads);
    thread::scope(|s| {
        for t in 0..threads {
            let index = index.clone();
            s.spawn(move || {
                for key in (t * chunk)..((t + 1) * chunk).min(records) {
                    assert!(index.insert(key), "initial load: insert({key}) failed");
                }
            });
        }
    });
    start.elapsed()
}

fn oltp_worker(
    index: &dyn YcsbIndex,
    cfg: &Config,
    chooser: &KeyChooser,
    next_fresh: &AtomicU64,
    next_expire: &AtomicU64,
    phase: &AtomicU8,
    seed: u64,
) -> ThreadStats {
    let mut rng = Rng::new(seed);
    let mut stats = ThreadStats::new();
    let scan_len = cfg.scan_len;
    let sliding = cfg.deletes == DeleteKeys::Oldest || matches!(cfg.dist, Dist::Latest(..));

    while phase.load(Relaxed) != STOP {
        let measuring = phase.load(Relaxed) == MEASURE;
        let op = cfg.mix.pick(&mut rng);
        let key = match op {
            Op::Insert => next_fresh.fetch_add(1, Relaxed),
            Op::Delete if cfg.deletes == DeleteKeys::Oldest => next_expire.fetch_add(1, Relaxed),
            _ if sliding => chooser.next(
                &mut rng,
                next_expire.load(Relaxed),
                next_fresh.load(Relaxed).saturating_sub(1)),
            _ => chooser.next(&mut rng, 0, cfg.records - 1),
        };

        let t = Instant::now();
        let ok = match op {
            Op::Read => match index.read(key) {
                ReadOutcome::Hit => true,
                ReadOutcome::Miss => false,
                ReadOutcome::Corrupt => { stats.violations += 1; false }
            },
            Op::Update => index.update(key),
            Op::Insert => index.insert(key),
            Op::Delete => index.delete(key),
            Op::Scan => {
                let len = 1 + rng.below(scan_len);
                let out = index.scan(key, len);
                stats.violations += out.violations;
                stats.scanned_records += out.records;
                out.records > 0
            }
            Op::ReadModifyWrite => index.read(key) == ReadOutcome::Hit && index.update(key),
        };
        let ns = t.elapsed().as_nanos() as u64;

        if measuring {
            stats.record(op, ok, ns);
        }
    }
    stats
}

fn olap_worker(index: &dyn YcsbIndex, cfg: &Config, next_fresh: &AtomicU64, next_expire: &AtomicU64, phase: &AtomicU8, seed: u64) -> (u64, u64, u64, Histogram) {
    let mut rng = Rng::new(seed);
    let (mut scans, mut records, mut violations) = (0, 0, 0);
    let mut latency = Histogram::new();
    let sliding = cfg.deletes == DeleteKeys::Oldest;

    while phase.load(Relaxed) != STOP {
        let measuring = phase.load(Relaxed) == MEASURE;
        // Scan inside the live window: the oldest keys are expired in the sliding-window workload.
        let (lo, hi) = if sliding {
            (next_expire.load(Relaxed), next_fresh.load(Relaxed))
        } else {
            (0, cfg.records)
        };
        let start = lo + rng.below(hi.saturating_sub(lo).saturating_sub(cfg.olap_range).max(1));
        let t = Instant::now();
        let out = index.scan(start, cfg.olap_range);
        let ns = t.elapsed().as_nanos() as u64;
        violations += out.violations;
        if measuring {
            scans += 1;
            records += out.records;
            latency.record(ns);
        }
    }
    (scans, records, violations, latency)
}

fn run(cfg: Config) -> Result<(), String> {
    let opts = SystemOptions { gc: cfg.gc, root_index: parse_root_index(&cfg.root_index)?, value: cfg.value };
    let index = make_system(&cfg.system, opts)?;

    println!("# system={} workload={} mix={:?} dist={:?} scramble={} deletes={:?}",
             cfg.system, cfg.workload, cfg.mix.0, cfg.dist, cfg.scramble, cfg.deletes);
    println!("# records={} value={} bytes oltp_threads={} olap_threads={} olap_range={} warmup={}s measure={}s gc={}",
             cfg.records, cfg.value.bytes(), cfg.threads, cfg.olap_threads, cfg.olap_range, cfg.warmup, cfg.secs, cfg.gc);

    let load_time = load(&index, cfg.records, cfg.load_threads);
    println!("# loaded {} records in {:.2}s", cfg.records, load_time.as_secs_f64());

    let chooser = KeyChooser::new(cfg.dist, cfg.records, cfg.scramble);
    let next_fresh = AtomicU64::new(cfg.records);
    let next_expire = AtomicU64::new(0);
    let phase = AtomicU8::new(WARMUP);

    let (oltp, olap, measured) = thread::scope(|s| {
        let oltp = (0..cfg.threads).map(|t| {
            let (index, cfg, phase, next_fresh, next_expire) = (&index, &cfg, &phase, &next_fresh, &next_expire);
            let chooser = chooser.share();
            s.spawn(move || oltp_worker(index.as_ref(), cfg, &chooser, next_fresh, next_expire, phase,
                                        cfg.seed.wrapping_add(t as u64 * 7919 + 1)))
        }).collect::<Vec<_>>();

        let olap = (0..cfg.olap_threads).map(|t| {
            let (index, cfg, phase, next_fresh, next_expire) = (&index, &cfg, &phase, &next_fresh, &next_expire);
            s.spawn(move || olap_worker(index.as_ref(), cfg, next_fresh, next_expire, phase,
                                        cfg.seed.wrapping_add(1_000_003 + t as u64 * 104729)))
        }).collect::<Vec<_>>();

        thread::sleep(Duration::from_secs_f64(cfg.warmup));
        phase.store(MEASURE, Release);
        let t0 = Instant::now();
        thread::sleep(Duration::from_secs_f64(cfg.secs));
        phase.store(STOP, Release);
        let measured = t0.elapsed();

        let oltp = oltp.into_iter().map(|h| h.join().unwrap()).collect::<Vec<_>>();
        let olap = olap.into_iter().map(|h| h.join().unwrap()).collect::<Vec<_>>();
        (oltp, olap, measured)
    });
    let _ = phase.load(Acquire);

    let mut total = ThreadStats::new();
    oltp.iter().for_each(|t| total.merge(t));

    let secs = measured.as_secs_f64();
    let oltp_ops = total.ops.iter().map(|o| o.attempted).sum::<u64>();
    let (olap_scans, olap_records, olap_violations) = olap.iter()
        .fold((0, 0, 0), |a, o| (a.0 + o.0, a.1 + o.1, a.2 + o.2));
    let mut olap_lat = Histogram::new();
    olap.iter().for_each(|o| olap_lat.merge(&o.3));
    let violations = total.violations + olap_violations;

    println!("\n{:<8} {:>12} {:>10} {:>12} {:>12} {:>12}", "op", "attempted", "ok %", "p50 us", "p99 us", "p99.9 us");
    for op in OPS {
        let s = &total.ops[op.index()];
        if s.attempted > 0 {
            println!("{:<8} {:>12} {:>9.1}% {:>12.1} {:>12.1} {:>12.1}", op.name(), s.attempted,
                     100.0 * s.succeeded as f64 / s.attempted as f64,
                     s.latency.quantile(0.5) as f64 / 1e3,
                     s.latency.quantile(0.99) as f64 / 1e3,
                     s.latency.quantile(0.999) as f64 / 1e3);
        }
    }
    println!("\nOLTP throughput: {:.0} ops/s ({} ops, {} scanned records)", oltp_ops as f64 / secs, oltp_ops, total.scanned_records);
    if cfg.olap_threads > 0 {
        println!("OLAP throughput: {:.1} scans/s, {:.0} records/s, p50 {:.2} ms, p99 {:.2} ms",
                 olap_scans as f64 / secs, olap_records as f64 / secs,
                 olap_lat.quantile(0.5) as f64 / 1e6, olap_lat.quantile(0.99) as f64 / 1e6);
    }
    println!("Snapshot violations: {violations}");

    write_csv(&cfg, secs, oltp_ops, &total, olap_scans, olap_records, &olap_lat, violations)
        .map_err(|e| format!("writing {}: {e}", cfg.csv))?;

    if violations > 0 {
        return Err(format!("{violations} reads/scans returned corrupted data"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_csv(cfg: &Config, secs: f64, oltp_ops: u64, total: &ThreadStats, olap_scans: u64,
             olap_records: u64, olap_lat: &Histogram, violations: u64) -> std::io::Result<()> {
    let existed = std::path::Path::new(&cfg.csv).exists();
    let mut f = OpenOptions::new().create(true).append(true).open(&cfg.csv)?;
    if !existed {
        writeln!(f, "system,workload,mix,dist,scramble,records,oltp_threads,olap_threads,olap_range,gc,value_bytes,theta,secs,\
oltp_ops,oltp_ops_per_s,read_p50_us,read_p99_us,update_p50_us,update_p99_us,insert_p50_us,insert_p99_us,\
delete_p50_us,delete_p99_us,scan_p50_us,scan_p99_us,olap_scans,olap_scans_per_s,olap_records_per_s,\
olap_p50_ms,olap_p99_ms,violations")?;
    }
    let q = |op: Op, q: f64| format!("{:.1}", total.ops[op.index()].latency.quantile(q) as f64 / 1e3);
    let mut row = vec![
        cfg.system.clone(),
        cfg.workload.clone(),
        cfg.mix.0.iter().map(|m| m.to_string()).collect::<Vec<_>>().join(":"),
        format!("{:?}", cfg.dist).replace(',', ";"),
        cfg.scramble.to_string(),
        cfg.records.to_string(),
        cfg.threads.to_string(),
        cfg.olap_threads.to_string(),
        cfg.olap_range.to_string(),
        cfg.gc.to_string(),
        cfg.value.bytes().to_string(),
        match cfg.dist { Dist::Zipfian(t) | Dist::Latest(t) => t.to_string(), _ => "0".to_string() },
        format!("{secs:.2}"),
        oltp_ops.to_string(),
        format!("{:.0}", oltp_ops as f64 / secs),
    ];
    for op in [Op::Read, Op::Update, Op::Insert, Op::Delete, Op::Scan] {
        row.push(q(op, 0.5));
        row.push(q(op, 0.99));
    }
    row.extend([
        olap_scans.to_string(),
        format!("{:.1}", olap_scans as f64 / secs),
        format!("{:.0}", olap_records as f64 / secs),
        format!("{:.2}", olap_lat.quantile(0.5) as f64 / 1e6),
        format!("{:.2}", olap_lat.quantile(0.99) as f64 / 1e6),
        violations.to_string(),
    ]);
    writeln!(f, "{}", row.join(","))
}
