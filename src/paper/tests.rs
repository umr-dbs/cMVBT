//! Tests of the experiment drivers: workload generation + replay, snapshot stability, CSV output, retry statistics.

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use super::systems::{make_system, PaperIndex};
use super::{apply_all, load_workload, run_concurrent, FileOp};
use crate::mv_utils::retry_stats;

use crate::mv_sync::TEST_SERIAL as SERIAL;

const ALL: [&str; 5] = ["cmvbt", "chain", "frugal", "vweaver", "skiplist"];

fn temp(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("cmvbt-paper-test-{}-{name}", std::process::id()))
}

/// `generate <file> <init> <blocks> <ins> <upd> <del> <skew>` and returns the workload.
fn generated_workload(name: &str, init: usize, blocks: usize, ins: usize, upd: usize, del: usize, skew: &str) -> (std::path::PathBuf, Vec<FileOp>) {
    let path = temp(name);
    let _ = std::fs::remove_file(&path);
    let parms: Vec<String> = ["cMVBT", "generate", path.to_str().unwrap(), &init.to_string(), &blocks.to_string(),
        &ins.to_string(), &upd.to_string(), &del.to_string(), skew].iter().map(|s| s.to_string()).collect();
    crate::mv_test::main_generate(parms);
    let ops = load_workload(path.to_str().unwrap());
    (path, ops)
}

fn live_after(ops: &[FileOp]) -> HashSet<u64> {
    let mut live = HashSet::new();
    for op in ops {
        match op {
            FileOp::Insert(k) => assert!(live.insert(*k), "generator inserted {k} twice"),
            FileOp::Update(k) => assert!(live.contains(k), "generator updated absent key {k}"),
            FileOp::Delete(k) => assert!(live.remove(k), "generator deleted absent key {k}"),
        }
    }
    live
}

#[test]
fn generated_workload_is_serially_valid_and_has_the_requested_shape() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (path, ops) = generated_workload("shape.dat", 500, 40, 20, 50, 30, "0");
    assert_eq!(ops.len(), 500 + 40 * 100);
    assert!(ops[..500].iter().all(|o| matches!(o, FileOp::Insert(_))), "the first operations are the initial insertions");
    let count = |f: fn(&FileOp) -> bool| ops[500..].iter().filter(|o| f(o)).count();
    assert_eq!(count(|o| matches!(o, FileOp::Insert(_))), 40 * 20);
    assert_eq!(count(|o| matches!(o, FileOp::Update(_))), 40 * 50);
    assert_eq!(count(|o| matches!(o, FileOp::Delete(_))), 40 * 30);
    assert_eq!(live_after(&ops).len(), 500 + 40 * 20 - 40 * 30);
    let _ = std::fs::remove_file(path);
}

/// Replayed serially, the generated file must succeed completely on every system, with and without GC, and leave
/// exactly the keys the file implies.
#[test]
fn serial_replay_succeeds_everywhere_and_leaves_the_expected_data() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (path, ops) = generated_workload("replay.dat", 1000, 60, 20, 50, 30, "0");
    let expected = live_after(&ops).len();
    for name in ALL {
        for gc in [false, true] {
            let index = make_system(name, "fg", gc).unwrap();
            let counts = apply_all(index.as_ref(), &ops);
            assert_eq!((counts.executed, counts.failed), (ops.len() as u64, 0), "{name} gc={gc}: replay");
            assert_eq!(index.scan_at(index.newest_version()), expected, "{name} gc={gc}: final data set");
            assert_eq!(index.scan_fresh(), expected, "{name} gc={gc}: freshest snapshot");
        }
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn old_versions_stay_scannable_without_gc() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (path, ops) = generated_workload("versions.dat", 300, 30, 30, 50, 20, "0");
    for name in ALL {
        let index = make_system(name, "fg", false).unwrap();
        // the data set size after every operation, i.e., what a scan at that operation's version must return
        let mut sizes = vec![];
        let mut live = 0usize;
        let mut versions = vec![];
        for op in &ops {
            versions.push(index.apply_versioned(*op).expect("serial replay succeeds"));
            live = match op { FileOp::Insert(_) => live + 1, FileOp::Delete(_) => live - 1, _ => live };
            sizes.push(live);
        }
        assert!(versions.windows(2).all(|w| w[0] < w[1]), "{name}: versions increase with every operation");
        for i in (0..ops.len()).step_by(37) {
            assert_eq!(index.scan_at(versions[i]), sizes[i], "{name}: scan at the version after operation {i}");
        }
    }
    let _ = std::fs::remove_file(path);
}

/// A scan of an old version must not notice concurrent writers.
#[test]
fn historical_snapshots_are_stable_under_concurrent_writes() {
    const N: u64 = 12_000;
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for name in ALL {
        let index: Arc<dyn PaperIndex> = make_system(name, "fg", false).unwrap();
        let version = (0..N).map(|k| index.apply_versioned(FileOp::Insert(k)).unwrap()).last().unwrap();
        assert_eq!(index.scan_at(version), N as usize, "{name}: baseline snapshot");

        let stop = AtomicBool::new(false);
        std::thread::scope(|s| {
            for t in 0..4u64 {
                let index = &index;
                let stop = &stop;
                s.spawn(move || {
                    let mut i = 0u64;
                    while !stop.load(Relaxed) {
                        let k = (i * 7 + t * 1013) % N;
                        index.apply(FileOp::Update(k));
                        if i % 5 == 0 { index.apply(FileOp::Delete(k)); }
                        index.apply(FileOp::Insert(N + t * 1_000_000 + i));
                        i += 1;
                    }
                });
            }
            let checker = s.spawn(|| {
                let started = std::time::Instant::now();
                while started.elapsed().as_millis() < 500 {
                    assert_eq!(index.scan_at(version), N as usize, "{name}: a historical snapshot changed while writers ran");
                }
            });
            checker.join().unwrap();
            stop.store(true, Relaxed);
        });
        assert_eq!(index.scan_at(version), N as usize, "{name}: snapshot after the writers");
    }
}

#[test]
fn concurrent_replay_executes_everything_and_keeps_the_data_consistent() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (path, ops) = generated_workload("conc.dat", 1000, 80, 20, 60, 20, "0");
    let total = ops.len() - 1000;
    for name in ALL {
        for gc in [false, true] {
            let index = make_system(name, "fg", gc).unwrap();
            let init: Vec<FileOp> = ops[..1000].to_vec();
            assert_eq!(apply_all(index.as_ref(), &init).failed, 0);
            let (counts, oltp_ns, scans, olap_ns) = run_concurrent(&index, ops[1000..].to_vec(), 8, 2);
            assert_eq!(counts.executed as usize, total, "{name} gc={gc}: executed");
            assert!(counts.failed as usize <= total, "{name} gc={gc}: failed");
            assert!(oltp_ns > 0 && olap_ns >= oltp_ns, "{name} gc={gc}: timings");
            // every insertion has a fresh key and thus succeeds: at least that many records can exist, at most all of them
            let live = index.scan_fresh();
            assert!(live <= 1000 + total, "{name} gc={gc}: more records than ever inserted");
            assert_eq!(live, index.scan_at(index.newest_version()), "{name} gc={gc}: fresh snapshot vs. newest version");
            let _ = scans;
        }
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn skewed_generation_runs_and_replays() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (path, ops) = generated_workload("skew.dat", 200, 20, 10, 10, 10, "0.9");
    assert!(ops.len() > 200);
    let index = make_system("cmvbt", "fg", false).unwrap();
    // skewed blocks repeat one key within a block, so duplicates fail; the replay must still not break anything
    let counts = apply_all(index.as_ref(), &ops);
    assert_eq!(counts.executed as usize, ops.len());
    assert_eq!(index.scan_fresh(), index.scan_at(index.newest_version()));
    let _ = std::fs::remove_file(path);
}

#[test]
fn load_command_appends_a_csv_row_per_run() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (path, ops) = generated_workload("load.dat", 500, 30, 20, 50, 30, "0");
    let csv = temp("load.csv");
    let _ = std::fs::remove_file(&csv);
    unsafe {
        std::env::set_var("RESULTS_CSV", &csv);
        std::env::set_var("EXPERIMENT", "test");
        std::env::set_var("REPEAT", "3");
    }

    let load = |concurrent: &str, olaps: &str, threads: &str, gc: &str, system: &str| {
        let parms: Vec<String> = ["cMVBT", "load", path.to_str().unwrap(), concurrent, olaps, threads, "0", "max", "fg", gc, "false", "500", system]
            .iter().map(|s| s.to_string()).collect();
        super::main_load(parms);
    };
    for system in ALL {
        load("false", "1", "50", "false", system); // sequential: 50 scans at random versions
        load("true", "2", "4", "true", system);    // concurrent with GC
    }
    unsafe {
        std::env::remove_var("RESULTS_CSV");
        std::env::remove_var("EXPERIMENT");
        std::env::remove_var("REPEAT");
    }

    let text = std::fs::read_to_string(&csv).unwrap();
    let mut lines = text.lines();
    let header: Vec<&str> = lines.next().unwrap().split(',').collect();
    let col = |name: &str| header.iter().position(|h| *h == name).unwrap_or_else(|| panic!("column {name}"));
    let rows: Vec<Vec<&str>> = lines.map(|l| l.split(',').collect()).collect();
    assert_eq!(rows.len(), 10);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.len(), header.len(), "row {i} has all columns");
        assert_eq!(row[col("experiment")], "test");
        assert_eq!(row[col("repeat")], "3");
        assert_eq!(row[col("system")], ALL[i / 2]);
        assert_eq!(row[col("oltp_ops")].parse::<usize>().unwrap(), ops.len() - 500);
        assert!(row[col("oltp_time_ns")].parse::<u64>().unwrap() > 0);
        if i % 2 == 0 { // sequential run
            assert_eq!(row[col("concurrent")], "false");
            assert_eq!(row[col("oltp_failed")], "0", "a serial replay never fails");
            assert_eq!(row[col("scans")], "50");
        } else {
            assert_eq!(row[col("concurrent")], "true");
            assert_eq!(row[col("gc")], "true");
        }
    }
    let _ = std::fs::remove_file(&csv);
    let _ = std::fs::remove_file(path);
}

#[test]
fn retry_experiment_accounts_for_every_insertion() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let csv = temp("retries.csv");
    let _ = std::fs::remove_file(&csv);
    unsafe { std::env::set_var("RESULTS_CSV", &csv); }
    retry_stats::take(); // forget what earlier tests recorded
    for alpha in ["0", "0.8", "1.4"] {
        super::main_retry_exp(["cMVBT", "retry-exp", "4", "20000", alpha].iter().map(|s| s.to_string()).collect());
    }
    unsafe { std::env::remove_var("RESULTS_CSV"); }

    let text = std::fs::read_to_string(&csv).unwrap();
    let rows: Vec<Vec<&str>> = text.lines().skip(1).map(|l| l.split(',').collect()).collect();
    assert_eq!(rows.len(), 3);
    for row in rows {
        let groups: Vec<u64> = row[6..11].iter().map(|g| g.parse().unwrap()).collect();
        let total: u64 = row[11].parse().unwrap();
        assert_eq!(groups.iter().sum::<u64>(), total, "groups add up to the total");
        assert_eq!(total, 20_000, "one traversal per insertion");
    }
    let _ = std::fs::remove_file(&csv);
}

#[test]
fn node_counters_distinguish_fresh_allocations_from_reuse() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // a sliding window: constant live set, continuous splits and merges
    let churn = |index: &dyn PaperIndex| {
        (0..2000u64).for_each(|k| { index.apply(FileOp::Insert(k)); });
        index.reset_alloc_counts();
        for k in 2000..60_000u64 {
            index.apply(FileOp::Insert(k));
            index.apply(FileOp::Delete(k - 2000));
        }
    };

    let off = make_system("cmvbt", "fg", false).unwrap();
    churn(off.as_ref());
    let (alloc, reuse) = off.alloc_counts();
    assert!(alloc > 100 && reuse == 0, "without GC every node is allocated: {alloc} allocated, {reuse} reused");

    let on = make_system("cmvbt", "fg", true).unwrap();
    churn(on.as_ref());
    let (alloc, reuse) = on.alloc_counts();
    assert!(reuse > 20 * alloc, "with GC nearly all nodes come from the graveyard: {alloc} allocated, {reuse} reused");
    assert_eq!(on.scan_fresh(), 2000);
}
