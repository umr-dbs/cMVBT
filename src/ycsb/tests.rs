//! Correctness tests of the systems and of the benchmark driver beyond the model tests in `systems.rs`.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

use super::systems::{make_system, ReadOutcome, SystemOptions, YcsbIndex};
use super::workload::{preset, Dist, KeyChooser, Mix, Rng};
use super::value::ValueKind;

use crate::mv_sync::TEST_SERIAL as SERIAL;

const ALL: [&str; 5] = ["cmvbt", "chain", "frugal", "vweaver", "skiplist"];

thread_local! {
    static KIND: std::cell::Cell<ValueKind> = const { std::cell::Cell::new(ValueKind::Inline8) };
}

fn system(name: &str, gc: bool) -> std::sync::Arc<dyn YcsbIndex> {
    make_system(name, SystemOptions::new(gc, KIND.get())).unwrap()
}

/// Preload on a short-lived writer, matching the production YCSB loader. The worker explicitly
/// releases its last completed-version slot before the join boundary.
fn preload(index: &std::sync::Arc<dyn YcsbIndex>, keys: u64) {
    std::thread::scope(|scope| {
        scope.spawn(|| {
            (0..keys).for_each(|key| assert!(index.insert(key)));
            index.finish_thread();
        });
    });
}

/// Runs `f` for every system, with GC off and on, with 8-byte inline values and with 1 KB records.
fn for_each_system(mut f: impl FnMut(&str, bool)) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for kind in [ValueKind::Inline8, ValueKind::Blob1K] {
        KIND.set(kind);
        for name in ALL {
            for gc in [false, true] {
                println!("-- {name} gc={gc} value={kind:?}");
                f(name, gc)
            }
        }
    }
    KIND.set(ValueKind::Inline8);
}

#[test]
fn empty_index_and_extreme_keys() {
    for_each_system(|name, gc| {
        let index = system(name, gc);
        let ctx = format!("{name} gc={gc}");

        assert_eq!(index.read(0), ReadOutcome::Miss, "{ctx}: read on empty");
        assert!(!index.update(7), "{ctx}: update on empty");
        assert!(!index.delete(7), "{ctx}: delete on empty");
        assert_eq!(index.scan(0, 1000).records, 0, "{ctx}: scan on empty");

        let max_key = u64::MAX - 1;
        let keys = [0u64, 1, 2, 1 << 32, max_key - 1, max_key];
        keys.iter().for_each(|k| assert!(index.insert(*k), "{ctx}: insert {k}"));
        keys.iter().for_each(|k| assert_eq!(index.read(*k), ReadOutcome::Hit, "{ctx}: read {k}"));
        assert_eq!(index.scan(0, 3).records, 3, "{ctx}: low scan");
        assert_eq!(index.scan(max_key - 1, 2).records, 2, "{ctx}: high scan");

        keys.iter().for_each(|k| assert!(index.delete(*k), "{ctx}: delete {k}"));
        keys.iter().for_each(|k| assert_eq!(index.read(*k), ReadOutcome::Miss, "{ctx}: read deleted {k}"));
        assert_eq!(index.scan(0, u64::MAX).records, 0, "{ctx}: everything deleted");
    });
}

#[test]
fn updates_change_the_payload_and_nothing_else() {
    for_each_system(|name, gc| {
        let index = system(name, gc);
        let ctx = format!("{name} gc={gc}");
        (0..5000).for_each(|k| assert!(index.insert(k)));
        for round in 1..=3u64 {
            (0..5000).step_by(3).for_each(|k| assert!(index.update_value(k, k * 10 + round), "{ctx}: update {k}"));
        }
        for k in 0..5000u64 {
            if k % 3 == 0 {
                assert_eq!(index.read_value(k), Some(k * 10 + 3), "{ctx}: payload of {k}");
            } else {
                // untouched: still the record the insert wrote (its tag is system specific for 1 KB records)
                assert_eq!(index.read(k), ReadOutcome::Hit, "{ctx}: untouched {k}");
            }
        }
        (0..5000).step_by(7).for_each(|k| assert!(index.delete(k)));
        assert!(!index.update_value(0, 1), "{ctx}: update of a deleted key");
        assert_eq!(index.read_value(0), None, "{ctx}: deleted key still readable");
        assert_eq!(index.read_value(3), Some(33), "{ctx}: neighbour changed");
    });
}

/// Every thread updates its own keys with increasing values; no update may get lost.
#[test]
fn no_lost_updates() {
    const THREADS: u64 = 8;
    const KEYS: u64 = 4000;
    for_each_system(|name, gc| {
        let index = system(name, gc);
        preload(&index, KEYS);
        std::thread::scope(|s| {
            for t in 0..THREADS {
                let index = &index;
                s.spawn(move || {
                    for round in 1..=40u64 {
                        for k in (t..KEYS).step_by(THREADS as usize) {
                            assert!(index.update_value(k, round * 1_000_000 + k), "{name} gc={gc}: update {k}");
                        }
                    }
                    index.finish_thread();
                });
            }
        });
        for k in 0..KEYS {
            assert_eq!(index.read_value(k), Some(40 * 1_000_000 + k), "{name} gc={gc}: last update of {k} lost");
        }
        assert_eq!(index.scan(0, KEYS).records, KEYS, "{name} gc={gc}: records changed by updates");
    });
}

/// Writers race on the very same keys. A key can only be deleted once, updates never bring it back, and the final
/// state must agree with the number of successful deletes.
#[test]
fn contended_updates_and_deletes_on_shared_keys() {
    const THREADS: usize = 8;
    const HOT: u64 = 96;
    for_each_system(|name, gc| {
        let index = system(name, gc);
        preload(&index, HOT);
        let deleted: Vec<AtomicU64> = (0..HOT).map(|_| AtomicU64::new(0)).collect();
        let updates_after_delete = AtomicU64::new(0);

        std::thread::scope(|s| {
            for t in 0..THREADS {
                let (index, deleted, updates_after_delete) = (&index, &deleted, &updates_after_delete);
                s.spawn(move || {
                    let mut rng = Rng::new(77 + t as u64);
                    for _ in 0..30_000 {
                        let k = rng.below(HOT);
                        if rng.below(100) < 3 {
                            if index.delete(k) {
                                deleted[k as usize].fetch_add(1, Relaxed);
                            }
                        } else if index.update(k) && deleted[k as usize].load(Relaxed) > 0 {
                            // may legitimately overlap with a delete that just happened, counted for the record
                            updates_after_delete.fetch_add(1, Relaxed);
                        }
                    }
                    index.finish_thread();
                });
            }
        });
        let mut live = 0;
        for k in 0..HOT {
            let deletes = deleted[k as usize].load(Relaxed);
            assert!(deletes <= 1, "{name} gc={gc}: key {k} was deleted {deletes} times");
            assert_eq!(index.read(k) == ReadOutcome::Hit, deletes == 0, "{name} gc={gc}: final state of key {k}");
            live += (deletes == 0) as u64;
        }
        let scan = index.scan(0, HOT);
        assert_eq!((scan.records, scan.violations), (live, 0), "{name} gc={gc}: final scan");
    });
}

/// Racing inserts of the same key: the cMVBT lets exactly one win. (The baselines turn an insert of a live key into a
/// new version, which is outside their contract and never issued by the benchmark.)
#[test]
fn racing_inserts_of_the_same_key_have_one_winner() {
    const KEYS: u64 = 3000;
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for (gc, kind) in [(false, ValueKind::Inline8), (true, ValueKind::Inline8), (false, ValueKind::Blob1K), (true, ValueKind::Blob1K)] {
        KIND.set(kind);
        let index = system("cmvbt", gc);
        let wins = AtomicU64::new(0);
        std::thread::scope(|s| {
            for _ in 0..8 {
                let (index, wins) = (&index, &wins);
                s.spawn(move || {
                    (0..KEYS).for_each(|k| { if index.insert(k) { wins.fetch_add(1, Relaxed); } });
                    index.finish_thread();
                });
            }
        });
        assert_eq!(wins.load(Relaxed), KEYS, "gc={gc}: every key must be inserted exactly once");
        assert_eq!(index.scan(0, KEYS).records, KEYS);
    }
}

#[test]
fn hot_spot_growth_keeps_working_for_ascending_descending_and_random_loads() {
    for_each_system(|name, gc| {
        for order in 0..3 {
            let index = system(name, gc);
            let n = 30_000u64;
            let keys: Vec<u64> = match order {
                0 => (0..n).collect(),
                1 => (0..n).rev().collect(),
                _ => {
                    let mut rng = Rng::new(5);
                    let mut v: Vec<u64> = (0..n).collect();
                    (1..v.len()).rev().for_each(|i| v.swap(i, rng.below(i as u64 + 1) as usize));
                    v
                }
            };
            keys.iter().for_each(|k| assert!(index.insert(*k), "{name} gc={gc} order {order}: insert {k}"));
            let missing: Vec<u64> = (0..n).filter(|k| index.read(*k) != ReadOutcome::Hit).take(5).collect();
            assert!(missing.is_empty(), "{name} gc={gc} order {order}: missing {missing:?}");
            assert_eq!(index.scan(0, n).records, n, "{name} gc={gc} order {order}");
        }
    });
}

#[test]
fn key_choosers_stay_inside_their_window() {
    let mut rng = Rng::new(9);
    for dist in [Dist::Uniform, Dist::Zipfian(0.99), Dist::Zipfian(0.5), Dist::Latest(0.99),
                 Dist::Hotspot { hot_frac: 0.01, hot_prob: 0.9 }] {
        for scramble in [false, true] {
            let chooser = KeyChooser::new(dist, 5000, scramble);
            for (lo, hi) in [(0, 4999), (100, 4999), (1000, 1000), (2500, 7499)] {
                for _ in 0..2000 {
                    let k = chooser.next(&mut rng, lo, hi);
                    assert!(lo <= k && k <= hi, "{dist:?} scramble={scramble}: {k} outside [{lo}, {hi}]");
                }
            }
        }
    }
}

#[test]
fn hotspot_and_latest_are_skewed_as_configured() {
    let mut rng = Rng::new(3);
    let hot = KeyChooser::new(Dist::Hotspot { hot_frac: 0.01, hot_prob: 0.9 }, 100_000, false);
    let in_hot = (0..100_000).filter(|_| hot.next(&mut rng, 0, 99_999) < 1000).count();
    assert!((88_000..92_000).contains(&in_hot), "hotspot share {in_hot}");

    let latest = KeyChooser::new(Dist::Latest(0.99), 100_000, false);
    let near_newest = (0..100_000).filter(|_| latest.next(&mut rng, 0, 99_999) >= 99_000).count();
    assert!(near_newest > 50_000, "latest share {near_newest}");

    let uniform = KeyChooser::new(Dist::Uniform, 1000, false);
    let mut counts = [0u32; 10];
    (0..100_000).for_each(|_| counts[(uniform.next(&mut rng, 0, 999) / 100) as usize] += 1);
    assert!(counts.iter().all(|c| (9_000..11_000).contains(c)), "uniform buckets {counts:?}");
}

#[test]
fn mix_parsing_rejects_bad_input() {
    assert!(Mix::parse("50:50:0:0:0:0").is_ok());
    assert!(Mix::parse("50:50:0:0:0").is_err(), "too few components");
    assert!(Mix::parse("50:40:0:0:0:0").is_err(), "does not sum to 100");
    assert!(Mix::parse("a:50:0:0:0:50").is_err(), "not a number");
    assert!(Mix::parse("-1:51:0:0:0:50").is_err(), "negative component");
    assert!(Mix::parse("NaN:0:0:0:0:0").is_err(), "non-finite component");
    assert!(Dist::parse("zipf", 0.99, 0.0, 0.0).is_ok());
    assert!(Dist::parse("zipf", f64::NAN, 0.01, 0.9).is_err());
    assert!(Dist::parse("hotspot", 0.99, 1.1, 0.9).is_err());
    assert!(Dist::parse("hotspot", 0.99, 0.01, -0.1).is_err());
    assert!(Dist::parse("nonsense", 0.99, 0.0, 0.0).is_err());
    for name in ["a", "b", "c", "d", "e", "f", "churn", "update-heavy"] {
        assert!(preset(name, 0.99).is_some(), "missing workload preset {name}");
    }
}

/// The whole driver (load, concurrent OLTP + OLAP, statistics, CSV) in one production-shaped run.
/// System adapters, workloads, value kinds and GC modes are covered independently above and in
/// `systems.rs`; a real CLI invocation creates exactly one tree in its process.
#[test]
fn driver_runs_end_to_end_without_violations() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let csv = std::env::temp_dir().join(format!("ycsb-test-{}.csv", std::process::id()));
    let _ = std::fs::remove_file(&csv);
    let args: Vec<String> = ["--system", "cmvbt", "--workload", "churn", "--records", "30000", "--threads", "4",
        "--olap-threads", "2", "--olap-range", "2000", "--secs", "0.3", "--warmup", "0.1", "--gc", "true",
        "--value-size", "1024", "--csv", csv.to_str().unwrap()].iter().map(|s| s.to_string()).collect();
    let cfg = super::parse_args(&args).unwrap();
    super::run(cfg).unwrap();
    let rows = std::fs::read_to_string(&csv).unwrap().lines().count();
    assert_eq!(rows, 2, "one CSV row plus the header");
    let _ = std::fs::remove_file(&csv);
}

#[test]
fn driver_rejects_bad_arguments() {
    let bad = |args: &[&str]| super::parse_args(&args.iter().map(|s| s.to_string()).collect::<Vec<_>>()).is_err();
    assert!(bad(&["--workload", "zzz"]));
    assert!(bad(&["--records", "1"]));
    assert!(bad(&["--threads", "0", "--olap-threads", "0"]));
    assert!(bad(&["--theta", "abc"]));
    assert!(bad(&["--dist", "weird"]));
    assert!(bad(&["--records"]));
    assert!(bad(&["--value-size", "16"]));
    assert!(bad(&["--load-threads", "0"]));
    assert!(bad(&["--secs", "0"]));
    assert!(bad(&["--secs", "NaN"]));
    assert!(bad(&["--warmup", "-1"]));
    assert!(bad(&["--unknown", "value"]));
    assert!(bad(&["records", "5"]));
}

#[test]
fn driver_defaults_to_1000_record_olap_scans() {
    let cfg = super::parse_args(&[]).unwrap();
    assert_eq!(cfg.olap_range, 1_000);
}
