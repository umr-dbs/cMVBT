# cMVBT

Research implementation of the concurrent Multiversion B-tree described in *Multiversion Concurrency Control for Multiversion B-Trees* (EDBT 2027). The cMVBT keeps committed pages immutable for latch-free point and range reads. Writers use optimistic latch coupling and copy-on-write page reorganizations. Optional on-demand garbage collection reuses pages that are no longer reachable by an active snapshot.

This repository also contains the B+-tree/version-list baselines and a libmdbx copy-on-write baseline, driving all six systems through the same benchmark interface:

| `--system` | Index |
| --- | --- |
| `cmvbt` | Concurrent MVBT |
| `chain` | OLC B+-tree with version chains |
| `frugal` | OLC B+-tree with frugal lists |
| `vweaver` | OLC B+-tree with vWeaver lists |
| `skiplist` | OLC B+-tree with skip-list version indexes |
| `mdbx` | libmdbx copy-on-write B+-tree |

The current crate version is **0.0.110**. The baseline implementations are enabled by the default `dexa` feature.

## Build

The project requires a recent Rust toolchain with Rust 2024 edition support. The benchmark is intended for Linux; Linux builds use jemalloc.

```bash
cargo build --release
```

For measurements, use the paper profile. It enables full optimization, fat LTO, one code-generation unit, stripped symbols, and abort-on-panic:

```bash
cargo build --profile paper
# binary: target/paper/cMVBT
```

Build only the cMVBT, without the version-list baselines:

```bash
cargo build --release --no-default-features
```

## Test

Run the correctness suite with optimizations enabled. Several tests deliberately create large trees and high-contention writer/reader workloads, so debug builds are unnecessarily slow.

```bash
cargo test --release
```

The suite covers:

- CRUD behavior, extreme keys, failed operations, payload integrity, and exact model comparisons;
- ascending, descending, random, hot-key, and Zipfian workloads;
- concurrent writers on disjoint and shared keys, plus scanners running during structural modifications;
- historical snapshot stability with and without garbage collection;
- lazy scans held open across concurrent delete/insert reorganizations, including exact key and payload checks;
- release of reader registrations when a partially consumed lazy iterator is dropped;
- all five systems, GC off/on, and both 8-byte and 1 KiB values;
- workload generation, paper trace replay, CSV output, and the complete online YCSB driver.

The process-global commit clock is shared by trees, so tree tests serialize their setup internally. `CT_THREADS` and `CT_SCANNERS` can override the writer and scanner counts in the concurrent model tests.

## Online YCSB benchmark

The `ycsb` command generates operations online in each worker. It does not pre-generate a trace or replay a file order. All systems receive the same workload implementation, value representation, timing, and correctness checks.

```bash
target/paper/cMVBT ycsb \
  --system cmvbt \
  --workload a \
  --records 10000000 \
  --threads 32 \
  --olap-threads 16 \
  --olap-range 1000 \
  --secs 20 \
  --warmup 2 \
  --theta 0.99 \
  --scramble true \
  --value-size 1024 \
  --csv ycsb.csv
```

Use `target/paper/cMVBT ycsb --help` for the complete option list.

### Workloads

| Name | Mix | Key choice |
| --- | --- | --- |
| `a` | 50% read, 50% update | Zipfian by default |
| `b` | 95% read, 5% update | Zipfian by default |
| `c` | 100% read | Zipfian by default |
| `d` | 95% read, 5% fresh insert | Latest records |
| `e` | 95% scan, 5% fresh insert | Zipfian by default |
| `f` | 50% read, 50% read-modify-write | Zipfian by default |
| `update-heavy` | 10% read, 90% update | Zipfian by default |
| `churn` | 25% each read/update/insert/delete | Fresh inserts and FIFO expiry |
| `custom` | supplied by `--mix` | supplied by `--dist` |

A custom mix uses `read:update:insert:delete:scan:rmw` percentages:

```bash
target/paper/cMVBT ycsb \
  --workload custom \
  --mix 40:30:10:10:10:0 \
  --dist hotspot \
  --hot-frac 0.01 \
  --hot-prob 0.90
```

Available distributions are `uniform`, `zipf`, `latest`, and `hotspot`. `--theta 0` selects uniform access; positive Zipf alphas, including values greater than one, are supported. With `--scramble true`, hot ranks are spread across the key space instead of remaining in adjacent leaves.

Inserts always allocate fresh keys. Distribution-based deletes can fail after selecting an already deleted key; `churn` instead expires the oldest key to maintain a sliding window and continuously exercise splits, merges, and dead data.

### Values and verification

`--value-size 8` stores the value inline. `--value-size 1024` uses a YCSB-style 1 KiB record behind a `triomphe::Arc`; tree copies therefore require one atomic reference-count increment rather than copying 1 KiB.

Reads and scans validate their returned records. The CSV and console output include `violations`; the run returns an error if any corrupt or out-of-range record is observed. Operation success rates are reported separately because a valid workload may attempt updates, deletes, or reads on absent keys.

### GC and root indexes

`--gc true` enables the cMVBT graveyard/reuse mechanism. For the B+-tree baselines it enables removal of deleted records during page reorganization; retired version lists remain protected from optimistic readers until tree destruction.

The cMVBT root-version index can be selected with `--root-index fg|ll|sk|bt`:

- `fg`: frugal list (default)
- `ll`: linked list
- `sk`: skip list
- `bt`: B+-tree

## YCSB sweep and plots

The sweep script builds the paper-profile binary when necessary and runs workloads A-F plus churn across all systems, configured Zipf alphas, and both value sizes.

Every benchmark process is launched with `numactl --cpunodebind=0 --membind=0`. The script stops before running if `numactl` or NUMA node 0 is unavailable. The default paper binary is rebuilt on every invocation so measurements cannot accidentally use stale code.

```bash
scripts/run_ycsb.sh
scripts/plot_ycsb_results.py results/ycsb-<timestamp>
```

Use a small smoke run before a long experiment:

```bash
QUICK=1 scripts/run_ycsb.sh
```

Important environment overrides are documented at the top of `scripts/run_ycsb.sh`. Common ones include `SYSTEMS`, `WORKLOADS`, `THETAS`, `VALUE_SIZES`, `RECORDS`, `THREADS`, `OLAP_THREADS`, `OLAP_RANGE`, `SECS`, `WARMUP`, `GC`, `REPEATS`, `OUT`, and `BIN`.

The plotting script writes PDF and PNG figures for OLTP throughput, p99 latency, OLAP scan throughput, and the effect of record size.

## Reproduce the paper experiments

The paper runner uses the online `paper-ycsb` driver: operations are generated when workers request them, not replayed from a workload file. It loads 2,000,000 records by default, then runs every applicable experiment with uniform access and each scrambled Zipfian skew `0.1`, `0.4`, `0.8`, `0.99`, and `1.4`. OLAP operations select a 1,000-record range rather than scanning all 2,000,000 records; Figure 5 performs 10,000 such historical scans in total. The concurrent Figures 6-8 instead keep their scan workers active until the measured writers finish, matching their throughput/scalability protocol. Every 1,000-operation block has the exact requested update percentage, with the remainder divided equally between fresh inserts and deletes. Deletes expire live keys and transient update/delete races are retried internally, so the reported paper operations all take effect.

```bash
scripts/run_paper_experiments.sh all
scripts/plot_paper_results.py results/paper-<timestamp>
```

Run one experiment group with:

```bash
scripts/run_paper_experiments.sh latency
scripts/run_paper_experiments.sh concurrent
scripts/run_paper_experiments.sh concurrent-mdbx
scripts/run_paper_experiments.sh gc
scripts/run_paper_experiments.sh scalability
scripts/run_paper_experiments.sh retries
scripts/run_paper_experiments.sh allocations
```

For a short setup check:

```bash
QUICK=1 scripts/run_paper_experiments.sh all
```

The groups correspond to the submitted paper:

| Group | Paper protocol |
| --- | --- |
| `latency` | Figure 5: 10K random initial inserts, 10M random-key online writes, and 1K whole-version scans sampled uniformly from measured historical versions |
| `concurrent` | Figure 6: 1M writes, 1 writer and 16 concurrent 100K-record range readers, without GC |
| `concurrent-mdbx` | Figure 6 rerun for libmdbx only, using the same generation and load protocol as `concurrent` |
| `gc` | Figure 7: the same concurrent experiment with GC |
| `scalability` | Figure 8: separate OLAP-thread and OLTP-thread sweeps at a 60% update rate |
| `retries` | Figure 9: retry groups for 1M inserts under the paper's Zipf alphas |
| `allocations` | Figure 10: nodes allocated and reused by cMVBT with GC |

The integrated binary includes libmdbx for the CoW curves in Figures 5, 6, and 8. Figure 5 opens a read-only transaction immediately after the initial load and keeps that initial snapshot pinned while writes run; all repeated libmdbx range scans use that transaction. Figures 6 and 8 open fresh read transactions, matching their fresh-snapshot protocol. Figure 7 does not include libmdbx, matching the paper.

The unified `paper.csv` records the key distribution and Zipfian theta, throughput and node counters, plus count, average, p50, p95, p99, p99.9, and maximum latency for updates, inserts, deletes, and scans. The plotting script uses the uniform online-YCSB rows to create only Figures 5-8 and 10 with the same colors, markers, legends, and panel layouts as `EDBT_2027-1.pdf`; it does not create the former extra or record-size plots. `retries.csv` supplies the five distributions shown in Figure 9.

To profile the concurrent phase of Figure 6 with Linux hardware counters, run
`scripts/run_fig6_perf.sh`. The script excludes initial loading from the counters,
runs separate core, cache, and memory/TLB event groups to limit multiplexing, and
writes both raw `perf stat` files and a combined long-form `perf-events.csv`.
`PROTOCOL=pdf` (the default) uses 10K-record full scans as described in the
submitted PDF; `PROTOCOL=current` uses the current 2M-record/100K-range protocol.
Use `QUICK=1` for a short setup check before collecting full measurements. Runs
are stored below `scripts/perf/` and include normalized `analysis-runs.csv`,
aggregated `analysis-summary.csv`, and a concise `analysis.md` assessment.

For the targeted cache-miss test, use `scripts/run_fig6_reader_perf.sh` instead.
It opens counters separately in each scan thread before the start barrier, then
enables them only for that thread's scan loop. Loading, writers, and coordination
code are therefore excluded. Results go to `scripts/perf/fig6-reader-perf-<timestamp>/`
and include `reader-analysis.md` plus normalized run and summary CSV files. This
is the appropriate runner for testing whether MDBX writer activity causes cache
misses that are observable specifically in readers.

All experiment commands, including the general YCSB sweep, run through:

```bash
numactl --cpunodebind=0 --membind=0 target/paper/cMVBT ...
```

Important environment overrides are documented at the top of `scripts/run_paper_experiments.sh`. The script validates that each successful process appended a CSV row, records failures in `failures.txt`, and exits nonzero if any run fails or times out.

Figure 6 uses `FIG6_WRITERS=1` by default. It still uses the same `paper-ycsb`
online operation generator, initial-load phase, distributions, update rates, and
reader protocol as before. `WRITERS=32` remains the default for the other paper
experiments; set `FIG6_WRITERS` explicitly to run a comparison at another writer
count.

The former `generate` and `load` trace-replay commands remain available for compatibility and tests, but are no longer used by the reproduction script.

## Repository layout

| Path | Purpose |
| --- | --- |
| `src/mv_tree/` | cMVBT and structural modification operations |
| `src/mv_page_model/` | leaf/internal page layouts and version matching |
| `src/mv_query/` | latch-free point, eager range, and lazy range queries |
| `src/mv_sync/` | optimistic latches, commit clock, and synchronization |
| `src/mv_gc/` | active-reader tracking, graveyard, and GC safety tests |
| `src/mv_root/` | root* implementations |
| `src/dexa/` | integrated B+-tree/version-list baselines |
| `src/ycsb/` | online workload generator, shared system interface, statistics, and tests |
| `src/paper/` | paper experiment loader, shared baseline interface, and tests |
| `scripts/` | experiment and plotting scripts |

## Design notes

- Committed entries are immutable, allowing readers to traverse without acquiring page latches.
- Writers use optimistic latch coupling and restart when validation fails.
- Reorganizations are proactive so the write path does not need bottom-up latch coupling.
- Snapshot readers register with the GC tracker; lazy iterators release their registration both on exhaustion and on early drop.
- The commit clock uses a fixed array of 4096 slots. Thread IDs are recycled, avoiding reallocation while readers inspect slots lock-free.
- Baseline and cMVBT operations share one benchmark-facing interface, so comparisons run in one binary with the same generated operations.

## PiBench

The crate still exports the PiBench interface used by [pibench_ext](https://github.com/umr-dbs/pibench_ext). This path is independent of the paper and online YCSB drivers; see `src/lib.rs` for the exported operations.

## License and contact

See [LICENSE](LICENSE).

Amir Tonta<br>
Marburg University<br>
amir.tonta@mathematik.uni-marburg.de
