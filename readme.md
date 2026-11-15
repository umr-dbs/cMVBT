<p align="center">
  <img src="assets/cmvbt-logo.png" alt="cMVBT logo" width="200">
</p>

# cMVBT

Research implementation for **“Multiversion Concurrency Control for Multiversion B-Trees” (EDBT 2027)**.

The concurrent Multiversion B-tree (cMVBT) keeps committed pages immutable, enabling latch-free point and range reads. Writers use optimistic latch coupling and copy-on-write page reorganizations. Optional on-demand garbage collection reuses pages that are no longer reachable from an active snapshot.

## Included systems

All systems use the same benchmark interface and workload files.

| System | `--system` value |
| --- | --- |
| Concurrent MVBT | `cmvbt` |
| B+-tree with version chains | `chain` |
| B+-tree with frugal lists | `frugal` |
| B+-tree with vWeaver lists | `vweaver` |
| B+-tree with skip-list version indexes | `skiplist` |
| libmdbx copy-on-write B+-tree | `mdbx` |

The Rust crate version is **0.0.110**. The libmdbx baseline is integrated through the Rust `libmdbx` crate; the version used by this checkout is pinned by `Cargo.lock`.

## Requirements

The paper experiments are intended for Linux and require:

- a recent Rust toolchain with Rust 2024 edition support;
- `cargo`;
- `numactl` with NUMA node 0 available;
- Python 3 with `matplotlib` and `pandas` for plotting.

The full experiments are compute- and memory-intensive. The submitted paper used one socket of a dual-socket AMD EPYC 7742 system with 64 physical cores, 128 hardware threads, and 512 GiB of local memory.

## Setup

Clone the repository and enter it:

```bash
git clone https://github.com/umr-dbs/cMVBT.git
cd cMVBT
```

Build the optimized paper binary from source (recommended for measurements):

```bash
cargo build --profile paper
```

The resulting executable is `target/paper/cMVBT`.

This build uses `target-cpu=native`, so compile it on the machine that will run
the experiments. This enables the CPU-specific optimizations intended by the
paper configuration.

Run the correctness tests:

```bash
cargo test --release
```

Install the plotting dependencies if needed:

```bash
python3 -m pip install matplotlib pandas
```

## Prebuilt binary

A prebuilt executable is included for artifact inspection and smoke tests:

```text
artifacts/bin/cMVBT-v0.0.110-linux-x86_64-glibc238
```

It targets x86-64 Linux and requires glibc 2.38 or newer. Verify the file before
running it:

```bash
sha256sum --check artifacts/SHA256SUMS
artifacts/bin/cMVBT-v0.0.110-linux-x86_64-glibc238 ycsb --help
```

To use it with the experiment runner, set `BIN`:

```bash
BIN=artifacts/bin/cMVBT-v0.0.110-linux-x86_64-glibc238 \
OUT=results/paper-smoke QUICK=1 \
  scripts/run_paper_load_experiments.sh all
```

The prebuilt executable favors portability and is not the reference binary for
performance measurements. Build from source on the evaluation machine for
paper-result comparisons. Build provenance and compatibility details are in
[`artifacts/README.md`](artifacts/README.md).

## Reproduce the paper experiments

The canonical reproduction script is `scripts/run_paper_load_experiments.sh`. It generates each workload once per update rate, size and repetition (with a per-repetition seed, so a rerun reproduces the same bytes) and replays the same bytes for every applicable system. This keeps the initial data, operation types, keys, and operation order identical across systems. Measured processes are pinned to NUMA node 0 for both CPUs and memory.

Default configuration (what the script runs without overrides):

| Figure | Workload | Systems |
| --- | --- | --- |
| 5 | 10K initial unique-key insertions, then 10M operations (`FIG5_BLOCKS=10000` blocks of 1,000) by one writer, no GC; then 100K full scans of versions drawn uniformly from the versions created by the workload (time travel); average latency | cMVBT, Version Chain, Frugal List, vWeaver |
| 6 | 10K initial insertions, then 1M operations (`BLOCKS=1000`) per update rate; 32 writers (libmdbx: 1), 16 readers scanning the complete freshest snapshot (~10K records) until the writers finish; no GC | cMVBT, libmdbx, Version Chain, Frugal List |
| 7 | as Figure 6 with GC | cMVBT, Version Chain, Frugal List |
| 8 | as Figure 6 at 60% updates, sweeping reader:writer thread counts | cMVBT, libmdbx, Version Chain, Frugal List |

Update rates are 10, 20, 50, 75, 90 and 100%; every configuration is repeated `REPEATS=5` times.

libmdbx does not support time-travel queries and is not part of Figure 5; the `load` command refuses historical scans on it instead of scanning a different snapshot.

Concurrent replays partition the measured operations by key: all operations on one key run in the same writer, in trace order, so no operation fails and the live data set stays at the initial size. Each run logs the attempted, successful and failed operations per kind and the mean number of records per scan; failed writes appear as `WARNING` lines in `log.txt` and in `warnings.txt`. Readers of every system consume a scan the same way: the visible `(key, payload)` records are materialized and folded into a checksum.

`scripts/run_paper_experiments.sh` is an alternative that generates operations online (`paper-ycsb`). Its defaults follow the same parameters (10K initial records, 10M operations and 100K scans for Figure 5, 1M operations with 32 writers, 1 libmdbx writer, 16 full-snapshot readers for Figures 6 and 7, `REPEATS=5`). It deviates from the paper's protocol in one respect: with more than one writer it uses "sliding" keys (inserts above the live range, deletes of the oldest key) instead of random insert/delete keys, which it supports only for one writer (Figure 5). Its header lists the details.

### Smoke test

Run a reduced end-to-end check before starting the full experiments:

```bash
OUT=results/paper-smoke QUICK=1 \
  scripts/run_paper_load_experiments.sh all

OUT=results/paper-smoke QUICK=1 \
  scripts/run_paper_load_experiments.sh scalability-mdbx-one-writer

scripts/plot_paper_results.py results/paper-smoke
```

### Full paper run

Choose a stable output directory so the second command can reuse the exact workload files generated by the first:

```bash
OUT=results/edbt2027 \
  scripts/run_paper_load_experiments.sh all

OUT=results/edbt2027 \
  scripts/run_paper_load_experiments.sh scalability-mdbx-one-writer

scripts/plot_paper_results.py results/edbt2027
```

`REPEATS` defaults to 5; repetition `r` uses the workloads generated with seed `SEED_BASE + r` (`SEED_BASE=1000`). Existing correctly sized workload files in the selected output directory are validated and reused rather than regenerated. The plots show the mean over the repetitions with min–max error bars, and `paper-figures/summary.csv` / `summary.md` list mean, standard deviation, coefficient of variation, minimum and maximum of every plotted point. OLTP throughput counts successful operations only.

### Experiment groups

Individual groups can be run by passing one or more names to the runner:

| Group | Paper output |
| --- | --- |
| `latency` | Figure 5: historical scan latency |
| `concurrent` | Figure 6: concurrent scan and OLTP throughput without GC |
| `concurrent-mdbx` | Figure 6: libmdbx only (one writer; `concurrent` already includes it) |
| `gc` | Figure 7 and Figure 10: throughput with GC and cMVBT node reuse |
| `scalability` | Figure 8: OLAP and OLTP scalability |
| `scalability-mdbx-one-writer` | Figure 8: one-writer libmdbx measurement |
| `retries` | Figure 9: optimistic retry distribution |
| `all` | Main cMVBT and baseline measurements for Figures 5–10 |

For example:

```bash
OUT=results/latency REPEATS=5 \
  scripts/run_paper_load_experiments.sh latency
```

The script header documents all environment overrides, including workload sizes, update rates, thread counts, systems, repetitions, timeouts, and output location.

## Output

Each run records:

- CSV measurements used by the plotting script;
- generated workload files (named by initial size, operation count, seed and update rate) and their SHA-256 digests;
- the exact commands that were executed;
- Git revision and working-tree state;
- CPU, memory, and NUMA information;
- logs and any failed commands.

Important files include:

| File | Contents |
| --- | --- |
| `latency.csv` | Figure 5 measurements |
| `concurrent_nogc.csv` | Figure 6 measurements |
| `concurrent_gc.csv` | Figure 7 and Figure 10 measurements |
| `scalability.csv` | Figure 8 measurements |
| `scalability_mdbx_one_writer.csv` | Figure 8 one-writer libmdbx measurement |
| `retries.csv` | Figure 9 measurements |
| `machine.txt` | machine, Git, and experiment configuration |
| `log.txt` | complete execution log |
| `failures.txt` | failed commands, when present |
| `warnings.txt` | runs with failed writes, when present |
| `paper-figures/summary.csv`, `summary.md` | mean, stddev, CV, min, max per plotted point (written by the plot script) |

The plotting script writes PDF and PNG versions of the paper figures into the selected results directory.

## Additional benchmarks

The repository also contains an online YCSB driver and exploratory profiling scripts. These are not required for the canonical paper reproduction above. Run the following commands for their available options:

```bash
target/paper/cMVBT ycsb --help
target/paper/cMVBT paper-ycsb --help
```

## Repository layout

| Path | Purpose |
| --- | --- |
| `src/mv_tree/` | cMVBT and structural modification operations |
| `src/mv_page_model/` | leaf and internal page layouts |
| `src/mv_query/` | latch-free point and range queries |
| `src/mv_sync/` | optimistic latches, commit clock, and synchronization |
| `src/mv_gc/` | reader tracking, graveyard, and garbage collection |
| `src/mv_root/` | root-version indexes |
| `src/dexa/` | B+-tree and version-list baselines |
| `src/ycsb/` | online workload generator and statistics |
| `src/paper/` | paper experiment loaders and shared interfaces |
| `scripts/` | experiment and plotting scripts |

## License

The cMVBT source code and accompanying experiment scripts are released under the [Apache License, Version 2.0](LICENSE). Third-party dependencies remain subject to their respective licenses.

## Contact

Amir Tonta<br>
Marburg University<br>
amir.tonta@mathematik.uni-marburg.de
