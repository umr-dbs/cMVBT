# cMVBT (VLDB 2027 Vol. 20)
>## Prototype Build Date: 22.06.2026

>## Version: 0.0.109
---------------------------------------

# Re-running the experiments of the paper
    scripts/run_paper_experiments.sh [latency|concurrent|gc|scalability|retries|oltp|all]    # writes results/<timestamp>/*.csv
    scripts/plot_paper_results.py results/<timestamp>                                       # writes results/<timestamp>/figures/*.{pdf,png}
    QUICK=1 scripts/run_paper_experiments.sh                                                # minutes instead of hours, to check the setup
The script builds `target/paper/cMVBT` (`cargo build --profile paper`: LTO, `panic = "abort"`, ...) and follows Section 8: 10K initial
insertions, then 1M operations (1000 blocks) with an update rate of 10/20/50/75/90/100% (insertions = deletions); scan latency over
100K scans at uniformly drawn versions without GC (Figure 8); 32 writers + 16 readers scanning the freshest snapshot without (Figure 9)
and with GC (Figures 10 and 13); thread scaling at 60% (Figure 11); retries of the optimistic write traversal for Zipf alpha
(Figure 12, 1M insertions, alpha = 0, 0.4, ..., 1.4). Parameters can be overridden by environment variables, see the script header.
`load <file> <concurrent> <olap-threads> <oltp-threads|scans> <skew> <range> <root*> <gc> <gc-uip> <init-keys> [system]` is the
underlying command and runs every system (`cmvbt|chain|frugal|vweaver|skiplist`) on the same workload file; results go to
`$RESULTS_CSV` (default `oltp.csv`). The old cMVBT-only implementation remains available as `load-legacy`.

Caveats of the protocol: (1) the OLTP threads replay contiguous slices of the generated file, so an update or delete can run before
the insertion of its key (another thread's slice) and fail; `oltp_failed` counts those (about 30% in the 60% workload). Before the
fix in `dispatch.rs`, a failed cMVBT update of an absent key inserted a phantom record, which no other system did. (2) The paper
describes the concurrent readers as scanning the freshest snapshot; the old `load` drew a random version instead, which cannot
work with GC. The new `load` uses the freshest snapshot. (3) The reader threads are stopped when all writers are done. (4) The initial insertions run in a thread of their own. A
thread that committed and then idles (the main thread, waiting for the workers) holds the visible snapshot of all readers, and
the GC bound, at its last commit version; with the initial insertions in the main thread the readers never saw anything newer
than the initial data and the GC never recycled a node.

# Reproduce Paper Results (manual commands):
    1. Compile via (release config in Cargo.toml):
        overflow-checks = false
        opt-level = 3           
        lto = "fat"            
        codegen-units = 1        
        panic = "abort"          
        strip = true            
        debug = 0               
        incremental = false
    2. Generate workloads via 'generate <data-file> <initial-keys-count> <total-blocks> <inserts_per_block> <updates_per_block> <deletes_per_block> <skew>' 
       Otherwise, use load to load a specific generated workload via the generate command
    3. Execute workloads via 'load <data-file> <concurrent> <olap-threads> <oltp-threads> <olaps-skew> <key-range-max> <root*-index> <gc> <gc-uip> <initial-keys-to-load> 
       <data-file>: The path to the generated workload file
       <concurrent>: Enable concurrent execution of OLTP and OLAP threads: boolean. If false, then <oltp-threads> is the number of scans to execute after the OLTP workload is completed
       <olap-threads>: Number of OLAP threads to run concurrently: integer
       <oltp-threads>: Number of OLTP threads to run concurrently: integer
       <olaps-skew>: Skew factor for OLAP operations: 0 for paper tests
       <key-range-max>: Maximum key range for olaps scan range: integer or max
       <root*-index>: Root index for workload generation: ll = LinkedList, fg = FrugalList, sk = SkipList, bt = B+Tree (not fully supported) 
       <gc>: Enable garbage collection
       <gc-uip>: Enable garbage collection with updates-in-place
       <initial-keys-to-load>: Number of initial keys to load
       Note: mv_test.rs -> Method "main_load" is the dispatcher for this command; also, look at "main_load_yscb" for detailed implementations

### For Example, replicating the paper results, various figures; use the following commands:
Note that the BTreeVersionChains at https://github.com/umr-dbs/BTree-MVCC-Version-Chains can load the exact same commands, but data generation must be done with the cMVBT.
>### Concurrency level experiment: (Workload-Generation ./cMVBT generate 60.dat 10000 1000 200 600 200 0)
> 
    ./cMVBT load 60.dat true 1 2 0 max fg false false 10000; sleep 2;
    ./cMVBT load 60.dat true 2 4 0 max fg false false 10000; sleep 2;
    ./cMVBT load 60.dat true 4 8 0 max fg false false 10000; sleep 2;
    ./cMVBT load 60.dat true 6 12 0 max fg false false 10000; sleep 2;
    ./cMVBT load 60.dat true 7 14 0 max fg false false 10000; sleep 2;
    ./cMVBT load 60.dat true 8 16 0 max fg false false 10000; sleep 2;
    ./cMVBT load 60.dat true 10 20 0 max fg false false 10000; sleep 2;
    ./cMVBT load 60.dat true 12 24 0 max fg false false 10000; sleep 2;
    ./cMVBT load 60.dat true 14 28 0 max fg false false 10000; sleep 2;
    ./cMVBT load 60.dat true 16 32 0 max fg false false 10000

>### OLTP without versioning experiment: (Data-Generation as above, vary the number of inserts/updates/deletes)
> 
    ./cMVBT load 10.dat true 0 32 0 max fg false false 10000; sleep 2;
    ./cMVBT load 20.dat true 0 32 0 max fg false false 10000; sleep 2;
    ./cMVBT load 50.dat true 0 32 0 max fg false false 10000; sleep 2;
    ./cMVBT load 75.dat true 0 32 0 max fg false false 10000; sleep 2;
    ./cMVBT load 90.dat true 0 32 0 max fg false false 10000; sleep 2;
    ./cMVBT load 100.dat true 0 32 0 max fg false false 10000

>### OLTP mixed workload with GC: (Data-Generation as above, vary the number of inserts/updates/deletes)
> 
    ./cMVBT load 10.dat true 16 32 0 max fg true false 10000; sleep 2;
    ./cMVBT load 20.dat true 16 32 0 max fg true false 10000; sleep 2;
    ./cMVBT load 50.dat true 16 32 0 max fg true false 10000; sleep 2;
    ./cMVBT load 75.dat true 16 32 0 max fg true false 10000; sleep 2;
    ./cMVBT load 90.dat true 16 32 0 max fg true false 10000; sleep 2;
    ./cMVBT load 100.dat true 16 32 0 max fg true false 10000


>### OLTP mixed workload without GC: (Data-Generation as above, vary the number of inserts/updates/deletes)
> 
    ./cMVBT load 10.dat true 16 32 0 max fg false false 10000; sleep 2;
    ./cMVBT load 20.dat true 16 32 0 max fg false false 10000; sleep 2;
    ./cMVBT load 50.dat true 16 32 0 max fg false false 10000; sleep 2;
    ./cMVBT load 75.dat true 16 32 0 max fg false false 10000; sleep 2;
    ./cMVBT load 90.dat true 16 32 0 max fg false false 10000; sleep 2;
    ./cMVBT load 100.dat true 16 32 0 max fg false false 10000

# YCSB-style benchmark (cMVBT and the version-list baselines in one binary)
The baselines of the paper (Version Chains, Frugal Lists, vWeaver, skip lists as version-chain index on an OLC B+-tree;
formerly the separate repository `BTree-MVCC-Version-Chains`) live in `src/dexa/` behind the cargo feature `dexa`
(on by default; `--no-default-features` builds the cMVBT only). `src/ycsb/` drives all systems with one workload generator.
Operations are generated online per thread (no pre-generated trace that is replayed in a serial order), keys are sampled
per operation, and every read and scan verifies what it got (reported as *snapshot violations*, the run fails if there are
any): the 8-byte value must equal its key, a 1 KB record must carry its key and an intact body.

    ./cMVBT ycsb --system <cmvbt|chain|frugal|vweaver|skiplist> --workload <a|b|c|d|e|f|churn|update-heavy|custom>
                 --records 10000000 --threads 32 --olap-threads 16 --olap-range 10000 --secs 20 --warmup 2
                 --theta 0.99 --scramble true --csv ycsb.csv          # ./cMVBT ycsb --help for everything

* Workloads: YCSB A (50r/50u), B (95r/5u), C (100r), D (95r/5i, latest), E (95 scans/5i), F (50r/50 read-modify-write),
  plus `churn` (not YCSB): 25r/25u/25i/25d with fresh inserts and FIFO expiry of the oldest key, i.e., a sliding window of
  constant size with continuous node reorganization and dead data. Any mix via `--mix read:update:insert:delete:scan:rmw`.
* Records: `--value-size 1024` (default, YCSB's 1 KB record: 10 fields of 100 bytes) or `--value-size 8`. A page slot is 8 bytes in
  both cases: the 8-byte value is stored inline, the 1 KB record behind a `triomphe::Arc` (an `Arc` without weak counter), so the
  copies the trees make (reorganizations, scan results) are one atomic increment. Updates and inserts allocate a fresh record.
* Skew: `--dist uniform|zipf|latest|hotspot`, `--theta` = Zipf alpha (default 0.99). Any alpha > 0 works, also 1 and the paper's
  values above 1 (rejection-inversion sampling, checked against the exact distribution in the tests). `--scramble true` spreads the hot
  ranks over the key range (hot keys hit many leaves, long chains per key), `false` keeps them contiguous (a few hot leaves).
* Sweep: `scripts/run_ycsb.sh` runs every workload (A-F, churn) at alpha = 0, 0.5, 0.8, 0.99, 1.2, 1.4 for both record kinds and all
  five systems; `scripts/plot_ycsb_results.py results/ycsb-<run>` plots throughput, p99 latency and OLAP scan rate over alpha
  per workload, and the effect of the record size. Everything is overridable by environment variables (see the script header).
* OLAP: `--olap-threads/--olap-range` add dedicated long snapshot scans next to the OLTP threads; their throughput and latency
  are reported separately.
* `--gc true` enables garbage collection: the cMVBT's graveyard, and for the baselines the removal of deleted keys during page splits.
* Inserts always use fresh keys; updates/deletes of absent keys fail on every system (see the notes below).

### Memory-safety fixes (cMVBT and baselines)
* The per-thread commit slots (`clock.rs`, in the cMVBT and in the baselines) lived in a `Vec` that was reallocated when more
  threads than physical cores had registered, while other threads read it lock-free (heap use-after-free, found with
  AddressSanitizer on a 16-core machine; at most as many threads as cores never triggered it). The array is fixed (4096 slots) now and
  thread ids are recycled when threads end.

### Memory-safety fixes in the cMVBT found while generating the workloads (`generate` crashed in about half of the runs)
* `retrieve_root_write_internal_olc` returned a read guard that borrowed a local `SmartCell` handle (stack-use-after-return, found with
  AddressSanitizer). Read guards now carry a bitwise copy of the cell's `Arc` pointer instead of a reference to the handle.
* The random descent of `UpdateRand`/`DeleteRand`/`InsertRand` (workload generation only) repaired several children of the same node
  one after the other without re-checking the node's own room, and pushed entries beyond the node's capacity (heap overflow). It
  restarts when the node becomes unsafe.

### Notes on the baselines (`src/dexa/`)
Imported from the published repository `umr-dbs/BTree-MVCC-Version-Chains` at `9aae3f5` (2026-05-31). An older checkout named
`DEXA-VersionLists-BPlusTree` (state 2026-02-04) is an ancestor of it and lost live keys once the tree outgrew the root
(fixed upstream since), so do not use it. The code is unchanged apart from these edits, found by checking all systems
against a plain `HashSet` model (`cargo test --release`; tests that run trees hold a common lock, so parallel execution is fine; the tests include a concurrent one: 8 writers on disjoint, interleaved key sets
with exactly predictable results, two scanners, and an exact comparison of the final state, for every system with and without GC):
* edition 2024: two pattern/borrow adjustments in `mvb_utils/smart_cell.rs`.
* vWeaver scans up to `Key::MAX` never terminated (`inc_key` saturates); they stop now when the lower bound cannot advance.
* `update` of a deleted key resurrected it and `delete` of a deleted key reported success (`node.rs`); both fail now, as in
  the cMVBT. The paper's workloads never touch deleted keys, so their results are not affected.
* `do_underflow_correction` read the separator of the right-most child unchecked (`get_unchecked(child_pos)`) and now uses
  the parent's fence there.
* The baselines' GC (removal of deleted keys during page splits) freed the removed records' version lists immediately, while
  optimistic readers may still traverse them (use-after-free; crashes with GC and concurrent scans). The removed records are kept
  in a per-tree limbo list until the tree is dropped now (`limbo` in `bplus_tree.rs`).
* `AtomicVersionList::clone` (Version Chains) re-appended all but the oldest version as live ones, so cloning a record, e.g., when
  GC makes leaves underflow and they are merged, resurrected deleted keys. It keeps every version's deletion marker now.
* vWeaver's scan stopped one key early (`lower >= upper` on an inclusive interval).
* Splitting a *leaf* root handed the caller the guard of the old root block, which had become the left leaf, so the key that
  triggered the split was inserted into the left leaf even when it belongs to the right one, and was lost for lookups
  (exactly one key once, at the first split; the tree is empty-rooted only at the start, so the effect on the paper's numbers is negligible). The traversal restarts on the
  new root now. This affected all four baselines.

The benchmark only inserts fresh keys.

# PiBench Benchmark (Irrelevant for paper results; look at lib.rs for implementation details)
>PiBench Integration: https://github.com/umr-dbs/pibench_ext
--------------------------------------

# Contact
    Name:               Amir Tonta
    E-Mail:             amir.tonta@mathematik.uni-marburg.de
