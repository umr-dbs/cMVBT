# cMVBT (VLDB 2027 Vol. 20)
>## Prototype Build Date: 22.06.2026

>## Version: 0.0.109
---------------------------------------

# Reproduce Paper Results:
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
per operation, and every read and scan verifies the workload invariant `payload == key` (reported as *snapshot violations*,
the run fails if there are any).

    ./cMVBT ycsb --system <cmvbt|chain|frugal|vweaver|skiplist> --workload <a|b|c|d|e|f|churn|update-heavy|custom>
                 --records 10000000 --threads 32 --olap-threads 16 --olap-range 10000 --secs 20 --warmup 2
                 --theta 0.99 --scramble true --csv ycsb.csv          # ./cMVBT ycsb --help for everything

* Workloads: YCSB A (50r/50u), B (95r/5u), C (100r), D (95r/5i, latest), E (95 scans/5i), F (50r/50 read-modify-write),
  plus `churn` (not YCSB): 25r/25u/25i/25d with fresh inserts and FIFO expiry of the oldest key, i.e., a sliding window of
  constant size with continuous node reorganization and dead data. Any mix via `--mix read:update:insert:delete:scan:rmw`.
* Skew: `--dist uniform|zipf|latest|hotspot`, `--theta` (YCSB zipfian, default 0.99). `--scramble true` spreads the hot
  ranks over the key range (hot keys hit many leaves, long chains per key), `false` keeps them contiguous (a few hot leaves).
* OLAP: `--olap-threads/--olap-range` add dedicated long snapshot scans next to the OLTP threads; their throughput and latency
  are reported separately.
* `--gc true` enables the cMVBT's garbage collection. The `dexa` baselines accept the flag, but their code does not use it.
* Inserts always use fresh keys; updates/deletes of absent keys fail on every system (see the notes below).

### Notes on the baselines (`src/dexa/`)
Imported from the published repository `umr-dbs/BTree-MVCC-Version-Chains` at `9aae3f5` (2026-05-31). An older checkout named
`DEXA-VersionLists-BPlusTree` (state 2026-02-04) is an ancestor of it and lost live keys once the tree outgrew the root
(fixed upstream since), so do not use it. The code is unchanged apart from these edits, found by checking all systems
against a plain `HashSet` model (`cargo test --release --bin cMVBT -- --test-threads=1`):
* edition 2024: two pattern/borrow adjustments in `mvb_utils/smart_cell.rs`.
* `update` of a deleted key resurrected it and `delete` of a deleted key reported success (`node.rs`); both fail now, as in
  the cMVBT. The paper's workloads never touch deleted keys, so their results are not affected.
* `do_underflow_correction` read the separator of the right-most child unchecked (`get_unchecked(child_pos)`) and now uses
  the parent's fence there.
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
