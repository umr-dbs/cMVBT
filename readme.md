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

# PiBench Benchmark (Irrelevant for paper results; look at lib.rs for implementation details)
>PiBench Integration: https://github.com/umr-dbs/pibench_ext
--------------------------------------

# Contact
    Name:               Amir Tonta
    E-Mail:             amir.tonta@mathematik.uni-marburg.de
