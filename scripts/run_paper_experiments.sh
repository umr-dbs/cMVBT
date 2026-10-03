#!/usr/bin/env bash
# Re-runs the experiments of the paper (Section 8) for the cMVBT and the version-list baselines and writes
# the measurements as CSV files.
#
#   scripts/run_paper_experiments.sh [experiment ...]
#
# Experiments (default: all):
#   latency      Figure 8   scan latency vs. update rate, no GC, 100K scans over all versions   (cmvbt chain vweaver)
#   concurrent   Figure 9   32 writers + 16 readers vs. update rate, no GC                       (cmvbt chain frugal)
#   gc           Figure 10  as above with GC; Figure 13 (node reuse) comes from the cmvbt rows
#   scalability  Figure 11  threads vs. throughput at 60% updates                                (cmvbt frugal)
#   retries      Figure 12  retry probability of optimistic write traversals vs. Zipf alpha      (cmvbt)
#   oltp         OLTP only (no readers), 32 writers, vs. update rate                             (cmvbt chain frugal)
#
# Environment:
#   OUT=results/<timestamp>   output directory (resume into an existing one to add experiments)
#   REPEATS=1                 repetitions of every run
#   QUICK=1                   tiny smoke-test scale (100K operations, 2K scans, 200K insertions)
#   RUN_TIMEOUT=7200          seconds before a single run is aborted and logged to failures.txt
#   SLEEP=2                   pause between runs
#   BIN=target/paper/cMVBT    binary; built with `cargo build --profile paper` if missing
#   UPDATE_RATES="10 20 50 75 90 100"   ZIPF_ALPHAS="0 0.4 0.8 1.0 1.2 1.4"
#   WRITERS=32 READERS=16 INIT=10000 BLOCKS=1000 SCANS=100000 RETRY_INSERTIONS=1000000
#   SCALE_PAIRS="1:2 2:4 4:8 6:12 7:14 8:16 10:20 12:24 14:28 16:32"   (readers:writers)
set -u
cd "$(dirname "$0")/.."

QUICK=${QUICK:-0}
REPEATS=${REPEATS:-1}
OUT=${OUT:-results/$(date +%Y%m%d-%H%M%S)}
UPDATE_RATES=${UPDATE_RATES:-"10 20 50 75 90 100"}
ZIPF_ALPHAS=${ZIPF_ALPHAS:-"0 0.4 0.8 1.0 1.2 1.4"}
WRITERS=${WRITERS:-32}
READERS=${READERS:-16}
INIT=${INIT:-10000}
SCALE_PAIRS=${SCALE_PAIRS:-"1:2 2:4 4:8 6:12 7:14 8:16 10:20 12:24 14:28 16:32"}
if [ "$QUICK" = 1 ]; then
  BLOCKS=${BLOCKS:-100}; SCANS=${SCANS:-2000}; RETRY_INSERTIONS=${RETRY_INSERTIONS:-200000}; RUN_TIMEOUT=${RUN_TIMEOUT:-600}; SLEEP=${SLEEP:-0}
else
  BLOCKS=${BLOCKS:-1000}; SCANS=${SCANS:-100000}; RETRY_INSERTIONS=${RETRY_INSERTIONS:-1000000}; RUN_TIMEOUT=${RUN_TIMEOUT:-7200}; SLEEP=${SLEEP:-2}
fi
BLOCK_OPS=1000   # operations per block: BLOCKS * BLOCK_OPS operations follow the initial insertions
BIN=${BIN:-target/paper/cMVBT}

if [ ! -x "$BIN" ]; then
  echo ">> building $BIN (profile paper)"; cargo build --profile paper || exit 1
fi
BIN=$(realpath "$BIN")
mkdir -p "$OUT/workloads"; OUT=$(realpath "$OUT")
LOG="$OUT/log.txt"; FAILS="$OUT/failures.txt"

{
  echo "date: $(date -Is)"; echo "git: $(git rev-parse HEAD 2>/dev/null) $(git status --porcelain 2>/dev/null | wc -l) uncommitted files"
  echo "binary: $BIN"; echo "args: QUICK=$QUICK REPEATS=$REPEATS UPDATE_RATES=$UPDATE_RATES WRITERS=$WRITERS READERS=$READERS INIT=$INIT BLOCKS=$BLOCKS SCANS=$SCANS"
  lscpu 2>/dev/null | grep -E "Model name|^CPU\(s\)|Thread|Core|Socket"; free -g 2>/dev/null | head -2
} > "$OUT/machine.txt"

run() { # run <csv> <experiment> <repeat> <command...>
  local csv=$1 exp=$2 rep=$3; shift 3
  echo ">> [$exp #$rep] $*" | tee -a "$LOG"
  RESULTS_CSV="$OUT/$csv" EXPERIMENT=$exp REPEAT=$rep timeout "$RUN_TIMEOUT" "$@" >> "$LOG" 2>&1
  local rc=$?
  if [ $rc -ne 0 ]; then echo "[$exp #$rep] rc=$rc: $*" | tee -a "$FAILS"; fi
  sleep "$SLEEP"
}

# Workload of an update rate u: the first INIT operations are insertions, then BLOCKS blocks of 1000 operations with
# u% updates; insertions and deletions share the rest equally, so the number of live records stays constant.
workload() {
  local u=$1 f="$OUT/workloads/$1.dat"
  if [ ! -s "$f" ]; then
    local upd=$((u * BLOCK_OPS / 100)) rest=$((BLOCK_OPS - u * BLOCK_OPS / 100))
    echo ">> generating $f (updates $upd, inserts $((rest / 2)), deletes $((rest - rest / 2)) per block)" | tee -a "$LOG" >&2
    "$BIN" generate "$f" "$INIT" "$BLOCKS" $((rest / 2)) "$upd" $((rest - rest / 2)) 0 >> "$LOG" 2>&1 || { rm -f "$f"; echo "generate $u failed" | tee -a "$FAILS" >&2; }
  fi
  echo "$f"
}

exp_latency() {
  for rep in $(seq 1 "$REPEATS"); do for u in $UPDATE_RATES; do f=$(workload "$u"); [ -s "$f" ] || continue
    for sys in cmvbt chain vweaver; do
      run latency.csv latency "$rep" "$BIN" load "$f" false 1 "$SCANS" 0 max fg false false "$INIT" "$sys"
    done; done; done
}

exp_concurrent() { # <gc> <csv> <experiment>
  local gc=$1 csv=$2 exp=$3
  for rep in $(seq 1 "$REPEATS"); do for u in $UPDATE_RATES; do f=$(workload "$u"); [ -s "$f" ] || continue
    for sys in cmvbt chain frugal; do
      run "$csv" "$exp" "$rep" "$BIN" load "$f" true "$READERS" "$WRITERS" 0 max fg "$gc" false "$INIT" "$sys"
    done; done; done
}

exp_scalability() {
  f=$(workload 60); [ -s "$f" ] || return
  for rep in $(seq 1 "$REPEATS"); do for pair in $SCALE_PAIRS; do
    r=${pair%%:*}; w=${pair##*:}
    for sys in cmvbt frugal; do
      run scalability.csv scalability "$rep" "$BIN" load "$f" true "$r" "$w" 0 max fg false false "$INIT" "$sys"
    done; done; done
}

exp_oltp() {
  for rep in $(seq 1 "$REPEATS"); do for u in $UPDATE_RATES; do f=$(workload "$u"); [ -s "$f" ] || continue
    for sys in cmvbt chain frugal; do
      run oltp_only.csv oltp_only "$rep" "$BIN" load "$f" true 0 "$WRITERS" 0 max fg false false "$INIT" "$sys"
    done; done; done
}

exp_retries() {
  for rep in $(seq 1 "$REPEATS"); do for a in $ZIPF_ALPHAS; do
    run retries.csv retries "$rep" "$BIN" retry-exp "$WRITERS" "$RETRY_INSERTIONS" "$a"
  done; done
}

[ $# -eq 0 ] && set -- all
for e in "$@"; do
  case $e in
    latency) exp_latency ;;
    concurrent) exp_concurrent false concurrent_nogc.csv concurrent_nogc ;;
    gc) exp_concurrent true concurrent_gc.csv concurrent_gc ;;
    scalability) exp_scalability ;;
    retries) exp_retries ;;
    oltp) exp_oltp ;;
    all) exp_latency; exp_concurrent false concurrent_nogc.csv concurrent_nogc; exp_concurrent true concurrent_gc.csv concurrent_gc
         exp_scalability; exp_retries; exp_oltp ;;
    *) echo "unknown experiment '$e'" >&2; exit 2 ;;
  esac
done
echo ">> done; results in $OUT"; [ -s "$FAILS" ] && { echo ">> FAILED RUNS:"; cat "$FAILS"; }
exit 0
