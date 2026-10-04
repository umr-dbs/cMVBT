#!/usr/bin/env bash
# Reproduce Section 8 / Figures 5-10 with operations generated online at execution time.
# Every measured process is restricted to NUMA node 0 for CPUs and memory.
#
#   scripts/run_paper_experiments.sh [latency|concurrent|gc|scalability|retries|allocations|all ...]
#
# Current reproduction defaults:
#   Figure 5: 2M initial inserts, 10M writes, 1K historical 100K-record scans, no GC.
#   Figures 6/7: 2M initial inserts, 1M writes, 32 writers, 16 fresh 100K-record readers.
#   Figure 8: 60% updates; independent OLAP and OLTP thread scalability sweeps.
#   Figure 9: 1M insertions with uniform access and Zipf alphas 0.1, 0.4, 0.8, 0.99, 1.4.
#   Figure 10: cMVBT node allocation/reuse under the OLTP workload with GC.
#
# All online rows also contain update/insert/delete/scan count, average, p50, p95, p99,
# p99.9 and maximum latency. libmdbx pins its initial read transaction for Figure 5;
# Figures 6 and 8 use fresh read transactions. Figure 7 does not include libmdbx.
#
# Environment overrides:
#   OUT=results/paper-<timestamp> REPEATS=1 BIN=target/paper/cMVBT RUN_TIMEOUT=7200 SLEEP=2
#   UPDATE_RATES="10 20 50 75 90 100" INIT=2000000 WRITERS=32 READERS=16
#   DISTRIBUTIONS="uniform zipf:0.1 zipf:0.4 zipf:0.8 zipf:0.99 zipf:1.4"
#   LATENCY_OPERATIONS=10000000 THROUGHPUT_OPERATIONS=1000000 SCANS=1000 SCAN_RANGE=100000
#   FIG5_SYSTEMS="cmvbt mdbx chain frugal vweaver" FIG6_SYSTEMS="cmvbt mdbx chain frugal"
#   FIG7_SYSTEMS="cmvbt chain frugal" SCALE_SYSTEMS="cmvbt mdbx chain frugal"
#   OLAP_LEVELS="1 2 4 6 8 16 32"
#   OLTP_LEVELS="2 4 8 16 32 64" ZIPF_ALPHAS="0 0.1 0.4 0.8 0.99 1.4"
#   NUMACTL=numactl QUICK=1 (small smoke-test sizes)
set -uo pipefail
cd "$(dirname "$0")/.."

QUICK=${QUICK:-0}
REPEATS=${REPEATS:-1}
OUT=${OUT:-results/paper-$(date +%Y%m%d-%H%M%S)}
BIN=${BIN:-target/paper/cMVBT}
NUMACTL=${NUMACTL:-numactl}
RUN_TIMEOUT=${RUN_TIMEOUT:-7200}
SLEEP=${SLEEP:-2}
UPDATE_RATES=${UPDATE_RATES:-"10 20 50 75 90 100"}
ZIPF_ALPHAS=${ZIPF_ALPHAS:-"0 0.1 0.4 0.8 0.99 1.4"}
DISTRIBUTIONS=${DISTRIBUTIONS:-"uniform zipf:0.1 zipf:0.4 zipf:0.8 zipf:0.99 zipf:1.4"}
INIT=${INIT:-2000000}
WRITERS=${WRITERS:-32}
READERS=${READERS:-16}
SCANS=${SCANS:-1000}
SCAN_RANGE=${SCAN_RANGE:-100000}
LATENCY_OPERATIONS=${LATENCY_OPERATIONS:-10000000}
THROUGHPUT_OPERATIONS=${THROUGHPUT_OPERATIONS:-1000000}
RETRY_INSERTIONS=${RETRY_INSERTIONS:-1000000}
FIG5_SYSTEMS=${FIG5_SYSTEMS:-"cmvbt mdbx chain frugal vweaver"}
FIG6_SYSTEMS=${FIG6_SYSTEMS:-"cmvbt mdbx chain frugal"}
FIG7_SYSTEMS=${FIG7_SYSTEMS:-"cmvbt chain frugal"}
SCALE_SYSTEMS=${SCALE_SYSTEMS:-"cmvbt mdbx chain frugal"}
OLAP_LEVELS=${OLAP_LEVELS:-"1 2 4 6 8 16 32"}
OLTP_LEVELS=${OLTP_LEVELS:-"2 4 8 16 32 64"}

if [ "$QUICK" = 1 ]; then
  # Keep enough headroom for temporary insert/delete imbalance inside a 1,000-op block,
  # so every bounded historical range can still return SCAN_RANGE records.
  INIT=2000
  LATENCY_OPERATIONS=10000
  THROUGHPUT_OPERATIONS=10000
  SCANS=100
  SCAN_RANGE=1000
  RETRY_INSERTIONS=10000
  UPDATE_RATES="10 60 100"
  OLAP_LEVELS="1 2"
  OLTP_LEVELS="2 4"
  RUN_TIMEOUT=600
  SLEEP=0
fi

command -v "$NUMACTL" >/dev/null 2>&1 || { echo "error: '$NUMACTL' is required" >&2; exit 2; }
NUMA_HARDWARE=$("$NUMACTL" --hardware 2>/dev/null) || { echo "error: cannot query NUMA topology" >&2; exit 2; }
grep -q '^node 0 cpus:' <<< "$NUMA_HARDWARE" || {
  echo "error: NUMA node 0 is unavailable" >&2; exit 2;
}

# Always rebuild the default binary, so a run can never silently use stale source code.
if [ "$BIN" = "target/paper/cMVBT" ]; then
  cargo build --profile paper || exit 1
elif [ ! -x "$BIN" ]; then
  echo "error: custom BIN '$BIN' is not executable" >&2
  exit 2
fi
BIN=$(realpath "$BIN")
mkdir -p "$OUT"
OUT=$(realpath "$OUT")
CSV="$OUT/paper.csv"
RETRY_CSV="$OUT/retries.csv"
LOG="$OUT/log.txt"
FAILS="$OUT/failures.txt"

{
  echo "date: $(date -Is)"
  echo "git: $(git rev-parse HEAD 2>/dev/null) ($(git status --porcelain 2>/dev/null | wc -l) uncommitted files)"
  echo "binary: $BIN"
  echo "numa: $NUMACTL --cpunodebind=0 --membind=0"
  echo "INIT=$INIT DISTRIBUTIONS=$DISTRIBUTIONS UPDATE_RATES=$UPDATE_RATES WRITERS=$WRITERS READERS=$READERS LATENCY_OPERATIONS=$LATENCY_OPERATIONS THROUGHPUT_OPERATIONS=$THROUGHPUT_OPERATIONS SCANS=$SCANS SCAN_RANGE=$SCAN_RANGE REPEATS=$REPEATS"
  lscpu 2>/dev/null | grep -E "Model name|^CPU\(s\)|NUMA node|Thread|Core|Socket"
  free -g 2>/dev/null | head -2
  echo "$NUMA_HARDWARE"
} > "$OUT/machine.txt"

failures=0
run() { # run <label> <expected-csv> <command...>
  local label=$1 csv=$2
  shift 2
  local before=0 after rc
  [ -f "$csv" ] && before=$(wc -l < "$csv")
  echo ">> [$label] $NUMACTL --cpunodebind=0 --membind=0 $*" | tee -a "$LOG"
  timeout "$RUN_TIMEOUT" "$NUMACTL" --cpunodebind=0 --membind=0 "$@" >> "$LOG" 2>&1
  rc=$?
  after=$before
  [ -f "$csv" ] && after=$(wc -l < "$csv")
  if [ "$rc" -ne 0 ] || [ "$after" -le "$before" ]; then
    echo "[$label] rc=$rc csv-lines=$before->$after: $*" | tee -a "$FAILS"
    failures=$((failures + 1))
  fi
  sleep "$SLEEP"
}

online() { # label experiment repeat system distribution theta rate gc operations writers readers historical-scans scan-threads
  local label=$1 experiment=$2 rep=$3 system=$4
  local distribution=$5 theta=$6 rate=$7 gc=$8 operations=$9
  shift 9
  local writers=$1 readers=$2 historical=$3 scan_threads=$4
  run "$label" "$CSV" "$BIN" paper-ycsb --experiment "$experiment" --repeat "$rep" \
    --system "$system" --distribution "$distribution" --theta "$theta" --scramble true \
    --update-rate "$rate" --gc "$gc" --records "$INIT" \
    --operations "$operations" --writers "$writers" --readers "$readers" \
    --historical-scans "$historical" --scan-range "$SCAN_RANGE" --scan-threads "$scan_threads" \
    --seed "$((41 + rep))" --csv "$CSV"
}

for_distribution() { # callback remaining-args...
  local callback=$1
  shift
  local spec distribution theta
  for spec in $DISTRIBUTIONS; do
    if [[ "$spec" == zipf:* ]]; then
      distribution=zipf
      theta=${spec#zipf:}
    elif [ "$spec" = uniform ]; then
      distribution=uniform
      theta=0
    else
      echo "error: bad distribution '$spec' (use uniform or zipf:<theta>)" >&2
      exit 2
    fi
    "$callback" "$distribution" "$theta" "$@"
  done
}

latency_distribution() {
  local distribution=$1 theta=$2 rep=$3 rate=$4 system=$5
  online "fig5/$system/$distribution$theta/u$rate/#$rep" fig5_scan_latency "$rep" "$system" \
    "$distribution" "$theta" "$rate" false "$LATENCY_OPERATIONS" 1 0 "$SCANS" 1
}

exp_latency() {
  for rep in $(seq 1 "$REPEATS"); do
    for rate in $UPDATE_RATES; do
      for system in $FIG5_SYSTEMS; do
        for_distribution latency_distribution "$rep" "$rate" "$system"
      done
    done
  done
}

exp_concurrent() { # gc experiment
  local gc=$1 experiment=$2
  local systems=$FIG6_SYSTEMS
  [ "$gc" = true ] && systems=$FIG7_SYSTEMS
  for rep in $(seq 1 "$REPEATS"); do
    for rate in $UPDATE_RATES; do
      for system in $systems; do
        for_distribution concurrent_distribution "$rep" "$rate" "$system" "$gc" "$experiment"
      done
    done
  done
}

concurrent_distribution() {
  local distribution=$1 theta=$2 rep=$3 rate=$4 system=$5 gc=$6 experiment=$7
  online "$experiment/$system/$distribution$theta/u$rate/#$rep" "$experiment" "$rep" "$system" \
    "$distribution" "$theta" "$rate" "$gc" "$THROUGHPUT_OPERATIONS" "$WRITERS" "$READERS" 0 1
}

scalability_distribution() {
  local distribution=$1 theta=$2 rep=$3 system=$4 dimension=$5 threads=$6
  if [ "$dimension" = olap ]; then
    online "fig8-olap/$system/$distribution$theta/r$threads/#$rep" fig8_olap_scalability "$rep" "$system" \
      "$distribution" "$theta" 60 false "$THROUGHPUT_OPERATIONS" "$WRITERS" "$threads" 0 1
  else
    online "fig8-oltp/$system/$distribution$theta/w$threads/#$rep" fig8_oltp_scalability "$rep" "$system" \
      "$distribution" "$theta" 60 false "$THROUGHPUT_OPERATIONS" "$threads" "$READERS" 0 1
  fi
}

exp_scalability() {
  for rep in $(seq 1 "$REPEATS"); do
    for system in $SCALE_SYSTEMS; do
      for readers in $OLAP_LEVELS; do
        for_distribution scalability_distribution "$rep" "$system" olap "$readers"
      done
      for writers in $OLTP_LEVELS; do
        for_distribution scalability_distribution "$rep" "$system" oltp "$writers"
      done
    done
  done
}

exp_retries() {
  for rep in $(seq 1 "$REPEATS"); do
    for alpha in $ZIPF_ALPHAS; do
      RESULTS_CSV="$RETRY_CSV" EXPERIMENT=fig9_retries REPEAT="$rep" \
        run "fig9/alpha$alpha/#$rep" "$RETRY_CSV" "$BIN" retry-exp "$WRITERS" "$RETRY_INSERTIONS" "$alpha"
    done
  done
}

exp_allocations() {
  for rep in $(seq 1 "$REPEATS"); do
    for rate in $UPDATE_RATES; do
      for_distribution allocation_distribution "$rep" "$rate"
    done
  done
}


allocation_distribution() {
  local distribution=$1 theta=$2 rep=$3 rate=$4
  online "fig10/$distribution$theta/u$rate/#$rep" fig10_node_reuse "$rep" cmvbt \
    "$distribution" "$theta" "$rate" true "$THROUGHPUT_OPERATIONS" "$WRITERS" 0 0 1
}

[ "$#" -eq 0 ] && set -- all
for experiment in "$@"; do
  case "$experiment" in
    latency) exp_latency ;;
    concurrent) exp_concurrent false fig6_throughput_nogc ;;
    gc) exp_concurrent true fig7_throughput_gc ;;
    scalability) exp_scalability ;;
    retries) exp_retries ;;
    allocations) exp_allocations ;;
    all)
      exp_latency
      exp_concurrent false fig6_throughput_nogc
      exp_concurrent true fig7_throughput_gc
      exp_scalability
      exp_retries
      exp_allocations
      ;;
    *) echo "unknown experiment '$experiment'" >&2; exit 2 ;;
  esac
done

if [ "$failures" -ne 0 ]; then
  echo ">> $failures run(s) failed; see $FAILS" >&2
  exit 1
fi
echo ">> complete: results in $OUT"
