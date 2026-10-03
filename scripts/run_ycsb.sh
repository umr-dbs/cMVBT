#!/usr/bin/env bash
# YCSB-style benchmark of the cMVBT and the version-list baselines: every workload at several Zipf skews (alpha) and for
# both record kinds (YCSB's default 1 KB record behind a pointer, and the 8-byte value stored inline).
#
#   scripts/run_ycsb.sh                  # writes results/ycsb-<timestamp>/ycsb.csv
#   scripts/plot_ycsb_results.py results/ycsb-<timestamp>
#
# Environment (defaults):
#   SYSTEMS="cmvbt chain frugal vweaver skiplist"   WORKLOADS="a b c d e f churn"
#   THETAS="0 0.5 0.8 0.99 1.2 1.4"   (0 = uniform; alpha above 1 is supported)   VALUE_SIZES="1024 8"
#   RECORDS=10000000 THREADS=32 OLAP_THREADS=16 OLAP_RANGE=10000 SECS=20 WARMUP=2 GC=false REPEATS=1
#   RUN_TIMEOUT=3600   OUT=results/ycsb-<timestamp>   BIN=target/paper/cMVBT   QUICK=1 (tiny scale for a smoke test)
# Workload D reads the latest records (zipfian distance from the newest), E scans, churn is not YCSB (see readme); for
# those the same alpha parameterizes the key choice.
set -u
cd "$(dirname "$0")/.."

QUICK=${QUICK:-0}
SYSTEMS=${SYSTEMS:-"cmvbt chain frugal vweaver skiplist"}
WORKLOADS=${WORKLOADS:-"a b c d e f churn"}
THETAS=${THETAS:-"0 0.5 0.8 0.99 1.2 1.4"}
VALUE_SIZES=${VALUE_SIZES:-"1024 8"}
THREADS=${THREADS:-32}
OLAP_THREADS=${OLAP_THREADS:-16}
OLAP_RANGE=${OLAP_RANGE:-10000}
WARMUP=${WARMUP:-2}
GC=${GC:-false}
REPEATS=${REPEATS:-1}
if [ "$QUICK" = 1 ]; then
  RECORDS=${RECORDS:-50000}; SECS=${SECS:-1}; WARMUP=${WARMUP:-0.5}; RUN_TIMEOUT=${RUN_TIMEOUT:-300}
else
  RECORDS=${RECORDS:-10000000}; SECS=${SECS:-20}; RUN_TIMEOUT=${RUN_TIMEOUT:-3600}
fi
OUT=${OUT:-results/ycsb-$(date +%Y%m%d-%H%M%S)}
BIN=${BIN:-target/paper/cMVBT}

[ -x "$BIN" ] || { echo ">> building $BIN (profile paper)"; cargo build --profile paper || exit 1; }
BIN=$(realpath "$BIN"); mkdir -p "$OUT"; OUT=$(realpath "$OUT")
{ echo "date: $(date -Is)"; echo "git: $(git rev-parse HEAD 2>/dev/null)"; echo "binary: $BIN"
  echo "SYSTEMS=$SYSTEMS WORKLOADS=$WORKLOADS THETAS=$THETAS VALUE_SIZES=$VALUE_SIZES RECORDS=$RECORDS THREADS=$THREADS OLAP_THREADS=$OLAP_THREADS SECS=$SECS GC=$GC"
  lscpu 2>/dev/null | grep -E "Model name|^CPU\(s\)|Thread|Core|Socket"; free -g 2>/dev/null | head -2; } > "$OUT/machine.txt"

for rep in $(seq 1 "$REPEATS"); do
 for value in $VALUE_SIZES; do
  for workload in $WORKLOADS; do
   for theta in $THETAS; do
    for system in $SYSTEMS; do
      echo ">> [$rep] $system workload=$workload theta=$theta value=${value}B" | tee -a "$OUT/log.txt"
      timeout "$RUN_TIMEOUT" "$BIN" ycsb --system "$system" --workload "$workload" --theta "$theta" --value-size "$value" \
        --records "$RECORDS" --threads "$THREADS" --olap-threads "$OLAP_THREADS" --olap-range "$OLAP_RANGE" \
        --secs "$SECS" --warmup "$WARMUP" --gc "$GC" --seed "$((41 + rep))" --csv "$OUT/ycsb.csv" >> "$OUT/log.txt" 2>&1
      rc=$?
      [ $rc -ne 0 ] && echo "[$rep] rc=$rc: $system $workload theta=$theta value=$value" | tee -a "$OUT/failures.txt"
    done
   done
  done
 done
done
echo ">> done; results in $OUT"; [ -s "$OUT/failures.txt" ] && { echo ">> FAILED RUNS:"; cat "$OUT/failures.txt"; }
exit 0
