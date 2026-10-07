#!/usr/bin/env bash
# Canonical reproduction of the EDBT paper experiments with the original
# file-based protocol: generate each workload once, then replay the same bytes
# through `load` for every system.
#
# Usage:
#   scripts/run_paper_load_experiments.sh [latency|concurrent|concurrent-mdbx|gc|scalability|scalability-mdbx-one-writer|retries|all ...]
#
# Environment overrides:
#   OUT=results/paper-load-<timestamp> REPEATS=1 QUICK=1
#   INIT=10000 BLOCKS=1000 SCANS=100000 WRITERS=32 READERS=16
#   FIG6_MDBX_WRITERS=1 UPDATE_RATES="10 20 50 75 90 100"
#   FIG5_SYSTEMS="cmvbt mdbx chain frugal vweaver"
#   FIG6_SYSTEMS="cmvbt mdbx chain frugal"
#   FIG7_SYSTEMS="cmvbt chain frugal"
#   SCALE_SYSTEMS="cmvbt mdbx chain frugal"
#   SCALE_PAIRS="1:2 2:4 4:8 6:12 7:14 8:16 10:20 12:24 14:28 16:32"
#   ZIPF_ALPHAS="0 0.1 0.4 0.8 0.99 1.4"
#   BIN=target/paper/cMVBT NUMACTL=numactl RUN_TIMEOUT=7200 SLEEP=2
set -uo pipefail
cd "$(dirname "$0")/.."

QUICK=${QUICK:-0}
REPEATS=${REPEATS:-1}
OUT=${OUT:-results/paper-load-$(date +%Y%m%d-%H%M%S)}
BIN=${BIN:-target/paper/cMVBT}
NUMACTL=${NUMACTL:-numactl}
RUN_TIMEOUT=${RUN_TIMEOUT:-7200}
SLEEP=${SLEEP:-2}
UPDATE_RATES=${UPDATE_RATES:-"10 20 50 75 90 100"}
ZIPF_ALPHAS=${ZIPF_ALPHAS:-"0 0.1 0.4 0.8 0.99 1.4"}
INIT=${INIT:-10000}
BLOCKS=${BLOCKS:-1000}
SCANS=${SCANS:-100000}
WRITERS=${WRITERS:-32}
READERS=${READERS:-16}
FIG6_MDBX_WRITERS=${FIG6_MDBX_WRITERS:-1}
RETRY_INSERTIONS=${RETRY_INSERTIONS:-1000000}
FIG5_SYSTEMS=${FIG5_SYSTEMS:-"cmvbt mdbx chain frugal vweaver"}
FIG6_SYSTEMS=${FIG6_SYSTEMS:-"cmvbt mdbx chain frugal"}
FIG7_SYSTEMS=${FIG7_SYSTEMS:-"cmvbt chain frugal"}
SCALE_SYSTEMS=${SCALE_SYSTEMS:-"cmvbt mdbx chain frugal"}
SCALE_PAIRS=${SCALE_PAIRS:-"1:2 2:4 4:8 6:12 7:14 8:16 10:20 12:24 14:28 16:32"}
BLOCK_OPS=1000

if [ "$QUICK" = 1 ]; then
  BLOCKS=${QUICK_BLOCKS:-10}
  SCANS=${QUICK_SCANS:-100}
  RETRY_INSERTIONS=${QUICK_RETRY_INSERTIONS:-10000}
  UPDATE_RATES=${QUICK_UPDATE_RATES:-"10 60 100"}
  SCALE_PAIRS=${QUICK_SCALE_PAIRS:-"1:2 2:4"}
  RUN_TIMEOUT=600
  SLEEP=0
fi

command -v "$NUMACTL" >/dev/null 2>&1 || { echo "error: '$NUMACTL' is required" >&2; exit 2; }
NUMA_HARDWARE=$("$NUMACTL" --hardware 2>/dev/null) || { echo "error: cannot query NUMA topology" >&2; exit 2; }
grep -q '^node 0 cpus:' <<< "$NUMA_HARDWARE" || { echo "error: NUMA node 0 is unavailable" >&2; exit 2; }

if [ "$BIN" = "target/paper/cMVBT" ]; then
  cargo build --profile paper || exit 1
elif [ ! -x "$BIN" ]; then
  echo "error: custom BIN '$BIN' is not executable" >&2
  exit 2
fi
BIN=$(realpath "$BIN")
mkdir -p "$OUT/workloads"
OUT=$(realpath "$OUT")
LOG="$OUT/log.txt"
FAILS="$OUT/failures.txt"

{
  echo "date: $(date -Is)"
  echo "git: $(git rev-parse HEAD 2>/dev/null) ($(git status --porcelain 2>/dev/null | wc -l) uncommitted files)"
  echo "binary: $BIN"
  echo "protocol: file-based generate/load"
  echo "numa: $NUMACTL --cpunodebind=0 --membind=0"
  echo "INIT=$INIT BLOCKS=$BLOCKS BLOCK_OPS=$BLOCK_OPS SCANS=$SCANS WRITERS=$WRITERS READERS=$READERS FIG6_MDBX_WRITERS=$FIG6_MDBX_WRITERS REPEATS=$REPEATS"
  echo "UPDATE_RATES=$UPDATE_RATES SCALE_PAIRS=$SCALE_PAIRS ZIPF_ALPHAS=$ZIPF_ALPHAS"
  lscpu 2>/dev/null | grep -E 'Model name|^CPU\(s\)|NUMA node|Thread|Core|Socket'
  free -g 2>/dev/null | head -2
  echo "$NUMA_HARDWARE"
} > "$OUT/machine.txt"

failures=0
run() { # label csv experiment repeat command...
  local label=$1 csv=$2 experiment=$3 repeat=$4
  shift 4
  local before=0 after rc
  [ -f "$csv" ] && before=$(wc -l < "$csv")
  echo ">> [$label] $NUMACTL --cpunodebind=0 --membind=0 $*" | tee -a "$LOG"
  RESULTS_CSV="$csv" EXPERIMENT="$experiment" REPEAT="$repeat" \
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

workload() {
  local rate=$1 file="$OUT/workloads/$1.dat"
  local operations=$((BLOCKS * BLOCK_OPS)) expected=$(((INIT + BLOCKS * BLOCK_OPS) * 9))
  local size updates remainder inserts deletes tmp rc digest
  if [ -e "$file" ]; then
    size=$(stat -c %s "$file") || return 1
    if [ "$size" -ne "$expected" ]; then
      echo "error: $file is $size bytes; expected $expected for INIT=$INIT BLOCKS=$BLOCKS" >&2
      return 2
    fi
  else
    updates=$((rate * BLOCK_OPS / 100))
    remainder=$((BLOCK_OPS - updates))
    inserts=$((remainder / 2))
    deletes=$((remainder - inserts))
    tmp=$(mktemp "$OUT/workloads/.$rate.dat.XXXXXX") || return 1
    echo ">> generating $file ($INIT initial; $BLOCKS blocks of $inserts inserts, $updates updates, $deletes deletes)" | tee -a "$LOG" >&2
    timeout "$RUN_TIMEOUT" "$NUMACTL" --cpunodebind=0 --membind=0 \
      "$BIN" generate "$tmp" "$INIT" "$BLOCKS" "$inserts" "$updates" "$deletes" 0 \
      >> "$LOG" 2>&1
    rc=$?
    if [ "$rc" -ne 0 ]; then
      echo "generate update-rate $rate failed with rc=$rc (partial file: $tmp)" | tee -a "$FAILS" >&2
      return 1
    fi
    size=$(stat -c %s "$tmp") || return 1
    if [ "$size" -ne "$expected" ]; then
      echo "generate update-rate $rate wrote $size bytes; expected $expected (file: $tmp)" | tee -a "$FAILS" >&2
      return 1
    fi
    mv "$tmp" "$file"
  fi
  digest=$(sha256sum "$file") || return 1
  digest=${digest%% *}
  echo ">> workload u$rate operations=$operations sha256=$digest file=$file" | tee -a "$LOG" >&2
  echo "$file"
}

exp_latency() {
  local rep rate file system
  for rep in $(seq 1 "$REPEATS"); do
    for rate in $UPDATE_RATES; do
      file=$(workload "$rate") || { failures=$((failures + 1)); continue; }
      for system in $FIG5_SYSTEMS; do
        run "fig5/$system/u$rate/#$rep" "$OUT/latency.csv" fig5_scan_latency "$rep" \
          "$BIN" load "$file" false 1 "$SCANS" 0 max fg false false "$INIT" "$system"
      done
    done
  done
}

exp_concurrent() { # gc csv experiment systems [mdbx-writers]
  local gc=$1 csv=$2 experiment=$3 systems=$4 mdbx_writers=${5:-$WRITERS}
  local rep rate file system writers
  for rep in $(seq 1 "$REPEATS"); do
    for rate in $UPDATE_RATES; do
      file=$(workload "$rate") || { failures=$((failures + 1)); continue; }
      for system in $systems; do
        writers=$WRITERS
        [ "$system" = mdbx ] && writers=$mdbx_writers
        run "$experiment/$system/u$rate/#$rep" "$OUT/$csv" "$experiment" "$rep" \
          "$BIN" load "$file" true "$READERS" "$writers" 0 max fg "$gc" false "$INIT" "$system"
      done
    done
  done
}

exp_scalability() {
  local file rep pair readers writers system
  file=$(workload 60) || { failures=$((failures + 1)); return; }
  for rep in $(seq 1 "$REPEATS"); do
    for pair in $SCALE_PAIRS; do
      readers=${pair%%:*}
      writers=${pair##*:}
      for system in $SCALE_SYSTEMS; do
        run "fig8/$system/r$readers-w$writers/#$rep" "$OUT/scalability.csv" fig8_scalability "$rep" \
          "$BIN" load "$file" true "$readers" "$writers" 0 max fg false false "$INIT" "$system"
      done
    done
  done
}

exp_scalability_mdbx_one_writer() {
  local file rep
  file=$(workload 60) || { failures=$((failures + 1)); return; }
  for rep in $(seq 1 "$REPEATS"); do
    run "fig8/mdbx/r$READERS-w1/#$rep" "$OUT/scalability_mdbx_one_writer.csv" \
      fig8_oltp_scalability "$rep" \
      "$BIN" load "$file" true "$READERS" 1 0 max fg false false "$INIT" mdbx
  done
}

exp_retries() {
  local rep alpha
  for rep in $(seq 1 "$REPEATS"); do
    for alpha in $ZIPF_ALPHAS; do
      run "fig9/alpha$alpha/#$rep" "$OUT/retries.csv" fig9_retries "$rep" \
        "$BIN" retry-exp "$WRITERS" "$RETRY_INSERTIONS" "$alpha"
    done
  done
}

[ "$#" -eq 0 ] && set -- all
for experiment in "$@"; do
  case "$experiment" in
    latency) exp_latency ;;
    concurrent) exp_concurrent false concurrent_nogc.csv fig6_throughput_nogc "$FIG6_SYSTEMS" "$WRITERS" ;;
    concurrent-mdbx) exp_concurrent false concurrent_nogc.csv fig6_throughput_nogc mdbx "$FIG6_MDBX_WRITERS" ;;
    gc) exp_concurrent true concurrent_gc.csv fig7_throughput_gc "$FIG7_SYSTEMS" "$WRITERS" ;;
    scalability) exp_scalability ;;
    scalability-mdbx-one-writer) exp_scalability_mdbx_one_writer ;;
    retries) exp_retries ;;
    all)
      exp_latency
      exp_concurrent false concurrent_nogc.csv fig6_throughput_nogc "$FIG6_SYSTEMS" "$WRITERS"
      exp_concurrent true concurrent_gc.csv fig7_throughput_gc "$FIG7_SYSTEMS" "$WRITERS"
      exp_scalability
      exp_retries
      ;;
    *) echo "unknown experiment '$experiment'" >&2; exit 2 ;;
  esac
done

if [ "$failures" -ne 0 ]; then
  echo ">> $failures run(s) failed; see $FAILS" >&2
  exit 1
fi
echo ">> complete: results in $OUT"
