#!/usr/bin/env bash
# Measure Figure 6 hardware counters in the scan threads only.
#
# Examples:
#   scripts/run_fig6_reader_perf.sh
#   QUICK=1 scripts/run_fig6_reader_perf.sh
#   PROTOCOL=current REPEATS=5 scripts/run_fig6_reader_perf.sh
#
# Each event group is a separate benchmark execution. Unlike run_fig6_perf.sh,
# this runner does not wrap the process in `perf stat`: paper-ycsb opens and
# closes per-thread counters around the reader loop itself, which prevents
# writers and initial loading from contaminating the measurements.
set -uo pipefail

cd "$(dirname "$0")/.."

PROTOCOL=${PROTOCOL:-pdf}
SYSTEMS=${SYSTEMS:-"cmvbt mdbx"}
UPDATE_RATES=${UPDATE_RATES:-"10 100"}
REPEATS=${REPEATS:-3}
OPERATIONS=${OPERATIONS:-1000000}
WRITERS=${WRITERS:-1}
READERS=${READERS:-16}
SEED=${SEED:-42}
QUICK=${QUICK:-0}
EVENT_GROUPS=${EVENT_GROUPS:-"core cache memory"}
BIN=${BIN:-target/paper/cMVBT}
NUMACTL=${NUMACTL:-numactl}
RUN_TIMEOUT=${RUN_TIMEOUT:-7200}
OUT=${OUT:-scripts/perf/fig6-reader-perf-$(date +%Y%m%d-%H%M%S)}

case "$PROTOCOL" in
  pdf)
    RECORDS=${RECORDS:-10000}
    SCAN_RANGE=${SCAN_RANGE:-$RECORDS}
    SCAN_MODE=${SCAN_MODE:-full}
    ;;
  current)
    RECORDS=${RECORDS:-2000000}
    SCAN_RANGE=${SCAN_RANGE:-100000}
    SCAN_MODE=${SCAN_MODE:-range}
    ;;
  *)
    echo "error: PROTOCOL must be 'pdf' or 'current'" >&2
    exit 2
    ;;
esac

if [ "$QUICK" = 1 ]; then
  OPERATIONS=${QUICK_OPERATIONS:-10000}
  REPEATS=1
  EVENT_GROUPS=${QUICK_EVENT_GROUPS:-"core cache memory"}
fi

command -v perf >/dev/null 2>&1 || {
  echo "error: perf is required to check hardware-counter permissions" >&2
  exit 2
}
command -v "$NUMACTL" >/dev/null 2>&1 || {
  echo "error: '$NUMACTL' is required for the same NUMA placement as the paper runner" >&2
  exit 2
}
if ! perf stat -e task-clock -- true >/dev/null 2>&1; then
  echo "error: per-thread perf counters are unavailable (check kernel.perf_event_paranoid)" >&2
  exit 2
fi

if [ "$BIN" = "target/paper/cMVBT" ]; then
  cargo build --profile paper || exit 1
elif [ ! -x "$BIN" ]; then
  echo "error: custom BIN '$BIN' is not executable" >&2
  exit 2
fi
BIN=$(realpath "$BIN")
mkdir -p "$OUT"
OUT=$(realpath "$OUT")

BENCHMARK_CSV="$OUT/benchmark.csv"
READER_PERF_CSV="$OUT/reader-perf-events.csv"
LOG="$OUT/log.txt"
FAILURES="$OUT/failures.txt"

for group in $EVENT_GROUPS; do
  case "$group" in
    core|cache|memory) ;;
    *)
      echo "error: unknown event group '$group' (use core, cache, or memory)" >&2
      exit 2
      ;;
  esac
done

{
  echo "date: $(date -Is)"
  echo "git: $(git rev-parse HEAD 2>/dev/null) ($(git status --porcelain 2>/dev/null | wc -l) uncommitted files)"
  echo "binary: $BIN"
  echo "measurement_scope: reader threads only"
  echo "protocol: $PROTOCOL"
  echo "systems: $SYSTEMS"
  echo "update_rates: $UPDATE_RATES"
  echo "event_groups: $EVENT_GROUPS"
  echo "records=$RECORDS operations=$OPERATIONS writers=$WRITERS readers=$READERS scan_range=$SCAN_RANGE scan_mode=$SCAN_MODE repeats=$REPEATS seed=$SEED"
  perf --version
  lscpu 2>/dev/null | grep -E 'Model name|^CPU\(s\)|NUMA node|Thread|Core|Socket'
  "$NUMACTL" --hardware 2>/dev/null
} > "$OUT/manifest.txt"

failures=0
for repeat in $(seq 1 "$REPEATS"); do
  for rate in $UPDATE_RATES; do
    for system in $SYSTEMS; do
      for group in $EVENT_GROUPS; do
        experiment="fig6_reader_perf_${PROTOCOL}_${group}_${system}_u${rate}_r${repeat}"
        run_log="$OUT/${system}-u${rate}-r${repeat}-${group}.log"
        echo ">> $experiment" | tee -a "$LOG"
        CMVBT_READER_PERF_GROUP="$group" \
        CMVBT_READER_PERF_CSV="$READER_PERF_CSV" \
        timeout "$RUN_TIMEOUT" \
          "$NUMACTL" --cpunodebind=0 --membind=0 \
          "$BIN" paper-ycsb \
          --experiment "$experiment" --repeat "$repeat" \
          --system "$system" --distribution uniform --theta 0 --scramble true \
          --update-rate "$rate" --gc false --records "$RECORDS" --key-mode sliding \
          --operations "$OPERATIONS" --writers "$WRITERS" --readers "$READERS" \
          --historical-scans 0 --scan-mode "$SCAN_MODE" --scan-range "$SCAN_RANGE" \
          --scan-threads 1 --seed "$SEED" --csv "$BENCHMARK_CSV" \
          > "$run_log" 2>&1
        rc=$?
        if [ "$rc" -ne 0 ]; then
          echo "$experiment rc=$rc (see $run_log)" | tee -a "$FAILURES"
          failures=$((failures + 1))
          continue
        fi
        tail -n 1 "$run_log" | tee -a "$LOG"
      done
    done
  done
done

if [ "$failures" -ne 0 ]; then
  echo "completed with $failures failed runs; see $FAILURES" >&2
  exit 1
fi

echo "wrote $BENCHMARK_CSV"
echo "wrote $READER_PERF_CSV"
python3 scripts/analyze_fig6_reader_perf.py "$OUT" || {
  echo "warning: automatic analysis failed; raw measurements are complete" >&2
}
echo "reader-only counters, analysis, and per-run logs: $OUT"
