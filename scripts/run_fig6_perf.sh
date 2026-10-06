#!/usr/bin/env bash
# Profile the measured concurrent phase of Figure 6 with Linux perf.
#
# Examples:
#   scripts/run_fig6_perf.sh
#   PROTOCOL=current REPEATS=5 UPDATE_RATES="10 100" scripts/run_fig6_perf.sh
#   QUICK=1 scripts/run_fig6_perf.sh
#
# PROTOCOL=pdf uses the workload described in the submitted PDF: 10K live
# records and full-snapshot scans. PROTOCOL=current uses the repository's
# current reproduction protocol: 2M live records and 100K-record range scans.
# Every event group is a separate benchmark execution to avoid heavy counter
# multiplexing. Counter values are comparable between systems within a group,
# but must not be added across groups.
set -uo pipefail

cd "$(dirname "$0")/.."

PROTOCOL=${PROTOCOL:-pdf}
SYSTEMS=${SYSTEMS:-"cmvbt mdbx"}
UPDATE_RATES=${UPDATE_RATES:-"10 100"}
REPEATS=${REPEATS:-3}
OPERATIONS=${OPERATIONS:-1000000}
WRITERS=${WRITERS:-32}
READERS=${READERS:-16}
SEED=${SEED:-42}
QUICK=${QUICK:-0}
EVENT_GROUPS=${EVENT_GROUPS:-"core cache memory"}
BIN=${BIN:-target/paper/cMVBT}
NUMACTL=${NUMACTL:-numactl}
RUN_TIMEOUT=${RUN_TIMEOUT:-7200}
OUT=${OUT:-results/fig6-perf-$(date +%Y%m%d-%H%M%S)}

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
  OPERATIONS=10000
  REPEATS=1
  EVENT_GROUPS=${QUICK_EVENT_GROUPS:-"core cache memory"}
fi

command -v perf >/dev/null 2>&1 || { echo "error: perf is not installed" >&2; exit 2; }
command -v "$NUMACTL" >/dev/null 2>&1 || {
  echo "error: '$NUMACTL' is required for the same NUMA placement as the paper runner" >&2
  exit 2
}

if ! perf stat -e task-clock -- true >/dev/null 2>&1; then
  echo "error: perf counters are unavailable for this user (check kernel.perf_event_paranoid)" >&2
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
PERF_CSV="$OUT/perf-events.csv"
LOG="$OUT/log.txt"
FAILURES="$OUT/failures.txt"
fifo_dir=
cleanup() {
  if [ -n "$fifo_dir" ] && [ -d "$fifo_dir" ]; then
    rm -rf "$fifo_dir"
  fi
}
trap cleanup EXIT INT TERM

printf '%s\n' \
  'experiment,system,update_rate,repeat,event_group,value,unit,event,event_runtime,enabled_percent,metric_value,metric_unit' \
  > "$PERF_CSV"

{
  echo "date: $(date -Is)"
  echo "git: $(git rev-parse HEAD 2>/dev/null) ($(git status --porcelain 2>/dev/null | wc -l) uncommitted files)"
  echo "binary: $BIN"
  echo "protocol: $PROTOCOL"
  echo "systems: $SYSTEMS"
  echo "update_rates: $UPDATE_RATES"
  echo "event_groups: $EVENT_GROUPS"
  echo "records=$RECORDS operations=$OPERATIONS writers=$WRITERS readers=$READERS scan_range=$SCAN_RANGE scan_mode=$SCAN_MODE repeats=$REPEATS seed=$SEED"
  perf --version
  lscpu 2>/dev/null | grep -E 'Model name|^CPU\(s\)|NUMA node|Thread|Core|Socket'
  "$NUMACTL" --hardware 2>/dev/null
} > "$OUT/manifest.txt"

events_for() {
  case "$1" in
    core)
      echo 'task-clock,cycles,instructions,branches,branch-misses,context-switches,cpu-migrations'
      ;;
    cache)
      echo 'cache-references,cache-misses,L1-dcache-loads,L1-dcache-load-misses'
      ;;
    memory)
      echo 'LLC-loads,LLC-load-misses,dTLB-loads,dTLB-load-misses,page-faults,minor-faults,major-faults'
      ;;
    *)
      echo "error: unknown event group '$1' (use core, cache, or memory)" >&2
      return 2
      ;;
  esac
}

append_perf_csv() {
  local raw=$1 experiment=$2 sys_name=$3 rate=$4 repeat=$5 group=$6
  awk -F';' -v OFS=',' \
    -v experiment="$experiment" -v sys_name="$sys_name" -v rate="$rate" \
    -v repeat="$repeat" -v group="$group" '
      $3 != "" && $1 !~ /^#/ {
        for (i = 1; i <= 8; i++) gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i)
        print experiment, sys_name, rate, repeat, group, $1, $2, $3, $4, $5, $6, $7
      }
    ' "$raw" >> "$PERF_CSV"
}

failures=0
for repeat in $(seq 1 "$REPEATS"); do
  for rate in $UPDATE_RATES; do
    for system in $SYSTEMS; do
      for group in $EVENT_GROUPS; do
        events=$(events_for "$group") || exit 2
        experiment="fig6_perf_${PROTOCOL}_${group}_${system}_u${rate}_r${repeat}"
        prefix="$OUT/${system}-u${rate}-r${repeat}-${group}"
        raw="$prefix.perf.csv"
        run_log="$prefix.log"
        fifo_dir=$(mktemp -d /tmp/cmvbt-perf.XXXXXX)
        control_fifo="$fifo_dir/control"
        ack_fifo="$fifo_dir/ack"
        mkfifo "$control_fifo" "$ack_fifo"

        echo ">> $experiment events=$events" | tee -a "$LOG"
        CMVBT_PERF_CONTROL_FIFO="$control_fifo" \
        CMVBT_PERF_ACK_FIFO="$ack_fifo" \
        timeout "$RUN_TIMEOUT" \
          perf stat -D -1 --control "fifo:$control_fifo,$ack_fifo" \
          -x ';' --no-big-num -e "$events" -o "$raw" -- \
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
        rm -rf "$fifo_dir"
        fifo_dir=

        if [ "$rc" -ne 0 ]; then
          echo "$experiment rc=$rc (see $run_log)" | tee -a "$FAILURES"
          failures=$((failures + 1))
          continue
        fi
        if ! append_perf_csv "$raw" "$experiment" "$system" "$rate" "$repeat" "$group"; then
          echo "$experiment could not parse $raw" | tee -a "$FAILURES"
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
echo "wrote $PERF_CSV"
echo "raw perf output and per-run logs: $OUT"
