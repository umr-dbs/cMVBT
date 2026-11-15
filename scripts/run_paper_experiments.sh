#!/usr/bin/env bash
# Alternative reproduction of Section 8 / Figures 5-10 with operations generated
# online at execution time (`paper-ycsb`, src/paper/online.rs). The canonical
# script is scripts/run_paper_load_experiments.sh (file replay through `load`).
# Every measured process is restricted to NUMA node 0 for CPUs and memory.
#
#   scripts/run_paper_experiments.sh [latency|concurrent|concurrent-mdbx|gc|scalability|retries|allocations|all ...]
#
# Defaults (what runs without overrides):
#   Figure 5: INIT=10000 random-key initial inserts, LATENCY_OPERATIONS=10M
#     random-key writes by one writer, no GC; then SCANS=100000 whole-version
#     scans of versions drawn uniformly from the measured workload (time travel).
#     Systems cMVBT, Version Chain, Frugal List, vWeaver (libmdbx has no
#     time-travel queries; it refuses historical scans).
#   Figure 6: INIT=10000 initial inserts, THROUGHPUT_OPERATIONS=1M writes,
#     WRITERS=32 writers (libmdbx: MDBX_WRITERS=1), READERS=16 readers scanning
#     the complete freshest snapshot (SCAN_MODE=full, ~INIT records), no GC.
#   Figure 7: the same workload with GC (cMVBT, Version Chain, Frugal List).
#   Figure 8: 60% updates; OLAP (readers, WRITERS writers) and OLTP (writers,
#     READERS readers) thread sweeps with full scans.
#   Figure 9: 1M insertions with uniform access and Zipf alphas 0.1 ... 1.4.
#   Figure 10: cMVBT node allocation/reuse under the OLTP workload with GC.
#   REPEATS=5 repetitions; repetition r uses seed 41+r.
#   DISTRIBUTIONS="uniform zipf:0.1 ..." also runs skewed variants of Figs 6-8/10;
#   the plot script uses only the uniform rows.
#
# Known deviations from the paper's protocol (use the canonical load script when
# exact equivalence matters):
#   - With more than one writer, keys use the "sliding" mode: inserts take the
#     next key above the live range and deletes remove the oldest live key
#     (FIFO); updates pick a live key from the requested distribution. The
#     paper's random insert/delete keys ("random" mode) are only supported with
#     one writer, so Figures 6-8 and 10 use sliding keys; Figure 5 uses random.
#   - Writers draw operations online (exact 1000-operation blocks of the mix)
#     instead of replaying one shared trace; libmdbx in the concurrent-mdbx
#     group replays a generated trace through `load` instead.
#
# All online rows also contain update/insert/delete/scan count, average, p50, p95, p99,
# p99.9 and maximum latency. Writers abort on any failed write, so the online
# throughput counts only successful operations.
#
# Environment overrides:
#   OUT=results/paper-<timestamp> REPEATS=5 BIN=target/paper/cMVBT RUN_TIMEOUT=7200 SLEEP=2
#   UPDATE_RATES="10 20 50 75 90 100" INIT=10000 FIG5_INIT=$INIT FIG6_MDBX_INIT=$INIT
#   WRITERS=32 FIG6_WRITERS=$WRITERS MDBX_WRITERS=1 READERS=16
#   DISTRIBUTIONS="uniform zipf:0.1 zipf:0.4 zipf:0.8 zipf:0.99 zipf:1.4"
#   LATENCY_OPERATIONS=10000000 THROUGHPUT_OPERATIONS=1000000 SCANS=100000
#   SCAN_MODE=full (or range, then SCAN_RANGE consecutive keys; needs SCAN_RANGE <= INIT)
#   FIG5_SYSTEMS="cmvbt chain frugal vweaver" FIG6_SYSTEMS="cmvbt mdbx chain frugal"
#   FIG7_SYSTEMS="cmvbt chain frugal" SCALE_SYSTEMS="cmvbt mdbx chain frugal"
#   FIG5_DISTRIBUTIONS="uniform" (the workload used by the paper's Figure 5)
#   OLAP_LEVELS="1 2 4 6 8 16 32"
#   OLTP_LEVELS="2 4 8 16 32 64" ZIPF_ALPHAS="0 0.1 0.4 0.8 0.99 1.4"
#   NUMACTL=numactl QUICK=1 (small smoke-test sizes, 2 repetitions)
set -uo pipefail
cd "$(dirname "$0")/.."

QUICK=${QUICK:-0}
REPEATS=${REPEATS:-5}
OUT=${OUT:-results/paper-$(date +%Y%m%d-%H%M%S)}
BIN=${BIN:-target/paper/cMVBT}
NUMACTL=${NUMACTL:-numactl}
RUN_TIMEOUT=${RUN_TIMEOUT:-7200}
SLEEP=${SLEEP:-2}
UPDATE_RATES=${UPDATE_RATES:-"10 20 50 75 90 100"}
ZIPF_ALPHAS=${ZIPF_ALPHAS:-"0 0.1 0.4 0.8 0.99 1.4"}
DISTRIBUTIONS=${DISTRIBUTIONS:-"uniform zipf:0.1 zipf:0.4 zipf:0.8 zipf:0.99 zipf:1.4"}
FIG5_DISTRIBUTIONS=${FIG5_DISTRIBUTIONS:-"uniform"}
INIT=${INIT:-10000}
FIG5_INIT=${FIG5_INIT:-$INIT}
FIG6_MDBX_INIT=${FIG6_MDBX_INIT:-$INIT}
WRITERS=${WRITERS:-32}
FIG6_WRITERS=${FIG6_WRITERS:-$WRITERS}
MDBX_WRITERS=${MDBX_WRITERS:-1}
READERS=${READERS:-16}
SCANS=${SCANS:-100000}
SCAN_MODE=${SCAN_MODE:-full}
SCAN_RANGE=${SCAN_RANGE:-1000}
LATENCY_OPERATIONS=${LATENCY_OPERATIONS:-10000000}
THROUGHPUT_OPERATIONS=${THROUGHPUT_OPERATIONS:-1000000}
RETRY_INSERTIONS=${RETRY_INSERTIONS:-1000000}
FIG5_SYSTEMS=${FIG5_SYSTEMS:-"cmvbt chain frugal vweaver"}
FIG6_SYSTEMS=${FIG6_SYSTEMS:-"cmvbt mdbx chain frugal"}
FIG7_SYSTEMS=${FIG7_SYSTEMS:-"cmvbt chain frugal"}
SCALE_SYSTEMS=${SCALE_SYSTEMS:-"cmvbt mdbx chain frugal"}
OLAP_LEVELS=${OLAP_LEVELS:-"1 2 4 6 8 16 32"}
OLTP_LEVELS=${OLTP_LEVELS:-"2 4 8 16 32 64"}

if [ "$QUICK" = 1 ]; then
  REPEATS=${QUICK_REPEATS:-2}
  INIT=2000
  FIG5_INIT=2000
  FIG6_MDBX_INIT=2000
  LATENCY_OPERATIONS=10000
  THROUGHPUT_OPERATIONS=10000
  SCANS=100
  SCAN_RANGE=500
  RETRY_INSERTIONS=10000
  UPDATE_RATES="10 60 100"
  OLAP_LEVELS="1 2"
  OLTP_LEVELS="2 4"
  RUN_TIMEOUT=600
  SLEEP=0
fi

for system in $FIG5_SYSTEMS; do
  if [ "$system" = mdbx ]; then
    echo "error: libmdbx does not support time-travel queries and is not part of Figure 5; remove it from FIG5_SYSTEMS" >&2
    exit 2
  fi
done

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
LEGACY_FIG6_CSV="$OUT/concurrent_nogc.csv"
RETRY_CSV="$OUT/retries.csv"
LOG="$OUT/log.txt"
FAILS="$OUT/failures.txt"

{
  echo "date: $(date -Is)"
  echo "git: $(git rev-parse HEAD 2>/dev/null) ($(git status --porcelain 2>/dev/null | wc -l) uncommitted files)"
  echo "binary: $BIN"
  echo "numa: $NUMACTL --cpunodebind=0 --membind=0"
  echo "INIT=$INIT FIG5_INIT=$FIG5_INIT FIG6_MDBX_INIT=$FIG6_MDBX_INIT DISTRIBUTIONS=$DISTRIBUTIONS FIG5_DISTRIBUTIONS=$FIG5_DISTRIBUTIONS UPDATE_RATES=$UPDATE_RATES FIG6_WRITERS=$FIG6_WRITERS MDBX_WRITERS=$MDBX_WRITERS WRITERS=$WRITERS READERS=$READERS LATENCY_OPERATIONS=$LATENCY_OPERATIONS THROUGHPUT_OPERATIONS=$THROUGHPUT_OPERATIONS SCANS=$SCANS SCAN_MODE=$SCAN_MODE SCAN_RANGE=$SCAN_RANGE REPEATS=$REPEATS"
  echo "FIG5_SYSTEMS=$FIG5_SYSTEMS FIG6_SYSTEMS=$FIG6_SYSTEMS FIG7_SYSTEMS=$FIG7_SYSTEMS SCALE_SYSTEMS=$SCALE_SYSTEMS"
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

online() { # label experiment repeat system distribution theta rate gc operations writers readers historical-scans scan-threads [records key-mode scan-mode]
  local label=$1 experiment=$2 rep=$3 system=$4
  local distribution=$5 theta=$6 rate=$7 gc=$8 operations=$9
  shift 9
  local writers=$1 readers=$2 historical=$3 scan_threads=$4
  local records=${5:-$INIT} key_mode=${6:-sliding} scan_mode=${7:-$SCAN_MODE}
  local effective_scan_range=$SCAN_RANGE
  [ "$scan_mode" = full ] && effective_scan_range=$records
  run "$label" "$CSV" "$BIN" paper-ycsb --experiment "$experiment" --repeat "$rep" \
    --system "$system" --distribution "$distribution" --theta "$theta" --scramble true \
    --update-rate "$rate" --gc "$gc" --records "$records" --key-mode "$key_mode" \
    --operations "$operations" --writers "$writers" --readers "$readers" \
    --historical-scans "$historical" --scan-mode "$scan_mode" \
    --scan-range "$effective_scan_range" --scan-threads "$scan_threads" \
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
    "$distribution" "$theta" "$rate" false "$LATENCY_OPERATIONS" 1 0 "$SCANS" 1 \
    "$FIG5_INIT" random full
}

exp_latency() {
  local saved_distributions=$DISTRIBUTIONS
  DISTRIBUTIONS=$FIG5_DISTRIBUTIONS
  for rep in $(seq 1 "$REPEATS"); do
    for rate in $UPDATE_RATES; do
      for system in $FIG5_SYSTEMS; do
        for_distribution latency_distribution "$rep" "$rate" "$system"
      done
    done
  done
  DISTRIBUTIONS=$saved_distributions
}

exp_concurrent() { # gc experiment [systems records scan-mode]
  local gc=$1 experiment=$2
  local systems=${3:-$FIG6_SYSTEMS}
  local records=${4:-$INIT} scan_mode=${5:-$SCAN_MODE}
  [ "$gc" = true ] && systems=$FIG7_SYSTEMS
  for rep in $(seq 1 "$REPEATS"); do
    for rate in $UPDATE_RATES; do
      for system in $systems; do
        for_distribution concurrent_distribution "$rep" "$rate" "$system" "$gc" "$experiment" "$records" "$scan_mode"
      done
    done
  done
}

concurrent_distribution() {
  local distribution=$1 theta=$2 rep=$3 rate=$4 system=$5 gc=$6 experiment=$7
  local records=${8:-$INIT} scan_mode=${9:-$SCAN_MODE}
  local writers=$WRITERS
  [ "$experiment" = fig6_throughput_nogc ] && writers=$FIG6_WRITERS
  # libmdbx is a single-writer engine; the paper measures it with one writer.
  [ "$system" = mdbx ] && writers=$MDBX_WRITERS
  online "$experiment/$system/$distribution$theta/u$rate/#$rep" "$experiment" "$rep" "$system" \
    "$distribution" "$theta" "$rate" "$gc" "$THROUGHPUT_OPERATIONS" "$writers" "$READERS" 0 1 \
    "$records" sliding "$scan_mode"
}

# Reuse the original paper protocol: generate one binary trace per update rate
# and repetition (seed 41+repetition, deterministic), then replay that exact
# trace through `load`. A workload already present in OUT
# is retained so an MDBX rerun can use byte-for-byte the same input as the other
# systems measured in that directory.
fig6_workload() {
  local rate=$1 rep=$2 blocks expected file tmp updates remainder inserts deletes rc size
  local seed=$((41 + rep))
  if [ $((THROUGHPUT_OPERATIONS % 1000)) -ne 0 ]; then
    echo "error: legacy Figure 6 generation requires THROUGHPUT_OPERATIONS to be divisible by 1000" >&2
    return 2
  fi
  blocks=$((THROUGHPUT_OPERATIONS / 1000))
  expected=$(((FIG6_MDBX_INIT + THROUGHPUT_OPERATIONS) * 9))
  file="$OUT/workloads/i${FIG6_MDBX_INIT}_ops${THROUGHPUT_OPERATIONS}_seed${seed}_u${rate}.dat"
  if [ -e "$file" ]; then
    size=$(stat -c %s "$file") || return 1
    if [ "$size" -ne "$expected" ]; then
      echo "error: existing workload $file is $size bytes; expected $expected for $FIG6_MDBX_INIT initial and $THROUGHPUT_OPERATIONS measured operations" >&2
      return 2
    fi
    echo "$file"
    return
  fi

  updates=$((rate * 10))
  remainder=$((1000 - updates))
  inserts=$((remainder / 2))
  deletes=$((remainder - inserts))
  mkdir -p "$OUT/workloads"
  tmp=$(mktemp "$OUT/workloads/.u$rate.dat.XXXXXX") || return 1
  echo ">> generating $file via generate ($FIG6_MDBX_INIT initial; $blocks blocks of $inserts inserts, $updates updates, $deletes deletes; seed $seed)" | tee -a "$LOG" >&2
  timeout "$RUN_TIMEOUT" "$NUMACTL" --cpunodebind=0 --membind=0 \
    "$BIN" generate "$tmp" "$FIG6_MDBX_INIT" "$blocks" "$inserts" "$updates" "$deletes" 0 "$seed" \
    >> "$LOG" 2>&1
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "generate Figure 6 update-rate $rate failed with rc=$rc (partial file: $tmp)" | tee -a "$FAILS" >&2
    return 1
  fi
  size=$(stat -c %s "$tmp") || return 1
  if [ "$size" -ne "$expected" ]; then
    echo "generate Figure 6 update-rate $rate wrote $size bytes; expected $expected (file: $tmp)" | tee -a "$FAILS" >&2
    return 1
  fi
  mv "$tmp" "$file"
  echo "$file"
}

exp_concurrent_mdbx() {
  local rep rate workload digest
  for rep in $(seq 1 "$REPEATS"); do
    for rate in $UPDATE_RATES; do
      workload=$(fig6_workload "$rate" "$rep") || {
        failures=$((failures + 1))
        continue
      }
      digest=$(sha256sum "$workload") || {
        failures=$((failures + 1))
        continue
      }
      digest=${digest%% *}
      echo ">> Figure 6 workload u$rate sha256=$digest file=$workload" | tee -a "$LOG"
      RESULTS_CSV="$LEGACY_FIG6_CSV" EXPERIMENT=fig6_throughput_nogc REPEAT="$rep" \
        run "fig6-mdbx/load/u$rate/#$rep" "$LEGACY_FIG6_CSV" \
        "$BIN" load "$workload" true "$READERS" "$MDBX_WRITERS" 0 max fg false false \
        "$FIG6_MDBX_INIT" mdbx
    done
  done
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
    concurrent-mdbx) exp_concurrent_mdbx ;;
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
