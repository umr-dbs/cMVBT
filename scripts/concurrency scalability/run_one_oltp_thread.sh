#!/usr/bin/env bash
# Run only the missing Figure 8 OLTP-scalability point.
#
# Protocol: generate one 60%-update trace with 10K initial keys and 1M
# operations, then replay the identical bytes through `load` for every system.
# The 16 OLAP readers stay fixed while the OLTP writer count is exactly one.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/../.." && pwd)
runner="$repo_root/scripts/run_paper_load_experiments.sh"

repeats=${REPEATS:-1}
systems=${SYSTEMS:-"cmvbt mdbx chain frugal"}
output=${OUT:-"$script_dir/results/fig8-one-oltp-$(date +%Y%m%d-%H%M%S)"}

if [ ! -x "$runner" ]; then
  echo "error: canonical generate/load runner is not executable: $runner" >&2
  exit 2
fi

OUT="$output" \
REPEATS="$repeats" \
INIT=10000 \
BLOCKS=1000 \
READERS=16 \
SCALE_SYSTEMS="$systems" \
SCALE_PAIRS="16:1" \
"$runner" scalability

echo
echo "Figure 8 one-writer measurements: $output/scalability.csv"
echo "Plot with:"
printf "python3 %q --base %q --one-writer %q\n" \
  "$script_dir/plot_figure8.py" \
  "$script_dir/oltp_60%ups_concurrent_degree__new.csv" \
  "$output/scalability.csv"
