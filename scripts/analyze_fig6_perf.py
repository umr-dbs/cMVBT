#!/usr/bin/env python3
"""Normalize and summarize a run produced by run_fig6_perf.sh."""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path


PAIR_RATES = {
    "cache_miss_rate_pct": ("cache-misses", "cache-references"),
    "l1d_miss_rate_pct": ("L1-dcache-load-misses", "L1-dcache-loads"),
    "dtlb_miss_rate_pct": ("dTLB-load-misses", "dTLB-loads"),
    "branch_miss_rate_pct": ("branch-misses", "branches"),
}

NORMALIZED_EVENTS = (
    "cycles",
    "instructions",
    "cache-references",
    "cache-misses",
    "L1-dcache-loads",
    "L1-dcache-load-misses",
    "dTLB-loads",
    "dTLB-load-misses",
    "page-faults",
    "minor-faults",
    "major-faults",
)


def number(value: str) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def mean(values: list[float]) -> float | None:
    return statistics.mean(values) if values else None


def stdev(values: list[float]) -> float | None:
    return statistics.stdev(values) if len(values) > 1 else None


def formatted(value: float | None, digits: int = 3) -> str:
    return "n.a." if value is None else f"{value:.{digits}f}"


def read_run(run: Path) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    benchmark_path = run / "benchmark.csv"
    perf_path = run / "perf-events.csv"
    if not benchmark_path.exists() or not perf_path.exists():
        raise SystemExit(f"expected {benchmark_path} and {perf_path}")
    with benchmark_path.open(newline="") as source:
        benchmark = list(csv.DictReader(source))
    with perf_path.open(newline="") as source:
        perf = list(csv.DictReader(source))
    return benchmark, perf


def normalized_runs(
    benchmark: list[dict[str, str]], perf: list[dict[str, str]]
) -> list[dict[str, object]]:
    benchmark_by_experiment = {row["experiment"]: row for row in benchmark}
    events: dict[str, dict[str, float]] = defaultdict(dict)
    groups: dict[str, str] = {}
    for row in perf:
        value = number(row["value"])
        if value is not None:
            events[row["experiment"]][row["event"]] = value
        groups[row["experiment"]] = row["event_group"]

    output: list[dict[str, object]] = []
    for experiment, event_values in events.items():
        source = benchmark_by_experiment.get(experiment)
        if source is None:
            continue
        scan_records = float(source["scan_records"])
        scan_count = float(source["scan_count"])
        operations = float(source["operations"])
        row: dict[str, object] = {
            "experiment": experiment,
            "system": source["system"],
            "update_rate": int(source["update_rate"]),
            "repeat": int(source["repeat"]),
            "event_group": groups[experiment],
            "scan_ops_per_s": float(source["scan_ops_per_s"]),
            "scan_avg_ms": float(source["scan_avg_ns"]) / 1e6,
            "oltp_ops_per_s": float(source["oltp_ops_per_s"]),
            "scan_count": scan_count,
            "scan_records": scan_records,
            "operations": operations,
        }
        for metric, (numerator, denominator) in PAIR_RATES.items():
            top = event_values.get(numerator)
            bottom = event_values.get(denominator)
            row[metric] = 100.0 * top / bottom if top is not None and bottom else None
        cycles = event_values.get("cycles")
        instructions = event_values.get("instructions")
        row["ipc"] = instructions / cycles if instructions is not None and cycles else None
        for event in NORMALIZED_EVENTS:
            value = event_values.get(event)
            safe = event.replace("-", "_")
            row[f"{safe}_per_scan"] = value / scan_count if value is not None and scan_count else None
            row[f"{safe}_per_scanned_record"] = (
                value / scan_records if value is not None and scan_records else None
            )
            row[f"{safe}_per_write"] = value / operations if value is not None and operations else None
        output.append(row)
    return sorted(
        output,
        key=lambda row: (
            str(row["event_group"]),
            str(row["system"]),
            int(row["update_rate"]),
            int(row["repeat"]),
        ),
    )


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fields: list[str] = []
    for row in rows:
        for field in row:
            if field not in fields:
                fields.append(field)
    with path.open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def summaries(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, int, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["system"]), int(row["update_rate"]), str(row["event_group"]))].append(row)
    metrics = (
        "scan_ops_per_s",
        "scan_avg_ms",
        "oltp_ops_per_s",
        "ipc",
        "cache_miss_rate_pct",
        "l1d_miss_rate_pct",
        "dtlb_miss_rate_pct",
        "cycles_per_scanned_record",
        "instructions_per_scanned_record",
        "cache_misses_per_scanned_record",
        "L1_dcache_load_misses_per_scanned_record",
        "dTLB_load_misses_per_scanned_record",
        "page_faults_per_write",
        "minor_faults_per_write",
        "major_faults_per_write",
    )
    output: list[dict[str, object]] = []
    for (system, update_rate, group), group_rows in sorted(grouped.items()):
        summary: dict[str, object] = {
            "system": system,
            "update_rate": update_rate,
            "event_group": group,
            "runs": len(group_rows),
        }
        for metric in metrics:
            values = [
                float(row[metric])
                for row in group_rows
                if row.get(metric) is not None
            ]
            summary[f"{metric}_mean"] = mean(values)
            summary[f"{metric}_stdev"] = stdev(values)
        output.append(summary)
    return output


def metric_mean(
    summary: list[dict[str, object]], system: str, update_rate: int, group: str, metric: str
) -> float | None:
    for row in summary:
        if (
            row["system"] == system
            and row["update_rate"] == update_rate
            and row["event_group"] == group
        ):
            value = row.get(f"{metric}_mean")
            return float(value) if value is not None else None
    return None


def overall_benchmark(
    benchmark: list[dict[str, str]], system: str, update_rate: int, metric: str
) -> tuple[float | None, float | None]:
    values = [
        float(row[metric])
        for row in benchmark
        if row["system"] == system and int(row["update_rate"]) == update_rate
    ]
    return mean(values), stdev(values)


def write_report(
    path: Path,
    benchmark: list[dict[str, str]],
    perf: list[dict[str, str]],
    summary: list[dict[str, object]],
) -> None:
    lines = [
        "# Figure 6 perf analysis",
        "",
        f"Complete input: {len(benchmark)} benchmark runs and {len(perf)} counter rows.",
        "Counters cover the complete concurrent phase (writers and readers together); they are not reader-only counters.",
        "",
        "## Throughput and scan latency",
        "",
        "| System | Updates | Scan throughput mean ± SD (scans/s) | Mean scan latency (ms) | OLTP throughput (ops/s) |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for system in ("cmvbt", "mdbx"):
        for update_rate in (10, 100):
            scan_mean, scan_sd = overall_benchmark(benchmark, system, update_rate, "scan_ops_per_s")
            latency_mean, _ = overall_benchmark(benchmark, system, update_rate, "scan_avg_ns")
            oltp_mean, _ = overall_benchmark(benchmark, system, update_rate, "oltp_ops_per_s")
            latency_ms = latency_mean / 1e6 if latency_mean is not None else None
            lines.append(
                f"| {system} | {update_rate}% | {formatted(scan_mean, 1)} ± {formatted(scan_sd, 1)} | "
                f"{formatted(latency_ms, 3)} | {formatted(oltp_mean, 0)} |"
            )

    lines.extend(
        [
            "",
            "## Cache and memory counters",
            "",
            "| System | Updates | Cache miss rate | L1D miss rate | dTLB miss rate | Page faults/write |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for system in ("cmvbt", "mdbx"):
        for update_rate in (10, 100):
            lines.append(
                f"| {system} | {update_rate}% | "
                f"{formatted(metric_mean(summary, system, update_rate, 'cache', 'cache_miss_rate_pct'))}% | "
                f"{formatted(metric_mean(summary, system, update_rate, 'cache', 'l1d_miss_rate_pct'))}% | "
                f"{formatted(metric_mean(summary, system, update_rate, 'memory', 'dtlb_miss_rate_pct'))}% | "
                f"{formatted(metric_mean(summary, system, update_rate, 'memory', 'page_faults_per_write'))} |"
            )

    mdbx_scan_10, _ = overall_benchmark(benchmark, "mdbx", 10, "scan_ops_per_s")
    mdbx_scan_100, _ = overall_benchmark(benchmark, "mdbx", 100, "scan_ops_per_s")
    scan_change = (
        100.0 * (mdbx_scan_100 / mdbx_scan_10 - 1.0)
        if mdbx_scan_10 and mdbx_scan_100
        else None
    )
    lines.extend(
        [
            "",
            "## Assessment",
            "",
            f"- libmdbx scan throughput changes by only {formatted(scan_change, 2)}% from 10% to 100% updates.",
            "- libmdbx is slightly faster than cMVBT in scan throughput in this run; the published Figure 6 gap is not reproduced.",
            "- libmdbx has lower measured process-wide generic cache- and L1D-miss rates than cMVBT at both update rates. This evidence does not support the proposed reader cache-miss explanation.",
            "- libmdbx has about five process-wide page faults per measured write, overwhelmingly minor faults. This is consistent with substantial copy-on-write/mapping activity and its very low OLTP throughput, but it does not produce lower scan throughput here.",
            "- Direct LLC events are unavailable on this AMD system (`<not supported>` in the raw files), so the generic cache events are the available proxy.",
            "- A reader-only conclusion requires counters opened inside the reader threads; the present process-wide counters mix reader and writer work.",
        ]
    )
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run", type=Path, help="run directory containing benchmark.csv and perf-events.csv")
    args = parser.parse_args()
    benchmark, perf = read_run(args.run)
    runs = normalized_runs(benchmark, perf)
    summary = summaries(runs)
    write_csv(args.run / "analysis-runs.csv", runs)
    write_csv(args.run / "analysis-summary.csv", summary)
    write_report(args.run / "analysis.md", benchmark, perf, summary)
    print(f"wrote {args.run / 'analysis-runs.csv'}")
    print(f"wrote {args.run / 'analysis-summary.csv'}")
    print(f"wrote {args.run / 'analysis.md'}")


if __name__ == "__main__":
    main()
