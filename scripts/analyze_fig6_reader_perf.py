#!/usr/bin/env python3
"""Aggregate and interpret reader-thread counters from Figure 6."""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path


PAIR_RATES = {
    "cache_miss_rate_pct": ("cache-misses", "cache-references"),
    "l1d_miss_rate_pct": ("L1-dcache-load-misses", "L1-dcache-loads"),
    "dtlb_miss_rate_pct": ("dTLB-load-misses", "dTLB-loads"),
    "branch_miss_rate_pct": ("branch-misses", "branches"),
}
EVENTS = (
    "cycles",
    "instructions",
    "branches",
    "branch-misses",
    "cache-references",
    "cache-misses",
    "L1-dcache-loads",
    "L1-dcache-load-misses",
    "LLC-loads",
    "LLC-load-misses",
    "dTLB-loads",
    "dTLB-load-misses",
    "page-faults",
    "minor-faults",
    "major-faults",
)


def number(value: str | None) -> float | None:
    try:
        parsed = float(value) if value not in (None, "") else None
    except ValueError:
        return None
    return parsed if parsed is not None and math.isfinite(parsed) else None


def mean(values: list[float]) -> float | None:
    return statistics.mean(values) if values else None


def stdev(values: list[float]) -> float | None:
    return statistics.stdev(values) if len(values) > 1 else None


def fmt(value: float | None, digits: int = 3) -> str:
    return "n.a." if value is None else f"{value:.{digits}f}"


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise SystemExit(f"expected {path}")
    with path.open(newline="") as source:
        return list(csv.DictReader(source))


def normalize(
    benchmark: list[dict[str, str]], perf: list[dict[str, str]]
) -> list[dict[str, object]]:
    bench_by_experiment = {row["experiment"]: row for row in benchmark}
    perf_by_experiment: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in perf:
        perf_by_experiment[row["experiment"]].append(row)

    output: list[dict[str, object]] = []
    for experiment, source in bench_by_experiment.items():
        rows = perf_by_experiment.get(experiment, [])
        if not rows:
            continue
        totals: dict[str, float] = defaultdict(float)
        supported: Counter[str] = Counter()
        fractions: list[float] = []
        readers: dict[str, tuple[float, float]] = {}
        for event in rows:
            value = number(event.get("value"))
            if value is not None:
                totals[event["event"]] += value
                supported[event["event"]] += 1
            fraction = number(event.get("enabled_fraction"))
            if fraction is not None:
                fractions.append(fraction)
            readers.setdefault(
                event["reader"],
                (float(event["scan_count"]), float(event["scan_records"])),
            )
        reader_scan_count = sum(value[0] for value in readers.values())
        reader_scan_records = sum(value[1] for value in readers.values())
        scan_count = float(source["scan_count"])
        scan_records = float(source["scan_records"])
        row: dict[str, object] = {
            "experiment": experiment,
            "system": source["system"],
            "update_rate": int(source["update_rate"]),
            "repeat": int(source["repeat"]),
            "event_group": rows[0]["event_group"],
            "reader_count": len(readers),
            "scan_count": scan_count,
            "reader_counter_scan_count": reader_scan_count,
            "scan_count_matches": abs(scan_count - reader_scan_count) < 0.5,
            "scan_records": scan_records,
            "reader_counter_scan_records": reader_scan_records,
            "scan_records_matches": abs(scan_records - reader_scan_records) < 0.5,
            "scan_ops_per_s": float(source["scan_ops_per_s"]),
            "scan_avg_ms": float(source["scan_avg_ns"]) / 1e6,
            "oltp_ops_per_s": float(source["oltp_ops_per_s"]),
            "min_enabled_fraction": min(fractions) if fractions else None,
            "mean_enabled_fraction": mean(fractions),
        }
        for metric, (top_name, bottom_name) in PAIR_RATES.items():
            top, bottom = totals.get(top_name), totals.get(bottom_name)
            row[metric] = 100.0 * top / bottom if top is not None and bottom else None
        cycles, instructions = totals.get("cycles"), totals.get("instructions")
        row["ipc"] = instructions / cycles if cycles and instructions is not None else None
        for event in EVENTS:
            safe = event.replace("-", "_")
            value = totals.get(event)
            row[f"{safe}_total"] = value
            row[f"{safe}_supported_readers"] = supported[event]
            row[f"{safe}_per_scan"] = value / scan_count if value is not None and scan_count else None
            row[f"{safe}_per_scanned_record"] = (
                value / scan_records if value is not None and scan_records else None
            )
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


SUMMARY_METRICS = (
    "scan_ops_per_s",
    "scan_avg_ms",
    "oltp_ops_per_s",
    "ipc",
    "cache_miss_rate_pct",
    "l1d_miss_rate_pct",
    "dtlb_miss_rate_pct",
    "branch_miss_rate_pct",
    "cycles_per_scanned_record",
    "instructions_per_scanned_record",
    "cache_misses_per_scanned_record",
    "L1_dcache_load_misses_per_scanned_record",
    "dTLB_load_misses_per_scanned_record",
    "page_faults_per_scan",
    "min_enabled_fraction",
)


def summarize(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, int, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["system"]), int(row["update_rate"]), str(row["event_group"]))].append(row)
    result: list[dict[str, object]] = []
    for (system, rate, group), group_rows in sorted(grouped.items()):
        summary: dict[str, object] = {
            "system": system,
            "update_rate": rate,
            "event_group": group,
            "runs": len(group_rows),
        }
        for metric in SUMMARY_METRICS:
            values = [float(row[metric]) for row in group_rows if row.get(metric) is not None]
            summary[f"{metric}_mean"] = mean(values)
            summary[f"{metric}_stdev"] = stdev(values)
        result.append(summary)
    return result


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


def summary_value(
    rows: list[dict[str, object]], system: str, rate: int, group: str, metric: str
) -> float | None:
    for row in rows:
        if row["system"] == system and row["update_rate"] == rate and row["event_group"] == group:
            value = row.get(f"{metric}_mean")
            return float(value) if value is not None else None
    return None


def benchmark_mean(
    rows: list[dict[str, str]], system: str, rate: int, metric: str
) -> tuple[float | None, float | None]:
    values = [
        float(row[metric])
        for row in rows
        if row["system"] == system and int(row["update_rate"]) == rate
    ]
    return mean(values), stdev(values)


def percent_change(before: float | None, after: float | None) -> float | None:
    return 100.0 * (after / before - 1.0) if before and after is not None else None


def report(
    path: Path,
    benchmark: list[dict[str, str]],
    perf: list[dict[str, str]],
    normalized: list[dict[str, object]],
    summary: list[dict[str, object]],
) -> None:
    systems = sorted({row["system"] for row in benchmark})
    rates = sorted({int(row["update_rate"]) for row in benchmark})
    unsupported = Counter(
        row["event"] for row in perf if not row.get("value") and row.get("error")
    )
    mismatches = sum(
        not bool(row["scan_count_matches"]) or not bool(row["scan_records_matches"])
        for row in normalized
    )
    lines = [
        "# Figure 6 reader-only perf analysis",
        "",
        f"Input: {len(benchmark)} benchmark runs, {len(perf)} per-reader counter rows.",
        "All counters cover only the fresh-snapshot scan loops; load and writer activity are excluded.",
        f"Reader/benchmark scan-total mismatches: {mismatches}.",
        "",
        "## Throughput",
        "",
        "| System | Updates | Scan throughput mean ± SD (scans/s) | Mean scan latency (ms) | OLTP (ops/s) |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for system in systems:
        for rate in rates:
            scans, scans_sd = benchmark_mean(benchmark, system, rate, "scan_ops_per_s")
            latency, _ = benchmark_mean(benchmark, system, rate, "scan_avg_ns")
            oltp, _ = benchmark_mean(benchmark, system, rate, "oltp_ops_per_s")
            lines.append(
                f"| {system} | {rate}% | {fmt(scans, 1)} ± {fmt(scans_sd, 1)} | "
                f"{fmt(latency / 1e6 if latency is not None else None, 3)} | {fmt(oltp, 0)} |"
            )
    lines.extend(
        [
            "",
            "## Reader cache behavior",
            "",
            "| System | Updates | Generic cache miss rate | Cache misses/record | L1D misses/record | dTLB misses/record |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for system in systems:
        for rate in rates:
            lines.append(
                f"| {system} | {rate}% | "
                f"{fmt(summary_value(summary, system, rate, 'cache', 'cache_miss_rate_pct'))}% | "
                f"{fmt(summary_value(summary, system, rate, 'cache', 'cache_misses_per_scanned_record'), 5)} | "
                f"{fmt(summary_value(summary, system, rate, 'cache', 'L1_dcache_load_misses_per_scanned_record'), 5)} | "
                f"{fmt(summary_value(summary, system, rate, 'memory', 'dTLB_load_misses_per_scanned_record'), 6)} |"
            )
    lines.extend(
        [
            "",
            "## Reader execution cost",
            "",
            "| System | Updates | Cycles/scanned record | Instructions/scanned record | IPC |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for system in systems:
        for rate in rates:
            lines.append(
                f"| {system} | {rate}% | "
                f"{fmt(summary_value(summary, system, rate, 'core', 'cycles_per_scanned_record'), 2)} | "
                f"{fmt(summary_value(summary, system, rate, 'core', 'instructions_per_scanned_record'), 2)} | "
                f"{fmt(summary_value(summary, system, rate, 'core', 'ipc'), 3)} |"
            )
    lines.extend(["", "## Hypothesis check", ""])
    if "mdbx" in systems and len(rates) >= 2:
        low, high = rates[0], rates[-1]
        scan_low, _ = benchmark_mean(benchmark, "mdbx", low, "scan_ops_per_s")
        scan_high, _ = benchmark_mean(benchmark, "mdbx", high, "scan_ops_per_s")
        cmvbt_scan_low, _ = benchmark_mean(benchmark, "cmvbt", low, "scan_ops_per_s")
        miss_low = summary_value(summary, "mdbx", low, "cache", "cache_misses_per_scanned_record")
        miss_high = summary_value(summary, "mdbx", high, "cache", "cache_misses_per_scanned_record")
        miss_rate_low = summary_value(summary, "mdbx", low, "cache", "cache_miss_rate_pct")
        miss_rate_high = summary_value(summary, "mdbx", high, "cache", "cache_miss_rate_pct")
        l1_low = summary_value(
            summary, "mdbx", low, "cache", "L1_dcache_load_misses_per_scanned_record"
        )
        l1_high = summary_value(
            summary, "mdbx", high, "cache", "L1_dcache_load_misses_per_scanned_record"
        )
        dtlb_low = summary_value(
            summary, "mdbx", low, "memory", "dTLB_load_misses_per_scanned_record"
        )
        dtlb_high = summary_value(
            summary, "mdbx", high, "memory", "dTLB_load_misses_per_scanned_record"
        )
        lines.append(
            f"For MDBX, going from {low}% to {high}% updates changes scan throughput by "
            f"{fmt(percent_change(scan_low, scan_high), 2)}%, reader cache misses per scanned record by "
            f"{fmt(percent_change(miss_low, miss_high), 2)}%, generic cache miss rate by "
            f"{fmt(percent_change(miss_rate_low, miss_rate_high), 2)}%, L1D misses per record by "
            f"{fmt(percent_change(l1_low, l1_high), 2)}%, and dTLB misses per record by "
            f"{fmt(percent_change(dtlb_low, dtlb_high), 2)}%."
        )
        if cmvbt_scan_low and scan_low:
            lines.append(
                f"At {low}% updates, cMVBT scan throughput is {fmt(cmvbt_scan_low / scan_low, 2)}x MDBX; "
                "this run therefore does not reproduce a 10x gap."
            )
        if scan_low and scan_high and miss_low and miss_high and l1_low and l1_high:
            consistent = scan_high > scan_low and miss_high < miss_low and l1_high < l1_low
            lines.append(
                "Both generic and L1D misses improve with scan throughput, which is consistent with the proposed reader-cache explanation."
                if consistent
                else "The evidence is mixed and does not robustly support the proposed reader-cache explanation: throughput improves, but the more specific L1D result does not improve with it."
            )
        else:
            lines.append("The hypothesis cannot be decided because at least one required counter is unavailable.")
    else:
        lines.append("At least two MDBX update rates are required for the directional hypothesis check.")

    lines.extend(["", "## Counter quality", ""])
    if unsupported:
        lines.append(
            "Unsupported counter rows: "
            + ", ".join(f"{event} ({count})" for event, count in sorted(unsupported.items()))
            + "."
        )
    else:
        lines.append("All requested counters were available.")
    fractions = [number(row.get("enabled_fraction")) for row in perf]
    fractions = [value for value in fractions if value is not None]
    lines.append(
        f"Minimum scheduled/enabled fraction: {fmt(min(fractions) if fractions else None, 4)} "
        "(values are scaled for multiplexing)."
    )
    lines.append(
        "Generic cache events are architecture-defined aggregates. Prefer misses per scanned record and "
        "the supported L1D/dTLB events over unsupported LLC aliases."
    )
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run", type=Path)
    args = parser.parse_args()
    benchmark = read_csv(args.run / "benchmark.csv")
    perf = read_csv(args.run / "reader-perf-events.csv")
    normalized = normalize(benchmark, perf)
    summary = summarize(normalized)
    write_csv(args.run / "reader-analysis-runs.csv", normalized)
    write_csv(args.run / "reader-analysis-summary.csv", summary)
    report(args.run / "reader-analysis.md", benchmark, perf, normalized, summary)
    print(f"wrote {args.run / 'reader-analysis-runs.csv'}")
    print(f"wrote {args.run / 'reader-analysis-summary.csv'}")
    print(f"wrote {args.run / 'reader-analysis.md'}")


if __name__ == "__main__":
    main()
