#!/usr/bin/env python3
"""Plots the measurements of scripts/run_ycsb.sh.

    scripts/plot_ycsb_results.py results/ycsb-<run> [--out DIR] [--formats pdf,png]

For every record kind (1 KB / 8 B) one figure each, a panel per workload, x = Zipf alpha, one line per system:
  ycsb_throughput_<bytes>B   OLTP operations per second
  ycsb_latency_<bytes>B      p99 latency of the slowest operation type [us]
  ycsb_olap_<bytes>B         scans per second of the OLAP threads
and, when both record kinds were measured, ycsb_record_size: throughput of the 8 B relative to the 1 KB record.
Repetitions are averaged; error bars show min/max.
"""
import argparse
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

LABELS = {"cmvbt": "cMVBT", "chain": "Version Chains", "frugal": "Frugal Lists", "vweaver": "vWeaver", "skiplist": "Skip Lists"}
STYLE = {"cmvbt": ("#0072B2", "o", "-"), "chain": ("#D55E00", "s", "--"), "frugal": ("#009E73", "^", "-."),
         "vweaver": ("#CC79A7", "v", ":"), "skiplist": ("#999999", "x", ":")}
ORDER = list(LABELS)
WORKLOADS = {"a": "A: 50% read / 50% update", "b": "B: 95% read / 5% update", "c": "C: read only", "d": "D: 95% read latest / 5% insert",
             "e": "E: 95% scan / 5% insert", "f": "F: read-modify-write", "churn": "churn: sliding window with deletes"}

plt.rcParams.update({"font.size": 8, "axes.grid": True, "grid.alpha": 0.3, "figure.dpi": 150,
                     "axes.spines.top": False, "axes.spines.right": False, "legend.frameon": False})


def panels(df, value_bytes, metric, ylabel, name, out, formats, log=True):
    part = df[df["value_bytes"] == value_bytes]
    present = [w for w in WORKLOADS if w in set(part["workload"])]
    if not present or part[metric].isna().all():
        return
    cols = min(4, len(present))
    rows = (len(present) + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(3.1 * cols, 2.5 * rows), squeeze=False)
    for ax, workload in zip(axes.flat, present):
        data = part[part["workload"] == workload]
        for system in [s for s in ORDER if s in set(data["system"])]:
            agg = data[data["system"] == system].groupby("theta")[metric].agg(["mean", "min", "max"]).sort_index()
            color, marker, ls = STYLE[system]
            ax.errorbar(agg.index, agg["mean"], yerr=[agg["mean"] - agg["min"], agg["max"] - agg["mean"]], color=color, marker=marker,
                        linestyle=ls, markersize=3.5, capsize=2, linewidth=1.2, label=LABELS[system])
        ax.set_title(WORKLOADS[workload], fontsize=8)
        ax.set_xlabel("Zipf alpha")
        ax.set_ylabel(ylabel)
        if log:
            ax.set_yscale("log")
    for ax in list(axes.flat)[len(present):]:
        ax.axis("off")
    axes.flat[0].legend(fontsize=7)
    fig.suptitle(f"{name}, {value_bytes}-byte records", fontsize=9)
    fig.tight_layout()
    for fmt in formats:
        fig.savefig(out / f"ycsb_{name.split()[0].lower()}_{value_bytes}B.{fmt}", bbox_inches="tight")
    plt.close(fig)
    print("wrote", f"ycsb_{name.split()[0].lower()}_{value_bytes}B")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run", type=Path)
    ap.add_argument("--out", type=Path)
    ap.add_argument("--formats", default="pdf,png")
    args = ap.parse_args()
    csv = args.run / "ycsb.csv"
    if not csv.exists():
        sys.exit(f"{csv} not found")
    df = pd.read_csv(csv)
    bad = df[df["violations"] > 0]
    if len(bad):
        print(f"WARNING: {len(bad)} runs reported snapshot violations", file=sys.stderr)
    out = args.out or args.run / "figures"
    out.mkdir(parents=True, exist_ok=True)
    formats = args.formats.split(",")

    p99 = [c for c in df.columns if c.endswith("_p99_us")]
    df["slowest_p99_us"] = df[p99].max(axis=1)
    for size in sorted(df["value_bytes"].unique(), reverse=True):
        panels(df, size, "oltp_ops_per_s", "OLTP operations / s", "throughput", out, formats)
        panels(df, size, "slowest_p99_us", "p99 latency [us]", "latency", out, formats)
        if (df["olap_threads"] > 0).any():
            panels(df[df["olap_threads"] > 0], size, "olap_scans_per_s", "OLAP scans / s", "olap", out, formats)

    sizes = sorted(df["value_bytes"].unique())
    if len(sizes) == 2:
        small, big = sizes
        key = ["system", "workload", "theta"]
        a = df[df["value_bytes"] == small].groupby(key)["oltp_ops_per_s"].mean()
        b = df[df["value_bytes"] == big].groupby(key)["oltp_ops_per_s"].mean()
        ratio = (a / b).dropna().reset_index(name="ratio")
        ratio = ratio[ratio["theta"].round(2) == 0.99] if (ratio["theta"].round(2) == 0.99).any() else ratio
        if not ratio.empty:
            pivot = ratio.groupby(["workload", "system"])["ratio"].mean().unstack()
            pivot = pivot.reindex([w for w in WORKLOADS if w in pivot.index])
            fig, ax = plt.subplots(figsize=(6.5, 2.8))
            pivot[[s for s in ORDER if s in pivot.columns]].rename(columns=LABELS).plot.bar(ax=ax, color=[STYLE[s][0] for s in ORDER if s in pivot.columns], width=0.8)
            ax.axhline(1, color="k", linewidth=0.7)
            ax.set_ylabel(f"throughput {small} B / {big} B record")
            ax.set_xlabel("workload")
            ax.legend(fontsize=7, ncol=2)
            for fmt in formats:
                fig.savefig(out / f"ycsb_record_size.{fmt}", bbox_inches="tight")
            plt.close(fig)
            print("wrote ycsb_record_size")


if __name__ == "__main__":
    main()
