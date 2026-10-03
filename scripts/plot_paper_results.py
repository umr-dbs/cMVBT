#!/usr/bin/env python3
"""Plots the measurements of scripts/run_paper_experiments.sh like the figures of the paper.

    scripts/plot_paper_results.py results/<run> [--out DIR] [--formats pdf,png]

Figure 8   scan latency vs. update rate               latency.csv
Figure 9   throughput, concurrent, without GC          concurrent_nogc.csv
Figure 10  throughput, concurrent, with GC             concurrent_gc.csv
Figure 11  scalability of throughput (60% updates)     scalability.csv
Figure 12  retry probability for Zipf distributions    retries.csv
Figure 13  node reuse vs. allocations with GC          concurrent_gc.csv (cMVBT rows)
extra      OLTP-only throughput                        oltp_only.csv
Repetitions are averaged; error bars show min/max. Missing CSV files are skipped.
"""
import argparse
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

LABELS = {"cmvbt": "cMVBT", "chain": "Version Chains", "frugal": "Frugal Lists (vWeaver)", "vweaver": "vWeaver",
          "skiplist": "Skip Lists"}
STYLE = {"cmvbt": ("#0072B2", "o", "-"), "chain": ("#D55E00", "s", "--"), "frugal": ("#009E73", "^", "-."),
         "vweaver": ("#CC79A7", "v", ":"), "skiplist": ("#999999", "x", ":")}
ORDER = ["cmvbt", "chain", "frugal", "vweaver", "skiplist"]

plt.rcParams.update({"font.size": 9, "axes.grid": True, "grid.alpha": 0.3, "figure.dpi": 150,
                     "axes.spines.top": False, "axes.spines.right": False, "legend.frameon": False})


def load(run: Path, name: str):
    path = run / name
    if not path.exists() or path.stat().st_size == 0:
        print(f"skip {name}: not found", file=sys.stderr)
        return None
    return pd.read_csv(path)


def workload_rate(df):
    df["update_rate"] = df["workload"].astype(int)  # workload files are named after their update rate
    return df


def lines(ax, df, x, y, group="system", yscale=None, order=ORDER, label=None):
    """One line per system: mean over repetitions, min/max as error bars."""
    for key in [k for k in order if k in set(df[group])]:
        part = df[df[group] == key]
        agg = part.groupby(x)[y].agg(["mean", "min", "max"]).sort_index()
        color, marker, ls = STYLE.get(key, ("k", "o", "-"))
        ax.errorbar(agg.index, agg["mean"], yerr=[agg["mean"] - agg["min"], agg["max"] - agg["mean"]],
                    color=color, marker=marker, linestyle=ls, markersize=4, capsize=2, linewidth=1.3,
                    label=(label or LABELS)[key] if isinstance(label or LABELS, dict) else key)
    if yscale:
        ax.set_yscale(yscale)


def save(fig, out: Path, name: str, formats):
    for fmt in formats:
        fig.savefig(out / f"{name}.{fmt}", bbox_inches="tight")
    plt.close(fig)
    print("wrote", ", ".join(f"{name}.{f}" for f in formats))


def fig8_scan_latency(run, out, formats):
    df = load(run, "latency.csv")
    if df is None:
        return
    df = workload_rate(df)
    df["avg_scan_ms"] = df["avg_scan_ns"] / 1e6
    fig, ax = plt.subplots(figsize=(4.2, 3))
    lines(ax, df, "update_rate", "avg_scan_ms", yscale="log")
    ax.set_xlabel("Update percentage")
    ax.set_ylabel("Average scan latency [ms]")
    ax.legend()
    save(fig, out, "fig8_scan_latency", formats)


def throughput_figure(run, out, formats, csv, name, title):
    df = load(run, csv)
    if df is None:
        return
    df = workload_rate(df)
    secs = df["oltp_time_ns"] / 1e9
    df["scans_per_s"] = df["scans"] / secs
    df["oltp_per_s"] = df["oltp_ops"] / secs
    fig, axes = plt.subplots(2, 1, figsize=(4.2, 5), sharex=True)
    lines(axes[0], df, "update_rate", "scans_per_s", yscale="log")
    lines(axes[1], df, "update_rate", "oltp_per_s", yscale="log")
    axes[0].set_ylabel("Scans / s")
    axes[1].set_ylabel("OLTP operations / s")
    axes[1].set_xlabel("Update percentage")
    axes[0].set_title(title, fontsize=9)
    axes[0].legend()
    save(fig, out, name, formats)


def fig11_scalability(run, out, formats):
    df = load(run, "scalability.csv")
    if df is None:
        return
    secs = df["oltp_time_ns"] / 1e9
    df["scans_per_s"] = df["scans"] / secs
    df["oltp_per_s"] = df["oltp_ops"] / secs
    fig, axes = plt.subplots(1, 2, figsize=(7.5, 3))
    for ax, y, label in ((axes[0], "oltp_per_s", "OLTP operations / s"), (axes[1], "scans_per_s", "Scans / s")):
        lines(ax, df, "oltp_threads", y, yscale="log")
        ax.set_xlabel("Writer threads (readers = writers / 2)")
        ax.set_ylabel(label)
    axes[0].legend()
    fig.suptitle("HTAP, 60% updates", fontsize=9)
    save(fig, out, "fig11_scalability", formats)


def fig12_retries(run, out, formats):
    df = load(run, "retries.csv")
    if df is None:
        return
    groups = ["g0", "g1_5", "g6_9", "g10_19", "g20p"]
    names = ["0", "1-5", "6-9", "10-19", "20+"]
    for g in groups:
        df[g] = df[g] / df["total"]
    fig, ax = plt.subplots(figsize=(4.4, 3))
    cmap = plt.get_cmap("viridis")
    alphas = sorted(df["alpha"].unique())
    for i, alpha in enumerate(alphas):
        mean = df[df["alpha"] == alpha][groups].mean()
        ax.plot(names, mean.values, marker="o", markersize=4, color=cmap(i / max(1, len(alphas) - 1)),
                label=f"α = {alpha:g}")
    ax.set_yscale("log")
    ax.set_xlabel("Retry group")
    ax.set_ylabel("Probability")
    ax.legend(ncol=2, fontsize=7)
    save(fig, out, "fig12_retry_probability", formats)


def fig13_node_reuse(run, out, formats):
    df = load(run, "concurrent_gc.csv")
    if df is None:
        return
    df = workload_rate(df)
    df = df[df["system"] == "cmvbt"]
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(4.2, 3))
    for col, label, color, marker in (("blocks_reused", "Nodes reused (GC list)", "#0072B2", "o"),
                                      ("blocks_allocated", "Nodes allocated (memory manager)", "#D55E00", "s")):
        agg = df.groupby("update_rate")[col].mean().sort_index()
        ax.plot(agg.index, agg.values, marker=marker, markersize=4, color=color, label=label)
    ax.set_yscale("log")
    ax.set_xlabel("Update percentage")
    ax.set_ylabel("Node allocations")
    ax.legend()
    reuse = df.groupby("update_rate").apply(
        lambda d: d["blocks_reused"].sum() / max(1, (d["blocks_reused"] + d["blocks_allocated"]).sum()),
        include_groups=False)
    print("node reuse share per update rate:", {int(k): f"{v:.1%}" for k, v in reuse.items()})
    save(fig, out, "fig13_node_reuse", formats)


def oltp_only(run, out, formats):
    df = load(run, "oltp_only.csv")
    if df is None:
        return
    df = workload_rate(df)
    df["oltp_per_s"] = df["oltp_ops"] / (df["oltp_time_ns"] / 1e9)
    fig, ax = plt.subplots(figsize=(4.2, 3))
    lines(ax, df, "update_rate", "oltp_per_s", yscale="log")
    ax.set_xlabel("Update percentage")
    ax.set_ylabel("OLTP operations / s")
    ax.legend()
    save(fig, out, "extra_oltp_only", formats)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run", type=Path, help="results directory of run_paper_experiments.sh")
    ap.add_argument("--out", type=Path, help="figure directory (default: <run>/figures)")
    ap.add_argument("--formats", default="pdf,png")
    args = ap.parse_args()

    out = args.out or args.run / "figures"
    out.mkdir(parents=True, exist_ok=True)
    formats = args.formats.split(",")

    fig8_scan_latency(args.run, out, formats)
    throughput_figure(args.run, out, formats, "concurrent_nogc.csv", "fig9_throughput_nogc", "Concurrent OLTP, without GC")
    throughput_figure(args.run, out, formats, "concurrent_gc.csv", "fig10_throughput_gc", "Concurrent OLTP, with GC")
    fig11_scalability(args.run, out, formats)
    fig12_retries(args.run, out, formats)
    fig13_node_reuse(args.run, out, formats)
    oltp_only(args.run, out, formats)


if __name__ == "__main__":
    main()
