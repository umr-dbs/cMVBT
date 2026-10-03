#!/usr/bin/env python3
"""Plot the unified CSV produced by run_paper_experiments.sh.

    scripts/plot_paper_results.py results/paper-<timestamp> [--out DIR] [--formats pdf,png]

Creates paper Figures 5-10 (for the systems integrated in this repository) and additional
operation- and scan-latency figures from the same measurements.
"""
import argparse
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

LABELS = {
    "cmvbt": "cMVBT",
    "chain": "Version Chains",
    "frugal": "Frugal Lists",
    "vweaver": "vWeaver",
    "skiplist": "Skip Lists",
}
STYLE = {
    "cmvbt": ("#E41A1C", "x", "-"),
    "chain": ("#377EB8", "o", "-"),
    "frugal": ("#111111", "^", "-"),
    "vweaver": ("#FF7F00", "o", "-"),
    "skiplist": ("#984EA3", "s", "--"),
}
ORDER = list(LABELS)

plt.rcParams.update({
    "font.size": 9,
    "axes.grid": True,
    "grid.alpha": 0.35,
    "figure.dpi": 150,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "legend.frameon": False,
})


def read_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists() or path.stat().st_size == 0:
        print(f"skip: {path} not found", file=sys.stderr)
        return None
    return pd.read_csv(path)


def aggregate_lines(ax, data, x, y, yscale="log"):
    for system in [name for name in ORDER if name in set(data["system"])]:
        part = data[data["system"] == system]
        values = part.groupby(x)[y].agg(["mean", "min", "max"]).sort_index()
        color, marker, linestyle = STYLE[system]
        ax.errorbar(
            values.index,
            values["mean"],
            yerr=[values["mean"] - values["min"], values["max"] - values["mean"]],
            color=color,
            marker=marker,
            linestyle=linestyle,
            linewidth=1.3,
            markersize=4,
            capsize=2,
            label=LABELS[system],
        )
    if yscale:
        ax.set_yscale(yscale)


def save(fig, out: Path, name: str, formats):
    for extension in formats:
        fig.savefig(out / f"{name}.{extension}", bbox_inches="tight")
    plt.close(fig)
    print("wrote", ", ".join(f"{name}.{extension}" for extension in formats))


def figure5(data, out, formats):
    part = data[data["experiment"] == "fig5_scan_latency"].copy()
    if part.empty:
        return
    part["scan_avg_ms"] = part["scan_avg_ns"] / 1e6
    fig, ax = plt.subplots(figsize=(4.4, 3.1))
    aggregate_lines(ax, part, "update_rate", "scan_avg_ms")
    ax.set_xlabel("Update percentage")
    ax.set_ylabel("Average scan latency [ms]")
    ax.legend()
    save(fig, out, "fig5_scan_latency", formats)


def throughput(data, experiment, name, out, formats):
    part = data[data["experiment"] == experiment]
    if part.empty:
        return
    fig, axes = plt.subplots(2, 1, figsize=(4.4, 5.2), sharex=True)
    aggregate_lines(axes[0], part, "update_rate", "scan_ops_per_s")
    aggregate_lines(axes[1], part, "update_rate", "oltp_ops_per_s")
    axes[0].set_ylabel("Scans / s")
    axes[1].set_ylabel("OLTP operations / s")
    axes[1].set_xlabel("Update percentage")
    axes[0].legend()
    save(fig, out, name, formats)


def figure8(data, out, formats):
    olap = data[data["experiment"] == "fig8_olap_scalability"]
    oltp = data[data["experiment"] == "fig8_oltp_scalability"]
    if olap.empty and oltp.empty:
        return
    fig, axes = plt.subplots(2, 1, figsize=(4.5, 5.4))
    if not olap.empty:
        aggregate_lines(axes[0], olap, "readers", "scan_ops_per_s")
    if not oltp.empty:
        aggregate_lines(axes[1], oltp, "writers", "oltp_ops_per_s")
    axes[0].set_xlabel("Concurrency level (OLAP threads)")
    axes[0].set_ylabel("Scan throughput [tx/s]")
    axes[1].set_xlabel("Concurrency level (OLTP threads)")
    axes[1].set_ylabel("OLTP throughput [tx/s]")
    axes[0].legend()
    save(fig, out, "fig8_scalability", formats)


def figure9(run, out, formats):
    retries = read_csv(run / "retries.csv")
    if retries is None:
        return
    groups = ["g0", "g1_5", "g6_9", "g10_19", "g20p"]
    labels = ["0", "1-5", "6-9", "10-19", "20+"]
    fig, ax = plt.subplots(figsize=(4.4, 3.1))
    colors = plt.get_cmap("viridis")
    alphas = sorted(retries["alpha"].unique())
    for position, alpha in enumerate(alphas):
        rows = retries[retries["alpha"] == alpha]
        probability = rows[groups].div(rows["total"], axis=0).mean()
        ax.plot(labels, probability, marker="o", linewidth=1.2,
                color=colors(position / max(1, len(alphas) - 1)), label=f"α={alpha:g}")
    ax.set_yscale("log")
    ax.set_xlabel("Retries")
    ax.set_ylabel("Probability")
    ax.legend(ncol=2, fontsize=7)
    save(fig, out, "fig9_retry_probability", formats)


def figure10(data, out, formats):
    part = data[data["experiment"] == "fig10_node_reuse"]
    if part.empty:
        return
    fig, ax = plt.subplots(figsize=(4.4, 3.1))
    for column, label, color, marker in (
        ("blocks_reused", "Nodes reused", "#7F0000", "P"),
        ("blocks_allocated", "Nodes allocated", "#B22222", "x"),
    ):
        values = part.groupby("update_rate")[column].mean().sort_index()
        ax.plot(values.index, values.values, color=color, marker=marker, label=label)
    ax.set_yscale("log")
    ax.set_xlabel("Update percentage")
    ax.set_ylabel("Total nodes")
    ax.legend()
    save(fig, out, "fig10_node_reuse", formats)


def latency_extras(data, out, formats):
    for experiment, suffix in (
        ("fig6_throughput_nogc", "nogc"),
        ("fig7_throughput_gc", "gc"),
    ):
        part = data[data["experiment"] == experiment].copy()
        if part.empty:
            continue
        fig, axes = plt.subplots(2, 2, figsize=(8.2, 5.8), sharex=True)
        for ax, operation in zip(axes.flat, ["update", "insert", "delete", "scan"]):
            part[f"{operation}_p99_us"] = part[f"{operation}_p99_ns"] / 1e3
            aggregate_lines(ax, part, "update_rate", f"{operation}_p99_us")
            ax.set_title(f"{operation.capitalize()} p99")
            ax.set_xlabel("Update percentage")
            ax.set_ylabel("Latency [µs]")
        axes.flat[0].legend()
        fig.tight_layout()
        save(fig, out, f"extra_operation_latency_{suffix}", formats)

        scan = part.copy()
        fig, axes = plt.subplots(1, 3, figsize=(10.5, 3))
        for ax, quantile in zip(axes, ["p50", "p95", "p99"]):
            scan[f"scan_{quantile}_ms"] = scan[f"scan_{quantile}_ns"] / 1e6
            aggregate_lines(ax, scan, "update_rate", f"scan_{quantile}_ms")
            ax.set_title(quantile)
            ax.set_xlabel("Update percentage")
            ax.set_ylabel("Scan latency [ms]")
        axes[0].legend()
        fig.tight_layout()
        save(fig, out, f"extra_scan_latency_{suffix}", formats)


def distribution_groups(data):
    """Yield one output-safe group per access distribution."""
    if "distribution" not in data.columns or "theta" not in data.columns:
        yield "legacy", data
        return
    for (distribution, theta), part in data.groupby(["distribution", "theta"], dropna=False):
        if distribution == "uniform":
            name = "uniform"
        else:
            name = f"zipf-{float(theta):g}"
        yield name, part


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run", type=Path)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--formats", default="pdf,png")
    args = parser.parse_args()

    data = read_csv(args.run / "paper.csv")
    if data is None:
        sys.exit(1)
    out = args.out or args.run / "figures"
    out.mkdir(parents=True, exist_ok=True)
    formats = [extension.strip() for extension in args.formats.split(",") if extension.strip()]

    for distribution, part in distribution_groups(data):
        distribution_out = out / distribution
        distribution_out.mkdir(parents=True, exist_ok=True)
        figure5(part, distribution_out, formats)
        throughput(part, "fig6_throughput_nogc", "fig6_throughput_nogc", distribution_out, formats)
        throughput(part, "fig7_throughput_gc", "fig7_throughput_gc", distribution_out, formats)
        figure8(part, distribution_out, formats)
        figure10(part, distribution_out, formats)
        latency_extras(part, distribution_out, formats)
    figure9(args.run, out, formats)


if __name__ == "__main__":
    main()
