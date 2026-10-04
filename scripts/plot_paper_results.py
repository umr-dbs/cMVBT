#!/usr/bin/env python3
"""Render online YCSB measurements like Figures 5-10 of EDBT_2027-1.pdf.

    scripts/plot_paper_results.py scripts/results/paper-<timestamp>

Figures 5-8 and 10 use the uniform rows from ``paper.csv``, matching the
paper. Figure 9 uses the paper's five retry curves from ``retries.csv``.
Repetitions are averaged. No error bars, box plots, or extra plots are made.
"""
import argparse
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import pandas as pd


LABELS = {
    "cmvbt": "cMVBT",
    "mdbx": "libmdbx (CoW)",
    "chain": "Version Chains",
    "frugal": "Frugal List",
    "vweaver": "vWeaver",
}

# Exact visual identity used by the plots embedded in EDBT_2027-1.pdf.
STYLE = {
    "cmvbt": ("#ff0000", "x", "-"),
    "mdbx": ("#008000", "s", "-"),
    "chain": ("#1f77b4", "p", "-"),
    "frugal": ("#000000", "^", "-"),
    "vweaver": ("#ff7f0e", "o", "-"),
}

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 9,
    "axes.grid": True,
    "axes.axisbelow": True,
    "axes.spines.top": True,
    "axes.spines.right": True,
    "grid.color": "#b0b0b0",
    "grid.alpha": 0.72,
    "grid.linewidth": 0.75,
    "legend.frameon": True,
    "legend.fancybox": True,
    "legend.framealpha": 0.82,
    "legend.edgecolor": "#cccccc",
    "lines.linewidth": 2.2,
    "figure.dpi": 150,
    "savefig.dpi": 300,
})


def read_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists() or path.stat().st_size == 0:
        print(f"skip: {path} not found", file=sys.stderr)
        return None
    return pd.read_csv(path)


def paper_rows(data: pd.DataFrame) -> pd.DataFrame:
    """Select the uniform workload used for paper Figures 5-8 and 10."""
    if "distribution" not in data.columns:
        return data
    rows = data[data["distribution"] == "uniform"].copy()
    if rows.empty:
        sys.exit("paper.csv has no uniform rows required by paper Figures 5-8 and 10")
    return rows


def add_panel_border(fig, bounds=(0.025, 0.045, 0.95, 0.92)):
    fig.add_artist(Rectangle(
        bounds[:2], bounds[2], bounds[3], transform=fig.transFigure,
        fill=False, edgecolor="black", linewidth=0.8, clip_on=False,
    ))


def draw_line(ax, data, system, x, y, label=None):
    rows = data[data["system"] == system]
    if rows.empty:
        return
    values = rows.groupby(x, sort=True)[y].mean()
    color, marker, linestyle = STYLE[system]
    ax.plot(
        values.index,
        values.values,
        color=color,
        marker=marker,
        linestyle=linestyle,
        markersize=5,
        markeredgewidth=1.1,
        label=label or LABELS[system],
    )


def single_panel(figsize=(4.6, 2.55)):
    fig = plt.figure(figsize=figsize)
    ax = fig.add_axes([0.17, 0.23, 0.76, 0.68])
    add_panel_border(fig)
    return fig, ax


def stacked_panels():
    fig = plt.figure(figsize=(4.65, 5.05))
    top = fig.add_axes([0.18, 0.62, 0.75, 0.31])
    bottom = fig.add_axes([0.18, 0.14, 0.75, 0.31])
    add_panel_border(fig, (0.035, 0.505, 0.93, 0.47))
    add_panel_border(fig, (0.035, 0.015, 0.93, 0.47))
    return fig, (top, bottom)


def percent_ticks(ax, values):
    ticks = sorted(values.unique())
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{value:g}%" for value in ticks])


def save(fig, out: Path, name: str, formats):
    for extension in formats:
        fig.savefig(out / f"{name}.{extension}")
    plt.close(fig)
    print("wrote", ", ".join(f"{name}.{extension}" for extension in formats))


def figure5(data, out, formats):
    part = data[data["experiment"] == "fig5_scan_latency"].copy()
    if part.empty:
        return
    part["scan_avg_ms"] = part["scan_avg_ns"] / 1e6
    fig, ax = single_panel()
    for system in ("mdbx", "frugal", "chain", "vweaver", "cmvbt"):
        draw_line(ax, part, system, "update_rate", "scan_avg_ms")
    percent_ticks(ax, part["update_rate"])
    ax.set_xlabel("Update Percentage")
    ax.set_ylabel("Average Latency (ms)")
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper right", fontsize=8)
    save(fig, out, "fig5_scan_latency", formats)


def throughput(data, experiment, number, gc, out, formats):
    part = data[data["experiment"] == experiment]
    if part.empty:
        return
    fig, (scan_ax, oltp_ax) = stacked_panels()
    suffix = " GC" if gc else ""

    for system in ("mdbx", "frugal", "chain", "cmvbt"):
        label = f"{LABELS[system]}{suffix}"
        draw_line(scan_ax, part, system, "update_rate", "scan_ops_per_s", label)
        draw_line(oltp_ax, part, system, "update_rate", "oltp_ops_per_s", label)

    for ax in (scan_ax, oltp_ax):
        ax.set_yscale("log")
        percent_ticks(ax, part["update_rate"])
        ax.set_xlabel("Update Percentage")
    scan_ax.set_ylabel("Scans Throughput (tx/s)")
    oltp_ax.set_ylabel("OLTP Throughput (tx/s)")
    # The paper puts the shared legend in the lower panel.
    oltp_ax.legend(loc="center right" if not gc else "lower left", fontsize=8)
    save(fig, out, f"fig{number}_throughput_{'gc' if gc else 'nogc'}", formats)


def figure8(data, out, formats):
    olap = data[data["experiment"] == "fig8_olap_scalability"]
    oltp = data[data["experiment"] == "fig8_oltp_scalability"]
    if olap.empty and oltp.empty:
        return
    fig, (scan_ax, oltp_ax) = stacked_panels()
    for system in ("mdbx", "frugal", "chain", "cmvbt"):
        draw_line(scan_ax, olap, system, "readers", "scan_ops_per_s")
        draw_line(oltp_ax, oltp, system, "writers", "oltp_ops_per_s")

    scan_ax.set_yscale("log")
    scan_ax.set_xticks(sorted(olap["readers"].unique()))
    scan_ax.set_xlabel("Concurrency Level (OLAP Threads)")
    scan_ax.set_ylabel("Scan Throughput (tx/s)")

    oltp_ax.set_yscale("log")
    oltp_ax.set_xticks(sorted(oltp["writers"].unique()))
    oltp_ax.set_xlabel("Concurrency Level (OLTP Threads)")
    oltp_ax.set_ylabel("OLTP Throughput (tx/s)")
    oltp_ax.legend(loc="center right", fontsize=8)
    save(fig, out, "fig8_scalability", formats)


def figure9(run, out, formats):
    retries = read_csv(run / "retries.csv")
    if retries is None:
        return
    groups = ["g0", "g1_5", "g6_9", "g10_19", "g20p"]
    labels = ["0", "1-5", "6-9", "10-19", "20+"]
    # These are the five curves in the local paper; 0.99 is displayed as 1.
    selected = [0.0, 0.4, 0.8, 0.99, 1.4]
    styles = {
        0.0: ("#000000", "d", r"$\alpha$=0.0"),
        0.4: ("#f2a900", ">", r"$\alpha$=0.4"),
        0.8: ("#0000ff", "^", r"$\alpha$=0.8"),
        0.99: ("#2ca02c", "x", r"$\alpha$=1"),
        1.4: ("#9467bd", "o", r"$\alpha$=1.4"),
    }
    fig, ax = single_panel()
    for alpha in selected:
        rows = retries[(retries["alpha"] - alpha).abs() < 1e-8]
        if rows.empty:
            continue
        probability = rows[groups].div(rows["total"], axis=0).mean()
        color, marker, label = styles[alpha]
        ax.plot(labels, probability, color=color, marker=marker, markersize=5,
                markeredgewidth=1.1, label=label)
    ax.set_yscale("log")
    ax.set_xlabel("Retries")
    ax.set_ylabel("Probability")
    ax.legend(loc="lower left", fontsize=7.5)
    save(fig, out, "fig9_retry_probability", formats)


def figure10(data, out, formats):
    part = data[data["experiment"] == "fig10_node_reuse"]
    if part.empty:
        return
    fig, ax = single_panel()
    for column, label, linestyle, marker in (
        ("blocks_reused", "Nodes Reused", "-", "P"),
        ("blocks_allocated", "Nodes Allocated", "--", "*"),
    ):
        values = part.groupby("update_rate", sort=True)[column].mean()
        # Zero is a valid measurement but has no position on the paper's log
        # axis. Mask it instead of drawing a false off-axis vertical segment.
        values = values.where(values > 0)
        ax.plot(values.index, values.values, color="#8b0000", marker=marker,
                linestyle=linestyle, markersize=5, markeredgewidth=1.0, label=label)
    ax.set_xticks(sorted(part["update_rate"].unique()))
    ax.set_xlabel("Update Percentage")
    ax.set_ylabel("Total Nodes")
    ax.set_yscale("log")
    ax.legend(loc="center right", fontsize=8)
    save(fig, out, "fig10_node_reuse", formats)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run", type=Path)
    parser.add_argument("--out", type=Path,
                        help="output directory (default: RUN/paper-figures)")
    parser.add_argument("--formats", default="pdf,png")
    args = parser.parse_args()

    data = read_csv(args.run / "paper.csv")
    if data is None:
        sys.exit(1)
    data = paper_rows(data)
    out = args.out or args.run / "paper-figures"
    out.mkdir(parents=True, exist_ok=True)
    formats = [extension.strip() for extension in args.formats.split(",") if extension.strip()]

    figure5(data, out, formats)
    throughput(data, "fig6_throughput_nogc", 6, False, out, formats)
    throughput(data, "fig7_throughput_gc", 7, True, out, formats)
    figure8(data, out, formats)
    figure9(args.run, out, formats)
    figure10(data, out, formats)


if __name__ == "__main__":
    main()
