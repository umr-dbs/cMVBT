#!/usr/bin/env python3
"""Reproduce paper Figure 8 and add the missing one-OLTP-thread points.

The upper sweep holds OLTP writers at 32 and varies OLAP readers. The lower
sweep holds OLAP readers at 16 and varies OLTP writers. Existing inputs are
never modified; new SVG and PNG files receive descriptive names.
"""
import argparse
from pathlib import Path
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
SYSTEMS = ("mdbx", "frugal", "chain", "cmvbt")
ALIASES = {
    "libmdbx": "mdbx",
    "mdbx": "mdbx",
    "FrugalSkipList2": "frugal",
    "FrugalSkipList": "frugal",
    "frugal": "frugal",
    "VANILLA": "chain",
    "chain": "chain",
    "cMVBT(MemOpt)": "cmvbt",
    "cMVBT": "cmvbt",
    "cmvbt": "cmvbt",
}
LABELS = {
    "mdbx": "libmdbx (CoW)",
    "frugal": "Frugal Lists",
    "chain": "Version Chains",
    "cmvbt": "cMVBT",
}
STYLE = {
    "mdbx": ("green", "s"),
    "frugal": ("black", "^"),
    "chain": ("#1f77b4", "p"),
    "cmvbt": ("red", "x"),
}
SCAN_THREADS = [1, 2, 4, 6, 8, 16, 32]
OLTP_THREADS = [1, 2, 4, 8, 16, 32, 64]


def false_rows(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(("false", "0"))


def normalize(path: Path, one_writer: bool = False) -> pd.DataFrame:
    if path.is_dir():
        path = path / "scalability.csv"
    if not path.exists():
        sys.exit(f"missing input CSV: {path}")
    raw = pd.read_csv(path)

    # Original Figure 8 CSV used by concurrency_level_10K.py/plot_olap.py.
    old_required = {
        "v_index", "oltp_threads", "olap_threads", "total_num_oltp_tx",
        "total_oltp_time", "total_num_scan_tx", "total_olap_time",
    }
    if old_required.issubset(raw.columns):
        if "gc" in raw.columns:
            raw = raw[false_rows(raw["gc"])]
        frame = pd.DataFrame({
            "system": raw["v_index"].map(ALIASES),
            "writers": raw["oltp_threads"],
            "readers": raw["olap_threads"],
            "oltp_ops_per_s": raw["total_num_oltp_tx"] / (raw["total_oltp_time"] / 1e9),
            "scan_ops_per_s": raw["total_num_scan_tx"] / (raw["total_olap_time"] / 1e9),
            "panel": "",
        })
    # Canonical generate/load CSV emitted by run_one_oltp_thread.sh.
    elif {"system", "oltp_threads", "olap_threads", "oltp_ops",
          "oltp_time_ns", "scans", "olap_time_ns"}.issubset(raw.columns):
        frame = pd.DataFrame({
            "system": raw["system"].map(ALIASES),
            "writers": raw["oltp_threads"],
            "readers": raw["olap_threads"],
            "oltp_ops_per_s": raw["oltp_ops"] / (raw["oltp_time_ns"] / 1e9),
            "scan_ops_per_s": raw["scans"] / (raw["olap_time_ns"] / 1e9),
            "panel": "oltp" if one_writer else "",
        })
        if "workload" in raw.columns and (pd.to_numeric(raw["workload"]) != 60).any():
            sys.exit(f"{path} contains rows other than the Figure 8 60% workload")
    # Unified paper.csv format is accepted for verification and migration.
    elif {"experiment", "system", "writers", "readers",
          "oltp_ops_per_s", "scan_ops_per_s"}.issubset(raw.columns):
        raw = raw[raw["experiment"].isin(
            ("fig8_olap_scalability", "fig8_oltp_scalability"))].copy()
        if "distribution" in raw.columns:
            raw = raw[raw["distribution"] == "uniform"]
        frame = pd.DataFrame({
            "system": raw["system"].map(ALIASES),
            "writers": raw["writers"],
            "readers": raw["readers"],
            "oltp_ops_per_s": raw["oltp_ops_per_s"],
            "scan_ops_per_s": raw["scan_ops_per_s"],
            "panel": raw["experiment"].map({
                "fig8_olap_scalability": "scan",
                "fig8_oltp_scalability": "oltp",
            }),
        })
    else:
        sys.exit(f"unrecognized Figure 8 CSV schema: {path}")

    frame = frame[frame["system"].isin(SYSTEMS)].copy()
    if frame.empty:
        sys.exit(f"{path} contains no recognized Figure 8 systems")
    for column in ("writers", "readers", "oltp_ops_per_s", "scan_ops_per_s"):
        frame[column] = pd.to_numeric(frame[column], errors="raise")
    return frame


def select_sweeps(base: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    explicit = base["panel"].ne("")
    if explicit.any():
        scan = base[base["panel"] == "scan"].copy()
        oltp = base[base["panel"] == "oltp"].copy()
    else:
        # This fixed-writer filter is missing from the old plot_olap.py and is
        # the main reason its scan curve can differ from the paper's panel.
        scan = base[base["writers"] == 32].copy()
        oltp = base[base["readers"] == 16].copy()
    scan = scan[scan["readers"].isin(SCAN_THREADS)]
    oltp = oltp[oltp["writers"].isin(OLTP_THREADS)]
    return scan, oltp


def add_one_writer(oltp: pd.DataFrame, overlay: pd.DataFrame) -> pd.DataFrame:
    invalid = (overlay["writers"] != 1) | (overlay["readers"] != 16)
    if invalid.any():
        sys.exit("one-writer CSV must contain only writers=1, readers=16 rows")
    for system in overlay["system"].unique():
        oltp = oltp[~((oltp["system"] == system) & (oltp["writers"] == 1))]
    return pd.concat([oltp, overlay], ignore_index=True)


def line(ax, data: pd.DataFrame, system: str, x: str, y: str, categories):
    rows = data[data["system"] == system]
    if rows.empty:
        return
    values = rows.groupby(x, sort=True)[y].mean()
    positions = {value: index for index, value in enumerate(categories)}
    values = values[values.index.isin(positions)]
    color, marker = STYLE[system]
    ax.plot([positions[value] for value in values.index], values.values,
            color=color, marker=marker, linewidth=2.6, markersize=5,
            markeredgewidth=1.1, label=LABELS[system])


def format_panel(ax, categories, xlabel, ylabel, ylim):
    ax.set_yscale("log")
    ax.set_ylim(*ylim)
    ax.set_xticks(range(len(categories)))
    ax.set_xticklabels(categories)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, color="#b0b0b0", alpha=0.75)
    ax.set_axisbelow(True)


def plot_panel(data, x, y, categories, xlabel, ylabel, ylim, legend, size):
    fig, ax = plt.subplots(figsize=size)
    for system in SYSTEMS:
        line(ax, data, system, x, y, categories)
    format_panel(ax, categories, xlabel, ylabel, ylim)
    if legend:
        ax.legend(loc="center right", fontsize=8)
    fig.tight_layout()
    return fig


def save(fig, stem: Path):
    fig.savefig(stem.with_suffix(".svg"))
    fig.savefig(stem.with_suffix(".png"), dpi=300)
    plt.close(fig)
    print(f"wrote {stem.with_suffix('.svg')} and {stem.with_suffix('.png')}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path,
                        default=SCRIPT_DIR / "oltp_60%ups_concurrent_degree__new.csv",
                        help="original complete Figure 8 CSV")
    parser.add_argument("--one-writer", type=Path, required=True,
                        help="scalability.csv from run_one_oltp_thread.sh")
    parser.add_argument("--out", type=Path, default=SCRIPT_DIR)
    args = parser.parse_args()

    base = normalize(args.base)
    overlay = normalize(args.one_writer, one_writer=True)
    scan, oltp = select_sweeps(base)
    oltp = add_one_writer(oltp, overlay)
    args.out.mkdir(parents=True, exist_ok=True)

    missing = set(SYSTEMS).difference(overlay["system"])
    if missing:
        print("warning: one-writer data missing systems: " + ", ".join(sorted(missing)),
              file=sys.stderr)

    scan_fig = plot_panel(
        scan, "readers", "scan_ops_per_s", SCAN_THREADS,
        "Concurrency Level (OLAP Threads)", "Scan Throughput (tx/s)",
        (2e2, 5e5), False, (5, 2.5))
    save(scan_fig, args.out / "ccolaps_corrected")

    oltp_fig = plot_panel(
        oltp, "writers", "oltp_ops_per_s", OLTP_THREADS,
        "Concurrency Level (OLTP Threads)", "OLTP Throughput (tx/s)",
        (5e3, 1.5e7), True, (5, 2.5))
    save(oltp_fig, args.out / "cclevelOpt_one_writer")

    fig, (scan_ax, oltp_ax) = plt.subplots(2, 1, figsize=(5, 5.15))
    for system in SYSTEMS:
        line(scan_ax, scan, system, "readers", "scan_ops_per_s", SCAN_THREADS)
        line(oltp_ax, oltp, system, "writers", "oltp_ops_per_s", OLTP_THREADS)
    format_panel(scan_ax, SCAN_THREADS, "Concurrency Level (OLAP Threads)",
                 "Scan Throughput (tx/s)", (2e2, 5e5))
    format_panel(oltp_ax, OLTP_THREADS, "Concurrency Level (OLTP Threads)",
                 "OLTP Throughput (tx/s)", (5e3, 1.5e7))
    oltp_ax.legend(loc="center right", fontsize=8)
    fig.tight_layout(h_pad=1.6)
    save(fig, args.out / "fig8_scalability_one_writer")


if __name__ == "__main__":
    main()
