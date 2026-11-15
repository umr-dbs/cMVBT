#!/usr/bin/env python3
"""Render paper measurements like Figures 5-10 of EDBT_2027-1.pdf.

    scripts/plot_paper_results.py results/paper-load-<timestamp>

The canonical file-based runner writes ``latency.csv``, ``concurrent_nogc.csv``,
``concurrent_gc.csv``, and ``scalability.csv``. These take precedence over
matching online-driver rows in ``paper.csv`` when both are present. Figure 9
uses the retry curves from ``retries.csv``.
Every point is the mean over the repetitions; error bars span the minimum to
the maximum of the repetitions (stated in each legend title). OLTP throughput
counts successful operations only: (oltp_ops - oltp_failed) / OLTP time.

Besides the figures, ``summary.csv`` and ``summary.md`` list for every plotted
point the number of repetitions, mean, standard deviation, coefficient of
variation (CV = stddev / mean), minimum and maximum, including the mean number
of records per scan. Warnings are printed if a replay had failed writes or if
the mean records per scan of Figures 6-8 deviate by more than 10% from the
initial data set size.
"""
import argparse
import math
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


def normalize_load_rows(path: Path, experiment: str) -> pd.DataFrame | None:
    """Normalize one CSV emitted by the file-based ``load`` command."""
    if not path.exists() or path.stat().st_size == 0:
        return None
    legacy = pd.read_csv(path)
    if "oltp_failed" not in legacy.columns:
        legacy["oltp_failed"] = 0
    required = {
        "repeat", "system", "workload", "oltp_threads", "olap_threads",
        "gc", "init_keys", "oltp_ops", "oltp_time_ns", "scans",
        "scanned_records", "avg_scan_ns", "p50_scan_ns", "p99_scan_ns",
        "blocks_allocated", "blocks_reused",
    }
    missing = required.difference(legacy.columns)
    if missing:
        sys.exit(f"{path} is missing required columns: {', '.join(sorted(missing))}")
    seconds = legacy["oltp_time_ns"] / 1e9
    if (seconds <= 0).any():
        sys.exit(f"{path} contains a non-positive OLTP duration")
    workload = legacy["workload"].astype(str).str.extract(r"(\d+)$", expand=False)
    if workload.isna().any():
        sys.exit(f"{path} has workload names without a trailing update percentage")
    return pd.DataFrame({
        "experiment": experiment,
        "repeat": legacy["repeat"],
        "system": legacy["system"],
        "distribution": "uniform",
        "theta": 0.0,
        "update_rate": workload.astype(int),
        "gc": legacy["gc"],
        "records": legacy["init_keys"],
        "operations": legacy["oltp_ops"],
        "writers": legacy["oltp_threads"],
        "readers": legacy["olap_threads"],
        "scan_mode": "fresh",
        "scan_range": legacy["init_keys"],
        "oltp_time_ns": legacy["oltp_time_ns"],
        "oltp_failed": legacy["oltp_failed"],
        # Successful operations only; failed writes did not change the data.
        "oltp_ops_per_s": (legacy["oltp_ops"] - legacy["oltp_failed"]) / seconds,
        # Readers stop immediately after the writers, so the legacy plot and
        # protocol normalize scan count by the measured writer interval.
        "scan_time_ns": legacy["oltp_time_ns"],
        "scan_ops_per_s": legacy["scans"] / seconds,
        "scan_count": legacy["scans"],
        "scan_records": legacy["scanned_records"],
        "scan_avg_ns": legacy["avg_scan_ns"],
        "scan_p50_ns": legacy["p50_scan_ns"],
        "scan_p99_ns": legacy["p99_scan_ns"],
        "blocks_allocated": legacy["blocks_allocated"],
        "blocks_reused": legacy["blocks_reused"],
    })


def legacy_paper_rows(run: Path) -> pd.DataFrame | None:
    """Load every canonical generate/load result and map it to paper figures."""
    frames = []
    latency = normalize_load_rows(run / "latency.csv", "fig5_scan_latency")
    if latency is not None:
        frames.append(latency)

    concurrent = normalize_load_rows(
        run / "concurrent_nogc.csv", "fig6_throughput_nogc")
    if concurrent is not None:
        # A dedicated one-writer MDBX rerun supersedes any 32-writer MDBX rows
        # in the same file; never average the two protocols into one curve.
        one_writer_mdbx = ((concurrent["system"] == "mdbx")
                           & (concurrent["writers"] == 1))
        if one_writer_mdbx.any():
            concurrent = concurrent[
                (concurrent["system"] != "mdbx") | one_writer_mdbx].copy()
        frames.append(concurrent)

    gc = normalize_load_rows(run / "concurrent_gc.csv", "fig7_throughput_gc")
    if gc is not None:
        frames.append(gc)
        allocation = gc.copy()
        allocation["experiment"] = "fig10_node_reuse"
        frames.append(allocation)

    scalability = normalize_load_rows(run / "scalability.csv", "fig8_scalability")
    if scalability is not None:
        olap = scalability.copy()
        olap["experiment"] = "fig8_olap_scalability"
        oltp = scalability.copy()
        oltp["experiment"] = "fig8_oltp_scalability"
        frames.extend((olap, oltp))

    # A one-writer MDBX run contributes only to Figure 8's OLTP-thread sweep.
    # It must not also become an OLAP point: that panel holds the writer count
    # fixed while varying readers.
    mdbx_one_writer = normalize_load_rows(
        run / "scalability_mdbx_one_writer.csv", "fig8_oltp_scalability")
    if mdbx_one_writer is not None:
        invalid = ((mdbx_one_writer["system"] != "mdbx")
                   | (mdbx_one_writer["writers"] != 1))
        if invalid.any():
            sys.exit("scalability_mdbx_one_writer.csv must contain only "
                     "one-writer MDBX rows")
        mdbx_one_writer["_point_overlay"] = True
        frames.append(mdbx_one_writer)

    return pd.concat(frames, ignore_index=True) if frames else None


def add_panel_border(fig, bounds=(0.025, 0.045, 0.95, 0.92)):
    fig.add_artist(Rectangle(
        bounds[:2], bounds[2], bounds[3], transform=fig.transFigure,
        fill=False, edgecolor="black", linewidth=0.8, clip_on=False,
    ))


def categorical_axis(values):
    """Return sorted values and evenly spaced plotting positions."""
    categories = sorted(values.dropna().unique())
    return categories, {value: position for position, value in enumerate(categories)}


ERROR_BARS = "mean; bars: min-max over runs"
SUMMARY = []  # rows of summary.csv / summary.md, filled while plotting


def summarize(figure, metric, x, rows, y):
    """Record mean, stddev, CV, min and max over the repetitions of each point."""
    for (system, x_value), values in rows.groupby(["system", x], sort=True)[y]:
        mean = values.mean()
        std = values.std(ddof=1) if len(values) > 1 else 0.0
        SUMMARY.append({
            "figure": figure, "metric": metric, "system": system, x: x_value,
            "x": f"{x}={x_value:g}", "n": len(values), "mean": mean,
            "stddev": std, "cv": std / mean if mean else math.nan,
            "min": values.min(), "max": values.max(),
        })


def draw_line(ax, data, system, x, y, label=None, positions=None):
    rows = data[data["system"] == system]
    if rows.empty:
        return
    grouped = rows.groupby(x, sort=True)[y]
    values = grouped.mean()
    low, high = grouped.min(), grouped.max()
    color, marker, linestyle = STYLE[system]
    plot_x = ([positions[value] for value in values.index]
              if positions is not None else values.index)
    ax.errorbar(
        plot_x,
        values.values,
        yerr=[values.values - low.values, high.values - values.values],
        color=color,
        marker=marker,
        linestyle=linestyle,
        markersize=5,
        markeredgewidth=1.1,
        elinewidth=0.9,
        capsize=2.5,
        label=label or LABELS[system],
    )


def add_records_per_scan(data):
    data = data.copy()
    if "scan_count" in data.columns and "scan_records" in data.columns:
        count = data["scan_count"].where(data["scan_count"] > 0)
        data["records_per_scan"] = data["scan_records"] / count
    return data


def sanity_checks(data):
    """Warn about failed writes and about scans that did not return ~the initial data set."""
    if "oltp_failed" in data.columns:
        failed = data[data["oltp_failed"].fillna(0) > 0]
        for row in failed.itertuples(index=False):
            print(f"WARNING: {row.experiment}/{row.system}/u{row.update_rate} repeat {row.repeat}: "
                  f"{row.oltp_failed} failed writes", file=sys.stderr)
    if "records_per_scan" not in data.columns:
        return
    fresh = data[data["experiment"].isin([
        "fig6_throughput_nogc", "fig7_throughput_gc",
        "fig8_olap_scalability", "fig8_oltp_scalability"])]
    fresh = fresh[fresh["records_per_scan"].notna()]
    if "scan_mode" in fresh.columns and "scan_range" in fresh.columns:
        # bounded range scans (online driver, SCAN_MODE=range) return scan_range records
        expected = fresh["records"].where(fresh["scan_range"] >= fresh["records"], fresh["scan_range"])
    else:
        expected = fresh["records"]
    keys = ["experiment", "system", "update_rate", "writers", "readers"]
    means = fresh.assign(expected=expected).groupby(keys)[["records_per_scan", "expected"]].mean()
    for key, row in means.iterrows():
        deviation = abs(row["records_per_scan"] - row["expected"]) / row["expected"]
        if deviation > 0.10:
            print(f"WARNING: {'/'.join(map(str, key))}: {row['records_per_scan']:.0f} records per "
                  f"scan, {deviation:.0%} off the expected {row['expected']:.0f}", file=sys.stderr)


def write_summary(out: Path):
    if not SUMMARY:
        return
    table = pd.DataFrame(SUMMARY)[
        ["figure", "metric", "system", "x", "n", "mean", "stddev", "cv", "min", "max"]]
    table.to_csv(out / "summary.csv", index=False, float_format="%.6g")
    lines = [
        "| figure | metric | system | x | n | mean | stddev | CV | min | max |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in table.itertuples(index=False):
        lines.append(f"| {row.figure} | {row.metric} | {row.system} | {row.x} | {row.n} | "
                     f"{row.mean:.4g} | {row.stddev:.3g} | {row.cv:.1%} | {row.min:.4g} | {row.max:.4g} |")
    (out / "summary.md").write_text("\n".join(lines) + "\n")
    print("wrote summary.csv, summary.md")


def single_panel(figsize=(4.6, 2.55)):
    fig = plt.figure(figsize=figsize)
    ax = fig.add_axes([0.17, 0.23, 0.76, 0.68])
    add_panel_border(fig)
    return fig, ax


def stacked_panels():
    fig = plt.figure(figsize=(4.65, 5.05))
    top = fig.add_axes([0.18, 0.62, 0.75, 0.31])
    bottom = fig.add_axes([0.18, 0.14, 0.75, 0.31])
    # Keep the frame outside the long, rotated throughput labels.
    add_panel_border(fig, (0.01, 0.505, 0.98, 0.47))
    add_panel_border(fig, (0.01, 0.015, 0.98, 0.47))
    return fig, (top, bottom)


def categorical_ticks(ax, categories, formatter=str):
    ax.set_xticks(range(len(categories)))
    ax.set_xticklabels([formatter(value) for value in categories])


def percent_ticks(ax, categories):
    categorical_ticks(ax, categories, lambda value: f"{value:g}%")


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
    updates, positions = categorical_axis(part["update_rate"])
    fig, ax = single_panel()
    if (part["system"] == "mdbx").any():
        print("WARNING: ignoring libmdbx rows in Figure 5 (no time-travel queries)", file=sys.stderr)
    part = part[part["system"] != "mdbx"]
    for system in ("frugal", "chain", "vweaver", "cmvbt"):
        draw_line(ax, part, system, "update_rate", "scan_avg_ms",
                  positions=positions)
    summarize("fig5", "scan_avg_ms", "update_rate", part, "scan_avg_ms")
    if "records_per_scan" in part.columns:
        summarize("fig5", "records_per_scan", "update_rate", part, "records_per_scan")
    percent_ticks(ax, updates)
    ax.set_xlabel("Update Percentage")
    ax.set_ylabel("Average Latency (ms)")
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper right", fontsize=8, title=ERROR_BARS, title_fontsize=6.5)
    save(fig, out, "fig5_scan_latency", formats)


def throughput(data, experiment, number, gc, out, formats):
    part = data[data["experiment"] == experiment]
    if part.empty:
        return
    fig, (scan_ax, oltp_ax) = stacked_panels()
    suffix = " GC" if gc else ""
    updates, positions = categorical_axis(part["update_rate"])

    for system in ("mdbx", "frugal", "chain", "cmvbt"):
        label = f"{LABELS[system]}{suffix}"
        draw_line(scan_ax, part, system, "update_rate", "scan_ops_per_s", label,
                  positions)
        draw_line(oltp_ax, part, system, "update_rate", "oltp_ops_per_s", label,
                  positions)
    figure = f"fig{number}"
    summarize(figure, "scan_ops_per_s", "update_rate", part, "scan_ops_per_s")
    summarize(figure, "oltp_ops_per_s", "update_rate", part, "oltp_ops_per_s")
    if "records_per_scan" in part.columns:
        summarize(figure, "records_per_scan", "update_rate", part, "records_per_scan")

    for ax in (scan_ax, oltp_ax):
        ax.set_yscale("log")
        percent_ticks(ax, updates)
        ax.set_xlabel("Update Percentage")
    scan_ax.set_ylabel("Scans Throughput (tx/s)")
    oltp_ax.set_ylabel("OLTP Throughput (tx/s)")
    if number == 6:
        # Keep the upper 10^5 decade visible instead of ending below it.
        lower, upper = scan_ax.get_ylim()
        scan_ax.set_ylim(lower, max(upper, 1.2e5))
    # The paper puts the shared legend in the lower panel.
    oltp_ax.legend(loc="center right" if not gc else "lower left", fontsize=8,
                   title=ERROR_BARS, title_fontsize=6.5)
    save(fig, out, f"fig{number}_throughput_{'gc' if gc else 'nogc'}", formats)


def figure8(data, out, formats):
    olap = data[data["experiment"] == "fig8_olap_scalability"]
    oltp = data[data["experiment"] == "fig8_oltp_scalability"]
    if olap.empty and oltp.empty:
        return
    fig, (scan_ax, oltp_ax) = stacked_panels()
    readers, reader_positions = categorical_axis(olap["readers"])
    writers, writer_positions = categorical_axis(oltp["writers"])
    for system in ("mdbx", "frugal", "chain", "cmvbt"):
        draw_line(scan_ax, olap, system, "readers", "scan_ops_per_s",
                  positions=reader_positions)
        draw_line(oltp_ax, oltp, system, "writers", "oltp_ops_per_s",
                  positions=writer_positions)
    summarize("fig8_olap", "scan_ops_per_s", "readers", olap, "scan_ops_per_s")
    summarize("fig8_oltp", "oltp_ops_per_s", "writers", oltp, "oltp_ops_per_s")

    scan_ax.set_yscale("log")
    categorical_ticks(scan_ax, readers, lambda value: f"{value:g}")
    scan_ax.set_xlabel("Concurrency Level (OLAP Threads)")
    scan_ax.set_ylabel("Scan Throughput (tx/s)")

    oltp_ax.set_yscale("log")
    categorical_ticks(oltp_ax, writers, lambda value: f"{value:g}")
    oltp_ax.set_xlabel("Concurrency Level (OLTP Threads)")
    oltp_ax.set_ylabel("OLTP Throughput (tx/s)")
    oltp_ax.legend(loc="center right", fontsize=8, title=ERROR_BARS, title_fontsize=6.5)
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
    updates, positions = categorical_axis(part["update_rate"])
    for column, label, linestyle, marker, zero_offset in (
        ("blocks_reused", "Nodes Reused", "-", "P", -0.025),
        ("blocks_allocated", "Nodes Allocated", "--", "*", 0.025),
    ):
        grouped = part.groupby("update_rate", sort=True)[column]
        values, low, high = grouped.mean(), grouped.min(), grouped.max()
        plot_x = [positions[value] + (zero_offset if count == 0 else 0)
                  for value, count in values.items()]
        # The two series are both zero at 100%. Give coincident zero markers a
        # tiny horizontal dodge so neither valid measurement hides the other.
        ax.errorbar(plot_x, values.values,
                    yerr=[values.values - low.values, high.values - values.values],
                    color="#8b0000", marker=marker, elinewidth=0.9, capsize=2.5,
                    linestyle=linestyle, markersize=5, markeredgewidth=1.0, label=label)
        summarize("fig10", column, "update_rate", part.assign(system="cmvbt"), column)
    percent_ticks(ax, updates)
    ax.set_xlabel("Update Percentage")
    ax.set_ylabel("Total Nodes")
    # A symmetric-log axis preserves the paper's logarithmic presentation for
    # positive counts while giving the valid zero measurements a visible home.
    ax.set_yscale("symlog", linthresh=1)
    ax.set_ylim(bottom=0)
    ax.legend(loc="center right", fontsize=8, title=ERROR_BARS, title_fontsize=6.5)
    save(fig, out, "fig10_node_reuse", formats)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run", type=Path)
    parser.add_argument("--fig5-run", type=Path,
                        help="optional run whose paper.csv supplies Figure 5")
    parser.add_argument("--out", type=Path,
                        help="output directory (default: RUN/paper-figures)")
    parser.add_argument("--formats", default="pdf,png")
    args = parser.parse_args()

    data = read_csv(args.run / "paper.csv")
    legacy = legacy_paper_rows(args.run)
    if data is None and legacy is None:
        sys.exit(1)
    if data is None:
        data = legacy
    elif legacy is not None:
        if "_point_overlay" in legacy.columns:
            point_overlay = legacy[legacy["_point_overlay"].fillna(False)].copy()
            replacement = legacy[~legacy["_point_overlay"].fillna(False)].copy()
        else:
            point_overlay = legacy.iloc[0:0]
            replacement = legacy
        for experiment, system in replacement[["experiment", "system"]].drop_duplicates().itertuples(index=False):
            replace = ((data["experiment"] == experiment) & (data["system"] == system))
            data = data[~replace]
        data = pd.concat([data, replacement], ignore_index=True)
        for row in point_overlay.itertuples(index=False):
            replace = ((data["experiment"] == row.experiment)
                       & (data["system"] == row.system)
                       & (data["writers"] == row.writers))
            data = data[~replace]
        data = pd.concat([data, point_overlay], ignore_index=True)
    data = add_records_per_scan(paper_rows(data))
    sanity_checks(data)
    fig5_data = data
    if args.fig5_run:
        fig5_data = read_csv(args.fig5_run / "paper.csv")
        if fig5_data is None:
            sys.exit(1)
        fig5_data = add_records_per_scan(paper_rows(fig5_data))
    out = args.out or args.run / "paper-figures"
    out.mkdir(parents=True, exist_ok=True)
    formats = [extension.strip() for extension in args.formats.split(",") if extension.strip()]

    figure5(fig5_data, out, formats)
    throughput(data, "fig6_throughput_nogc", 6, False, out, formats)
    throughput(data, "fig7_throughput_gc", 7, True, out, formats)
    figure8(data, out, formats)
    figure9(args.run, out, formats)
    figure10(data, out, formats)
    write_summary(out)


if __name__ == "__main__":
    main()
