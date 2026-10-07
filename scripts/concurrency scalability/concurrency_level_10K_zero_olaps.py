import pandas as pd
import matplotlib.pyplot as plt

x = [1,2,4,8,16,32,64,128]

file = "oltp_60%ups_concurrent_degree_10k_zero_olaps.csv"

# Load CSV
df = pd.read_csv(file)

# Convert time to milliseconds
df["total_oltp_time_ms"] = df["total_oltp_time"] / 1_000_000_000

# Compute throughput (tx/ms → tx/s if you want multiply by 1000)
df["oltp_throughput"] = df["total_num_oltp_tx"] / df["total_oltp_time_ms"]

# Filter thread counts
df = df[df["oltp_threads"].isin(x)]

# Split into OLTP vs HTAP
df_oltp = df[df["olap_threads"] == 0]
df_htap = df[df["olap_threads"] != 0]

# Create 2 subplots
fig, axes = plt.subplots(1, 2, figsize=(10, 3), sharey=True)

for ax, data, title in zip(
    axes,
    [df_oltp, df_htap],
    ["Pure OLTP (0 OLAP Threads)", "HTAP (OLAP Threads = OLTP Threads / 2)"]
):
    for v_index, group in data.groupby("v_index"):
        group = group.sort_values("oltp_threads")

        if v_index == "BTree(FG)":
            label = "Frugal Lists"
            color = "black"
            marker = "^"
        else:
            label = v_index
            color = "red"
            marker = "x"

        ax.plot(
            group["oltp_threads"],
            group["oltp_throughput"],
            marker=marker,
            label=label,
            color=color,
            linewidth=3,
        )
    # ax.set_xticks([1,2,32,64,128,256,512])
    ax.set_xscale("log", base=2)
    ax.set_xticks(x)
    ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())

    ax.set_xlabel("OLTP Threads")
    ax.set_title(title)
    ax.grid()

axes[0].set_ylabel("OLTP Throughput (tx/s)")
fig.suptitle("10K Initial Keys, 60% Updates", fontsize=14)

# One shared legend (cleaner)
handles, labels = axes[0].get_legend_handles_labels()
fig.legend(handles, labels, loc="upper left", ncol=2)

plt.tight_layout(rect=[0, 0, 1, 0.9])
plt.show()