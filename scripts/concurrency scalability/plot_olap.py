import pandas as pd
import matplotlib.pyplot as plt

# Load oltp.csv
df = pd.read_csv("oltp_60%ups_concurrent_degree__new.csv")

# Convert ns -> seconds
df["total_olap_time_s"] = df["total_olap_time"] / 1e9

# Compute throughput (tx/s)
df["olap_tx_per_sec"] = df["total_num_scan_tx"] / df["total_olap_time_s"]

# Sort by olap_threads (important for plotting)
df = df.sort_values("olap_threads")

order = ["libmdbx", "FrugalSkipList2", "VANILLA", "cMVBT(MemOpt)"]
df["v_index"] = pd.Categorical(df["v_index"], categories=order, ordered=True)
df = df.sort_values(by="v_index")

x = [1, 2, 4, 6, 8, 16, 32]
df = df[df["olap_threads"].isin(x)]
x_map = {v: i for i, v in enumerate(x)}

# plt.figure(figsize=(5, 2.5))
plt.figure(figsize=(3.5, 2.2))
for v_index, group in df.groupby("v_index"):
    group = group.sort_values("olap_threads")

    if v_index == "BTree(FG)":
        label = "Frugal Lists (Unoptimized)"
        color = "black"
        marker = "^"
        continue
    elif v_index == "cMVBT":
        color = "blue"
        marker = "o"
        label = "cMVBT (Unoptimized)"
        continue
    elif v_index == "cMVBT-Compact":
        color = "purple"
        marker = "o"
        label = "cMVBT-Compact"
        continue
    elif v_index == "cMVBT(MemOpt)":
        color = "red"
        marker = "x"
        label = "cMVBT"
    elif v_index == "FrugalSkipList":
        color = "grey"
        marker = "P"
        label = "Frugal Lists (MemOpt_1)"
        continue
    elif v_index == "FrugalSkipList2":
        # color = "darkgreen"
        # marker = "s"
        # v_index = "Frugal Lists (MemOpt_2)"
        label = "Frugal Lists"
        color = "black"
        marker = "^"
    elif v_index == "VANILLA":
        label = "Version Chains"
        color = "#1f77b4"
        marker = "p"
        # continue
    elif v_index == "cMVBTm":
        color = "green"
        marker = "s"
        label = "cMVBT-Bid"
        continue
    elif v_index == "libmdbx":
         color = "green"
         marker = "s"
         label = "libmdbx (CoW)"
    elif v_index == "cMVBTmNew3":
        color = "yellow"
        marker = "^"
        v_index = "cMVBT-Claude"
        continue
    elif v_index == "cMVBTmNew2":
        color = "blue"
        marker = "s"
        v_index = "cMVBT-BidNew2"
        continue
    else:
        # v_index = "cMVBT (MemOpt)"
        label = "cMVBTFix"
        color = "green"
        marker = "s"
        continue

    x_pos = group["olap_threads"].map(x_map)

    plt.plot(x_pos,
             group["olap_tx_per_sec"],
             marker=marker,
             color=color,
             linewidth=3,
             label=label)

plt.xticks(range(len(x)), x)
plt.yscale('log')
# plt.legend()
plt.xlabel("Concurrency Level (OLAP Threads)")
plt.ylabel("Scan Throughput (tx/s)")
plt.grid(True)
plt.tight_layout()
plt.savefig("ccolaps.svg")
plt.show()