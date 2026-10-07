import pandas as pd
import matplotlib.pyplot as plt

files = [
    # "oltp60%_opt_build.csv",
    "oltp_60%ups_concurrent_degree__new.csv"
    # "oltp_60%ups_concurrent_degree.csv",
    # "oltp_90%ups_concurrent_degree.csv",
    # "oltp_80%ups_concurrent_degree.csv"
]
for file in files:
    # Load CSV
    df = pd.read_csv(file)

    # Convert time to milliseconds
    df["total_oltp_time_ms"] = df["total_oltp_time"] / 1_000_000_000

    # Compute throughput (tx/ms)
    df["oltp_throughput"] = df["total_num_oltp_tx"] / df["total_oltp_time_ms"]
    plt.figure(figsize=(5, 2.5))

    # plt.figure(figsize=(3.5, 2.2))
    order = ["libmdbx", "FrugalSkipList2", "VANILLA","cMVBT(MemOpt)"]
    df["v_index"] = pd.Categorical(df["v_index"], categories=order, ordered=True)
    df = df.sort_values(by="v_index")

    x = [2, 4, 8,16, 32, 64,]
    # x_pos = range(32+5)
    # plt.axvspan(64*2, max(x_pos), color='gray', alpha=0.2)
    # start_idx = x.index(64)
    # plt.text(
    #     (start_idx + len(x_pos) - 1) / 1 +7,
    #     0.85,
    #     "SMT",
    #     transform=plt.gca().get_xaxis_transform(),
    #     ha="center",
    #     va="top",
    #     color="gray",
    #     rotation=90,
    #     fontsize=12,
    #     fontweight="bold",
    # )


    df = df[df["oltp_threads"].isin(x)]
    plt.xscale('log', base=2)
    plt.xticks(x, x)

    # plt.xticks([2,4,8,12,14,16,20,24,28,32,64])
    # plt.xlim(0, 64)
    # plt.xscale("log", base=2)
    # Plot per system
    for v_index, group in df.groupby("v_index"):
        group = group.sort_values("oltp_threads")
        if v_index == "BTree(FG)":
            v_index = "Frugal Lists (Unoptimized)"
            color = "black"
            marker = "^"
            continue
        elif v_index == "cMVBT":
            color = "blue"
            marker = "o"
            v_index = "cMVBT (Unoptimized)"
            continue
        elif v_index == "cMVBT-Compact":
             color = "purple"
             marker = "o"
             v_index = "cMVBT-Compact"
             continue
        elif v_index == "cMVBT(MemOpt)":
            color = "red"
            marker = "x"
            v_index = "cMVBT"
        elif v_index == "FrugalSkipList":
            color = "grey"
            marker = "P"
            v_index = "Frugal Lists (MemOpt_1)"
            continue
        elif v_index == "FrugalSkipList2":
            # color = "darkgreen"
            # marker = "s"
            # v_index = "Frugal Lists (MemOpt_2)"
            v_index = "Frugal List"
            color = "black"
            marker = "^"
        elif v_index == "VANILLA":
            v_index = "Version Chain"
            color = "#1f77b4"
            marker = "p"
            # continue
        elif v_index == "cMVBTmNew2":
            color = "blue"
            marker = "s"
            v_index = "cMVBT-BidNew2"
            continue
        elif v_index == "cMVBTmNew3":
            color = "yellow"
            marker = "^"
            v_index = "cMVBT-Claude"
            continue
        elif v_index == "cMVBTmNew":
            color = "green"
            marker = "s"
            v_index = "cMVBT-BidNew"
            continue
        elif v_index == "libmdbx":
            color = "green"
            marker = "s"
            v_index = "libmdbx (CoW)"
            # continue
        else:
            # v_index = "cMVBT (MemOpt)"
            v_index = "cMVBTNew"
            color = "green"
            marker = "s"
            continue

        plt.plot(
            group["oltp_threads"],
            group["oltp_throughput"],
            marker=marker,
            label=f"{v_index}",
            color=color,
            linewidth=3,
        )


    plt.yscale("log")
    # Labels
    plt.xlabel("Concurrency Level (OLTP Threads)")
    plt.ylabel("OLTP Throughput (tx/s)")
    # plt.title("10K Initial Keys")
    plt.tight_layout()
    plt.legend()
    plt.grid()

    plt.savefig("cclevelOpt.svg")
    plt.show()