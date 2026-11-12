#!/usr/bin/env python3
"""Chart for the two-node sorted-streams report.

Usage: plot_sorted.py DATA_DIR UNSORTED_DATA_DIR OUT_DIR
DATA_DIR holds this report's streams-*.tsv and streams-*-clickhouse.txt. UNSORTED_DATA_DIR is the two-node report's
data directory ({,lz4/}streams-par1.tsv and the matching -clickhouse.txt), for the unsorted baseline.
Writes sorted_streams.{png,svg,tsv}.
"""
import csv
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
from matplotlib.ticker import FixedLocator, NullLocator

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

DATA, UNSORTED, OUT = (Path(a) for a in sys.argv[1:4])
CPUS = 16
# (label, wall-time tsv, ClickHouse query-log table, color, marker, line style)
CONFIGS = (
    ("Unsorted", UNSORTED / "streams-par1.tsv", UNSORTED / "streams-par1-clickhouse.txt", "#2a78d6", "o", "-"),
    ("ORDER BY o_orderkey (the sort key)", DATA / "streams-nocompress-orderkey.tsv",
     DATA / "streams-nocompress-orderkey-clickhouse.txt", "#eb6834", "s", "-"),
    ("ORDER BY o_custkey", DATA / "streams-nocompress-custkey.tsv",
     DATA / "streams-nocompress-custkey-clickhouse.txt", "#1baf7a", "^", "-"),
    ("LZ4, unsorted", UNSORTED / "lz4" / "streams-par1.tsv", UNSORTED / "lz4" / "streams-par1-clickhouse.txt",
     "#2a78d6", "o", "--"),
    ("LZ4, ORDER BY o_orderkey", DATA / "streams-lz4-orderkey.tsv", DATA / "streams-lz4-orderkey-clickhouse.txt",
     "#eb6834", "s", "--"),
)


def load_walls(path):
    walls = defaultdict(list)
    for row in csv.DictReader(open(path), delimiter="\t"):
        walls[int(row["streams"])].append(float(row["wall_s"]))
    return {n: max(v) for n, v in sorted(walls.items())}


def load_cpu(path):
    """streams -> ClickHouse CPU-seconds per batch, from a PrettyCompactMonoBlock query_log table."""
    text = open(path).read().splitlines()
    header = [c.strip(" ─┌┐") for c in next(l for l in text if "┌" in l).split("┬")]
    col = header.index("server_cpu_s_per_batch")
    cpu = {}
    for line in (l for l in text if "│" in l):
        cells = [c.strip() for c in line.split("│")[1:-1]]
        cpu[int(cells[0])] = float(cells[col])
    return cpu


walls = {label: load_walls(w) for label, w, *_ in CONFIGS}
cpu = {label: load_cpu(c) for label, _, c, *_ in CONFIGS}
ns = sorted(walls[CONFIGS[0][0]])

fig, axes = plt.subplots(1, 2, figsize=(14, 5.6))
for label, _, _, color, marker, style in CONFIGS:
    axes[0].plot(ns, [walls[label][n] for n in ns], color=color, marker=marker, ls=style, lw=2, ms=7, label=label)
    axes[1].plot(ns, [cpu[label][n] / walls[label][n] for n in ns], color=color, marker=marker, ls=style, lw=2,
                 ms=7, label=label)
link_floor = 9.673331016 / 1.177  # uncompressed bytes / iperf3 link rate (two-node report)
axes[0].axhline(link_floor, color="#666666", ls=":", lw=1.2)
axes[0].text(ns[0], link_floor * 0.93, f"uncompressed link minimum {link_floor:.1f} s", ha="left",
             color="#444444", fontsize=9)
ticks = [6, 8, 12, 16, 24, 32, 48]
axes[0].set_yscale("log")
axes[0].yaxis.set_major_locator(FixedLocator(ticks))
axes[0].yaxis.set_minor_locator(NullLocator())
axes[0].set_yticklabels([str(t) for t in ticks])
axes[0].set_ylim(5.5, 48)
axes[0].set_ylabel("Batch wall time (s, log scale; slower of 2 runs; lower is better)")
axes[0].set_title("Runtime")
axes[1].axhline(CPUS, color="#666666", ls=":", lw=1.2)
axes[1].text(ns[0], CPUS + 0.3, f"all {CPUS} CPUs of the ClickHouse node", ha="left", color="#444444", fontsize=9)
axes[1].set_ylim(0, 18)
axes[1].set_ylabel("ClickHouse CPU-seconds per batch / batch wall time")
axes[1].set_title("Average ClickHouse CPUs busy")
for ax in axes:
    ax.set_xlabel("Concurrent read_adbc streams (o_orderkey ranges, each sorted as labeled)")
    ax.set_xticks(ns)
    ax.grid(axis="y", color="#e5e5e5")
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
axes[0].legend(frameon=False, fontsize=8.5, loc="upper right")
fig.suptitle("SF50 orders, per-stream ORDER BY, ClickHouse and DuckDB on separate nodes, 10 Gb/s link, "
             "parallel formatting, CPUs at 2.0 GHz")
fig.tight_layout()
fig.savefig(OUT / "sorted_streams.png", dpi=110)
fig.savefig(OUT / "sorted_streams.svg", metadata={"Date": None})
with open(OUT / "sorted_streams.tsv", "w") as out:
    names = ["unsorted", "orderkey", "custkey", "lz4_unsorted", "lz4_orderkey"]
    out.write("streams\t" + "\t".join(f"{k}_s\t{k}_ch_cpus" for k in names) + "\n")
    for n in ns:
        out.write(f"{n}\t" + "\t".join(f"{walls[l][n]:.3f}\t{cpu[l][n] / walls[l][n]:.1f}"
                                       for l, *_ in CONFIGS) + "\n")
print(open(OUT / "sorted_streams.tsv").read())
