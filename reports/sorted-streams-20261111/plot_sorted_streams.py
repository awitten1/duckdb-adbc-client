#!/usr/bin/env python3
"""Plot stream count vs runtime for parallel read_adbc key-range streams, unsorted and sorted per stream.

Usage: plot_sorted_streams.py DATA_DIR OUT_DIR
Reads DATA_DIR/{none,o_orderkey,o_custkey}.tsv (parallel_read_adbc output: streams, repeat, wall_s).
Each point is the median of the measured runs; with two runs that is the slower one.
"""
import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

DATA, OUT = Path(sys.argv[1]), Path(sys.argv[2])
SERIES = (
    ("none", "No ORDER BY", "#2a78d6", "o"),
    ("o_orderkey", "ORDER BY o_orderkey (the partition key)", "#1baf7a", "s"),
    ("o_custkey", "ORDER BY o_custkey (not the partition key)", "#eb6834", "^"),
)


def load(name):
    walls = defaultdict(list)
    for row in csv.DictReader(open(DATA / f"{name}.tsv"), delimiter="\t"):
        walls[int(row["streams"])].append(float(row["wall_s"]))
    return {n: statistics.median_high(v) for n, v in sorted(walls.items())}


rows = {name: load(name) for name, *_ in SERIES}
streams = sorted(rows["none"])

fig, axes = plt.subplots(1, 2, figsize=(12, 5))
for name, label, color, marker in SERIES:
    data = rows[name]
    axes[0].plot(streams, [data[n] for n in streams], color=color, marker=marker, lw=2, ms=7, label=label)
    axes[1].plot(streams, [data[1] / data[n] for n in streams], color=color, marker=marker, lw=2, ms=7, label=label)
axes[0].set_ylabel("Batch wall time (s; lower is better)")
axes[0].set_title("Runtime")
axes[0].set_ylim(0, None)
axes[1].plot(streams, streams, color="#999999", lw=1, ls=":")
axes[1].text(7.6, 8.4, "linear", color="#666666", rotation=33)
axes[1].set_ylabel("Speedup over 1 stream of the same query")
axes[1].set_title("Speedup")
axes[1].set_ylim(0, None)
for ax in axes:
    ax.set_xlabel("Concurrent read_adbc streams (o_orderkey ranges)")
    ax.set_xticks(streams)
    ax.grid(axis="y", color="#e5e5e5")
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
axes[0].legend(frameon=False)
fig.suptitle("SF50 orders, modified ClickHouse: each stream unsorted or sorted by ClickHouse (median of 2 runs)")
fig.tight_layout()
fig.savefig(OUT / "sorted_streams.png", dpi=110)
fig.savefig(OUT / "sorted_streams.svg", metadata={"Date": None})

with open(OUT / "sorted_streams.tsv", "w") as out:
    out.write("streams\t" + "\t".join(f"{name}_s\t{name}_speedup" for name, *_ in SERIES) + "\n")
    for n in streams:
        cells = []
        for name, *_ in SERIES:
            cells += [f"{rows[name][n]:.3f}", f"{rows[name][1] / rows[name][n]:.2f}"]
        out.write(f"{n}\t" + "\t".join(cells) + "\n")
print(open(OUT / "sorted_streams.tsv").read())
