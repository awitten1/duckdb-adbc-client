#!/usr/bin/env python3
"""Charts for the two-node 10 Gb/s report.

Usage: plot_two_node.py DATA_DIR OUT_DIR
Reads DATA_DIR/streams-par{1,0}.tsv (parallel_read_adbc output), DATA_DIR/curl.txt and DATA_DIR/iperf3.txt.
Writes streams.{png,svg,tsv}.
"""
import csv
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

DATA, OUT = Path(sys.argv[1]), Path(sys.argv[2])
CONFIGS = ((1, "Parallel formatting (both commits)", "#1baf7a", "^"),
           (0, "Serial formatting (same binary)", "#2a78d6", "o"))


def load_streams(parallel):
    walls = defaultdict(list)
    for row in csv.DictReader(open(DATA / f"streams-par{parallel}.tsv"), delimiter="\t"):
        walls[int(row["streams"])].append(float(row["wall_s"]))
    return {n: statistics.median_high(v) for n, v in sorted(walls.items())}


curl = defaultdict(list)
result_bytes = None
for line in open(DATA / "curl.txt"):
    m = re.match(r"parallel_formatting=(\d) run \d: ([\d.]+) s, (\d+) bytes", line)
    if m:
        curl[int(m.group(1))].append(float(m.group(2)))
        result_bytes = int(m.group(3))
link_gbs = max(float(m.group(1)) for m in re.finditer(r"= ([\d.]+) GB/s", open(DATA / "iperf3.txt").read()))
floor_s = result_bytes / 1e9 / link_gbs

streams = {p: load_streams(p) for p, *_ in CONFIGS}
ns = sorted(streams[1])
fig, axes = plt.subplots(1, 2, figsize=(13, 5))
for parallel, label, color, marker in CONFIGS:
    axes[0].plot(ns, [streams[parallel][n] for n in ns], color=color, marker=marker, lw=2, ms=7,
                 label=f"read_adbc, {label.lower()}")
    axes[1].plot(ns, [result_bytes / 1e9 / streams[parallel][n] for n in ns], color=color, marker=marker, lw=2,
                 ms=7, label=f"read_adbc, {label.lower()}")
    c = statistics.median_high(curl[parallel])
    axes[0].plot([1], [c], color=color, marker="*", ms=14, ls="none", markeredgecolor="white",
                 label=f"curl, one connection, {label.split(' (')[0].lower()}")
    axes[1].plot([1], [result_bytes / 1e9 / c], color=color, marker="*", ms=14, ls="none", markeredgecolor="white")
axes[0].axhline(floor_s, color="#666666", ls="--", lw=1)
axes[0].text(ns[-1], floor_s - 0.6, f"link minimum {floor_s:.1f} s", ha="right", color="#444444", fontsize=9)
axes[0].set_ylabel("Batch wall time (s; median of 2 runs; lower is better)")
axes[0].set_title("Runtime")
axes[1].axhline(link_gbs, color="#666666", ls="--", lw=1)
axes[1].text(ns[-1], link_gbs + 0.04, f"iperf3 link rate {link_gbs:.2f} GB/s", ha="right", color="#444444",
             fontsize=9)
axes[1].set_ylim(0, 1.4)
axes[1].set_ylabel("Throughput (GB/s of Arrow data)")
axes[1].set_title("Throughput")
for ax in axes:
    ax.set_xlabel("Concurrent read_adbc streams (o_orderkey ranges)")
    ax.set_xticks(ns)
    if ax is axes[0]:
        ax.set_ylim(0, None)
    ax.grid(axis="y", color="#e5e5e5")
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
axes[0].legend(frameon=False, fontsize=9)
fig.suptitle("SF50 orders, ClickHouse on one node and DuckDB on another, 10 Gb/s link, CPUs at 2.0 GHz")
fig.tight_layout()
fig.savefig(OUT / "streams.png", dpi=110)
fig.savefig(OUT / "streams.svg", metadata={"Date": None})
with open(OUT / "streams.tsv", "w") as out:
    out.write("streams\tparallel_s\tparallel_gb_per_s\tserial_s\tserial_gb_per_s\n")
    for n in ns:
        p, s = streams[1][n], streams[0][n]
        out.write(f"{n}\t{p:.3f}\t{result_bytes / 1e9 / p:.2f}\t{s:.3f}\t{result_bytes / 1e9 / s:.2f}\n")
print(open(OUT / "streams.tsv").read())
print(f"link {link_gbs:.3f} GB/s, floor {floor_s:.2f} s, curl medians: parallel {statistics.median_high(curl[1]):.2f}, serial {statistics.median_high(curl[0]):.2f}")
