#!/usr/bin/env python3
"""Charts for the direct-send report.

Usage: plot_direct_send.py DATA_DIR OUT_DIR
Reads DATA_DIR/curl-{parallel,direct}.txt (tables printed by scripts/parallel_arrow_timing.sh),
DATA_DIR/collector-threads.txt and DATA_DIR/streams-{serial,parallel,direct}.tsv (parallel_read_adbc output).
Writes single_stream.{png,svg,tsv} and streams.{png,svg,tsv}.
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
CONFIGS = (("serial", "Serial formatting (today)", "#2a78d6"),
           ("parallel", "Parallel formatting", "#eb6834"),
           ("direct", "Parallel formatting + direct send", "#1baf7a"))
SOCKETS = (("cpu9", "curl on ClickHouse's socket"), ("cpu19", "curl on the other socket"))


def style(ax, axis="y"):
    ax.grid(axis=axis, color="#e5e5e5")
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)


def curl_walls(path):
    """{(par0|par1, cpu9|cpu19): [server wall seconds]} from a timing table."""
    walls = defaultdict(list)
    for line in open(path):
        cells = [c.strip() for c in line.split("│")]
        if len(cells) > 3 and cells[1].startswith("par"):
            par, cpu, _rep = cells[1].split("-")
            walls[(par, cpu)].append(float(cells[2]))
    return walls


direct_run, parallel_run = curl_walls(DATA / "curl-direct.txt"), curl_walls(DATA / "curl-parallel.txt")
curl = {"serial": {c: direct_run[("par0", c)] for c, _ in SOCKETS},
        "parallel": {c: parallel_run[("par1", c)] for c, _ in SOCKETS},
        "direct": {c: direct_run[("par1", c)] for c, _ in SOCKETS}}

# Collector CPU before and after, from query_thread_log.
collector = {}
section = None
cpu = None
for line in open(DATA / "collector-threads.txt"):
    if line.startswith("##########"):
        section = "before" if "BEFORE" in line else "after"
    elif line.startswith("=== curl on CPU"):
        cpu = "cpu" + re.search(r"CPU (\d+)", line).group(1)
    elif "│ Collector" in line:
        cells = [c.strip() for c in line.split("│")]
        collector[(section, cpu)] = (float(cells[4]), float(cells[5]))  # user, kernel

fig, axes = plt.subplots(1, 2, figsize=(13, 4.8), gridspec_kw={"width_ratios": [1.2, 1]})
ax = axes[0]
width = 0.26
for i, (key, label, color) in enumerate(CONFIGS):
    xs = [j + (i - 1) * width for j in range(len(SOCKETS))]
    heights = [statistics.median(curl[key][c]) for c, _ in SOCKETS]
    ax.bar(xs, heights, width=width - 0.03, color=color, label=label)
    for x, h in zip(xs, heights):
        ax.text(x, h + 0.1, f"{h:.2f}", ha="center", fontsize=8, color="#333333")
ax.set_xticks(range(len(SOCKETS)), [label for _, label in SOCKETS])
ax.set_ylabel("Download time (s; median of 2 runs)")
ax.set_title("One full-table ArrowStream download")
ax.legend(frameon=False, fontsize=9, loc="upper left")
style(ax)

ax = axes[1]
rows = [(s, c) for c, _ in SOCKETS for s in ("before", "after")]
labels = [f"{'Copy' if s == 'before' else 'Direct send'}, {'same socket' if c == 'cpu9' else 'other socket'}"
          for s, c in rows][::-1]
user = [collector[r][0] for r in rows][::-1]
kernel = [collector[r][1] for r in rows][::-1]
ax.barh(labels, user, color="#eda100", edgecolor="white", linewidth=2, height=0.6, label="Userspace (the copy)")
ax.barh(labels, kernel, left=user, color="#6250d6", edgecolor="white", linewidth=2, height=0.6,
        label="Kernel (send)")
for y, (u, k) in enumerate(zip(user, kernel)):
    ax.text(u + k + 0.1, y, f"{u + k:.2f} s", va="center", fontsize=8, color="#333333")
ax.set_xlabel("Collector thread CPU (s)")
ax.set_xlim(0, 9)
ax.set_title("The collector thread")
ax.legend(frameon=False, fontsize=9, loc="upper right")
style(ax, "x")
fig.tight_layout()
fig.savefig(OUT / "single_stream.png", dpi=110)
fig.savefig(OUT / "single_stream.svg", metadata={"Date": None})
plt.close(fig)
with open(OUT / "single_stream.tsv", "w") as out:
    out.write("config\tsocket\tdownload_median_s\n")
    for key, _, _ in CONFIGS:
        for c, label in SOCKETS:
            out.write(f"{key}\t{label}\t{statistics.median(curl[key][c]):.2f}\n")
    out.write("\nrun\tcollector_user_s\tcollector_kernel_s\n")
    for s, c in rows:
        out.write(f"{s} {c}\t{collector[(s, c)][0]:.2f}\t{collector[(s, c)][1]:.2f}\n")


def load_streams(name):
    walls = defaultdict(list)
    for row in csv.DictReader(open(DATA / f"streams-{name}.tsv"), delimiter="\t"):
        walls[int(row["streams"])].append(float(row["wall_s"]))
    return {n: statistics.median_high(v) for n, v in sorted(walls.items())}


streams = {key: load_streams(key) for key, _, _ in CONFIGS}
ns = sorted(streams["serial"])
fig, ax = plt.subplots(figsize=(9, 5))
for (key, label, color), marker in zip(CONFIGS, ("o", "s", "^")):
    ax.plot(ns, [streams[key][n] for n in ns], color=color, marker=marker, lw=2, ms=7, label=label)
ax.set_xticks(ns)
ax.set_ylim(0, None)
ax.set_xlabel("Concurrent read_adbc streams (o_orderkey ranges)")
ax.set_ylabel("Batch wall time (s; median of 2 runs; lower is better)")
ax.set_title("SF50 orders, modified ClickHouse, DuckDB on the other socket")
ax.legend(frameon=False)
style(ax)
fig.tight_layout()
fig.savefig(OUT / "streams.png", dpi=110)
fig.savefig(OUT / "streams.svg", metadata={"Date": None})
with open(OUT / "streams.tsv", "w") as out:
    out.write("streams\t" + "\t".join(f"{key}_s" for key, _, _ in CONFIGS) + "\n")
    for n in ns:
        out.write(f"{n}\t" + "\t".join(f"{streams[key][n]:.3f}" for key, _, _ in CONFIGS) + "\n")
print(open(OUT / "single_stream.tsv").read())
print(open(OUT / "streams.tsv").read())
