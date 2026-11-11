#!/usr/bin/env python3
"""Charts for the parallel ArrowStream formatting report.

Usage: plot_parallel_arrowstream.py DATA_DIR OUT_DIR
Reads DATA_DIR/streams-par{0,1}.tsv (parallel_read_adbc output: streams, repeat, wall_s) and DATA_DIR/curl.txt
(the query_log/processors_profile_log table printed by scripts/parallel_arrow_timing.sh). Writes
parallel_streams.{png,svg,tsv} and output_stage.{png,svg,tsv}.
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
SERIAL, PARALLEL = "#2a78d6", "#eb6834"


def style(ax):
    ax.grid(axis="y", color="#e5e5e5")
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)


# Stream count vs runtime and speedup.
def load_streams(name):
    walls = defaultdict(list)
    for row in csv.DictReader(open(DATA / f"{name}.tsv"), delimiter="\t"):
        walls[int(row["streams"])].append(float(row["wall_s"]))
    return {n: statistics.median_high(v) for n, v in sorted(walls.items())}


serial, parallel = load_streams("streams-par0"), load_streams("streams-par1")
streams = sorted(serial)
fig, axes = plt.subplots(1, 2, figsize=(12, 5))
for data, label, color, marker in ((serial, "Serial formatting (today)", SERIAL, "o"),
                                   (parallel, "Parallel formatting (prototype)", PARALLEL, "s")):
    axes[0].plot(streams, [data[n] for n in streams], color=color, marker=marker, lw=2, ms=7, label=label)
    axes[1].plot(streams, [data[1] / data[n] for n in streams], color=color, marker=marker, lw=2, ms=7, label=label)
axes[0].set_ylabel("Batch wall time (s; lower is better)")
axes[0].set_title("Runtime")
axes[1].set_ylabel("Speedup over 1 stream with the same formatting")
axes[1].set_title("Speedup")
for ax in axes:
    ax.set_xlabel("Concurrent read_adbc streams (o_orderkey ranges)")
    ax.set_xticks(streams)
    ax.set_ylim(0, None)
    style(ax)
axes[0].legend(frameon=False)
fig.suptitle("SF50 orders, modified ClickHouse: ArrowStream formatted serially vs in parallel (median of 2 runs)")
fig.tight_layout()
fig.savefig(OUT / "parallel_streams.png", dpi=110)
fig.savefig(OUT / "parallel_streams.svg", metadata={"Date": None})
plt.close(fig)
with open(OUT / "parallel_streams.tsv", "w") as out:
    out.write("streams\tserial_s\tparallel_s\tparallel_vs_serial_pct\n")
    for n in streams:
        out.write(f"{n}\t{serial[n]:.3f}\t{parallel[n]:.3f}\t{100 * (parallel[n] / serial[n] - 1):+.1f}\n")

# One stream's output stage, from the processor profile.
runs = defaultdict(list)
for line in open(DATA / "curl.txt"):
    cells = [c.strip() for c in line.split("│")]
    if len(cells) < 10 or not cells[1].startswith("par"):
        continue
    run, wall, _cpu, _stage, _busy, wait, send, not_sending, _readers = cells[1:10]
    key = run.rsplit("-", 1)[0]  # par0-cpu9
    runs[key].append((float(wait), float(not_sending), float(send), float(wall)))

ROWS = (("par0-cpu9", "Serial, curl on ClickHouse's socket"),
        ("par1-cpu9", "Parallel, curl on ClickHouse's socket"),
        ("par0-cpu19", "Serial, curl on the other socket"),
        ("par1-cpu19", "Parallel, curl on the other socket"))
PARTS = (("Waiting for the first block", "#b8b8b8"),
         ("Encoding (serial) / copying chunks (parallel)", "#6250d6"),
         ("Blocked in send()", "#1baf7a"))
medians = {key: [statistics.median(v[i] for v in runs[key]) for i in range(4)] for key, _ in ROWS}
fig, ax = plt.subplots(figsize=(11, 4.6))
labels = [label for _, label in ROWS][::-1]
for i, (part, color) in enumerate(PARTS):
    left = [sum(medians[key][:i]) for key, _ in ROWS][::-1]
    width = [medians[key][i] for key, _ in ROWS][::-1]
    ax.barh(labels, width, left=left, color=color, edgecolor="white", linewidth=2, height=0.6, label=part)
for y, (key, _) in enumerate(ROWS[::-1]):
    ax.text(sum(medians[key][:3]) + 0.1, y, f"{medians[key][3]:.2f} s wall", va="center", fontsize=9, color="#333333")
ax.set_xlabel("Seconds of one full-table download (median of 2 runs)")
ax.set_xlim(0, 11)
ax.set_title("Where the output stage of one query spends its time")
ax.legend(frameon=False, loc="upper center", bbox_to_anchor=(0.4, -0.2), ncol=3, fontsize=9)
ax.grid(axis="x", color="#e5e5e5")
ax.set_axisbelow(True)
for side in ("top", "right"):
    ax.spines[side].set_visible(False)
fig.tight_layout()
fig.savefig(OUT / "output_stage.png", dpi=110)
fig.savefig(OUT / "output_stage.svg", metadata={"Date": None})
with open(OUT / "output_stage.tsv", "w") as out:
    out.write("run\tfirst_block_wait_s\tbusy_not_sending_s\tsend_blocked_s\twall_s\n")
    for key, label in ROWS:
        out.write(f"{label}\t" + "\t".join(f"{v:.2f}" for v in medians[key]) + "\n")
print(open(OUT / "parallel_streams.tsv").read())
print(open(OUT / "output_stage.tsv").read())
