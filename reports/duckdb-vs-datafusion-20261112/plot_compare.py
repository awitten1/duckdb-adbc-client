#!/usr/bin/env python3
"""Charts for the DuckDB vs DataFusion report.

Usage: plot_compare.py DATA_DIR DUCKDB_DATA_DIR OUT_DIR
DATA_DIR is this report's data/ (datafusion/... and interleaved.tsv); DUCKDB_DATA_DIR is the 25 Gb/s report's data/,
which holds the DuckDB runs in the same layout. Writes compare.{png,svg,tsv}, interleaved.{png,svg} and
interleaved_summary.tsv.
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

DATA, DUCKDB, OUT = (Path(a) for a in sys.argv[1:4])
DATAFUSION = DATA / "datafusion"
COLORS = {"DuckDB": "#2a78d6", "DataFusion": "#eb6834"}
MARKERS = {"DuckDB": "o", "DataFusion": "s"}
CONFIGS = (
    ("No compression, parallel formatting", "streams-par1.tsv"),
    ("No compression, serial formatting", "streams-par0.tsv"),
    ("LZ4, parallel formatting", "lz4/streams-par1.tsv"),
    ("LZ4, serial formatting", "lz4/streams-par0.tsv"),
    ("ORDER BY o_orderkey", "sorted/streams-nocompress-orderkey.tsv"),
    ("ORDER BY o_custkey", "sorted/streams-nocompress-custkey.tsv"),
    ("LZ4, ORDER BY o_orderkey", "sorted/streams-lz4-orderkey.tsv"),
)


def runs(path):
    w = defaultdict(list)
    for row in csv.DictReader(open(path), delimiter="\t"):
        w[int(row["streams"])].append(float(row["wall_s"]))
    return dict(sorted(w.items()))


def style(ax):
    ax.grid(axis="y", color="#e5e5e5")
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)


# 1. Every configuration, both clients.
data = {label: {"DuckDB": runs(DUCKDB / rel), "DataFusion": runs(DATAFUSION / rel)} for label, rel in CONFIGS}
ns = list(data[CONFIGS[0][0]]["DuckDB"])
fig, axes = plt.subplots(2, 4, figsize=(17, 8.2))
for ax, (label, _) in zip(axes.flat, CONFIGS):
    for client in ("DuckDB", "DataFusion"):
        r = data[label][client]
        ax.plot(ns, [max(r[n]) for n in ns], color=COLORS[client], marker=MARKERS[client], lw=1.8, ms=5.5,
                label=client)
        ax.vlines(ns, [min(r[n]) for n in ns], [max(r[n]) for n in ns], color=COLORS[client], lw=4, alpha=0.3)
    ax.set_title(label, fontsize=10.5)
    ax.set_ylim(0, None)
    ax.set_xticks(ns)
    style(ax)
for ax in axes[:, 0]:
    ax.set_ylabel("Batch wall time (s; line: slower of 2 runs)")
for ax in axes[1, :]:
    ax.set_xlabel("Concurrent streams")
axes[0, 0].legend(frameon=False, fontsize=9, loc="upper right")

ratio_ax = axes[1, 3]
ratios = {label: [max(data[label]["DataFusion"][n]) / max(data[label]["DuckDB"][n]) for n in ns] for label, _ in CONFIGS}
for label, values in ratios.items():
    ratio_ax.plot(ns, values, color="#9a9a9a", lw=1, alpha=0.8)
ratio_ax.plot(ns, [statistics.median(v[i] for v in ratios.values()) for i in range(len(ns))], color="#222222",
              lw=2.2, marker="D", ms=5, label="median of the 7 configurations")
ratio_ax.axhline(1, color="#666666", ls=":", lw=1.2)
ratio_ax.set_ylim(0.75, 1.1)
ratio_ax.set_title("DataFusion time / DuckDB time\n(grey: each configuration)", fontsize=10.5)
ratio_ax.set_xticks(ns)
ratio_ax.set_xlabel("Concurrent streams")
ratio_ax.legend(frameon=False, fontsize=8.5, loc="lower right")
style(ratio_ax)
fig.suptitle("SF50 orders over the 25 Gb/s link: the same ADBC streams read by DuckDB and by DataFusion "
             "(range bars: both runs)")
fig.tight_layout()
fig.savefig(OUT / "compare.png", dpi=105)
fig.savefig(OUT / "compare.svg", metadata={"Date": None})
with open(OUT / "compare.tsv", "w") as out:
    out.write("configuration\tstreams\tduckdb_runs_s\tdatafusion_runs_s\tdatafusion_over_duckdb_slower\n")
    for label, _ in CONFIGS:
        for i, n in enumerate(ns):
            d, f = data[label]["DuckDB"][n], data[label]["DataFusion"][n]
            out.write(f"{label}\t{n}\t{','.join(f'{x:.3f}' for x in d)}\t{','.join(f'{x:.3f}' for x in f)}\t"
                      f"{ratios[label][i]:.3f}\n")

# 2. Back-to-back single-stream runs and client CPU.
walls = defaultdict(list)  # (client, streams) -> wall times
cpu = defaultdict(dict)  # (client, streams) -> round -> user + sys
cpu_split = defaultdict(lambda: [0.0, 0.0])  # (client, streams) -> [user, sys] summed over rounds
curl = []
for line in open(DATA / "interleaved.tsv"):
    f = line.rstrip("\n").split("\t")
    if f[0] == "# cpu":
        _, rnd, client, streams, _, user, sys_ = f
        u, s = float(user.split("=")[1]), float(sys_.split("=")[1])
        cpu[(client, int(streams))][int(rnd)] = u + s
        cpu_split[(client, int(streams))][0] += u
        cpu_split[(client, int(streams))][1] += s
    elif f[0] == "# curl":
        curl.append(float(f[2]))
    elif f[0].isdigit():
        walls[(f[1], int(f[2]))].append(float(f[4]))

NAMES = {"duckdb": "DuckDB", "datafusion": "DataFusion"}
# A streams=1 process does 4 single-stream reads (warm-up + 3); a streams=10 process does 1 warm-up single-stream
# read and 3 ten-stream batches. Per-read CPU includes a quarter of process start-up and the bounds query.
per_read = {c: statistics.mean(v / 4 for v in cpu[(c, 1)].values()) for c in NAMES}
per_batch10 = {c: statistics.mean((cpu[(c, 10)][r] - cpu[(c, 1)][r] / 4) / 3 for r in cpu[(c, 10)]) for c in NAMES}
user_share = {(c, n): cpu_split[(c, n)][0] / sum(cpu_split[(c, n)]) for c in NAMES for n in (1, 10)}

fig, axes = plt.subplots(1, 2, figsize=(14, 5.4), gridspec_kw={"width_ratios": [1.15, 1]})
ax = axes[0]
groups = [("DuckDB", walls[("duckdb", 1)]), ("DataFusion", walls[("datafusion", 1)]), ("curl", curl)]
for i, (name, values) in enumerate(groups):
    color = COLORS.get(name, "#666666")
    jitter = [(k % 3 - 1) * 0.06 for k in range(len(values))]
    ax.scatter([i + j for j in jitter], values, color=color, s=34, alpha=0.85, edgecolor="white", linewidth=0.6,
               zorder=3)
    med = statistics.median(values)
    ax.hlines(med, i - 0.28, i + 0.28, color="#222222", lw=2, zorder=4)
ax.set_xticks(range(3), [f"{name}\n{len(values)} runs, median {statistics.median(values):.2f} s"
                         for name, values in [("DuckDB", groups[0][1]), ("DataFusion", groups[1][1]),
                                              ("curl, one connection", curl)]])
ax.set_xlim(-0.5, 2.5)
ax.set_ylim(0, 7.8)
ax.set_ylabel("Single-stream wall time (s)")
ax.set_title("One uncompressed stream, the clients alternating in 3 rounds (bar: median)")
style(ax)

ax = axes[1]
labels = ["one stream\n(per 9.67 GB read)", "ten streams\n(per batch, estimated)"]
width = 0.36
for k, c in enumerate(NAMES):
    name = NAMES[c]
    totals = [per_read[c], per_batch10[c]]
    users = [totals[0] * user_share[(c, 1)], totals[1] * user_share[(c, 10)]]
    xs = [x + (k - 0.5) * width for x in range(2)]
    ax.bar(xs, users, width * 0.95, color=COLORS[name], label=f"{name}: user")
    ax.bar(xs, [t - u for t, u in zip(totals, users)], width * 0.95, bottom=users, color=COLORS[name], alpha=0.45,
           label=f"{name}: kernel (sys)")
    for x, t in zip(xs, totals):
        ax.text(x, t + 0.3, f"{t:.1f}", ha="center", fontsize=9, color="#333333")
ax.set_xticks(range(2), labels)
ax.set_ylabel("Client CPU-seconds")
ax.set_title("Client CPU for the same data")
ax.legend(frameon=False, fontsize=8.5, loc="upper left")
style(ax)
fig.suptitle("DuckDB and DataFusion back to back on hp036 (no compression, parallel formatting, 25 Gb/s)")
fig.tight_layout()
fig.savefig(OUT / "interleaved.png", dpi=110)
fig.savefig(OUT / "interleaved.svg", metadata={"Date": None})
with open(OUT / "interleaved_summary.tsv", "w") as out:
    out.write("measure\tDuckDB\tDataFusion\tcurl\n")
    out.write(f"single_stream_median_s\t{statistics.median(walls[('duckdb', 1)]):.3f}\t"
              f"{statistics.median(walls[('datafusion', 1)]):.3f}\t{statistics.median(curl):.3f}\n")
    out.write(f"single_stream_min_max_s\t{min(walls[('duckdb', 1)]):.3f}-{max(walls[('duckdb', 1)]):.3f}\t"
              f"{min(walls[('datafusion', 1)]):.3f}-{max(walls[('datafusion', 1)]):.3f}\t"
              f"{min(curl):.3f}-{max(curl):.3f}\n")
    out.write(f"ten_stream_median_s\t{statistics.median(walls[('duckdb', 10)]):.3f}\t"
              f"{statistics.median(walls[('datafusion', 10)]):.3f}\t\n")
    out.write(f"ten_stream_min_max_s\t{min(walls[('duckdb', 10)]):.3f}-{max(walls[('duckdb', 10)]):.3f}\t"
              f"{min(walls[('datafusion', 10)]):.3f}-{max(walls[('datafusion', 10)]):.3f}\t\n")
    out.write(f"cpu_s_per_single_stream_read\t{per_read['duckdb']:.2f}\t{per_read['datafusion']:.2f}\t\n")
    out.write(f"cpu_s_per_ten_stream_batch_est\t{per_batch10['duckdb']:.2f}\t{per_batch10['datafusion']:.2f}\t\n")
    for n in (1, 10):
        out.write(f"user_share_streams_{n}\t{user_share[('duckdb', n)]:.2f}\t{user_share[('datafusion', n)]:.2f}\t\n")
print(open(OUT / "interleaved_summary.tsv").read())
print(open(OUT / "compare.tsv").read())
