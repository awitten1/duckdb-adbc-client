#!/usr/bin/env python3
"""Charts for the 25 Gb/s two-node report.

Usage: plot_25gbe.py DATA_DIR TEN_GBE_DATA_DIR OUT_DIR
DATA_DIR is this report's data/ ({,lz4/}streams-par{0,1}.tsv and -clickhouse.txt, {,lz4/}curl.txt, iperf3.txt,
sorted/streams-*.tsv and -clickhouse.txt). TEN_GBE_DATA_DIR is the 10 Gb/s report's data/, for the comparison.
Writes streams.*, sorted_streams.*, link_comparison.* (png, svg) and the plotted values as .tsv.
Every point is the slower of the two runs.
"""
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
from matplotlib.ticker import FixedLocator, NullLocator

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

DATA, TEN, OUT = (Path(a) for a in sys.argv[1:4])
ARROW_GB = 9.673331016  # uncompressed ArrowStream size of the full table
CPUS = 20
GREEN, BLUE, ORANGE, AQUA = "#1baf7a", "#2a78d6", "#eb6834", "#1baf7a"
GREY = "#666666"


def all_walls(path):
    w = defaultdict(list)
    for row in csv.DictReader(open(path), delimiter="\t"):
        w[int(row["streams"])].append(float(row["wall_s"]))
    return dict(sorted(w.items()))


def walls(path):
    return {n: max(v) for n, v in all_walls(path).items()}


def ch_cpu(path):
    """streams -> ClickHouse CPU-seconds per batch, from a PrettyCompactMonoBlock query_log table."""
    text = open(path).read().splitlines()
    header = [c.strip(" ─┌┐") for c in next(l for l in text if "┌" in l).split("┬")]
    col = header.index("server_cpu_s_per_batch")
    return {int(c[0]): float(c[col]) for c in ([x.strip() for x in l.split("│")[1:-1]] for l in text if "│" in l)}


def curl(path, pattern):
    """Slower of the runs matching pattern (group 1 = key, group 2 = seconds)."""
    out = defaultdict(float)
    for line in open(path):
        m = re.match(pattern, line)
        if m:
            out[m.group(1)] = max(out[m.group(1)], float(m.group(2)))
    return dict(out)


def style(ax, xlabel, ns):
    ax.set_xlabel(xlabel)
    ax.set_xticks(ns)
    ax.grid(axis="y", color="#e5e5e5")
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)


def log_seconds(ax, ticks, lo, hi):
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(FixedLocator(ticks))
    ax.yaxis.set_minor_locator(NullLocator())
    ax.set_yticklabels([str(t) for t in ticks])
    ax.set_ylim(lo, hi)


rates = [(int(m.group(1)), float(m.group(2))) for m in
         re.finditer(r"(\d) connection\(s\), hp034 -> hp036.*?= ([\d.]+) GB/s", open(DATA / "iperf3.txt").read())]
one_conn = dict(rates)[1]
link = max(r for _, r in rates)
floor_s = ARROW_GB / link
XLABEL = "Concurrent read_adbc streams (o_orderkey ranges)"

# 1. The stream sweep: compression x formatting.
CONFIGS = (("", 1, "No compression, parallel formatting", GREEN, "^", "-"),
           ("", 0, "No compression, serial formatting", BLUE, "o", "-"),
           ("lz4", 1, "LZ4 driver compression, parallel formatting", GREEN, "^", "--"),
           ("lz4", 0, "LZ4 driver compression, serial formatting", BLUE, "o", "--"))
sweep = {(s, p): walls(DATA / s / f"streams-par{p}.tsv") for s, p, *_ in CONFIGS}
sweep_cpu = {(s, p): ch_cpu(DATA / s / f"streams-par{p}-clickhouse.txt") for s, p, *_ in CONFIGS}
curls = {s: curl(DATA / s / "curl.txt", r"parallel_formatting=(\d) run \d: ([\d.]+) s") for s in ("", "lz4")}
ns = sorted(sweep[("", 1)])

fig, axes = plt.subplots(1, 2, figsize=(14, 5.6))
for s, p, label, color, marker, ls in CONFIGS:
    d = sweep[(s, p)]
    axes[0].plot(ns, [d[n] for n in ns], color=color, marker=marker, ls=ls, lw=2, ms=7, label=label)
    axes[1].plot(ns, [ARROW_GB / d[n] for n in ns], color=color, marker=marker, ls=ls, lw=2, ms=7)
    runs = all_walls(DATA / s / f"streams-par{p}.tsv")
    axes[0].vlines(ns, [min(runs[n]) for n in ns], [max(runs[n]) for n in ns], color=color, lw=4, alpha=0.35)
    c = curls[s][str(p)]
    axes[0].plot([1], [c], color=color, marker="*", ms=13, ls="none", markeredgecolor="white")
    axes[1].plot([1], [ARROW_GB / c], color=color, marker="*", ms=13, ls="none", markeredgecolor="white")
axes[0].plot([], [], color=GREY, marker="*", ms=13, ls="none", label="curl, one connection (same settings)")
axes[0].vlines([], [], [], color=GREY, lw=4, alpha=0.35, label="range of the 2 runs (line: slower run)")
axes[0].axhline(floor_s, color=GREY, ls=":", lw=1.2)
axes[0].text(ns[-1], floor_s * 0.92, f"uncompressed link minimum {floor_s:.2f} s", ha="right", color="#444444",
             fontsize=9)
log_seconds(axes[0], [3, 4, 6, 8, 12, 16, 24, 32], 2.8, 34)
axes[0].set_ylabel("Batch wall time (s, log scale; slower of 2 runs; lower is better)")
axes[0].set_title("Runtime")
axes[1].axhline(link, color=GREY, ls=":", lw=1.2)
axes[1].text(ns[-1], link + 0.06, f"link, 4 TCP connections: {link:.2f} GB/s", ha="right", color="#444444",
             fontsize=9)
axes[1].axhline(one_conn, color=GREY, ls="--", lw=1)
axes[1].text(4, one_conn - 0.13, f"one TCP connection: {one_conn:.2f} GB/s", ha="left", color="#444444",
             fontsize=9)
axes[1].set_ylim(0, 3.4)
axes[1].set_ylabel("Arrow data delivered (GB/s of uncompressed Arrow)")
axes[1].set_title("Throughput (LZ4 runs send 2.1× fewer bytes)")
for ax in axes:
    style(ax, XLABEL, ns)
axes[0].legend(frameon=False, fontsize=8.5, loc="upper right")
fig.suptitle("SF50 orders, ClickHouse and DuckDB on separate nodes, 25 Gb/s link, CPUs at 2.4 GHz")
fig.tight_layout()
for ext in ("png", "svg"):
    fig.savefig(OUT / f"streams.{ext}", **({"dpi": 110} if ext == "png" else {"metadata": {"Date": None}}))
with open(OUT / "streams.tsv", "w") as out:
    keys = [f"{s or 'nocompress'}_par{p}" for s, p, *_ in CONFIGS]
    out.write("streams\t" + "\t".join(f"{k}_s\t{k}_ch_cpus" for k in keys) + "\n")
    for n in ns:
        out.write(f"{n}\t" + "\t".join(f"{sweep[(s, p)][n]:.3f}\t{sweep_cpu[(s, p)][n] / sweep[(s, p)][n]:.1f}"
                                       for s, p, *_ in CONFIGS) + "\n")

# 2. Sorted streams.
SORTED = (("Unsorted", sweep[("", 1)], sweep_cpu[("", 1)], BLUE, "o", "-"),
          ("ORDER BY o_orderkey (the sort key)", walls(DATA / "sorted/streams-nocompress-orderkey.tsv"),
           ch_cpu(DATA / "sorted/streams-nocompress-orderkey-clickhouse.txt"), ORANGE, "s", "-"),
          ("ORDER BY o_custkey", walls(DATA / "sorted/streams-nocompress-custkey.tsv"),
           ch_cpu(DATA / "sorted/streams-nocompress-custkey-clickhouse.txt"), AQUA, "^", "-"),
          ("LZ4, unsorted", sweep[("lz4", 1)], sweep_cpu[("lz4", 1)], BLUE, "o", "--"),
          ("LZ4, ORDER BY o_orderkey", walls(DATA / "sorted/streams-lz4-orderkey.tsv"),
           ch_cpu(DATA / "sorted/streams-lz4-orderkey-clickhouse.txt"), ORANGE, "s", "--"))
fig, axes = plt.subplots(1, 2, figsize=(14, 5.6))
for label, w, cpu, color, marker, ls in SORTED:
    axes[0].plot(ns, [w[n] for n in ns], color=color, marker=marker, ls=ls, lw=2, ms=7, label=label)
    axes[1].plot(ns, [cpu[n] / w[n] for n in ns], color=color, marker=marker, ls=ls, lw=2, ms=7)
axes[0].axhline(floor_s, color=GREY, ls=":", lw=1.2)
axes[0].text(ns[0], floor_s * 0.92, f"uncompressed link minimum {floor_s:.2f} s", ha="left", color="#444444",
             fontsize=9)
log_seconds(axes[0], [3, 4, 6, 8, 12, 16, 24, 32], 2.8, 34)
axes[0].set_ylabel("Batch wall time (s, log scale; slower of 2 runs; lower is better)")
axes[0].set_title("Runtime")
axes[1].axhline(CPUS, color=GREY, ls=":", lw=1.2)
axes[1].text(ns[0], CPUS + 0.3, f"all {CPUS} CPUs of the ClickHouse node", ha="left", color="#444444", fontsize=9)
axes[1].set_ylim(0, 22)
axes[1].set_ylabel("ClickHouse CPU-seconds per batch / batch wall time")
axes[1].set_title("Average ClickHouse CPUs busy")
for ax in axes:
    style(ax, "Concurrent read_adbc streams (o_orderkey ranges, each sorted as labeled)", ns)
axes[0].legend(frameon=False, fontsize=8.5, loc="upper right")
fig.suptitle("SF50 orders, per-stream ORDER BY, 25 Gb/s link, parallel formatting, CPUs at 2.4 GHz")
fig.tight_layout()
for ext in ("png", "svg"):
    fig.savefig(OUT / f"sorted_streams.{ext}", **({"dpi": 110} if ext == "png" else {"metadata": {"Date": None}}))
with open(OUT / "sorted_streams.tsv", "w") as out:
    names = ["unsorted", "orderkey", "custkey", "lz4_unsorted", "lz4_orderkey"]
    out.write("streams\t" + "\t".join(f"{k}_s\t{k}_ch_cpus" for k in names) + "\n")
    for n in ns:
        out.write(f"{n}\t" + "\t".join(f"{w[n]:.3f}\t{cpu[n] / w[n]:.1f}" for _, w, cpu, *_ in SORTED) + "\n")

# 3. 10 Gb/s vs 25 Gb/s, parallel formatting.
COMPARE = (("No compression, 25 Gb/s", sweep[("", 1)], BLUE, "o", "-"),
           ("LZ4, 25 Gb/s", sweep[("lz4", 1)], ORANGE, "s", "-"),
           ("No compression, 10 Gb/s", walls(TEN / "streams-par1.tsv"), BLUE, "o", "--"),
           ("LZ4, 10 Gb/s", walls(TEN / "lz4" / "streams-par1.tsv"), ORANGE, "s", "--"))
fig, ax = plt.subplots(figsize=(8.5, 5.6))
for label, w, color, marker, ls in COMPARE:
    ax.plot(ns, [w[n] for n in ns], color=color, marker=marker, ls=ls, lw=2, ms=7, label=label)
    ax.annotate(f"{w[10]:.2f} s", (10, w[10]), textcoords="offset points", xytext=(8, -3), fontsize=8.5,
                color="#333333")
log_seconds(ax, [3, 4, 6, 8, 12, 16, 24, 32], 2.8, 34)
ax.set_ylabel("Batch wall time (s, log scale; slower of 2 runs; lower is better)")
ax.set_title("The LZ4 crossover: compression wins at 10 Gb/s and loses at 25 Gb/s\n"
             "(parallel formatting; 10 Gb/s nodes: 16 CPUs at 2.0 GHz; 25 Gb/s nodes: 20 CPUs at 2.4 GHz)",
             fontsize=10)
style(ax, XLABEL, ns)
ax.set_xlim(0.5, 11.2)
ax.legend(frameon=False, fontsize=9, loc="upper right")
fig.tight_layout()
for ext in ("png", "svg"):
    fig.savefig(OUT / f"link_comparison.{ext}", **({"dpi": 110} if ext == "png" else {"metadata": {"Date": None}}))
with open(OUT / "link_comparison.tsv", "w") as out:
    out.write("streams\t" + "\t".join(l.replace(", ", "_").replace(" ", "_").replace("/", "_per_")
                                      for l, *_ in COMPARE) + "\n")
    for n in ns:
        out.write(f"{n}\t" + "\t".join(f"{w[n]:.3f}" for _, w, *_ in COMPARE) + "\n")

for f in ("streams.tsv", "sorted_streams.tsv", "link_comparison.tsv"):
    print(f"## {f}\n" + open(OUT / f).read())
print(f"link {link:.3f} GB/s (one connection {one_conn:.3f}), floor {floor_s:.2f} s; curl slower runs: {curls}")
