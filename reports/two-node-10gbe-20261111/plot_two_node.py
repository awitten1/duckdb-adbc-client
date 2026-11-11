#!/usr/bin/env python3
"""Charts for the two-node 10 Gb/s report.

Usage: plot_two_node.py DATA_DIR OUT_DIR
Reads DATA_DIR/{,lz4/}streams-par{1,0}.tsv (parallel_read_adbc output), DATA_DIR/{,lz4/}curl.txt and
DATA_DIR/iperf3.txt. Writes streams.{png,svg,tsv}.
"""
import csv
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
from matplotlib.ticker import FixedLocator, NullLocator

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

DATA, OUT = Path(sys.argv[1]), Path(sys.argv[2])
# (subdirectory, parallel_formatting, label, color, marker, line style)
CONFIGS = (("", 1, "No compression, parallel formatting", "#1baf7a", "^", "-"),
           ("", 0, "No compression, serial formatting", "#2a78d6", "o", "-"),
           ("lz4", 1, "LZ4 driver compression, parallel formatting", "#1baf7a", "^", "--"),
           ("lz4", 0, "LZ4 driver compression, serial formatting", "#2a78d6", "o", "--"))
ARROW_BYTES = 9_673_331_016  # uncompressed ArrowStream size of the full table (data/curl.txt)


def load_streams(sub, parallel):
    walls = defaultdict(list)
    for row in csv.DictReader(open(DATA / sub / f"streams-par{parallel}.tsv"), delimiter="\t"):
        walls[int(row["streams"])].append(float(row["wall_s"]))
    return {n: statistics.median_high(v) for n, v in sorted(walls.items())}


def load_curl(sub):
    times = defaultdict(list)
    for line in open(DATA / sub / "curl.txt"):
        m = re.match(r"parallel_formatting=(\d) run \d: ([\d.]+) s", line)
        if m:
            times[int(m.group(1))].append(float(m.group(2)))
    return {p: statistics.median_high(v) for p, v in times.items()}


link_gbs = max(float(m.group(1)) for m in re.finditer(r"= ([\d.]+) GB/s", open(DATA / "iperf3.txt").read()))
floor_s = ARROW_BYTES / 1e9 / link_gbs
streams = {(sub, p): load_streams(sub, p) for sub, p, *_ in CONFIGS}
curl = {sub: load_curl(sub) for sub in ("", "lz4")}
ns = sorted(streams[("", 1)])

fig, axes = plt.subplots(1, 2, figsize=(14, 5.6))
for sub, parallel, label, color, marker, style in CONFIGS:
    data = streams[(sub, parallel)]
    axes[0].plot(ns, [data[n] for n in ns], color=color, marker=marker, ls=style, lw=2, ms=7, label=label)
    axes[1].plot(ns, [ARROW_BYTES / 1e9 / data[n] for n in ns], color=color, marker=marker, ls=style, lw=2, ms=7)
    c = curl[sub][parallel]
    axes[0].plot([1], [c], color=color, marker="*", ms=13, ls="none", markeredgecolor="white")
    axes[1].plot([1], [ARROW_BYTES / 1e9 / c], color=color, marker="*", ms=13, ls="none", markeredgecolor="white")
axes[0].plot([], [], color="#666666", marker="*", ms=13, ls="none", label="curl, one connection (same settings)")
axes[0].axhline(floor_s, color="#666666", ls=":", lw=1.2)
axes[0].text(ns[1], floor_s * 0.93, f"uncompressed link minimum {floor_s:.1f} s", ha="left", color="#444444",
             fontsize=9)
axes[0].set_yscale("log")
ticks = [4, 6, 8, 12, 16, 24, 32, 48, 64, 96]
axes[0].yaxis.set_major_locator(FixedLocator(ticks))
axes[0].yaxis.set_minor_locator(NullLocator())
axes[0].set_yticklabels([str(t) for t in ticks])
axes[0].set_ylim(4, 100)
axes[0].set_ylabel("Batch wall time (s, log scale; median of 2 runs; lower is better)")
axes[0].set_title("Runtime")
axes[1].axhline(link_gbs, color="#666666", ls=":", lw=1.2)
axes[1].text(ns[-1], link_gbs + 0.04, f"link rate {link_gbs:.2f} GB/s", ha="right", color="#444444", fontsize=9)
axes[1].set_ylim(0, 1.8)
axes[1].set_ylabel("Arrow data delivered (GB/s of uncompressed Arrow)")
axes[1].set_title("Throughput (compressed runs send 2.1× fewer bytes)")
for ax in axes:
    ax.set_xlabel("Concurrent read_adbc streams (o_orderkey ranges)")
    ax.set_xticks(ns)
    ax.grid(axis="y", color="#e5e5e5")
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
axes[0].legend(frameon=False, fontsize=8.5, loc="upper right")
fig.suptitle("SF50 orders, ClickHouse and DuckDB on separate nodes, 10 Gb/s link, CPUs at 2.0 GHz")
fig.tight_layout()
fig.savefig(OUT / "streams.png", dpi=110)
fig.savefig(OUT / "streams.svg", metadata={"Date": None})
with open(OUT / "streams.tsv", "w") as out:
    keys = [(sub or "nocompress", p) for sub, p, *_ in CONFIGS]
    out.write("streams\t" + "\t".join(f"{k}_par{p}_s" for k, p in keys) + "\n")
    for n in ns:
        out.write(f"{n}\t" + "\t".join(f"{streams[(sub, p)][n]:.3f}" for sub, p, *_ in CONFIGS) + "\n")
print(open(OUT / "streams.tsv").read())
print(f"link {link_gbs:.3f} GB/s, floor {floor_s:.2f} s; curl medians: {curl}")

# A chart of its own for the LZ4 run: runtime, and throughput both on the wire and in Arrow bytes delivered.
WIRE_BYTES = {}
for line in open(DATA / "lz4" / "curl.txt"):
    m = re.match(r"parallel_formatting=(\d) run \d: [\d.]+ s, (\d+) bytes", line)
    if m:
        WIRE_BYTES[int(m.group(1))] = int(m.group(2))
best_uncompressed = min(min(streams[("", p)].values()) for p in (0, 1))
fig, axes = plt.subplots(1, 2, figsize=(14, 5.4))
for sub, parallel, label, color, marker, style in CONFIGS:
    if sub != "lz4":
        continue
    data = streams[(sub, parallel)]
    short = "parallel formatting" if parallel else "serial formatting"
    axes[0].plot(ns, [data[n] for n in ns], color=color, marker=marker, lw=2, ms=7, label=f"read_adbc, {short}")
    axes[0].plot([1], [curl["lz4"][parallel]], color=color, marker="*", ms=14, ls="none", markeredgecolor="white",
                 label=f"curl, one connection, {short}")
    for n in (1, 10):
        axes[0].annotate(f"{data[n]:.1f} s", (n, data[n]), textcoords="offset points",
                         xytext=(8, 4) if n == 1 else (-34, -15 if parallel else 9), fontsize=8.5, color="#333333")
    axes[1].plot(ns, [WIRE_BYTES[parallel] / 1e9 / data[n] for n in ns], color=color, marker=marker, lw=2, ms=7,
                 ls="--", label=f"on the wire (LZ4 bytes), {short}")
    axes[1].plot(ns, [ARROW_BYTES / 1e9 / data[n] for n in ns], color=color, marker=marker, lw=2, ms=7,
                 label=f"Arrow data delivered, {short}")
axes[0].axhline(best_uncompressed, color="#666666", ls=":", lw=1.2)
axes[0].text(ns[-1], best_uncompressed * 1.07, f"best uncompressed batch {best_uncompressed:.2f} s", ha="right",
             color="#444444", fontsize=9)
axes[0].set_yscale("log")
axes[0].yaxis.set_major_locator(FixedLocator(ticks))
axes[0].yaxis.set_minor_locator(NullLocator())
axes[0].set_yticklabels([str(t) for t in ticks])
axes[0].set_ylim(4, 100)
axes[0].set_ylabel("Batch wall time (s, log scale; median of 2 runs; lower is better)")
axes[0].set_title("Runtime with the driver's LZ4 compression")
axes[1].axhline(link_gbs, color="#666666", ls=":", lw=1.2)
axes[1].text(ns[0], link_gbs + 0.04, f"link rate {link_gbs:.2f} GB/s", ha="left", color="#444444", fontsize=9)
axes[1].set_ylim(0, 1.8)
axes[1].set_ylabel("GB/s")
axes[1].set_title(f"Throughput: LZ4 sends {ARROW_BYTES / WIRE_BYTES[1]:.1f}× fewer bytes than uncompressed Arrow")
for ax in axes:
    ax.set_xlabel("Concurrent read_adbc streams (o_orderkey ranges)")
    ax.set_xticks(ns)
    ax.grid(axis="y", color="#e5e5e5")
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.legend(frameon=False, fontsize=8.5, loc="upper right" if ax is axes[0] else "upper left")
fig.suptitle("SF50 orders over the 10 Gb/s link with the ADBC driver's LZ4 compression (ClickHouse compresses on one thread per query)")
fig.tight_layout()
fig.savefig(OUT / "lz4_streams.png", dpi=110)
fig.savefig(OUT / "lz4_streams.svg", metadata={"Date": None})
with open(OUT / "lz4_streams.tsv", "w") as out:
    out.write("streams\tparallel_s\tparallel_wire_gb_per_s\tparallel_arrow_gb_per_s\t"
              "serial_s\tserial_wire_gb_per_s\tserial_arrow_gb_per_s\n")
    for n in ns:
        cells = []
        for parallel in (1, 0):
            t = streams[("lz4", parallel)][n]
            cells += [f"{t:.3f}", f"{WIRE_BYTES[parallel] / 1e9 / t:.2f}", f"{ARROW_BYTES / 1e9 / t:.2f}"]
        out.write(f"{n}\t" + "\t".join(cells) + "\n")
print(open(OUT / "lz4_streams.tsv").read())
