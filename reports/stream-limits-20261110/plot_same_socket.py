#!/usr/bin/env python3
"""Plot stream count vs runtime with DuckDB on ClickHouse's socket vs the other socket.

Usage: plot_same_socket.py DATA_DIR OUT_DIR
Reads data/same-socket-{current,reserve}-{same,other}.tsv (parallel_read_adbc output: streams, repeat, wall_s).
Each point is the slower of the two measured runs; the whisker reaches down to the faster one.
"""
import csv
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

DATA, OUT = Path(sys.argv[1]), Path(sys.argv[2])
SERIES = (
    ("current", "same", "Current connector, same socket", "#b45320", "-", "s"),
    ("current", "other", "Current connector, other socket", "#b45320", "--", "o"),
    ("reserve", "same", "Reserve fix, same socket", "#2369a8", "-", "s"),
    ("reserve", "other", "Reserve fix, other socket", "#2369a8", "--", "o"),
)


def load(connector, placement):
    walls = defaultdict(list)
    for row in csv.DictReader(open(DATA / f"same-socket-{connector}-{placement}.tsv"), delimiter="\t"):
        walls[int(row["streams"])].append(float(row["wall_s"]))
    return {n: (min(v), max(v)) for n, v in sorted(walls.items())}


fig, ax = plt.subplots(figsize=(10, 5.5))
rows = {}
for connector, placement, label, color, style, marker in SERIES:
    data = load(connector, placement)
    rows[(connector, placement)] = data
    streams = list(data)
    slow = [data[n][1] for n in streams]
    fast = [data[n][0] for n in streams]
    ax.errorbar(streams, slow, yerr=[[s - f for s, f in zip(slow, fast)], [0] * len(slow)], color=color,
                linestyle=style, marker=marker, lw=2.2, ms=6, capsize=3, label=label)
ax.set_xticks(sorted({n for d in rows.values() for n in d}))
ax.set_ylim(0, None)
ax.set_xlabel("Concurrent read_adbc streams (threads in one DuckDB process)")
ax.set_ylabel("Batch wall time (s; lower is better)")
ax.set_title("SF50 orders, modified ClickHouse: DuckDB limited to 4 CPUs on ClickHouse's socket vs the other socket\n"
             "Slower of 2 runs; whisker down to the faster run")
ax.grid(axis="y", color="#dddddd")
ax.set_axisbelow(True)
ax.legend()
fig.tight_layout()
fig.savefig(OUT / "same_socket_streams.png", dpi=110)
fig.savefig(OUT / "same_socket_streams.svg", metadata={"Date": None})

with open(OUT / "same_socket_streams.tsv", "w") as out:
    out.write("streams\t" + "\t".join(f"{c}_{p}_slower_s\t{c}_{p}_faster_s" for c, p, *_ in SERIES) + "\n")
    for n in sorted(rows[("current", "same")]):
        cells = []
        for connector, placement, *_ in SERIES:
            fast, slow = rows[(connector, placement)][n]
            cells += [f"{slow:.3f}", f"{fast:.3f}"]
        out.write(f"{n}\t" + "\t".join(cells) + "\n")
print(open(OUT / "same_socket_streams.tsv").read())
