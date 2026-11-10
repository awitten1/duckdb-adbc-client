#!/usr/bin/env python3
"""Plot stream count vs runtime and speedup over one stream for both ClickHouse builds.

Usage: plot_streams.py PACKAGED_TSV MODIFIED_TSV OUT_DIR
Each TSV is a results.tsv written by run.sh (one row per stream per batch).
"""

import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

EXPECTED_ROWS = 75_000_000
SERIES = (
    ("ClickHouse 26.9.12.8 (packaged)", sys.argv[1], "#b45320", "s"),
    ("ClickHouse 26.11.1.1 + Arrow IPC output change", sys.argv[2], "#2369a8", "o"),
)
OUT = Path(sys.argv[3])
OUT.mkdir(parents=True, exist_ok=True)


def batches(path):
    """Return {streams: [batch wall seconds]} for measured (non-warmup) batches."""
    walls, rows = defaultdict(dict), defaultdict(int)
    for row in csv.DictReader(open(path), delimiter="\t"):
        if row["tag"].startswith("warmup"):
            continue
        walls[int(row["streams"])][row["tag"]] = float(row["batch_wall_s"])
        rows[row["tag"]] += int(row["rows"])
    bad = [tag for tag, count in rows.items() if count != EXPECTED_ROWS]
    if bad:
        raise SystemExit(f"{path}: batches without {EXPECTED_ROWS} rows: {bad}")
    return {n: sorted(by_tag.values()) for n, by_tag in walls.items()}


def save(fig, stem):
    fig.savefig(OUT / f"{stem}.png", dpi=110)
    fig.savefig(OUT / f"{stem}.svg", metadata={"Date": None})
    plt.close(fig)


data = [(label, batches(path), color, marker) for label, path, color, marker in SERIES]
streams = sorted(data[0][1])

fig, ax = plt.subplots(figsize=(10, 5.5))
for label, by_n, color, marker in data:
    medians = [statistics.median(by_n[n]) for n in streams]
    lo = [m - min(by_n[n]) for m, n in zip(medians, streams)]
    hi = [max(by_n[n]) - m for m, n in zip(medians, streams)]
    ax.errorbar(streams, medians, yerr=[lo, hi], color=color, marker=marker, lw=2.5, ms=7,
                capsize=4, label=label)
    for n, m in zip(streams, medians):
        if n in (1, streams[-1]):
            ax.annotate(f"{m:.2f} s", (n, m), textcoords="offset points", xytext=(8, 4),
                        color=color, fontsize=9)
ax.set_xticks(streams)
ax.set_xlim(0.5, streams[-1] + 0.9)
ax.set_ylim(0, None)
ax.set_xlabel("Concurrent ADBC streams (contiguous o_orderkey ranges)")
ax.set_ylabel("Batch wall time (s; lower is better)")
ax.set_title("SF50 orders (75M rows, 9 columns): parallel ADBC scan time by stream count\n"
             "DuckDB and ClickHouse each on 16 CPUs at a fixed 2.6 GHz; median of 3 runs, whiskers min–max")
ax.grid(axis="y", color="#dddddd")
ax.set_axisbelow(True)
ax.legend()
fig.tight_layout()
save(fig, "streams_runtime")

# Speedup over each server's own single-stream median: 1.0 at N=1 by definition.
fig, ax = plt.subplots(figsize=(10, 5.5))
ax.plot(streams, streams, color="#999999", lw=1.5, ls="--", label="Ideal (speedup = streams)")
for label, by_n, color, marker in data:
    base = statistics.median(by_n[1])
    speedups = [base / statistics.median(by_n[n]) for n in streams]
    ax.plot(streams, speedups, color=color, marker=marker, lw=2.5, ms=7, label=label)
    ax.annotate(f"{speedups[-1]:.2f}×", (streams[-1], speedups[-1]), textcoords="offset points",
                xytext=(8, -3), color=color, fontsize=9)
ax.set_xticks(streams)
ax.set_xlim(0.5, streams[-1] + 0.9)
ax.set_ylim(0, 5)
ax.set_xlabel("Concurrent ADBC streams (contiguous o_orderkey ranges)")
ax.set_ylabel("Speedup over one stream (× ; higher is better)")
ax.set_title("SF50 orders: speedup over each server's own single-stream time\n"
             "Fixed 2.6 GHz; ratio of median batch times. Relative to each server, not to each other")
ax.grid(axis="y", color="#dddddd")
ax.set_axisbelow(True)
ax.legend(loc="upper left")
fig.tight_layout()
save(fig, "streams_speedup")

with open(OUT / "streams_runtime.tsv", "w") as out:
    out.write("streams\tpackaged_median_s\tpackaged_min_s\tpackaged_max_s\t"
              "modified_median_s\tmodified_min_s\tmodified_max_s\t"
              "packaged_speedup\tmodified_speedup\n")
    for n in streams:
        cells = []
        for _, by_n, _, _ in data:
            cells += [statistics.median(by_n[n]), min(by_n[n]), max(by_n[n])]
        speedups = [statistics.median(by_n[1]) / statistics.median(by_n[n]) for _, by_n, _, _ in data]
        out.write(f"{n}\t" + "\t".join(f"{v:.3f}" for v in cells + speedups) + "\n")
print(open(OUT / "streams_runtime.tsv").read())
