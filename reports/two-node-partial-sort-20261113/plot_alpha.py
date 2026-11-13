#!/usr/bin/env python3
"""Plot the SF50 partial-sort alpha sweep from the report's measured timings."""

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
ALPHAS = (0, 0.25, 0.5, 0.75, 1)
KEYS = ("o_orderkey", "o_orderdate", "o_totalprice", "o_clerk")
DUCKDB = "#2a78d6"
DATAFUSION = "#cb501f"


def load_timings(filename, case_column, time_column):
    with (ROOT / "data" / filename).open(newline="") as handle:
        return {
            (row["sort_key"], row[case_column]): float(row[time_column])
            for row in csv.DictReader(handle, delimiter="\t")
        }


def main():
    duckdb = load_timings("duckdb.tsv", "case", "query_s")
    datafusion = load_timings("datafusion.tsv", "alpha", "wall_s")
    fig, axes = plt.subplots(2, 2, figsize=(10.5, 7.1), sharex=True, sharey=True)
    for ax, key in zip(axes.flat, KEYS):
        ax.plot(
            ALPHAS, [duckdb[key, f"partial{a:g}"] for a in ALPHAS],
            color=DUCKDB, marker="o", linewidth=2, label="DuckDB partial sort",
        )
        ax.plot(
            ALPHAS, [datafusion[key, f"{a:g}"] for a in ALPHAS],
            color=DATAFUSION, marker="s", linewidth=2, label="DataFusion physical plan",
        )
        ax.axhline(duckdb[key, "native"], color="#495362", linestyle="--",
                   linewidth=1.5, label="DuckDB ORDER BY")
        ax.axhline(duckdb[key, "read_sorted"], color="#495362", linestyle=":",
                   linewidth=1.5, label="ClickHouse ORDER BY")
        ax.set_title(key, loc="left", fontfamily="monospace", fontweight="bold")
        ax.set_xticks(ALPHAS, ("0", "0.25", "0.50", "0.75", "1"))
        ax.set_ylim(0, 47)
        ax.set_yticks(range(0, 41, 10))
        ax.set_axisbelow(True)
        ax.grid(axis="y", color="#e1e5ea")
        ax.spines[["top", "right"]].set_visible(False)
        ax.spines[["bottom", "left"]].set_color("#acb4bf")

    fig.suptitle("Partial sort runtime by pushdown ratio", fontsize=18, fontweight="bold", y=0.97)
    fig.text(0.5, 0.925, "TPC-H SF50 orders · 75 million rows · one measured pass",
             ha="center", fontsize=10, color="#495362")
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.909),
               ncol=4, frameon=False, fontsize=9)
    fig.supxlabel("Pushdown ratio α (fraction of key range sorted in ClickHouse)", y=0.026)
    fig.supylabel("Runtime (seconds)", x=0.028)
    fig.subplots_adjust(left=0.10, right=0.97, top=0.81, bottom=0.11, hspace=0.32, wspace=0.15)
    fig.savefig(ROOT / "alpha_sweep.png", dpi=150)
    svg = ROOT / "alpha_sweep.svg"
    fig.savefig(svg, metadata={"Date": None})
    svg.write_text("".join(line.rstrip() + "\n" for line in svg.read_text().splitlines()))


if __name__ == "__main__":
    main()
