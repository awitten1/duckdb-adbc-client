#!/usr/bin/env python3
"""Summarize the lineitem matrix and render charts.

Usage: analyze_lineitem.py RAW_DIR OUT_DIR [ORDERS_SF50_DIR]
RAW_DIR mirrors build/experiments/lineitem-20261110 (summaries only).
ORDERS_SF50_DIR optionally points at report-repro-20261110/sf50 for comparison.
"""

import json
import statistics
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RAW = Path(sys.argv[1])
OUT = Path(sys.argv[2])
ORDERS50 = Path(sys.argv[3]) if len(sys.argv) > 3 else None
OUT.mkdir(parents=True, exist_ok=True)

KEYS = ("orderkey", "shipdate", "extendedprice", "comment")
ALPHAS = ("0", "0.25", "0.5", "0.75", "1")
TITLES = {"orderkey": "l_orderkey (physical order)", "shipdate": "l_shipdate",
          "extendedprice": "l_extendedprice", "comment": "l_comment"}
ROWS = {5: 29_999_795, 10: 59_986_052, 25: 149_996_355}
BLUE, ORANGE, GREEN, GREY = "#2369a8", "#b45320", "#338269", "#666666"
problems = []
md = []


def load(path):
    data = json.loads(path.read_text())
    if data.get("exit") != 0 or "error" in data:
        problems.append(f"{path.relative_to(path.parents[1])}: {str(data.get('error', data.get('exit')))[:200]}")
        return None
    return data


def runs(root, prefix, case):
    out = []
    for r in (1, 2, 3):
        path = root / f"{prefix}-r{r}-{case}-summary.json"
        if path.exists():
            data = load(path)
            if data:
                out.append(data)
    return out


def times(root, prefix, case):
    return [d["query"]["total_time"] for d in runs(root, prefix, case)]


def med(values):
    return statistics.median(values) if values else float("nan")


def fmt(value, digits=3):
    return "—" if value != value else f"{value:.{digits}f}"


def ratio(a, b):
    return "—" if a != a or b != b or not b else f"{a / b:.2f}×"


def extra(data):
    for node in data["operators"]:
        info = node.get("extra_info") or {}
        if info.get("Function") == "READ_ADBC_PARTIAL_SORT":
            return info
    return {}


def metric(root, prefix, case, name):
    return med([float(extra(d)[name]) for d in runs(root, prefix, case) if name in extra(d)])


def spill(root, prefix, case):
    return med([d["system"].get("peak_temp_dir_size", 0) / 1e9 for d in runs(root, prefix, case)])


def alpha_chart(sf, root, keys, read):
    ncols = 2
    nrows = (len(keys) + 1) // 2
    fig, axes = plt.subplots(nrows, ncols, figsize=(12, 4.25 * nrows), squeeze=False)
    xs = [float(a) for a in ALPHAS]
    for ax, key in zip(axes.flat, keys):
        series = [times(root, key, "partial" + a) for a in ALPHAS]
        medians = [med(s) for s in series]
        lo = [m - min(s) if s else 0 for m, s in zip(medians, series)]
        hi = [max(s) - m if s else 0 for m, s in zip(medians, series)]
        ax.errorbar(xs, medians, yerr=[lo, hi], color=BLUE, marker="o", lw=2.5, ms=6, capsize=4, label="Hybrid")
        ax.axhline(med(times(root, key, "native")), color=ORANGE, lw=2, ls="--", label="DuckDB sort")
        ax.axhline(med(times(root, key, "read_sorted")), color=GREEN, lw=2, ls=":", label="ClickHouse sort")
        ax.axhline(read, color=GREY, lw=2, ls="-.", label="Unsorted read")
        ax.set_title(TITLES[key])
        ax.set_xticks(xs, ["0", ".25", ".5", ".75", "1"])
        ax.set_xlim(-0.05, 1.05)
        ax.set_ylim(0, None)
        ax.grid(axis="y", color="#dddddd")
        ax.set_xlabel("Alpha: key-range fraction sorted remotely")
        ax.set_ylabel("Query time (s; lower is better)")
        ax.legend(loc="best", fontsize=9, framealpha=0.9)
    for ax in list(axes.flat)[len(keys):]:
        ax.axis("off")
    fig.suptitle(f"SF{sf} lineitem ({ROWS[sf] / 1e6:.0f}M rows) on CloudLab: alpha sweep by sort key "
                 "(16 threads, 56 GiB)", fontsize=14)
    fig.tight_layout()
    for ext in ("png", "svg"):
        fig.savefig(OUT / f"lineitem_sf{sf}_alpha_sweep.{ext}", dpi=110)
    plt.close(fig)


def section(sf, keys, narrow=True):
    root = RAW / f"sf{sf}"
    read = med(times(root, "read-wide", "read"))
    if read != read:
        md.append(f"\n_No SF{sf} data._\n")
        return
    alpha_chart(sf, root, keys, read)
    md.append(f"\n## SF{sf} lineitem ({ROWS[sf]:,} rows)\n")
    md.append(f"![SF{sf} lineitem alpha sweep](./lineitem_sf{sf}_alpha_sweep.png)\n")
    md.append("Median of three measured runs after a warmup pass; **bold** is the fastest path per key.\n")
    md.append("| Sort key | DuckDB | ClickHouse | α=0 | α=.25 | α=.5 | α=.75 | α=1 | Best |")
    md.append("|---|---:|---:|---:|---:|---:|---:|---:|---|")
    cases = ["native", "read_sorted"] + ["partial" + a for a in ALPHAS]
    names = ["DuckDB", "ClickHouse"] + ["α=" + a for a in ALPHAS]
    for key in keys:
        values = [med(times(root, key, c)) for c in cases]
        best = min(range(len(values)), key=lambda i: values[i] if values[i] == values[i] else 1e9)
        cells = [f"**{fmt(v)}**" if i == best else fmt(v) for i, v in enumerate(values)]
        md.append(f"| `l_{key}` | " + " | ".join(cells) + f" | {names[best]} |")
    logical = med([d["query"]["total_intermediate_size_bytes"] / 1e9 for d in runs(root, "read-wide", "read")])
    line = f"\nUnsorted `SELECT *` ADBC read: **{fmt(read)} s** ({fmt(logical, 2)} GB logical intermediate output)."
    if narrow:
        line += f" Two-column `l_orderkey,l_extendedprice` read: {fmt(med(times(root, 'read-narrow', 'read')))} s."
    md.append(line)
    md.append("\n| Sort key | DuckDB / read | ClickHouse / read | best hybrid / read | best α |")
    md.append("|---|---:|---:|---:|---:|")
    for key in keys:
        alpha_vals = [med(times(root, key, "partial" + a)) for a in ALPHAS]
        ib = min(range(5), key=lambda i: alpha_vals[i])
        md.append(f"| `l_{key}` | {ratio(med(times(root, key, 'native')), read)} | "
                  f"{ratio(med(times(root, key, 'read_sorted')), read)} | {ratio(alpha_vals[ib], read)} | {ALPHAS[ib]} |")
    md.append("\nHybrid α=.5 internals (median; phases overlap and are not additive):\n")
    md.append("| Sort key | Rows copied (M) | Merge s | Merge loop s | Local sort s | Remote read/buffer s | Peak remote queue GB | Spill GB |")
    md.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for key in keys:
        m = lambda name: metric(root, key, "partial0.5", name)  # noqa: E731
        md.append(f"| `l_{key}` | {fmt(m('Output Copied Rows') / 1e6, 2)} | {fmt(m('Merge Seconds'))} | "
                  f"{fmt(m('Merge Loop Seconds'))} | {fmt(m('Local Sort Seconds'))} | "
                  f"{fmt(m('Remote Read and Buffer Seconds'))} | {fmt(m('Remote Buffer Peak Queued Bytes') / 1e9, 2)} | "
                  f"{fmt(spill(root, key, 'partial0.5'), 2)} |")
    spills = [(key, c, spill(root, key, c)) for key in keys for c in cases]
    spilled = [f"`l_{k}` {c} {s:.2f} GB" for k, c, s in spills if s == s and s > 0.01]
    md.append("\nDuckDB temporary-directory spill: " + (", ".join(spilled) if spilled else "none in any case") + ".")
    if narrow:
        md.append("\nSorted by `l_extendedprice`, full width vs `l_orderkey,l_extendedprice`:\n")
        md.append("| Path | 16 columns | 2 columns |")
        md.append("|---|---:|---:|")
        md.append(f"| Unsorted read | {fmt(read)} | {fmt(med(times(root, 'read-narrow', 'read')))} |")
        labels = {"native": "DuckDB sort", "read_sorted": "ClickHouse sort", "partial0": "Hybrid α=0",
                  "partial0.5": "Hybrid α=.5", "partial1": "Hybrid α=1"}
        for c, label in labels.items():
            md.append(f"| {label} | {fmt(med(times(root, 'extendedprice', c)))} | "
                      f"{fmt(med(times(root, 'extendedprice-narrow', c)))} |")


def scaling():
    md.append("\n## Scaling with row count\n")
    md.append("Seconds per million rows (median time / rows). Flat values mean linear scaling.\n")
    md.append("| Path | SF5 | SF10 | SF25 |")
    md.append("|---|---:|---:|---:|")
    rows = [("Unsorted read", "read-wide", "read")]
    for key in ("shipdate", "extendedprice"):
        rows += [(f"`l_{key}` DuckDB sort", key, "native"), (f"`l_{key}` ClickHouse sort", key, "read_sorted"),
                 (f"`l_{key}` hybrid α=.5", key, "partial0.5"), (f"`l_{key}` hybrid α=.75", key, "partial0.75")]
    for label, prefix, case in rows:
        cells = []
        for sf in (5, 10, 25):
            t = med(times(RAW / f"sf{sf}", prefix, case))
            cells.append(fmt(t / (ROWS[sf] / 1e6), 4))
        md.append(f"| {label} | " + " | ".join(cells) + " |")


def orders_comparison():
    if not ORDERS50 or not ORDERS50.exists():
        return
    li = RAW / "sf10"
    md.append("\n## lineitem SF10 vs orders SF50 (same connector and settings)\n")
    md.append("orders SF50 is 75M rows × 9 columns; lineitem SF10 is 60M rows × 16 columns. "
              "Both use the schema-only, no-compression connector at 16 threads and 56 GiB.\n")
    md.append("| Path | orders SF50, date | lineitem SF10, shipdate | orders SF50, price | lineitem SF10, extendedprice |")
    md.append("|---|---:|---:|---:|---:|")
    o_read = med(times(ORDERS50, "read-wide", "read"))
    l_read = med(times(li, "read-wide", "read"))
    md.append(f"| Unsorted read | {fmt(o_read)} | {fmt(l_read)} | {fmt(o_read)} | {fmt(l_read)} |")
    for label, case in (("DuckDB sort", "native"), ("ClickHouse sort", "read_sorted"),
                        ("Hybrid α=.5", "partial0.5"), ("Hybrid α=.75", "partial0.75")):
        md.append(f"| {label} | {fmt(med(times(ORDERS50, 'orderdate', case)))} | {fmt(med(times(li, 'shipdate', case)))} | "
                  f"{fmt(med(times(ORDERS50, 'totalprice', case)))} | {fmt(med(times(li, 'extendedprice', case)))} |")
    o_bytes = med([d["query"]["total_intermediate_size_bytes"] / 1e9 for d in runs(ORDERS50, "read-wide", "read")])
    l_bytes = med([d["query"]["total_intermediate_size_bytes"] / 1e9 for d in runs(li, "read-wide", "read")])
    md.append(f"\nLogical bytes read: orders SF50 {fmt(o_bytes, 2)} GB, lineitem SF10 {fmt(l_bytes, 2)} GB. "
              f"Unsorted read throughput: {fmt(o_bytes / o_read, 2)} vs {fmt(l_bytes / l_read, 2)} GB/s.")


md.append("# lineitem sort experiments on CloudLab\n")
section(5, KEYS)
section(10, KEYS)
section(25, ("shipdate", "extendedprice"), narrow=False)
scaling()
orders_comparison()
if problems:
    md.append("\n## Failed or invalid runs\n")
    md.extend(f"- {p}" for p in problems)
(OUT / "results.md").write_text("\n".join(md) + "\n")
print(f"wrote {OUT / 'results.md'}; {len(problems)} problem runs")
