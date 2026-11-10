#!/usr/bin/env python3
"""Summarize the CloudLab report-repro matrix and render charts.

Usage: analyze.py RAW_DIR OUT_DIR
RAW_DIR mirrors build/experiments/report-repro-20261110 (summaries only).
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
OUT.mkdir(parents=True, exist_ok=True)

KEYS = ("orderkey", "orderdate", "totalprice", "clerk")
ALPHAS = ("0", "0.25", "0.5", "0.75", "1")
TITLES = {"orderkey": "o_orderkey (physical order)", "orderdate": "o_orderdate",
          "totalprice": "o_totalprice", "clerk": "o_clerk"}
BLUE, ORANGE, GREEN, GREY, PURPLE = "#2369a8", "#b45320", "#338269", "#666666", "#7c5ca7"

# Workstation report values (4 physical cores/engine, threads=4, 9 GiB).
# Columns: DuckDB, Remote, alpha 0, .25, .5, .75, 1
REPORT = {
    5: {"orderkey": (1.123, 1.052, 1.218, 0.992, 0.764, 0.826, 1.010),
        "orderdate": (1.098, 1.513, 1.339, 1.247, 1.029, 1.071, 1.337),
        "totalprice": (1.208, 1.822, 1.392, 1.885, 1.839, 1.425, 1.611),
        "clerk": (1.685, 1.618, 1.841, 1.819, 1.437, 1.192, 1.362),
        "read": 1.029, "read_narrow": 0.171,
        "narrow": {"native": 0.267, "read_sorted": 0.462, "partial0": 0.300, "partial0.5": 0.582, "partial1": 0.420}},
    10: {"orderkey": (2.159, 2.065, 2.438, 2.047, 1.579, 1.572, 1.984),
         "orderdate": (2.197, 2.894, 2.621, 2.372, 2.012, 2.115, 2.538),
         "totalprice": (2.333, 3.627, 2.703, 4.073, 3.621, 2.704, 3.203),
         "clerk": (3.446, 3.090, 3.704, 3.242, 2.678, 2.326, 2.678),
         "read": 1.969, "read_narrow": 0.277,
         "narrow": {"native": 0.527, "read_sorted": 0.909, "partial0": 0.544, "partial0.5": 1.102, "partial1": 0.800}},
}
# SF50 report: 9 GiB DuckDB, schema-only no-compression connector.
REPORT50_PATH = {"orderkey": (7.841, 4.724, 10.489, 4.871), "orderdate": (8.414, 7.481, 11.877, 7.726),
                 "totalprice": (9.013, 15.469, 18.309, 13.085), "clerk": (15.898, 9.252, 19.132, 10.776)}
REPORT50_ALPHA9 = {"orderdate": (38.797, 23.763, 11.877, 7.726, 8.214),
                   "totalprice": (63.708, 45.662, 18.309, 13.085, 14.662)}
REPORT50_MEM = {("orderdate", "native"): (8.414, 4.471), ("orderdate", "partial0.5"): (11.877, 7.623),
                ("orderdate", "partial0.75"): (7.726, 7.447), ("totalprice", "native"): (9.013, 5.157),
                ("totalprice", "partial0.5"): (18.309, 14.512)}
REPORT50_NARROW = {"read": 0.489, "native": 1.670, "read_sorted": 5.284, "partial0.5": 4.768}
REPORT50_READ = 3.308
REPORT10_COMPRESSION = {  # (LZ4, off)
    ("read", "read"): (1.985, 0.694), ("date", "native"): (2.176, 0.978), ("date", "partial0.5"): (1.944, 1.447),
    ("date", "read_sorted"): (2.880, 1.772), ("price", "native"): (2.351, 1.069),
    ("price", "partial0.5"): (3.643, 3.090), ("price", "read_sorted"): (3.637, 3.052)}
REPORT10_SCHEMA_OFF = {  # date wide, price wide, price narrow
    "read": (0.674, 0.674, 0.104), "native": (0.977, 1.046, 0.321),
    "read_sorted": (1.453, 2.738, 0.755), "partial0.5": (1.507, 2.967, 1.008)}

problems = []


def load(path):
    data = json.loads(path.read_text())
    if data.get("exit") != 0 or "error" in data:
        problems.append(f"{path.relative_to(RAW)}: {str(data.get('error', 'exit ' + str(data.get('exit'))))[:200]}")
        return None
    return data


def runs(subdir, tag_prefix, case, repeats=(1, 2, 3)):
    out = []
    for r in repeats:
        path = RAW / subdir / f"{tag_prefix}-r{r}-{case}-summary.json"
        if path.exists():
            data = load(path)
            if data:
                out.append(data)
    return out


def times(subdir, tag_prefix, case):
    return [d["query"]["total_time"] for d in runs(subdir, tag_prefix, case)]


def med(values):
    return statistics.median(values) if values else float("nan")


def extra(data):
    for node in data["operators"]:
        info = node.get("extra_info") or {}
        if info.get("Function") == "READ_ADBC_PARTIAL_SORT":
            return info
    return {}


def spill_gb(subdir, tag_prefix, case):
    return med([d["system"].get("peak_temp_dir_size", 0) / 1e9 for d in runs(subdir, tag_prefix, case)])


def fmt(value, digits=3):
    return "—" if value != value else f"{value:.{digits}f}"


def ratio(a, b):
    return "—" if a != a or b != b or not b else f"{a / b:.2f}×"


def alpha_chart(sf, subdir, stem, title, keys=KEYS, prefix_fn=lambda k: k, read=None, ncols=2, remote_from=None):
    rows = (len(keys) + ncols - 1) // ncols
    fig, axes = plt.subplots(rows, ncols, figsize=(12, 4.25 * rows), squeeze=False)
    xs = [float(a) for a in ALPHAS]
    for ax, key in zip(axes.flat, keys):
        prefix = prefix_fn(key)
        series = [times(subdir, prefix, "partial" + a) for a in ALPHAS]
        medians = [med(s) for s in series]
        lo = [m - min(s) if s else 0 for m, s in zip(medians, series)]
        hi = [max(s) - m if s else 0 for m, s in zip(medians, series)]
        ax.errorbar(xs, medians, yerr=[lo, hi], color=BLUE, marker="o", lw=2.5, ms=6, capsize=4, label="Hybrid")
        duck = med(times(subdir, prefix, "native"))
        ax.axhline(duck, color=ORANGE, lw=2, ls="--", label="DuckDB sort")
        remote = med(times(remote_from, key, "read_sorted") if remote_from else times(subdir, prefix, "read_sorted"))
        if remote == remote:
            ax.axhline(remote, color=GREEN, lw=2, ls=":", label="ClickHouse sort")
        if read is not None:
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
    fig.suptitle(title, fontsize=14)
    fig.tight_layout()
    for ext in ("png", "svg"):
        fig.savefig(OUT / f"{stem}.{ext}", dpi=110)
    plt.close(fig)


def bar_chart(stem, title, labels, groups, ylabel="Query time (s; lower is better)"):
    """groups: list of (series label, values, color)."""
    fig, ax = plt.subplots(figsize=(11, 5))
    width = 0.8 / len(groups)
    for i, (name, values, color) in enumerate(groups):
        pos = [j + (i - (len(groups) - 1) / 2) * width for j in range(len(labels))]
        bars = ax.bar(pos, values, width, label=name, color=color)
        ax.bar_label(bars, fmt="%.2f", fontsize=8, padding=2)
    ax.set_xticks(range(len(labels)), labels)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(axis="y", color="#dddddd")
    ax.set_axisbelow(True)
    ax.legend()
    fig.tight_layout()
    for ext in ("png", "svg"):
        fig.savefig(OUT / f"{stem}.{ext}", dpi=110)
    plt.close(fig)


md = []


def section_alpha(sf, subdir, label):
    read = med(times(subdir, "read-wide", "read"))
    read_narrow = med(times(subdir, "read-narrow", "read"))
    if read != read:
        md.append(f"\n_No {label} data yet._\n")
        return
    alpha_chart(sf, subdir, f"sf{sf}_alpha_sweep",
                f"SF{sf} orders on CloudLab: alpha sweep by sort key (16 threads, 56 GiB)", read=read)
    rep = REPORT.get(sf)
    md.append(f"\n![SF{sf} alpha sweep](./sf{sf}_alpha_sweep.png)\n")
    md.append("Each cell: CloudLab median of three runs; workstation report value in parentheses where one exists. "
              "**Bold** marks the fastest path per key on CloudLab.\n")
    md.append("| Sort key | DuckDB | Remote | α=0 | α=.25 | α=.5 | α=.75 | α=1 | Best (CloudLab) | Best (report) |")
    md.append("|---|---:|---:|---:|---:|---:|---:|---:|---|---|")
    cases = ["native", "read_sorted"] + ["partial" + a for a in ALPHAS]
    names = ["DuckDB", "Remote"] + ["α=" + a for a in ALPHAS]
    for key in KEYS:
        values = [med(times(subdir, key, c)) for c in cases]
        best = min(range(len(values)), key=lambda i: values[i] if values[i] == values[i] else 1e9)
        cells = []
        for i, v in enumerate(values):
            text = fmt(v)
            if rep:
                text += f" ({rep[key][i]:.3f})"
            cells.append(f"**{text}**" if i == best else text)
        rbest = names[min(range(7), key=lambda i: rep[key][i])] if rep else "—"
        md.append(f"| `o_{key}` | " + " | ".join(cells) + f" | {names[best]} | {rbest} |")
    md.append(f"\nUnsorted `SELECT *` ADBC read: **{fmt(read)} s**"
              + (f" (report {rep['read']:.3f})" if rep else "")
              + f"; two-column read: {fmt(read_narrow)} s" + (f" (report {rep['read_narrow']:.3f})." if rep else "."))
    # relative view: hardware-independent shape
    md.append("\nNormalized to the unsorted read on the same machine (hardware-independent shape):\n")
    md.append("| Sort key | DuckDB / read | Remote / read | best α / read | CloudLab best α | report best α |")
    md.append("|---|---:|---:|---:|---:|---:|")
    for key in KEYS:
        alpha_vals = [med(times(subdir, key, "partial" + a)) for a in ALPHAS]
        ib = min(range(5), key=lambda i: alpha_vals[i])
        line = (f"| `o_{key}` | {ratio(med(times(subdir, key, 'native')), read)}"
                f"{' (' + ratio(rep[key][0], rep['read']) + ')' if rep else ''} | "
                f"{ratio(med(times(subdir, key, 'read_sorted')), read)}"
                f"{' (' + ratio(rep[key][1], rep['read']) + ')' if rep else ''} | "
                f"{ratio(alpha_vals[ib], read)}"
                f"{' (' + ratio(min(rep[key][2:]), rep['read']) + ')' if rep else ''} | "
                f"{ALPHAS[ib]} | {ALPHAS[min(range(5), key=lambda i: rep[key][2 + i])] if rep else '—'} |")
        md.append(line)
    # merge copy at alpha .5
    md.append("\nHybrid α=.5 merge behaviour (median):\n")
    md.append("| Sort key | Output rows copied (M) | Merge s | Merge loop s | Local sort s | Remote read/buffer s | Spill GB |")
    md.append("|---|---:|---:|---:|---:|---:|---:|")
    copy_rows, merge_s = [], []
    for key in KEYS:
        data = runs(subdir, key, "partial0.5")
        info = [extra(d) for d in data]

        def m(name):
            return med([float(i[name]) for i in info if name in i])
        copy_rows.append(m("Output Copied Rows") / 1e6)
        merge_s.append(m("Merge Seconds"))
        md.append(f"| `o_{key}` | {fmt(m('Output Copied Rows') / 1e6, 2)} | {fmt(m('Merge Seconds'))} | "
                  f"{fmt(m('Merge Loop Seconds'))} | {fmt(m('Local Sort Seconds'))} | "
                  f"{fmt(m('Remote Read and Buffer Seconds'))} | {fmt(spill_gb(subdir, key, 'partial0.5'), 2)} |")
    fig, ax1 = plt.subplots(figsize=(10, 4.5))
    pos = range(len(KEYS))
    ax1.bar([p - 0.2 for p in pos], copy_rows, 0.4, color=BLUE, label="Rows copied (M)")
    ax1.set_ylabel("Output rows copied (millions)")
    ax2 = ax1.twinx()
    ax2.bar([p + 0.2 for p in pos], merge_s, 0.4, color=ORANGE, label="Merge seconds")
    ax2.set_ylabel("Merge elapsed (s)")
    ax1.set_xticks(list(pos), [f"o_{k}" for k in KEYS])
    ax1.set_title(f"SF{sf} hybrid α=.5: rows copied and merge time by sort key")
    fig.legend(loc="upper left", bbox_to_anchor=(0.08, 0.9))
    fig.tight_layout()
    for ext in ("png", "svg"):
        fig.savefig(OUT / f"sf{sf}_merge_copy.{ext}", dpi=110)
    plt.close(fig)
    md.append(f"\n![SF{sf} merge copy](./sf{sf}_merge_copy.png)\n")
    # narrow projection
    narrow_cases = ("native", "read_sorted", "partial0", "partial0.5", "partial1")
    md.append("Two-column `o_orderkey,o_totalprice` projection, sorted by `o_totalprice`:\n")
    md.append("| Path | 9 columns | 2 columns | 2 columns (report) |")
    md.append("|---|---:|---:|---:|")
    md.append(f"| Unsorted read | {fmt(read)} | {fmt(read_narrow)} | {rep['read_narrow'] if rep else '—'} |")
    labels = {"native": "DuckDB sort", "read_sorted": "ClickHouse sort", "partial0": "Hybrid α=0",
              "partial0.5": "Hybrid α=.5", "partial1": "Hybrid α=1"}
    for c in narrow_cases:
        md.append(f"| {labels[c]} | {fmt(med(times(subdir, 'totalprice', c)))} | "
                  f"{fmt(med(times(subdir, 'totalprice-narrow', c)))} | {rep['narrow'][c] if rep else '—'} |")


def section_compression():
    sub = "sf10-compression"
    if not (RAW / sub).exists():
        return
    md.append("\n## SF10: connector with and without HTTP response compression\n")
    md.append("Both drivers built from `adbc_clickhouse` 2c374ad (0.1.1); the only change is "
              "`with_compression(Compression::None)`. Median of three passes, alternating driver order.\n")
    md.append("| Sort key | Path | LZ4 | Off | LZ4 (report) | Off (report) |")
    md.append("|---|---|---:|---:|---:|---:|")
    labels, lz4s, offs = [], [], []
    for (key, case), (rl, ro) in REPORT10_COMPRESSION.items():
        src = "price" if case == "read" else key
        l = med(times(sub, f"{src}-lz4", case))
        o = med(times(sub, f"{src}-nocompress", case))
        name = {"read": "Unsorted read", "native": "DuckDB sort", "partial0.5": "Hybrid α=.5", "read_sorted": "ClickHouse sort"}[case]
        md.append(f"| {'either' if case == 'read' else '`o_' + ('orderdate' if key == 'date' else 'totalprice') + '`'} | {name} | {fmt(l)} | {fmt(o)} | {rl} | {ro} |")
        labels.append(("" if case == "read" else key + "\n") + name)
        lz4s.append(l)
        offs.append(o)
    bar_chart("sf10_compression", "SF10: ADBC with LZ4 vs uncompressed HTTP responses (CloudLab)",
              labels, [("LZ4", lz4s, ORANGE), ("Compression off", offs, BLUE)])
    md.append("\n![SF10 compression](./sf10_compression.png)\n")


def section_schema_off():
    sub = "sf10-schema-off"
    if not (RAW / sub).exists():
        return
    md.append("\n## SF10: schema-only connector with compression off\n")
    md.append("`ExecuteSchema` via `DESCRIBE (<query>)` plus no response compression. Median of three runs after warmup.\n")
    md.append("| Path | Date, 9 cols | Price, 9 cols | Price, 2 cols | Report (date / price / price-2) |")
    md.append("|---|---:|---:|---:|---|")
    names = {"read": "Unsorted read", "native": "DuckDB sort", "read_sorted": "ClickHouse sort", "partial0.5": "Hybrid α=.5"}
    for case, rep in REPORT10_SCHEMA_OFF.items():
        md.append(f"| {names[case]} | {fmt(med(times(sub, 'date', case)))} | {fmt(med(times(sub, 'price', case)))} | "
                  f"{fmt(med(times(sub, 'price-narrow', case)))} | {rep[0]} / {rep[1]} / {rep[2]} |")
    bind = [float(extra(d).get("Bind Seconds", "nan")) for d in runs(sub, "price", "partial0.5")]
    md.append(f"\nHybrid bind (LIMIT-1 probe) median: {fmt(med(bind))} s.")


def curl_section(sf, subdir, null=False):
    results = {}
    for suffix in ("a", "b"):
        root = RAW / f"{subdir}-{suffix}"
        for path in sorted(root.glob("*-r*/result.json")):
            data = json.loads(path.read_text())
            if data.get("warmup"):
                continue
            results.setdefault(data["case"], []).append(data)
    if not results:
        return
    md.append(f"\n## SF{sf}: curl output-format controls\n")
    md.append("| Format | Median wall s (min–max) | Runs | Network bytes (GB) | Report |")
    md.append("|---|---:|---:|---:|---:|")
    reports = {10: {"http_native": 1.060, "http_arrow": 1.238}, 50: {"http_native": 1.662, "http_arrow": 2.629}}[sf]
    if null:
        p = RAW / "sf50-curl-null/results.jsonl"
        if p.exists():
            rows = [json.loads(line) for line in p.read_text().splitlines()]
            wall = [r["wall_seconds"] for r in rows if not r["warmup"]]
            md.append(f"| Null | {fmt(med(wall))} ({fmt(min(wall))}–{fmt(max(wall))}) | {len(wall)} | 0 | 1.278 |")
    for case, label in (("http_native", "Native"), ("http_arrow", "ArrowStream")):
        rows = results.get(case, [])
        wall = [r["wall_seconds"] for r in rows]
        net = [int(r["server_queries"][0]["ProfileEvents"].get("NetworkSendBytes", 0)) / 1e9 for r in rows]
        md.append(f"| {label} | {fmt(med(wall))} ({fmt(min(wall))}–{fmt(max(wall))}) | {len(wall)} | {fmt(med(net))} | {reports[case]} |")


def section_sf50():
    sub = "sf50"
    read = med(times(sub, "read-wide", "read"))
    if read != read:
        md.append("\n_No SF50 data yet._\n")
        return
    md.append("\n## SF50 orders (75M rows)\n")
    md.append("Connector: schema-only + no compression + separate schema query IDs, with the Date/Decimal "
              "mapping fixes. Main matrix at 56 GiB; the report's main table used 9 GiB, so the 9 GiB "
              "rows from the memory phase are the like-for-like comparison.\n")
    section_alpha(50, sub, "SF50")
    md.append("\n### Sort paths vs the SF50 report\n")
    md.append("| Sort key | DuckDB 56G | DuckDB 9G | Report DuckDB 9G | ClickHouse | Report CH | α=.5 9G | Report α=.5 | α=.75 9G | Report α=.75 |")
    md.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for key in KEYS:
        rep = REPORT50_PATH[key]
        m9 = lambda c: med(times("sf50-memory", f"{key}-m9", c))  # noqa: E731
        md.append(f"| `o_{key}` | {fmt(med(times(sub, key, 'native')))} | {fmt(m9('native'))} | {rep[0]} | "
                  f"{fmt(med(times(sub, key, 'read_sorted')))} | {rep[1]} | {fmt(m9('partial0.5'))} | {rep[2]} | "
                  f"{fmt(m9('partial0.75'))} | {rep[3]} |")
    md.append(f"\nUnsorted read: {fmt(read)} s (report {REPORT50_READ}).")
    if not (RAW / "sf50-memory").exists():
        return
    md.append("\n### SF50 alpha and DuckDB memory\n")
    alpha_chart(50, "sf50-memory", "sf50_alpha_sweep_9gib",
                "SF50 on CloudLab at memory_limit=9GiB (16 threads)", keys=("orderdate", "totalprice"),
                prefix_fn=lambda k: f"{k}-m9", read=read, remote_from=sub)
    md.append("![SF50 9 GiB alpha sweep](./sf50_alpha_sweep_9gib.png)\n")
    md.append("The ClickHouse-sort and unsorted-read baselines come from the 56 GiB runs; neither path sorts in DuckDB.\n")
    md.append("| α | date 9G | date report | date spill GB | price 9G | price report | price spill GB | date 20G | price 20G | date 56G | price 56G |")
    md.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for i, a in enumerate(ALPHAS):
        c = "partial" + a
        md.append(
            f"| {a} | {fmt(med(times('sf50-memory', 'orderdate-m9', c)))} | {REPORT50_ALPHA9['orderdate'][i]} | "
            f"{fmt(spill_gb('sf50-memory', 'orderdate-m9', c), 2)} | {fmt(med(times('sf50-memory', 'totalprice-m9', c)))} | "
            f"{REPORT50_ALPHA9['totalprice'][i]} | {fmt(spill_gb('sf50-memory', 'totalprice-m9', c), 2)} | "
            f"{fmt(med(times('sf50-memory', 'orderdate-m20', c)))} | {fmt(med(times('sf50-memory', 'totalprice-m20', c)))} | "
            f"{fmt(med(times(sub, 'orderdate', c)))} | {fmt(med(times(sub, 'totalprice', c)))} |")
    md.append("\n| Path | 9 GiB | 20 GiB | 56 GiB | Report 9 / 20 GiB | Spill 9 GiB (GB) |")
    md.append("|---|---:|---:|---:|---|---:|")
    labels, g9, g20, g56 = [], [], [], []
    for (key, case), rep in REPORT50_MEM.items():
        v9 = med(times("sf50-memory", f"{key}-m9", case))
        v20 = med(times("sf50-memory", f"{key}-m20", case))
        v56 = med(times(sub, key, case))
        name = {"native": "DuckDB sort", "partial0.5": "Hybrid α=.5", "partial0.75": "Hybrid α=.75"}[case]
        md.append(f"| {key[5:] if key.startswith('order') else key}: {name} | {fmt(v9)} | {fmt(v20)} | {fmt(v56)} | "
                  f"{rep[0]} / {rep[1]} | {fmt(spill_gb('sf50-memory', f'{key}-m9', case), 2)} |")
        labels.append(f"{'date' if key == 'orderdate' else 'price'}\n{name}")
        g9.append(v9)
        g20.append(v20)
        g56.append(v56)
    bar_chart("sf50_memory", "SF50 on CloudLab: DuckDB memory_limit sensitivity (16 threads)", labels,
              [("9 GiB", g9, ORANGE), ("20 GiB", g20, PURPLE), ("56 GiB", g56, BLUE)])
    md.append("\n![SF50 memory](./sf50_memory.png)\n")
    md.append("Two-column projection (price): "
              + ", ".join(f"{n} {fmt(med(times(sub, 'totalprice-narrow' if c != 'read' else 'read-narrow', c)))} s "
                          f"(report {REPORT50_NARROW[c]})"
                          for n, c in (("read", "read"), ("DuckDB", "native"), ("ClickHouse", "read_sorted"),
                                       ("α=.5", "partial0.5"))) + ".")


PERF_NOTES = {
    "sf50-price-remote": "Report (SF50 price remote sort): ColumnString::insertFrom 15.79%, Decimal compareAt 14.33%, "
                         "ZSTD decompression 12.16%, Decimal greater-than 9.28%.",
    "sf50-price-hybrid05": "Report (SF50 price α=.5 client): memmove 15.57%; DuckDB ZSTD ~14% (spill at 9 GiB).",
    "sf50-date-hybrid05": "Report (SF50 date α=.5 client): memmove 24.0%; DuckDB ZSTD ~20% (spill at 9 GiB).",
    "sf10-price-hybrid05": "Report (SF10 price α=.5 client): memmove 17.2%, VectorStringBuffer::CopyInternal 13.5%, memcmp 10.4%.",
}


def section_perf(top=8):
    root = RAW / "perf"
    if not root.exists():
        return
    import re
    pattern = re.compile(r"^\s+([\d.]+)%\s+(.*\S)\s*$")
    md.append("\n## perf leaf-symbol shares (user-space cycles, one profiled run each)\n")
    md.append("Profiled runs are separate from the timed matrix. Shares are of sampled user cycles, not wall time.\n")
    for name in sorted(p.name for p in root.iterdir() if p.is_dir() and p.name.startswith("sf")):
        for kind in ("server", "client"):
            path = root / name / f"{kind}.txt"
            if not path.exists():
                continue
            rows = []
            for line in path.read_text().splitlines():
                match = pattern.match(line)
                if match and not line.lstrip().startswith("#"):
                    symbol = re.sub(r"\s+\[[.k]\]\s+", "  ", match.group(2))
                    symbol = re.sub(r"(\s+-)+$", "", symbol)
                    symbol = re.sub(r"\s{2,}", "  ", symbol)
                    rows.append((float(match.group(1)), symbol[:110]))
            if not rows:
                continue
            md.append(f"\n**{name}** ({kind}): " + PERF_NOTES.get(name, "") + "\n")
            md.append("| Share | Symbol |")
            md.append("|---:|---|")
            for share, symbol in rows[:top]:
                md.append(f"| {share:.2f}% | `{symbol}` |")


md.append("# CloudLab reproduction of the SF5 / SF10 / SF50 sort reports\n")
for sf, sub in ((5, "sf5"), (10, "sf10")):
    md.append(f"\n## SF{sf} alpha and sort-key sweep (stock dbc 0.1.1 driver, LZ4)\n")
    section_alpha(sf, sub, f"SF{sf}")
section_compression()
section_schema_off()
curl_section(10, "sf10-curl")
section_sf50()
curl_section(50, "sf50-curl", null=True)
section_perf()
if problems:
    md.append("\n## Failed or invalid runs\n")
    md.extend(f"- {p}" for p in problems)
(OUT / "results.md").write_text("\n".join(md) + "\n")
print(f"wrote {OUT / 'results.md'}; {len(problems)} problem runs")
