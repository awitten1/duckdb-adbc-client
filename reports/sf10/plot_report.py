#!/usr/bin/env python3
"""Render tracked SF10 chart data; optionally refresh it from raw summaries."""

import argparse
import json
from pathlib import Path
import statistics
import subprocess

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]
RAW = REPO / "build/local-experiments/sf10-sweep"
TRANSPORT_RAW = (
    REPO / "build/local-experiments/sf10-curl-formats-20261109",
    REPO / "build/local-experiments/sf10-curl-formats-reverse-20261109",
)
COMPRESSION_RAW = REPO / "build/local-experiments/sf10-adbc-compression-20261109"


def runs(key, case):
    values = []
    for repeat in (1, 2, 3):
        path = RAW / f"{key}-r{repeat}-{case}-summary.json"
        if path.exists():
            values.append(json.loads(path.read_text()))
    if len(values) < 2:
        raise RuntimeError(f"Expected at least two summaries for {key} {case}")
    return values


def times(key, case):
    return [item["query"]["total_time"] for item in runs(key, case)]


def metric(key, case, name):
    values = []
    for item in runs(key, case):
        info = next(
            node["extra_info"]
            for node in item["operators"]
            if node.get("extra_info", {}).get("Function") == "READ_ADBC_PARTIAL_SORT"
        )
        values.append(float(info[name]))
    return statistics.median(values)


def write_data():
    unsorted = statistics.median(times("read-wide", "read"))
    for key in ("orderkey", "orderdate", "totalprice", "clerk"):
        duckdb = statistics.median(times(key, "native"))
        remote = statistics.median(times(key, "read_sorted"))
        rows = []
        for alpha in ("0", "0.25", "0.5", "0.75", "1"):
            values = times(key, "partial" + alpha)
            rows.append(
                f"{alpha}\t{statistics.median(values):.6f}\t{min(values):.6f}"
                f"\t{max(values):.6f}\t{duckdb:.6f}\t{remote:.6f}"
                f"\t{unsorted:.6f}"
            )
        (ROOT / f"alpha_{key}.tsv").write_text("\n".join(rows) + "\n")

    rows = []
    for key in ("orderkey", "orderdate", "totalprice", "clerk"):
        rows.append(
            f"{key}\t{metric(key, 'partial0.5', 'Output Copied Rows') / 1e6:.6f}"
            f"\t{metric(key, 'partial0.5', 'Merge Seconds'):.6f}"
        )
    (ROOT / "merge_alpha05.tsv").write_text("\n".join(rows) + "\n")

    cases = (
        ("Unsorted read", "read-wide", "read-narrow", "read"),
        ("DuckDB sort", "totalprice", "totalprice-narrow", "native"),
        ("Alpha 0", "totalprice", "totalprice-narrow", "partial0"),
        ("Alpha 0.5", "totalprice", "totalprice-narrow", "partial0.5"),
        ("Alpha 1", "totalprice", "totalprice-narrow", "partial1"),
        ("Remote sort", "totalprice", "totalprice-narrow", "read_sorted"),
    )
    rows = [
        f'"{label}"\t{statistics.median(times(wide, case)):.6f}'
        f"\t{statistics.median(times(narrow, case)):.6f}"
        for label, wide, narrow, case in cases
    ]
    (ROOT / "projection_width.tsv").write_text("\n".join(rows) + "\n")


def write_transport_data():
    rows = []
    for case, label, processor in (
        ("http_native", "Native", "Native"),
        ("http_arrow", "ArrowStream", "ArrowIPCBlockOutputFormat"),
    ):
        results = [
            json.loads((root / f"{case}-r{repeat}" / "result.json").read_text())
            for root in TRANSPORT_RAW
            for repeat in range(3)
        ]
        wall = [item["wall_seconds"] for item in results]
        output = [
            next(
                node["elapsed_us"]
                for node in item["processors"]
                if node["name"] == processor
            ) / 1e6
            for item in results
        ]
        wire = [
            int(item["server_queries"][0]["ProfileEvents"]["NetworkSendBytes"]) / 1e9
            for item in results
        ]
        rows.append(
            f'"{label}"\t{statistics.median(wall):.6f}\t{min(wall):.6f}'
            f"\t{max(wall):.6f}\t{statistics.median(output):.6f}"
            f"\t{statistics.median(wire):.6f}"
        )
    (ROOT / "curl_formats.tsv").write_text("\n".join(rows) + "\n")


def write_compression_data():
    for key in ("date", "price"):
        rows = []
        for label, case in (
            ("Unsorted", "read"),
            ("DuckDB sort", "native"),
            ("Alpha 0.5", "partial0.5"),
            ("Remote sort", "read_sorted"),
        ):
            times_by_kind = {}
            for kind in ("lz4", "nocompress"):
                source_key = "price" if case == "read" else key
                times_by_kind[kind] = [
                    json.loads(
                        (
                            COMPRESSION_RAW
                            / f"{source_key}-{kind}-r{repeat}-{case}-summary.json"
                        ).read_text()
                    )["query"]["total_time"]
                    for repeat in (1, 2, 3)
                ]
            rows.append(
                f'"{label}"\t{statistics.median(times_by_kind["lz4"]):.6f}'
                f'\t{statistics.median(times_by_kind["nocompress"]):.6f}'
            )
        (ROOT / f"compression_{key}.tsv").write_text("\n".join(rows) + "\n")


def render(stem):
    source = (ROOT / f"{stem}.gp").read_text()
    subprocess.run(["gnuplot"], input=source, text=True, cwd=ROOT, check=True)
    svg_path = ROOT / f"{stem}.svg"
    lines = [line.rstrip() for line in svg_path.read_text().splitlines()]
    while lines and not lines[-1]:
        lines.pop()
    svg_path.write_text("\n".join(lines) + "\n")
    png = source.replace(
        "set terminal svg size 1200,850 dynamic noenhanced font 'DejaVu Sans,12' background rgb 'white'",
        "set terminal pngcairo size 1200,850 noenhanced font 'DejaVu Sans,12' background rgb 'white'",
    ).replace(f"set output '{stem}.svg'", f"set output '{stem}.png'")
    if png == source:
        raise RuntimeError(f"Could not build PNG version of {stem}")
    subprocess.run(["gnuplot"], input=png, text=True, cwd=ROOT, check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh-data", action="store_true", help="Read raw profiles under build/")
    args = parser.parse_args()
    if args.refresh_data:
        write_data()
        write_transport_data()
        write_compression_data()
    for chart in ("alpha_sweep", "merge_copy", "projection_width", "curl_formats", "adbc_compression"):
        render(chart)
        print(f"Rendered {chart}.svg and {chart}.png")
