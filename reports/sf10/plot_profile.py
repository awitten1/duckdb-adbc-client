#!/usr/bin/env python3
"""Render tracked SF10 profiling charts or refresh their data from local logs."""

import argparse
import json
from pathlib import Path
import statistics

from plot_report import render


ROOT = Path(__file__).resolve().parent
RAW = ROOT.parents[1] / "build/local-experiments/sf10-deep-profile-20261109"


def median(values):
    return statistics.median(values)


def summary(stem):
    return json.loads((RAW / f"{stem}-summary.json").read_text())


def main_query(stem):
    rows = [json.loads(line) for line in (RAW / f"{stem}-server.jsonl").read_text().splitlines()]
    return max(rows, key=lambda row: row["result_rows"])


def processor_index():
    rows = {}
    for line in (RAW / "processor_rows.jsonl").read_text().splitlines():
        item = json.loads(line)
        rows.setdefault(item["query_id"], []).append(item)
    return rows


def processor_seconds(index, query_id, name):
    return sum(
        item["elapsed_us"] for item in index[query_id] if item["name"] == name
    ) / 1e6


def write_data():
    processors = processor_index()

    scan_rows = []
    for kind, label in (("lz4", "LZ4"), ("nocompress", "Off")):
        stems = [f"price-{kind}-r{repeat}-read" for repeat in (1, 2, 3)]
        runs = [summary(stem) for stem in stems]
        queries = [main_query(stem) for stem in stems]
        scan_rows.append(
            f'"{label}"\t{median(run["query"]["total_time"] for run in runs):.6f}'
            f'\t{median(processor_seconds(processors, query["query_id"], "ArrowIPCBlockOutputFormat") for query in queries):.6f}'
            f'\t{median((query["ProfileEvents"]["UserTimeMicroseconds"] + query["ProfileEvents"]["SystemTimeMicroseconds"]) / 1e6 for query in queries):.6f}'
            f'\t{median(run["user_seconds"] + run["system_seconds"] for run in runs):.6f}'
        )
    (ROOT / "profile_scan.tsv").write_text("\n".join(scan_rows) + "\n")

    remote_rows = []
    schema_rows = []
    for key, label in (("date", "Date"), ("price", "Price")):
        for kind, format_label in (("lz4", "LZ4"), ("nocompress", "Off")):
            stems = [f"{key}-{kind}-r{repeat}-read_sorted" for repeat in (1, 2, 3)]
            queries = [main_query(stem) for stem in stems]
            remote_rows.append(
                f'"{label} {format_label}"'
                f'\t{median(processor_seconds(processors, query["query_id"], "MergingSortedTransform") for query in queries):.6f}'
                f'\t{median(processor_seconds(processors, query["query_id"], "ArrowIPCBlockOutputFormat") for query in queries):.6f}'
            )
            original = [summary(stem)["query"]["total_time"] for stem in stems]
            fixed = [
                summary(f"schema-{key}-{kind}-r{repeat}-read_sorted")["query"]["total_time"]
                for repeat in (1, 2, 3)
            ]
            schema_rows.append(
                f'"{label} {format_label}"\t{median(original):.6f}\t{median(fixed):.6f}'
            )
    (ROOT / "profile_remote.tsv").write_text("\n".join(remote_rows) + "\n")
    (ROOT / "profile_schema.tsv").write_text("\n".join(schema_rows) + "\n")

    hybrid_rows = []
    for key, label in (
        ("date", "Date, 9 cols"),
        ("price", "Price, 9 cols"),
        ("price-narrow", "Price, 2 cols"),
    ):
        metrics = [
            next(
                item["extra_info"]
                for item in summary(f"instrumented-{key}-nocompress-r{repeat}-partial0.5")["operators"]
                if item.get("extra_info", {}).get("Function") == "READ_ADBC_PARTIAL_SORT"
            )
            for repeat in (1, 2, 3)
        ]
        names = (
            "Merge Loop Seconds",
            "Merge Local Fetch Seconds",
            "Merge Remote Fetch Seconds",
            "Merge Project Seconds",
        )
        values = [median(float(item[name]) for item in metrics) for name in names]
        hybrid_rows.append(f'"{label}"\t' + "\t".join(f"{value:.6f}" for value in values))
    (ROOT / "profile_hybrid.tsv").write_text("\n".join(hybrid_rows) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh-data", action="store_true")
    args = parser.parse_args()
    if args.refresh_data:
        write_data()
    for stem in ("profile_scan", "profile_remote", "profile_hybrid"):
        render(stem)
        print(f"Rendered {stem}.svg and {stem}.png")
