#!/usr/bin/env python3
"""Compare full ClickHouse/DuckDB sorts and partial-sort pushdown over ADBC."""

import argparse
import csv
from decimal import Decimal, InvalidOperation
import json
import math
import os
from pathlib import Path
import random
import statistics
import subprocess
import tempfile
import time
import uuid

from test_clickhouse_partial_sort import (
    Harness,
    find_partial_sort_metrics,
    identifier,
    literal,
    system_metric,
)

VARIANTS = ("clickhouse", "duckdb", "partial")
RATIO_PRECISION = Decimal("0.000000001")


def ratio_argument(text):
    try:
        value = Decimal(text)
        if not value.is_finite() or not 0 <= value <= 1:
            raise ValueError
        if value != value.quantize(RATIO_PRECISION):
            raise ValueError
        return format(value.normalize(), "f") if value else "0"
    except (InvalidOperation, ValueError):
        raise argparse.ArgumentTypeError(
            "ratios must be between 0 and 1 with at most 9 decimal places"
        ) from None


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", default="build/release/duckdb")
    parser.add_argument(
        "--extension", default="build/release/extension/adbc/adbc.duckdb_extension"
    )
    parser.add_argument("--profile", help="Existing ClickHouse ADBC profile URI")
    parser.add_argument(
        "--url", default=os.environ.get("ADBC_CLICKHOUSE_URL", "http://localhost:8123/")
    )
    parser.add_argument("--rows", type=int, default=1000000)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument(
        "--warmups", type=int, default=1, help="Unmeasured runs per case"
    )
    parser.add_argument(
        "--seed", type=int, default=0, help="Seed for shuffled run order"
    )
    parser.add_argument(
        "--ratios",
        nargs="+",
        type=ratio_argument,
        default=["0", "0.25", "0.5", "0.75", "1"],
    )
    parser.add_argument(
        "--mode",
        choices=("all",) + VARIANTS,
        default="all",
        help="all: full ClickHouse, full DuckDB, and partial-sort pushdown",
    )
    parser.add_argument(
        "--distributions",
        nargs="+",
        choices=("dense", "skewed"),
        default=["dense", "skewed"],
    )
    parser.add_argument(
        "--orderings",
        nargs="+",
        choices=("aligned", "unaligned"),
        default=["aligned", "unaligned"],
    )
    parser.add_argument("--memory-limit", default="256MB")
    parser.add_argument("--threads", type=int, default=1, help="DuckDB threads per run")
    parser.add_argument(
        "--timeout", type=float, default=1800, help="Seconds per benchmark trial"
    )
    parser.add_argument(
        "--output", type=Path, default=Path("build/partial-sort-benchmark.json")
    )
    parser.add_argument(
        "--csv",
        type=Path,
        help="Summary CSV; defaults to the JSON path with .csv suffix",
    )
    parser.add_argument(
        "--profiles-dir", type=Path, help="Keep individual DuckDB JSON profiles"
    )
    args = parser.parse_args()
    if args.rows < 2 or args.repeats < 1 or args.warmups < 0 or args.payload_bytes < 0:
        parser.error(
            "--rows must be at least 2, --repeats positive, and --warmups/--payload-bytes nonnegative"
        )
    if args.threads < 1 or args.timeout <= 0 or not math.isfinite(args.timeout):
        parser.error("--threads and --timeout must be positive")
    args.ratios = list(dict.fromkeys(args.ratios))
    args.distributions = list(dict.fromkeys(args.distributions))
    args.orderings = list(dict.fromkeys(args.orderings))
    args.csv = args.csv or args.output.with_suffix(".csv")
    if args.output.resolve() == args.csv.resolve():
        parser.error("JSON and CSV output paths must differ")
    return args


def case_query(h, table, keys, variant, ratio):
    order = ", ".join(f"{identifier(key)} ASC NULLS LAST" for key in keys)
    if variant == "clickhouse":
        return "SELECT * FROM " + h.read(
            f"SELECT * FROM {h.source(table)} ORDER BY {order}"
        )
    if variant == "duckdb":
        return (
            "SELECT * FROM "
            + h.read(f"SELECT * FROM {h.source(table)}")
            + " ORDER BY "
            + order
        )
    return "SELECT * FROM " + h.function(table, ratio, keys)


def copied_rows(output):
    # LOAD/SET normally have no output, but accept multiple JSON result sets.
    remaining = output.strip()
    rows = None
    decoder = json.JSONDecoder()
    while remaining:
        rows, end = decoder.raw_decode(remaining)
        remaining = remaining[end:].strip()
    if not isinstance(rows, list) or len(rows) != 1:
        raise ValueError("COPY did not return its row count")
    values = {key.lower(): value for key, value in rows[0].items()}
    return int(values["count"])


def run_case(h, case, args, temporary, repeat, warmup):
    entry = {**case, "repeat": repeat, "status": "ok"}
    tag = f"{case['distribution']}-{case['ordering']}-{case['variant']}-{case['ratio'] or 'baseline'}-{repeat}"
    profiling = Path(temporary) / f"{tag}.json"
    settings = (
        f"SET memory_limit={literal(args.memory_limit)}; SET threads={args.threads}; "
    )
    if not warmup:
        settings += f"PRAGMA enable_profiling='json'; SET profiling_output={literal(profiling)}; "
    sql = (
        settings
        + f"COPY ({case['sql']}) TO {literal(os.devnull)} (FORMAT CSV, HEADER false, RETURN_FILES true)"
    )
    started = time.perf_counter()
    try:
        result = h.run(sql, timeout=args.timeout, check=False)
        entry["wall_seconds_including_binding"] = time.perf_counter() - started
        if result.returncode:
            raise RuntimeError(
                result.stderr.strip() or f"DuckDB exited with code {result.returncode}"
            )
        entry["rows_consumed"] = copied_rows(result.stdout)
        if entry["rows_consumed"] != args.rows:
            raise ValueError(f"Expected {args.rows} rows, got {entry['rows_consumed']}")
        if not warmup:
            metrics = json.loads(profiling.read_text())
            entry["peak_memory_bytes"] = system_metric(metrics, "peak_buffer_memory")
            entry["peak_spill_bytes"] = system_metric(metrics, "peak_temp_dir_size")
            entry["partial_sort"] = find_partial_sort_metrics(metrics)
    except (
        AssertionError,
        RuntimeError,
        ValueError,
        KeyError,
        OSError,
        subprocess.TimeoutExpired,
    ) as error:
        entry["wall_seconds_including_binding"] = time.perf_counter() - started
        entry["status"] = "error"
        entry["error"] = str(error)
    if not warmup and args.profiles_dir and profiling.exists():
        saved = args.profiles_dir / f"{tag}.json"
        saved.write_text(profiling.read_text())
        entry["profile_path"] = str(saved.resolve())
    label = "warmup" if warmup else "run"
    result_text = (
        f"{entry['wall_seconds_including_binding']:.3f}s"
        if entry["status"] == "ok"
        else f"ERROR: {entry['error']}"
    )
    print(
        f"{label} {repeat + 1}: {case['distribution']} {case['ordering']} "
        f"{case['variant']} ratio={case['ratio'] if case['ratio'] is not None else '-'}: {result_text}",
        flush=True,
    )
    return entry


def summarize(runs):
    groups = {}
    for run in runs:
        key = (run["distribution"], run["ordering"], run["variant"], run["ratio"])
        groups.setdefault(key, []).append(run)
    summaries = []
    for (distribution, ordering, variant, ratio), trials in groups.items():
        times = [
            trial["wall_seconds_including_binding"]
            for trial in trials
            if trial["status"] == "ok"
        ]
        summaries.append(
            {
                "distribution": distribution,
                "ordering": ordering,
                "variant": variant,
                "ratio": ratio,
                "successful_runs": len(times),
                "failed_runs": len(trials) - len(times),
                "median_seconds": statistics.median(times) if times else None,
                "mean_seconds": statistics.mean(times) if times else None,
                "min_seconds": min(times) if times else None,
                "max_seconds": max(times) if times else None,
                "stdev_seconds": (
                    statistics.stdev(times)
                    if len(times) > 1
                    else (0 if times else None)
                ),
            }
        )
    baselines = {
        (row["distribution"], row["ordering"], row["variant"]): row["median_seconds"]
        for row in summaries
        if row["variant"] in ("clickhouse", "duckdb")
    }
    for row in summaries:
        for variant in ("clickhouse", "duckdb"):
            baseline = baselines.get((row["distribution"], row["ordering"], variant))
            row[f"speedup_vs_{variant}"] = (
                baseline / row["median_seconds"]
                if baseline and row["median_seconds"]
                else None
            )
    return sorted(
        summaries,
        key=lambda row: (
            row["distribution"],
            row["ordering"],
            VARIANTS.index(row["variant"]),
            Decimal(row["ratio"]) if row["ratio"] is not None else Decimal(-1),
        ),
    )


def save_report(report, args):
    report["summary"] = summarize(report["runs"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = args.output.with_name(args.output.name + ".tmp")
    temporary_output.write_text(json.dumps(report, indent=2) + "\n")
    temporary_output.replace(args.output)
    if report["summary"]:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        with args.csv.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(report["summary"][0]))
            writer.writeheader()
            writer.writerows(report["summary"])


def print_summary(summary):
    print(
        "\nMedian wall times (seconds; startup, binding, full ADBC transfer, sort, and CSV sink included):"
    )
    print(
        f"{'data':<8} {'order':<9} {'variant':<12} {'ratio':>11} {'runs':>5} {'median':>9} {'min':>9} {'vs DuckDB':>10}"
    )
    for row in summary:
        median = (
            f"{row['median_seconds']:.3f}"
            if row["median_seconds"] is not None
            else "ERROR"
        )
        minimum = f"{row['min_seconds']:.3f}" if row["min_seconds"] is not None else "-"
        speedup = (
            f"{row['speedup_vs_duckdb']:.2f}x"
            if row["speedup_vs_duckdb"] is not None
            else "-"
        )
        ratio = row["ratio"] if row["ratio"] is not None else "-"
        print(
            f"{row['distribution']:<8} {row['ordering']:<9} {row['variant']:<12} {ratio:>11} {row['successful_runs']:>5} {median:>9} {minimum:>9} {speedup:>10}"
        )


def main():
    args = arguments()
    if args.profiles_dir:
        args.profiles_dir.mkdir(parents=True, exist_ok=True)
    variants = VARIANTS if args.mode == "all" else (args.mode,)
    randomizer = random.Random(args.seed)
    with tempfile.TemporaryDirectory() as temporary:
        profile_uri = args.profile
        if not profile_uri:
            path = Path(temporary) / "clickhouse.toml"
            path.write_text(
                f'profile_version = 1\ndriver = "clickhouse"\n[Options]\nuri = {json.dumps(args.url)}\n'
            )
            path.chmod(0o600)
            profile_uri = "profile://" + str(path)
        h = Harness(
            args.duckdb,
            args.extension,
            profile_uri,
            f"adbc_sort_bench_{os.getpid()}_{uuid.uuid4().hex[:8]}",
        )
        report = {
            "rows": args.rows,
            "payload_bytes": args.payload_bytes,
            "memory_limit": args.memory_limit,
            "threads": args.threads,
            "timeout_seconds": args.timeout,
            "mode": args.mode,
            "variants": list(variants),
            "ratios": args.ratios,
            "distributions": args.distributions,
            "orderings": args.orderings,
            "ratio_semantics": {
                "partial": "Fraction of integer key-range width assigned to remote sorting",
                "baselines": "No ratio; all sorting in the selected engine",
            },
            "repeats": args.repeats,
            "warmups_per_case": args.warmups,
            "seed": args.seed,
            "measurement": "End-to-end wall time in fresh DuckDB processes; all rows transferred over ADBC and consumed by CSV COPY to the null device. Baselines run once per repeat/workload, independent of ratios. Memory/spill metrics cover DuckDB, not ClickHouse or all driver allocations.",
            "duckdb": h.rows("SELECT version() AS version"),
            "clickhouse": h.rows(
                "SELECT * FROM " + h.read("SELECT version() AS version")
            ),
            "duckdb_settings": h.rows(
                f"SET memory_limit={literal(args.memory_limit)}; SET threads={args.threads}; SELECT current_setting('memory_limit') AS memory_limit, current_setting('threads') AS threads"
            ),
            "clickhouse_settings": h.rows(
                "SELECT * FROM "
                + h.read(
                    "SELECT name, value FROM system.settings WHERE name IN ('max_threads', 'max_memory_usage', 'max_execution_time') ORDER BY name"
                )
            ),
            "driver": subprocess.run(
                ["dbc", "list", "--json"], text=True, capture_output=True
            ).stdout,
            "warmup_runs": [],
            "runs": [],
        }
        save_report(report, args)
        h.execute(f"CREATE DATABASE {identifier(h.database)}")
        try:
            for distribution in args.distributions:
                h.create(
                    distribution,
                    "id UInt64, seq UInt64, score Int64, payload String",
                    order="(id, seq)",
                )
                key = (
                    "number"
                    if distribution == "dense"
                    else f"if(number < {args.rows * 9 // 10}, number % 100, number)"
                )
                h.execute(
                    f"INSERT INTO {h.source(distribution)} SELECT {key}, number, "
                    f"toInt64(number * 7919 % {args.rows}), repeat('x',{args.payload_bytes}) FROM numbers({args.rows})"
                )
                for ordering in args.orderings:
                    keys = ("id", "seq") if ordering == "aligned" else ("score", "seq")
                    cases = [
                        {
                            "distribution": distribution,
                            "ordering": ordering,
                            "storage_aligned": ordering == "aligned",
                            "variant": variant,
                            "mode": variant,
                            "ratio": ratio,
                            "sql": case_query(h, distribution, keys, variant, ratio),
                        }
                        for variant in variants
                        for ratio in (args.ratios if variant == "partial" else [None])
                    ]
                    for warmup, rounds in ((True, args.warmups), (False, args.repeats)):
                        for repeat in range(rounds):
                            shuffled = cases.copy()
                            randomizer.shuffle(shuffled)
                            for case in shuffled:
                                entry = run_case(
                                    h, case, args, temporary, repeat, warmup
                                )
                                report["warmup_runs" if warmup else "runs"].append(
                                    entry
                                )
                                save_report(report, args)
        finally:
            h.execute(f"DROP DATABASE IF EXISTS {identifier(h.database)}")
        print_summary(report["summary"])
        print(f"\nJSON: {args.output}\nCSV:  {args.csv}")
        failed = sum(
            run["status"] != "ok" for run in report["runs"] + report["warmup_runs"]
        )
        if failed:
            raise SystemExit(
                f"{failed} benchmark trials failed; see the JSON report for errors"
            )


if __name__ == "__main__":
    main()
