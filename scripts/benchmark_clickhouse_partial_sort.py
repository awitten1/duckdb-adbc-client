#!/usr/bin/env python3
"""Benchmark key-range partial sorting, including discovery and full ADBC transfer."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

from test_clickhouse_partial_sort import (
    Harness,
    find_partial_sort_metrics,
    identifier,
    literal,
    system_metric,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", default="build/release/duckdb")
    parser.add_argument(
        "--extension", default="build/release/extension/adbc/adbc.duckdb_extension"
    )
    parser.add_argument("--profile")
    parser.add_argument(
        "--url", default=os.environ.get("ADBC_CLICKHOUSE_URL", "http://localhost:8123/")
    )
    parser.add_argument("--rows", type=int, default=1000000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--memory-limit", default="256MB")
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--profiles-dir", type=Path, help="Keep individual DuckDB JSON profiles"
    )
    args = parser.parse_args()
    if args.profiles_dir:
        args.profiles_dir.mkdir(parents=True, exist_ok=True)
    if args.rows < 2 or args.repeats < 1:
        parser.error("--rows must be at least 2 and --repeats must be positive")
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
            f"adbc_partial_sort_bench_{os.getpid()}",
        )
        report = {
            "rows": args.rows,
            "memory_limit": args.memory_limit,
            "duckdb": h.rows("SELECT version() AS version"),
            "clickhouse": h.rows(
                "SELECT * FROM " + h.read("SELECT version() AS version")
            ),
            "driver": subprocess.run(
                ["dbc", "list", "--json"], text=True, capture_output=True
            ).stdout,
            "runs": [],
        }
        h.execute(f"CREATE DATABASE {identifier(h.database)}")
        try:
            for distribution in ("dense", "skewed"):
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
                    f"toInt64(number * 7919 % {args.rows}), repeat('x',64) FROM numbers({args.rows})"
                )
                for aligned in (True, False):
                    keys = ("id", "seq") if aligned else ("score", "seq")
                    for ratio in ("0", "0.25", "0.5", "0.75", "1"):
                        for repeat in range(args.repeats):
                            profiling = Path(temporary) / "profile.json"
                            sql = (
                                f"SET memory_limit={literal(args.memory_limit)}; SET threads=1; "
                                f"PRAGMA enable_profiling='json'; SET profiling_output={literal(profiling)}; "
                                f"COPY (SELECT * FROM {h.function(distribution, ratio, keys)}) "
                                f"TO {literal(os.devnull)} (FORMAT CSV)"
                            )
                            start = time.perf_counter()
                            h.run(sql, timeout=1800)
                            elapsed = time.perf_counter() - start
                            metrics = json.loads(profiling.read_text())
                            if args.profiles_dir:
                                saved = (
                                    args.profiles_dir
                                    / f"{distribution}-{aligned}-{ratio}-{repeat}.json"
                                )
                                saved.write_text(json.dumps(metrics, indent=2) + "\n")
                            report["runs"].append(
                                {
                                    "distribution": distribution,
                                    "storage_aligned": aligned,
                                    "ratio": ratio,
                                    "repeat": repeat,
                                    "wall_seconds_including_binding": elapsed,
                                    "peak_memory_bytes": system_metric(
                                        metrics, "peak_buffer_memory"
                                    ),
                                    "peak_spill_bytes": system_metric(
                                        metrics, "peak_temp_dir_size"
                                    ),
                                    "partial_sort": find_partial_sort_metrics(metrics),
                                }
                            )
                            print(
                                f"{distribution} aligned={aligned} ratio={ratio}: {elapsed:.3f}s",
                                flush=True,
                            )
        finally:
            h.execute(f"DROP DATABASE {identifier(h.database)}")
        output = json.dumps(report, indent=2)
        if args.output:
            args.output.write_text(output + "\n")
        else:
            print(output)


if __name__ == "__main__":
    main()
