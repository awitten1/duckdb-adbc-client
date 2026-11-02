#!/usr/bin/env python3
"""Measure format/projection combinations without perf, then profile numeric output."""

import argparse
import json
from pathlib import Path
import random
import statistics
import subprocess
import sys
import time
from urllib.parse import urlencode
import uuid

import record_queries as profiler


NUMERIC = "o_orderkey, o_custkey, o_totalprice, o_orderdate, o_shippriority"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--clickhouse-client", type=Path, default=Path(profiler.CLI))
    parser.add_argument(
        "--url",
        default=profiler.HTTP_URL,
        help="ClickHouse HTTP base URL without query parameters",
    )
    parser.add_argument("--database", default="tpch_sf25")
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--expected-rows", type=int, default=37500000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--skip-profiles",
        action="store_true",
        help="Only measure; do not launch perf afterward",
    )
    parser.add_argument("--server-service", default="adbc-clickhouse.service")
    parser.add_argument("--server-pid", type=int)
    args = parser.parse_args()
    if min(args.threads, args.repeats, args.expected_rows) < 1 or args.warmups < 0:
        parser.error(
            "threads/repeats/expected rows must be positive; warmups must be nonnegative"
        )
    if "?" in args.url or "#" in args.url:
        parser.error("--url must not contain query parameters or a fragment")
    profiler.CLI = str(args.clickhouse_client.resolve())
    profiler.SETTINGS = f" SETTINGS max_threads={args.threads}, log_processors_profiles=1, log_query_settings=1, use_query_cache=0"
    source = '"' + args.database.replace('"', '""') + '".orders'
    root = args.root.resolve()
    root.mkdir(parents=True)
    (root / "configuration.json").write_text(
        json.dumps(
            {
                key: str(value) if isinstance(value, Path) else value
                for key, value in vars(args).items()
            },
            indent=2,
        )
    )
    cases = [
        (projection, fmt)
        for projection in ("all", "no_strings")
        for fmt in ("Native", "ArrowStream")
    ]
    rng = random.Random(args.seed)
    runs = []
    for repeat in range(-args.warmups, args.repeats):
        shuffled = list(cases)
        rng.shuffle(shuffled)
        for projection, fmt in shuffled:
            others = profiler.control(
                "SELECT query_id, query FROM system.processes WHERE query_id != currentQueryID()"
            )
            if others:
                raise RuntimeError("Another query is running: " + str(others))
            query_id = "no-strings-" + uuid.uuid4().hex
            params = {
                "query_id": query_id,
                "default_format": fmt,
                "compress": "0",
                "output_format_arrow_compression_method": "none",
            }
            if fmt == "Native":
                params["client_protocol_version"] = "54492"
            columns = "*" if projection == "all" else NUMERIC
            sql = (
                "SELECT "
                + columns
                + f" FROM {source} ORDER BY o_orderdate ASC NULLS LAST"
                + profiler.SETTINGS
            )
            command = [
                "curl",
                "-fsS",
                "--max-time",
                "180",
                "--output",
                "/dev/null",
                "--write-out",
                "%{json}",
                "--data-binary",
                sql,
                args.url.rstrip("/") + "/?" + urlencode(params),
            ]
            start = time.perf_counter()
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                timeout=190,
                env=profiler.ENV,
            )
            wall = time.perf_counter() - start
            profiler.control("SYSTEM FLUSH LOGS")
            query = profiler.control(
                "SELECT query_id,query_duration_ms,read_rows,read_bytes,result_rows,result_bytes,"
                "memory_usage,interface,Settings,ProfileEvents,query FROM system.query_log "
                "WHERE type='QueryFinish' AND query_id='" + query_id + "'"
            )[0]
            assert int(query["result_rows"]) == args.expected_rows, query
            processors = profiler.control(
                "SELECT * FROM system.processors_profile_log WHERE query_id='"
                + query_id
                + "'"
            )
            row = {
                "projection": projection,
                "format": fmt,
                "repeat": repeat,
                "warmup": repeat < 0,
                "wall_seconds": wall,
                "curl": json.loads(result.stdout),
                "query": query,
                "processors": processors,
                "command": command,
            }
            (root / (query_id + ".json")).write_text(json.dumps(row, indent=2))
            runs.append(row)
            (root / "runs.json").write_text(json.dumps(runs, indent=2))
            print(
                json.dumps(
                    {
                        "projection": projection,
                        "format": fmt,
                        "repeat": repeat,
                        "seconds": row["curl"]["time_total"],
                        "bytes": row["curl"]["size_download"],
                        "query_id": query_id,
                    }
                ),
                flush=True,
            )
    summary = []
    for projection, fmt in cases:
        selected = [
            r
            for r in runs
            if r["projection"] == projection and r["format"] == fmt and not r["warmup"]
        ]
        summary.append(
            {
                "projection": projection,
                "format": fmt,
                "median_seconds": statistics.median(
                    r["curl"]["time_total"] for r in selected
                ),
                "median_server_cpu_seconds": statistics.median(
                    r["query"]["ProfileEvents"].get("OSCPUVirtualTimeMicroseconds", 0)
                    / 1e6
                    for r in selected
                ),
                "median_bytes": statistics.median(
                    r["curl"]["size_download"] for r in selected
                ),
                "query_ids": [r["query"]["query_id"] for r in selected],
            }
        )
    (root / "summary.json").write_text(json.dumps(summary, indent=2))
    print("SUMMARY " + json.dumps(summary), flush=True)
    profile_sql = (
        "SELECT " + NUMERIC + f" FROM {source} ORDER BY o_orderdate ASC NULLS LAST"
    )
    (root / "no-strings.sql").write_text(profile_sql + ";\n")
    if not args.skip_profiles:
        command = [
            sys.executable,
            str(Path(__file__).with_name("record_queries.py")),
            "--root",
            str(root / "profiles"),
            "--no-warmup",
            "--sql-file",
            str(root / "no-strings.sql"),
            "--clickhouse-client",
            str(args.clickhouse_client.resolve()),
            "--url",
            args.url,
            "--threads",
            str(args.threads),
            "--expected-rows",
            str(args.expected_rows),
            "--server-service",
            args.server_service,
            "--min-network-bytes",
            "0",
        ]
        if args.server_pid:
            command += ["--server-pid", str(args.server_pid)]
        subprocess.run(command, check=True, env=profiler.ENV)


if __name__ == "__main__":
    main()
