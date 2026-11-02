#!/usr/bin/env python3
"""Repeat original/optimized SF25 sorts and validate complete rows and ordering."""

from pathlib import Path
import os
import argparse
import json
import math
import random
import statistics
import subprocess
import sys
import time

from run_cases import REPO, identifier, quote


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", "--work", dest="root", type=Path, required=True)
    parser.add_argument("--original-extension", type=Path, required=True)
    parser.add_argument(
        "--optimized-extension",
        type=Path,
        default=REPO / "build/release/extension/adbc/adbc.duckdb_extension",
    )
    parser.add_argument(
        "--before-short-probe-extension",
        type=Path,
        help="Optional third build for the alternating-key microbenchmark",
    )
    parser.add_argument("--duckdb", type=Path, default=REPO / "build/release/duckdb")
    parser.add_argument("--profile", default="profile://myclickhouse")
    parser.add_argument("--database", default="tpch_sf25")
    parser.add_argument("--expected-rows", type=int, default=37500000)
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--memory-limit", default="56GiB")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--interleaved-rows", type=int, default=8000000)
    parser.add_argument("--skip-interleaved", action="store_true")
    parser.add_argument("--skip-integration", action="store_true")
    parser.add_argument(
        "--no-server-metrics",
        action="store_true",
        help="Skip run_cases.py systemd and server-log sampling",
    )
    args = parser.parse_args()
    if (
        min(args.threads, args.repeats, args.expected_rows) < 1
        or args.warmups < 0
        or args.timeout <= 0
        or not math.isfinite(args.timeout)
    ):
        parser.error(
            "threads, repeats, expected rows and timeout must be positive; warmups must be nonnegative"
        )
    if args.interleaved_rows < 2 or args.interleaved_rows % 2:
        parser.error("--interleaved-rows must be positive and even")
    for path in (
        args.original_extension,
        args.optimized_extension,
        args.before_short_probe_extension,
    ):
        if path is not None and not path.is_file():
            parser.error(f"Extension does not exist: {path}")
    work = args.root.resolve()
    work.mkdir(parents=True, exist_ok=False)
    results_dir = work / "results"
    results_dir.mkdir()
    normal = str(args.optimized_extension.resolve())
    original = str(args.original_extension.resolve())
    duckdb = str(args.duckdb.resolve())
    before_short_probe = (
        str(args.before_short_probe_extension.resolve())
        if args.before_short_probe_extension
        else None
    )
    os.chdir(REPO)
    runner = str(Path(__file__).with_name("run_cases.py"))
    (work / "configuration.json").write_text(
        json.dumps(
            {
                key: str(value) if isinstance(value, Path) else value
                for key, value in vars(args).items()
            },
            indent=2,
        )
    )
    measurements = []

    def run_case(tag, extension, case):
        with (work / "benchmarks.log").open("a") as log:
            subprocess.run(
                [
                    sys.executable,
                    runner,
                    "--root",
                    str(results_dir),
                    "--tag",
                    tag,
                    "--threads",
                    str(args.threads),
                    "--memory-limit",
                    args.memory_limit,
                    "--extension",
                    extension,
                    "--duckdb",
                    duckdb,
                    "--profile",
                    args.profile,
                    "--database",
                    args.database,
                    "--expected-rows",
                    str(args.expected_rows),
                    "--cases",
                    case,
                ]
                + (["--no-server-metrics"] if args.no_server_metrics else []),
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=args.timeout,
            )
        result = json.loads(
            (results_dir / (tag + "-" + case + "-summary.json")).read_text()
        )
        assert result["exit"] == 0, result.get("error")
        assert (
            max(node.get("intermediate_rows", 0) for node in result["operators"])
            == args.expected_rows
        )
        print(
            tag,
            "wall",
            round(result["elapsed"], 3),
            "explain",
            round(result["query"]["total_time"], 3),
            flush=True,
        )
        return result

    for warmup in range(args.warmups):
        for label, extension, case in [
            ("original", original, "partial0.5"),
            ("optimized", normal, "partial0.5"),
            ("native", original, "native"),
        ]:
            run_case(f"warm-{warmup}-" + label, extension, case)
    randomizer = random.Random(args.seed)
    for repeat in range(args.repeats):
        cases = [
            ("original", original, "partial0.5"),
            ("optimized", normal, "partial0.5"),
            ("native", original, "native"),
        ]
        randomizer.shuffle(cases)
        for label, extension, case in cases:
            measurements.append(run_case(f"r{repeat}-{label}", extension, case))
    (results_dir / "repeated-summary.json").write_text(
        json.dumps(measurements, indent=2)
    )
    for label in ("original", "optimized", "native"):
        selected = [
            r
            for r in measurements
            if r["name"].startswith(
                tuple(f"r{i}-{label}-" for i in range(args.repeats))
            )
        ]
        print(
            "MEDIAN",
            label,
            statistics.median(r["elapsed"] for r in selected),
            flush=True,
        )

    columns = "o_orderkey,o_custkey,o_orderstatus,o_totalprice,o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment"
    aggregate = f"count(*) AS n, sum(hash({columns}))::VARCHAR AS hash_sum, bit_xor(hash({columns}))::VARCHAR AS hash_xor, sum(o_totalprice)::VARCHAR AS price_total"
    checksums = {}
    queries = [
        (
            "baseline",
            original,
            f"SELECT {aggregate} FROM read_adbc({quote(args.profile)}, {quote('SELECT * FROM ' + identifier(args.database) + '.orders')})",
        ),
        (
            "optimized",
            normal,
            f"SELECT {aggregate}, count(*) FILTER (WHERE o_orderdate < previous_date) AS order_violations FROM (SELECT *, lag(o_orderdate) OVER () AS previous_date FROM read_adbc_partial_sort({quote(args.profile)},{quote(args.database)},'orders',partition_key:='o_orderkey',sort_columns:=['o_orderdate'],pushdown_ratio:=0.5)) t",
        ),
    ]
    for label, extension, query in queries:
        sql = f"LOAD {quote(extension)}; SET threads={args.threads}; SET memory_limit={quote(args.memory_limit)}; {query};"
        (results_dir / ("checksum-" + label + ".sql")).write_text(sql)
        result = subprocess.run(
            [duckdb, "-unsigned", "-batch", "-bail", "-json", "-no-init"],
            input=sql,
            text=True,
            capture_output=True,
            timeout=args.timeout,
            check=True,
        )
        (results_dir / ("checksum-" + label + ".json")).write_text(result.stdout)
        checksums[label] = json.loads(result.stdout)[0]
        print("CHECKSUM", label, checksums[label], flush=True)
    assert checksums["optimized"].pop("order_violations") == 0
    assert checksums["baseline"] == checksums["optimized"]
    assert checksums["baseline"]["n"] == args.expected_rows
    print("Full SF25 row checksums and global ordering passed.", flush=True)

    if not args.skip_interleaved:
        # Exercise the other merge extreme: every output row alternates between branches.
        sys.path.insert(0, str(REPO / "scripts"))
        from test_clickhouse_partial_sort import Harness
        import uuid

        micro_db = "adbc_perf_" + uuid.uuid4().hex[:12]
        h = Harness(duckdb, normal, args.profile, micro_db)
        h.execute("CREATE DATABASE " + identifier(micro_db))
        try:
            h.create("alternating", "id UInt64, score UInt64, payload String")
            h.execute(
                f"INSERT INTO {h.source('alternating')} SELECT number, (number % {args.interleaved_rows // 2}) * 2 + toUInt64(number >= {args.interleaved_rows // 2}), repeat('x',32) FROM numbers({args.interleaved_rows})"
            )
            libraries = [("optimized", normal), ("original", original)]
            if before_short_probe:
                libraries.append(("before-short-probe", before_short_probe))
            micro_results = []
            for repeat in range(args.warmups + args.repeats):
                shuffled = list(libraries)
                randomizer.shuffle(shuffled)
                for label, library in shuffled:
                    scanner = Harness(duckdb, library, args.profile, micro_db)
                    sql = (
                        f"SET threads={args.threads}; SET memory_limit={quote(args.memory_limit)}; EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM "
                        + scanner.function("alternating")
                    )
                    started = time.perf_counter()
                    result = scanner.run(sql)
                    elapsed = time.perf_counter() - started
                    profile = json.loads(result.stdout)
                    (results_dir / f"interleaved-{repeat}-{label}.json").write_text(
                        result.stdout
                    )
                    if repeat >= args.warmups:
                        micro_results.append(
                            {
                                "variant": label,
                                "repeat": repeat,
                                "wall_seconds": elapsed,
                                "explain_seconds": profile["query"]["total_time"],
                            }
                        )
                    print("INTERLEAVED", repeat, label, round(elapsed, 3), flush=True)
            (results_dir / "interleaved-summary.json").write_text(
                json.dumps(micro_results, indent=2)
            )
            ordered = (
                "SELECT *, lag(score) OVER () AS previous_score FROM "
                + h.function("alternating")
            )
            checked = h.rows(
                f"SET threads={args.threads}; SET memory_limit={quote(args.memory_limit)}; SELECT count(*) AS n, min(score) AS minimum, max(score) AS maximum, count(*) FILTER (WHERE score <= previous_score) AS order_violations, count(*) FILTER (WHERE score <> (id % {args.interleaved_rows // 2}) * 2 + (id >= {args.interleaved_rows // 2})::UBIGINT OR payload <> repeat('x',32)) AS bad_values FROM ("
                + ordered
                + ") t"
            )[0]
            checked["minimum"] = int(checked["minimum"])
            checked["maximum"] = int(checked["maximum"])
            assert checked == {
                "n": args.interleaved_rows,
                "minimum": 0,
                "maximum": args.interleaved_rows - 1,
                "order_violations": 0,
                "bad_values": 0,
            }, checked
            (results_dir / "interleaved-correctness.json").write_text(
                json.dumps(checked, indent=2)
            )
            print("Interleaved correctness passed.", flush=True)
        finally:
            h.execute("DROP DATABASE " + identifier(micro_db))

    if not args.skip_integration:
        with (work / "integration.log").open("w") as log:
            result = subprocess.run(
                [
                    sys.executable,
                    str(REPO / "scripts/test_clickhouse_partial_sort.py"),
                    "--profile",
                    args.profile,
                    "--duckdb",
                    duckdb,
                    "--extension",
                    normal,
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=args.timeout,
            )
        print((work / "integration.log").read_text()[-6000:], flush=True)
        assert result.returncode == 0, "Integration suite failed; see integration.log"
    print("Comparison and validation completed.", flush=True)


if __name__ == "__main__":
    main()
