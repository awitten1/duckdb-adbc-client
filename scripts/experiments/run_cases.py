#!/usr/bin/env python3
"""Profile SF25 orders sorts through ADBC; optionally sample Linux process usage."""

import argparse
import json
import os
from pathlib import Path
import resource
import subprocess
import time

REPO = Path(__file__).resolve().parents[2]
CASES = ("partial0", "partial0.25", "partial0.5", "partial0.75", "partial1", "native", "read", "read_sorted")
DEFAULT_CASES = ("partial0", "partial0.5", "partial1", "native", "read", "read_sorted")


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def identifier(value):
    return '"' + str(value).replace('"', '""') + '"'


def cpu(pid):
    if pid is None:
        return None
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    except FileNotFoundError:
        return None


def operators(node):
    for child in node.get("operator", []) + node.get("children", []):
        yield child
        yield from operators(child)


def query(args, case):
    source = identifier(args.database) + '."orders"'
    ordering = ", ".join(identifier(column) + " ASC NULLS LAST" for column in args.sort_columns)
    sort_columns = "[" + ", ".join(quote(column) for column in args.sort_columns) + "]"
    if case.startswith("partial"):
        return (
            f"SELECT {args.projection} FROM read_adbc_partial_sort("
            f"{quote(args.profile)}, {quote(args.database)}, 'orders', "
            f"partition_key:={quote(args.partition_key)}, sort_columns:={sort_columns}, "
            f"pushdown_ratio:={case[7:]})"
        )
    remote = f"SELECT {args.projection} FROM {source}"
    if case == "read_sorted":
        remote += " ORDER BY " + ordering
    sql = f"SELECT {args.projection} FROM read_adbc({quote(args.profile)}, {quote(remote)})"
    # Historical label: native means a DuckDB ORDER BY, not ClickHouse Native TCP.
    if case == "native":
        sql += " ORDER BY " + ordering
    return sql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="Directory for SQL, profiles, and summaries",
    )
    parser.add_argument(
        "--extension",
        type=Path,
        default=REPO / "build/release/extension/adbc/adbc.duckdb_extension",
    )
    parser.add_argument("--duckdb", type=Path, default=REPO / "build/release/duckdb")
    parser.add_argument(
        "--clickhouse-client",
        type=Path,
        default=REPO / "clickhouse/.clickhouse/bin/clickhouse-client",
    )
    parser.add_argument("--profile", default="profile://myclickhouse")
    parser.add_argument("--database", default="tpch_sf25")
    parser.add_argument("--partition-key", default="o_orderkey")
    parser.add_argument("--sort-columns", nargs="+", default=["o_orderdate"])
    parser.add_argument("--tag", default="current")
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--memory-limit", default="56GiB")
    parser.add_argument("--expected-rows", type=int, default=37500000)
    parser.add_argument(
        "--projection", default="*", help="SQL projection, e.g. o_orderkey,o_orderdate"
    )
    parser.add_argument("--cases", nargs="+", choices=CASES, default=list(DEFAULT_CASES))
    parser.add_argument("--server-service", default="adbc-clickhouse.service")
    parser.add_argument("--server-pid", type=int)
    parser.add_argument(
        "--no-server-metrics",
        action="store_true",
        help="Skip systemd, /proc, and ClickHouse log collection",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write/print SQL without contacting either engine",
    )
    args = parser.parse_args()
    if (
        args.threads < 1
        or args.expected_rows < 0
        or (args.server_pid is not None and args.server_pid < 1)
        or not args.sort_columns
    ):
        parser.error(
            "threads/server PID must be positive; expected rows must be nonnegative"
        )
    if not args.tag or Path(args.tag).name != args.tag or args.tag in (".", ".."):
        parser.error("--tag must be a filename component")
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    server_pid = None
    if not args.dry_run and not args.no_server_metrics:
        server_pid = args.server_pid or int(
            subprocess.check_output(
                ["systemctl", "show", args.server_service, "--value", "-p", "MainPID"],
                text=True,
            )
        )
        if server_pid <= 0:
            parser.error(
                "ClickHouse service is not running; pass --server-pid or --no-server-metrics"
            )
    client = [
        str(args.clickhouse_client.resolve()),
        "--format",
        "JSONEachRow",
        "--query",
    ]
    env = dict(os.environ, TZ="UTC")
    failed = False
    for case in args.cases:
        name = args.tag + "-" + case
        if (root / (name + ".sql")).exists():
            parser.error(f"Refusing to overwrite {name}; use a new --root or --tag")
        statements = (
            f"LOAD {quote(args.extension.resolve())};\n"
            f"SET threads={args.threads};\nSET memory_limit={quote(args.memory_limit)};\n"
            "EXPLAIN (ANALYZE, FORMAT JSON) " + query(args, case) + ";\n"
        )
        (root / (name + ".sql")).write_text(statements)
        if args.dry_run:
            print(statements, flush=True)
            continue
        started_epoch = time.time()
        started = time.perf_counter()
        prior = resource.getrusage(resource.RUSAGE_CHILDREN)
        server_start = cpu(server_pid)
        samples = []
        with (root / (name + ".json")).open("w") as output, (
            root / (name + ".stderr")
        ).open("w") as errors:
            process = subprocess.Popen(
                [
                    str(args.duckdb.resolve()),
                    "-unsigned",
                    "-batch",
                    "-bail",
                    "-json",
                    "-no-init",
                ],
                stdin=subprocess.PIPE,
                stdout=output,
                stderr=errors,
                text=True,
            )
            try:
                process.stdin.write(statements)
                process.stdin.close()
                if args.no_server_metrics:
                    process.wait()
                else:
                    while process.poll() is None:
                        status = Path(f"/proc/{process.pid}/status")
                        try:
                            fields = dict(
                                line.split(":", 1)
                                for line in status.read_text().splitlines()
                                if ":" in line
                            )
                            server_now = cpu(server_pid)
                            samples.append(
                                {
                                    "elapsed": time.perf_counter() - started,
                                    "duckdb_cpu": cpu(process.pid),
                                    "server_cpu": (
                                        server_now - server_start
                                        if server_now is not None
                                        and server_start is not None
                                        else None
                                    ),
                                    "rss_kib": int(
                                        fields.get("VmRSS", "0 kB").split()[0]
                                    ),
                                    "threads": int(fields.get("Threads", "0")),
                                }
                            )
                        except FileNotFoundError:
                            pass
                        time.sleep(0.5)
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=30)
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        result = {
            "name": name,
            "case": case,
            "projection": args.projection,
            "threads": args.threads,
            "elapsed": time.perf_counter() - started,
            "start_epoch": started_epoch,
            "extension": str(args.extension.resolve()),
            "profile": args.profile,
            "database": args.database,
            "partition_key": args.partition_key,
            "sort_columns": args.sort_columns,
            "memory_limit": args.memory_limit,
            "expected_rows": args.expected_rows,
            "user_seconds": usage.ru_utime - prior.ru_utime,
            "system_seconds": usage.ru_stime - prior.ru_stime,
            "exit": process.returncode,
            "samples": samples,
        }
        if process.returncode == 0:
            profile = json.loads((root / (name + ".json")).read_text())
            result["query"] = profile.get("query", {})
            result["system"] = profile.get("system", {})
            result["operators"] = [
                {
                    k: node[k]
                    for k in ("type", "timing", "intermediate_rows", "extra_info")
                    if k in node
                }
                for node in operators(profile)
            ]
            rows = max(
                (node.get("intermediate_rows", 0) for node in result["operators"]),
                default=0,
            )
            if rows != args.expected_rows:
                result["error"] = (
                    f"Expected {args.expected_rows} rows, profile contains {rows}"
                )
        else:
            result["error"] = (root / (name + ".stderr")).read_text()
        if not args.no_server_metrics:
            subprocess.run(
                client + ["SYSTEM FLUSH LOGS"], env=env, check=True, capture_output=True
            )
            log_sql = (
                "SELECT query_id,query_start_time_microseconds,query_duration_ms,read_rows,read_bytes,"
                "result_rows,result_bytes,memory_usage,ProfileEvents,query FROM system.query_log "
                f"WHERE type='QueryFinish' AND query_start_time_microseconds>=fromUnixTimestamp64Micro({int(started_epoch * 1000000)}) "
                "AND query LIKE '%orders%' AND query NOT LIKE '%system.query_log%' ORDER BY query_start_time_microseconds"
            )
            logs = subprocess.run(
                client + [log_sql], env=env, check=True, capture_output=True, text=True
            )
            (root / (name + "-server.jsonl")).write_text(logs.stdout)
        (root / (name + "-summary.json")).write_text(json.dumps(result, indent=2))
        print(
            json.dumps(
                {k: v for k, v in result.items() if k not in ("samples", "query")}
            ),
            flush=True,
        )
        failed |= "error" in result
    if failed:
        raise SystemExit(
            "One or more cases failed; inspect the summaries and stderr files"
        )


if __name__ == "__main__":
    main()
