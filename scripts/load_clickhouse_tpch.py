#!/usr/bin/env python3
"""Generate TPC-H with DuckDB, export Parquet, and load ClickHouse (SF25 by default)."""

import argparse
from decimal import Decimal, InvalidOperation
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
import uuid


REPO = Path(__file__).resolve().parents[1]
SORT_KEYS = {
    "region": ("r_regionkey",),
    "nation": ("n_nationkey",),
    "supplier": ("s_suppkey",),
    "customer": ("c_custkey",),
    "part": ("p_partkey",),
    "partsupp": ("ps_partkey", "ps_suppkey"),
    "orders": ("o_orderkey",),
    "lineitem": ("l_orderkey", "l_linenumber"),
}


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def scale_factor(value):
    try:
        number = Decimal(value)
        if not number.is_finite() or number <= 0:
            raise ValueError
        return format(number.normalize(), "f")
    except (InvalidOperation, ValueError):
        raise argparse.ArgumentTypeError(
            "scale factor must be a positive finite number"
        )


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scale-factor",
        type=scale_factor,
        default="25",
        help="TPC-H scale factor (default: 25)",
    )
    parser.add_argument(
        "--duckdb",
        default=shutil.which("duckdb") or str(REPO / "build/release/duckdb"),
        help="DuckDB CLI; defaults to duckdb on PATH, then build/release/duckdb",
    )
    parser.add_argument("--data-dir", type=Path, help="Default: build/tpch-sf<SF>")
    parser.add_argument("--database", help="ClickHouse database; default: tpch_sf<SF>")
    parser.add_argument(
        "--url", default=os.environ.get("ADBC_CLICKHOUSE_URL", "http://localhost:8123/")
    )
    parser.add_argument("--user", default=os.environ.get("CLICKHOUSE_USER", "default"))
    parser.add_argument(
        "--threads", type=int, default=16, help="DuckDB generation/export threads"
    )
    parser.add_argument(
        "--memory-limit", default="8GiB", help="DuckDB generation/export memory limit"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=7200,
        help="ClickHouse socket timeout in seconds",
    )
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--generate-only", action="store_true")
    modes.add_argument(
        "--load-only",
        action="store_true",
        help="Load previously exported Parquet and manifest",
    )
    args = parser.parse_args()
    if args.threads < 1 or not 0 < args.timeout < float("inf"):
        parser.error("--threads and --timeout must be positive and finite")
    label = args.scale_factor.replace(".", "_")
    if args.database is None:
        args.database = f"tpch_sf{label}"
    if not args.database:
        parser.error("--database must not be empty")
    args.data_dir = (
        args.data_dir or REPO / "build" / f"tpch-sf{args.scale_factor}"
    ).resolve()
    return args


class DuckDB:
    def __init__(self, args):
        self.args = args
        self.database = args.data_dir / "tpch.duckdb"

    def query(self, sql):
        settings = (
            f"SET threads={self.args.threads}; "
            f"SET memory_limit={literal(self.args.memory_limit)}; "
        )
        result = subprocess.run(
            [
                self.args.duckdb,
                "-unsigned",
                "-batch",
                "-bail",
                "-json",
                "-no-init",
                str(self.database),
            ],
            input=settings + sql,
            text=True,
            capture_output=True,
        )
        if result.returncode:
            raise RuntimeError(f"DuckDB failed:\n{result.stderr.strip()}")
        # Some CLI versions print multiple JSON result sets for a SQL script.
        remaining = result.stdout.strip()
        rows = []
        decoder = json.JSONDecoder()
        while remaining:
            rows, end = decoder.raw_decode(remaining)
            remaining = remaining[end:].strip()
        return rows


class ClickHouse:
    def __init__(self, args):
        self.url = urlsplit(args.url)
        if self.url.scheme not in ("http", "https") or not self.url.hostname:
            raise ValueError("--url must be an HTTP or HTTPS ClickHouse endpoint")
        if self.url.username is not None or self.url.password is not None:
            raise ValueError(
                "Use --user and the CLICKHOUSE_PASSWORD environment variable for credentials"
            )
        self.user = args.user
        self.password = os.environ.get("CLICKHOUSE_PASSWORD", "")
        self.timeout = args.timeout

    def query(self, sql, parquet=None):
        connection_type = (
            http.client.HTTPSConnection
            if self.url.scheme == "https"
            else http.client.HTTPConnection
        )
        connection = connection_type(
            self.url.hostname,
            self.url.port,
            timeout=self.timeout,
            blocksize=1024 * 1024,
        )
        params = dict(parse_qsl(self.url.query))
        params.update(query=sql, wait_end_of_query="1", async_insert="0")
        path = urlunsplit(("", "", self.url.path or "/", urlencode(params), ""))
        headers = {"X-ClickHouse-User": self.user, "X-ClickHouse-Key": self.password}
        try:
            if parquet is None:
                connection.request("POST", path, body=b"", headers=headers)
            else:
                headers["Content-Type"] = "application/octet-stream"
                headers["Content-Length"] = str(parquet.stat().st_size)
                with parquet.open("rb") as stream:
                    connection.request("POST", path, body=stream, headers=headers)
            response = connection.getresponse()
            body = response.read().decode("utf-8")
            if response.status != 200:
                raise RuntimeError(f"ClickHouse HTTP {response.status}: {body.strip()}")
            return body.strip()
        finally:
            connection.close()


def clickhouse_type(duckdb_type):
    types = {"BIGINT": "Int64", "INTEGER": "Int32", "VARCHAR": "String", "DATE": "Date"}
    if duckdb_type in types:
        return types[duckdb_type]
    decimal = re.fullmatch(r"DECIMAL\((\d+),\s*(\d+)\)", duckdb_type)
    if decimal:
        return f"Decimal({decimal[1]}, {decimal[2]})"
    raise ValueError(f"Unsupported TPC-H column type: {duckdb_type}")


def generate(args):
    duckdb = DuckDB(args)
    existing = {
        row["table_name"]
        for row in duckdb.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        )
    }
    if "__tpch_loader" in existing:
        stored = duckdb.query("SELECT scale_factor FROM __tpch_loader")[0][
            "scale_factor"
        ]
        if stored != args.scale_factor:
            raise ValueError(
                f"Data directory contains SF{stored}; choose another --data-dir"
            )
        print(f"Reusing generated SF{stored} database: {duckdb.database}", flush=True)
    elif existing:
        raise ValueError(
            f"Unrecognized DuckDB tables in {duckdb.database}; choose an empty --data-dir"
        )
    else:
        print(
            f"Generating all TPC-H tables at SF{args.scale_factor} in {duckdb.database}",
            flush=True,
        )
        # Commit the scale marker with the generated tables so interrupted generation can be retried.
        duckdb.query(
            "INSTALL tpch; LOAD tpch; BEGIN TRANSACTION; "
            f"CALL dbgen(sf = {args.scale_factor}); "
            f"CREATE TABLE __tpch_loader AS SELECT {literal(args.scale_factor)} AS scale_factor; "
            "COMMIT;"
        )

    manifest = {"scale_factor": args.scale_factor, "tables": {}}
    for table in SORT_KEYS:
        name = identifier(table)
        count = duckdb.query(f"SELECT count(*) AS n FROM {name}")[0]["n"]
        schema = duckdb.query(f"DESCRIBE SELECT * FROM {name}")
        columns = [
            (row["column_name"], clickhouse_type(row["column_type"])) for row in schema
        ]
        parquet = args.data_dir / f"{table}.parquet"
        if parquet.exists():
            cached = duckdb.query(
                f"SELECT count(*) AS n FROM read_parquet({literal(parquet)})"
            )[0]["n"]
            cached_schema = duckdb.query(
                f"DESCRIBE SELECT * FROM read_parquet({literal(parquet)})"
            )
            cached_columns = [
                (row["column_name"], clickhouse_type(row["column_type"]))
                for row in cached_schema
            ]
            if cached != count or cached_columns != columns:
                raise ValueError(
                    f"{parquet} schema/count differs from the generated table; remove it and retry"
                )
            print(f"Reusing {parquet.name}: {count:,} rows", flush=True)
        else:
            print(f"Exporting {table}: {count:,} rows", flush=True)
            partial = parquet.with_suffix(".parquet.partial")
            duckdb.query(
                f"COPY {name} TO {literal(partial)} (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            partial.replace(parquet)
        manifest["tables"][table] = {"rows": count, "columns": columns}
    path = args.data_dir / "manifest.json"
    partial = path.with_suffix(".json.partial")
    partial.write_text(json.dumps(manifest, indent=2) + "\n")
    partial.replace(path)
    return manifest


def load(args, clickhouse, manifest):
    database = identifier(args.database)
    clickhouse.query(f"CREATE DATABASE IF NOT EXISTS {database}")
    for table, keys in SORT_KEYS.items():
        metadata = manifest["tables"][table]
        expected = metadata["rows"]
        target = f"{database}.{identifier(table)}"
        if clickhouse.query(f"EXISTS TABLE {target}") == "1":
            actual = int(clickhouse.query(f"SELECT count() FROM {target}"))
            schema = json.loads(
                clickhouse.query(f"DESCRIBE TABLE {target} FORMAT JSON")
            )["data"]
            columns = [(column["name"], column["type"]) for column in schema]
            wanted = [tuple(column) for column in metadata["columns"]]
            if actual != expected or columns != wanted:
                raise ValueError(
                    f"{target} already exists with different schema/count; choose another --database"
                )
            print(
                f"Skipping {target}: schema and {actual:,} rows already match",
                flush=True,
            )
            continue
        staging = (
            f"{database}.{identifier('_tpch_load_' + table + '_' + uuid.uuid4().hex)}"
        )
        columns = ", ".join(
            f"{identifier(name)} {kind}" for name, kind in metadata["columns"]
        )
        order = ", ".join(identifier(key) for key in keys)
        print(f"Loading {target}: {expected:,} rows", flush=True)
        clickhouse.query(
            f"CREATE TABLE {staging} ({columns}) ENGINE = MergeTree ORDER BY ({order})"
        )
        try:
            clickhouse.query(
                f"INSERT INTO {staging} FORMAT Parquet",
                args.data_dir / f"{table}.parquet",
            )
            actual = int(clickhouse.query(f"SELECT count() FROM {staging}"))
            if actual != expected:
                raise ValueError(f"{target}: loaded {actual} rows, expected {expected}")
            clickhouse.query(f"RENAME TABLE {staging} TO {target}")
        finally:
            # Only remove this invocation's staging table; never truncate or replace an existing target.
            try:
                clickhouse.query(f"DROP TABLE IF EXISTS {staging}")
            except Exception as error:
                print(f"Could not clean up {staging}: {error}", file=sys.stderr)
        print(f"Verified {target}: {actual:,} rows", flush=True)


def main():
    args = arguments()
    started = time.monotonic()
    args.data_dir.mkdir(parents=True, exist_ok=True)
    clickhouse = None
    if not args.generate_only:
        clickhouse = ClickHouse(args)
        clickhouse.query("SELECT 1")
    if args.load_only:
        manifest = json.loads((args.data_dir / "manifest.json").read_text())
        if manifest["scale_factor"] != args.scale_factor:
            raise ValueError("Manifest scale factor does not match --scale-factor")
    else:
        manifest = generate(args)
    if not args.generate_only:
        load(args, clickhouse, manifest)
        print(f"ClickHouse dataset ready: {args.database}", flush=True)
    print(
        f"Data retained in {args.data_dir}; finished in {time.monotonic() - started:.1f}s",
        flush=True,
    )


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, http.client.HTTPException) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
