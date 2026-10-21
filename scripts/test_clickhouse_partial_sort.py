#!/usr/bin/env python3
"""ClickHouse integration tests. All setup, metadata and data access use ADBC."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


def literal(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def identifier(value):
    return '"' + value.replace("\\", "\\\\").replace('"', '""') + '"'


def sql_list(values):
    return (
        "["
        + ",".join(
            "true" if v is True else "false" if v is False else literal(v)
            for v in values
        )
        + "]"
    )


class Harness:
    def __init__(self, duckdb, extension, profile, database):
        self.duckdb = str(Path(duckdb).resolve())
        self.extension = str(Path(extension).resolve())
        self.profile = profile
        self.database = database

    def run(self, sql, *, timeout=180, check=True):
        result = subprocess.run(
            [self.duckdb, "-unsigned", "-batch", "-bail", "-json", "-no-agent"],
            input=f"LOAD {literal(self.extension)};\n{sql};\n",
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        if check and result.returncode:
            raise AssertionError(result.stderr)
        return result

    def rows(self, sql):
        output = self.run(sql).stdout.strip()
        decoder = json.JSONDecoder()
        rows = []
        while output:
            rows, end = decoder.raw_decode(output)
            output = output[end:].strip()
        return rows

    def execute(self, sql):
        self.run(f"CALL adbc_execute({literal(self.profile)}, {literal(sql)})")

    def read(self, sql):
        return f"read_adbc({literal(self.profile)}, {literal(sql)})"

    def source(self, table):
        return f"{identifier(self.database)}.{identifier(table)}"

    def function(
        self,
        table,
        ratio="0.5",
        keys=("score", "id"),
        descending=None,
        nulls_first=None,
        partition="id",
    ):
        options = [
            f"partition_key := {literal(partition)}",
            f"sort_columns := {sql_list(keys)}",
            f"pushdown_ratio := {ratio}",
        ]
        if descending is not None:
            options.append(f"descending := {sql_list(descending)}")
        if nulls_first is not None:
            options.append(f"nulls_first := {sql_list(nulls_first)}")
        return (
            f"read_adbc_partial_sort({literal(self.profile)}, {literal(self.database)}, {literal(table)}, "
            + ", ".join(options)
            + ")"
        )

    def create(self, table, columns, order="id", settings=""):
        self.execute(
            f"CREATE TABLE {self.source(table)} ({columns}) ENGINE = MergeTree ORDER BY {order} {settings}"
        )


class PartialSortTests(unittest.TestCase):
    harness = None

    @classmethod
    def setUpClass(cls):
        h = cls.harness
        h.execute(f"CREATE DATABASE {identifier(h.database)}")
        cls.addClassCleanup(
            h.execute, f"DROP DATABASE IF EXISTS {identifier(h.database)}"
        )
        h.create("events", "id UInt64, score Nullable(Int64), payload String")
        h.execute(
            f"INSERT INTO {h.source('events')} SELECT number, "
            "if(number % 13 = 0, NULL, toInt64(number * 17 % 101)), concat('row-', toString(number)) FROM numbers(12000)"
        )

    def assert_matches(
        self,
        table,
        *,
        ratio="0.5",
        keys=("score", "id"),
        descending=None,
        nulls_first=None,
        partition="id",
    ):
        h = self.harness
        descending = descending or [False] * len(keys)
        nulls_first = nulls_first or [False] * len(keys)
        order = ", ".join(
            f"{identifier(k)} {'DESC' if d else 'ASC'} NULLS {'FIRST' if n else 'LAST'}"
            for k, d, n in zip(keys, descending, nulls_first)
        )
        expected = h.rows(
            f"SELECT * FROM {h.read('SELECT * FROM ' + h.source(table))} ORDER BY {order}"
        )
        actual = h.rows(
            "SELECT * FROM "
            + h.function(table, ratio, keys, descending, nulls_first, partition)
        )
        self.assertEqual(expected, actual)
        return actual

    def test_ratios_and_ordering(self):
        for ratio in ("0", "0.000000001", "0.25", "0.5", "0.75", "1"):
            with self.subTest(ratio=ratio):
                self.assert_matches(
                    "events",
                    ratio=ratio,
                    descending=[True, False],
                    nulls_first=[True, False],
                )
        self.assert_matches("events", keys=("id",), descending=[True])
        self.assert_matches("events", keys=("score", "id"), descending=[False, True])

    def test_empty_null_constant_sparse_and_duplicates(self):
        h = self.harness
        h.create("empty", "id Int64, score Int64")
        for ratio in ("0", "0.5", "1"):
            self.assert_matches("empty", ratio=ratio)
        h.create(
            "nullable",
            "id Nullable(Int64), score Int64",
            settings="SETTINGS allow_nullable_key = 1",
        )
        h.execute(
            f"INSERT INTO {h.source('nullable')} VALUES (NULL,3),(NULL,1),(NULL,2)"
        )
        for ratio in ("0", "0.5", "1"):
            self.assert_matches("nullable", ratio=ratio)
        h.execute(
            f"INSERT INTO {h.source('nullable')} VALUES (-10,7),(1000,9),(1000,4),(0,6)"
        )
        self.assert_matches("nullable", nulls_first=[True, True])
        h.create("constant", "id UInt64, score Int64")
        h.execute(f"INSERT INTO {h.source('constant')} VALUES (7,3),(7,1),(7,2)")
        self.assert_matches("constant")
        h.create("duplicates", "id Int64, score Int64, payload String")
        h.execute(
            f"INSERT INTO {h.source('duplicates')} VALUES (0,1,'x'),(10,1,'x'),(10,1,'x'),(20,1,'x')"
        )
        self.assert_matches("duplicates")

    def test_integer_widths_and_full_ranges(self):
        h = self.harness
        for bits in (8, 16, 32, 64):
            for unsigned in (False, True):
                with self.subTest(bits=bits, unsigned=unsigned):
                    table = f"{'u' if unsigned else 'i'}{bits}"
                    low = 0 if unsigned else -(2 ** (bits - 1))
                    high = 2**bits - 1 if unsigned else 2 ** (bits - 1) - 1
                    h.create(
                        table,
                        f"id {'U' if unsigned else ''}Int{bits}, score Nullable(Int64)",
                    )
                    h.execute(
                        f"INSERT INTO {h.source(table)} VALUES ({low},NULL),({high},-2),(1,3)"
                    )
                    for ratio in ("0", "0.5", "0.999999999", "1"):
                        self.assert_matches(
                            table,
                            ratio=ratio,
                            keys=("id",),
                            descending=[True],
                            nulls_first=[True],
                        )

    def test_composition_and_reexecution(self):
        h = self.harness
        function = h.function("events")
        self.assertEqual(
            h.rows(f"SELECT count(*) AS n FROM {function}"), [{"n": 12000}]
        )
        self.assertEqual(len(h.rows(f"SELECT payload FROM {function} LIMIT 1")), 1)
        rows = h.rows(
            f"PREPARE p AS SELECT id, score FROM {function} LIMIT 5; EXECUTE p; EXECUTE p"
        )
        self.assertEqual(len(rows), 5)
        self.assertEqual(len(h.rows(f"SELECT * FROM {function} WHERE id < 10")), 10)

    def test_quoted_identifiers_and_composite_storage_key(self):
        h = self.harness
        table = 'table "quoted"'
        key = 'key "quoted"'
        h.create(
            table,
            f"{identifier(key)} Int64, score Int64",
            order=f"({identifier(key)}, score)",
        )
        h.execute(f"INSERT INTO {h.source(table)} VALUES (0,3),(50,2),(100,1)")
        self.assert_matches(table, partition=key, keys=("score", key))

    def test_invalid_arguments(self):
        h = self.harness
        cases = [
            (h.function("events", "-0.1"), "between 0 and 1"),
            (h.function("events", "1.1"), "between 0 and 1"),
            (h.function("events", "NULL"), "requires pushdown_ratio"),
            (h.function("events", keys=()), "must not be empty"),
            (h.function("events", descending=[True]), "must match"),
            (h.function("events", partition="score"), "simple leading column"),
            (h.function("events", keys=("payload",)), "must be an integer"),
            (h.function("events", keys=("missing",)), "does not exist"),
        ]
        for function, message in cases:
            with self.subTest(message=message):
                result = h.run("SELECT * FROM " + function, check=False)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)
        self.assertEqual(
            len(h.rows("SELECT * FROM " + h.function("events") + " LIMIT 1")), 1
        )

    def test_branch_errors_and_schema_change(self):
        h = self.harness
        for bad_id in (10, 90):
            table = f"failure_{bad_id}"
            h.create(table, "id UInt64")
            h.execute(f"INSERT INTO {h.source(table)} SELECT number FROM numbers(100)")
            # Existing parts lack this column, so its default is evaluated while reading.
            h.execute(
                f"ALTER TABLE {h.source(table)} ADD COLUMN score Int64 "
                f"DEFAULT intDiv(1, toInt64(id) - {bad_id})"
            )
            result = h.run(
                "SELECT * FROM " + h.function(table), check=False, timeout=60
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("division", result.stderr.lower())
            if bad_id == 90:
                self.assertIn("remote branch", result.stderr)

        h.create("changing", "id UInt64, score Int64")
        h.execute(f"INSERT INTO {h.source('changing')} VALUES (0,1),(100,2)")
        alter = f"ALTER TABLE {h.source('changing')} MODIFY COLUMN score UInt32"
        result = h.run(
            f"PREPARE changed AS SELECT * FROM {h.function('changing')}; "
            f"CALL adbc_execute({literal(h.profile)}, {literal(alter)}); EXECUTE changed",
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("schema changed", result.stderr)

    def test_profile_overlap_queue_and_spill(self):
        h = self.harness
        h.create("large", "id UInt64, score Int64, payload String")
        h.execute(
            f"INSERT INTO {h.source('large')} SELECT number, toInt64(2000000-number), repeat('x',128) FROM numbers(2000000)"
        )
        with tempfile.TemporaryDirectory() as temporary:
            profile = Path(temporary) / "profile.json"
            for force_external in (False, True):
                with self.subTest(force_external=force_external):
                    query = (
                        "SET memory_limit='64MB'; SET threads=1; "
                        f"SET debug_force_external={'true' if force_external else 'false'}; "
                        f"PRAGMA enable_profiling='json'; SET profiling_output={literal(profile)}; "
                        f"COPY (SELECT * FROM {h.function('large')}) TO {literal(os.devnull)} (FORMAT CSV)"
                    )
                    h.run(query)
                    data = json.loads(profile.read_text())
                    extra = find_partial_sort_metrics(data)
                    self.assertIsNotNone(extra)
                    self.assertEqual(int(extra["Local Rows"]), 1000000)
                    self.assertEqual(int(extra["Remote Rows"]), 1000000)
                    self.assertLessEqual(int(extra["Queue High Watermark"]), 2)
                    self.assertGreater(float(extra["Branch Overlap Seconds"]), 0)
                    self.assertGreater(system_metric(data, "peak_temp_dir_size"), 0)

        # Verify server-side overlap too, rather than only worker lifetimes.
        h.execute("SYSTEM FLUSH LOGS")
        prefix = f'SELECT "id", "score", "payload" FROM {h.source("large")} WHERE'
        intervals = h.rows(
            "SELECT * FROM "
            + h.read(
                "SELECT position(query, 'ORDER BY') > 0 AS remote, "
                "toUnixTimestamp64Micro(query_start_time_microseconds) AS start_us, "
                "query_duration_ms * 1000 AS duration_us FROM system.query_log "
                f"WHERE type = 'QueryFinish' AND startsWith(query, {literal(prefix)})"
            )
        )
        local = [row for row in intervals if not row["remote"]]
        remote = [row for row in intervals if row["remote"]]
        self.assertTrue(
            any(
                max(int(left["start_us"]), int(right["start_us"]))
                < min(
                    int(left["start_us"]) + int(left["duration_us"]),
                    int(right["start_us"]) + int(right["duration_us"]),
                )
                for left in local
                for right in remote
            ),
            intervals,
        )

    def test_range_pruning_over_adbc(self):
        h = self.harness
        query = f"EXPLAIN indexes = 1 SELECT * FROM {h.source('events')} WHERE id >= 9000 ORDER BY id"
        rows = h.rows("SELECT * FROM " + h.read(query))
        explanation = "\n".join(str(value) for row in rows for value in row.values())
        self.assertIn("PrimaryKey", explanation)
        self.assertIn("9000", explanation)


def find_partial_sort_metrics(node):
    extra = node.get("extra_info", {})
    if "Local Rows" in extra:
        return extra
    for child in node.get("children", []) + node.get("operator", []):
        found = find_partial_sort_metrics(child)
        if found:
            return found
    return None


def system_metric(profile, name):
    # The pinned v2.0 revision groups metrics; older JSON profiles use flat keys.
    return profile.get("system", {}).get(name, profile.get("system_" + name, 0))


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", default="build/release/duckdb")
    parser.add_argument(
        "--extension", default="build/release/extension/adbc/adbc.duckdb_extension"
    )
    parser.add_argument(
        "--profile",
        help="Existing ADBC profile URI; otherwise create a temporary ClickHouse profile",
    )
    parser.add_argument(
        "--url", default=os.environ.get("ADBC_CLICKHOUSE_URL", "http://localhost:8123/")
    )
    parser.add_argument(
        "--test", action="append", help="Run only this test method (repeatable)"
    )
    return parser.parse_args()


def main():
    args = arguments()
    with tempfile.TemporaryDirectory() as temporary:
        profile = args.profile
        if not profile:
            path = Path(temporary) / "clickhouse.toml"
            path.write_text(
                f'profile_version = 1\ndriver = "clickhouse"\n[Options]\nuri = {json.dumps(args.url)}\n'
            )
            path.chmod(0o600)
            profile = "profile://" + str(path)
        PartialSortTests.harness = Harness(
            args.duckdb,
            args.extension,
            profile,
            f"adbc_partial_sort_test_{os.getpid()}",
        )
        suite = (
            unittest.TestSuite(PartialSortTests(name) for name in args.test)
            if args.test
            else unittest.defaultTestLoader.loadTestsFromTestCase(PartialSortTests)
        )
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        raise SystemExit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
