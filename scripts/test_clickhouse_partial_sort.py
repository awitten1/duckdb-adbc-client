#!/usr/bin/env python3
"""ClickHouse integration tests. All setup, metadata and data access use ADBC."""

import argparse
import csv
import filecmp
import json
import os
from pathlib import Path
import subprocess
import signal
import tempfile
import time
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
    def __init__(self, duckdb, extension, profile, database, partitioned=False):
        self.duckdb = str(Path(duckdb).resolve())
        self.extension = str(Path(extension).resolve())
        self.profile = profile
        self.database = database
        self.partitioned = partitioned

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
        partition_count=None,
        max_remote_concurrency=None,
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
        if partition_count is not None:
            options.append(f"partition_count := {partition_count}")
        if max_remote_concurrency is not None:
            options.append(f"max_remote_concurrency := {max_remote_concurrency}")
        name = (
            "read_adbc_partitioned_sort"
            if self.partitioned
            else "read_adbc_partial_sort"
        )
        return (
            f"{name}({literal(self.profile)}, {literal(self.database)}, {literal(table)}, "
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

    def test_local_sort_thread_settings(self):
        h = self.harness
        h.create(
            "threaded",
            "id UInt64, score Nullable(Int64), payload String, items Array(Int32)",
        )
        h.execute(
            f"INSERT INTO {h.source('threaded')} SELECT number, "
            "if(number % 17 = 0, NULL, toInt64(number * 7919 % 1009)), "
            "concat(toString(number), repeat('x', number % 257)), "
            "[toInt32(number), -toInt32(number)] FROM numbers(250003)"
        )
        with tempfile.TemporaryDirectory() as temporary:
            expected = Path(temporary) / "expected.csv"
            h.run(
                "SET threads=1; COPY (SELECT * FROM "
                + h.read(f"SELECT * FROM {h.source('threaded')}")
                + f" ORDER BY score DESC NULLS FIRST, id) TO {literal(expected)} (FORMAT CSV)"
            )
            for threads in (1, 4):
                for ratio in ("0", "0.5", "1"):
                    with self.subTest(threads=threads, ratio=ratio):
                        actual = Path(temporary) / f"actual-{threads}-{ratio}.csv"
                        profile = Path(temporary) / "profile.json"
                        function = h.function(
                            "threaded",
                            ratio,
                            descending=[True, False],
                            nulls_first=[True, False],
                        )
                        h.run(
                            f"SET threads={threads}; SET memory_limit='64MB'; "
                            f"SET debug_force_external={'true' if threads > 1 else 'false'}; "
                            f"PRAGMA enable_profiling='json'; SET profiling_output={literal(profile)}; "
                            f"COPY (SELECT * FROM {function}) TO {literal(actual)} (FORMAT CSV)"
                        )
                        self.assertTrue(filecmp.cmp(expected, actual, shallow=False))
                        metrics = json.loads(profile.read_text())
                        extra = find_partial_sort_metrics(metrics)
                        if not h.partitioned:
                            workers = int(extra["Local Sort Workers"])
                            active = int(extra["Peak Active Local Sort Tasks"])
                            if ratio == "1":
                                self.assertEqual((workers, active), (0, 0))
                            elif threads == 1:
                                self.assertEqual((workers, active), (1, 1))
                            else:
                                self.assertEqual(workers, threads)
                                self.assertGreater(active, 1)
                                self.assertLessEqual(active, threads)
                        if not h.partitioned and threads > 1 and ratio == "0":
                            self.assertGreater(
                                system_metric(metrics, "peak_temp_dir_size"), 0
                            )

        if not h.partitioned:
            function = h.function("threaded", "0")
            result = h.run(
                f"SET threads=4; SET memory_limit='1MB'; SELECT count(*) FROM {function}",
                check=False,
                timeout=30,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("out of memory", result.stderr.lower())
            result = h.run(
                f"PREPARE interrupted AS SELECT count(*) FROM {function}; "
                "SET threads=4; SET max_execution_time=25; EXECUTE interrupted",
                check=False,
                timeout=30,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("maximum execution time", result.stderr.lower())
            self.assertEqual(
                h.rows(f"SET threads=4; SELECT count(*) AS n FROM {function}"),
                [{"n": 250003}],
            )

    def test_read_adbc_full_remote_sort(self):
        h = self.harness
        query = (
            "SELECT toInt64(number) AS id, toInt64(1000000-number) AS score, "
            "repeat('x',128) AS payload FROM numbers(1000000) ORDER BY score ASC NULLS LAST"
        )
        source = h.read(query)
        # Fully consume a result large enough to keep the HTTP session active.
        h.run(f"COPY (SELECT * FROM {source}) TO {literal(os.devnull)} (FORMAT CSV)")
        self.assertEqual(
            h.rows(f"SELECT id, score FROM {source} LIMIT 2"),
            [{"id": 999999, "score": 1}, {"id": 999998, "score": 2}],
        )
        self.assertEqual(
            h.rows(
                f"PREPARE full_sort AS SELECT count(*) AS n, sum(id)::BIGINT AS total FROM {source}; "
                "EXECUTE full_sort; EXECUTE full_sort"
            ),
            [{"n": 1000000, "total": 499999500000}],
        )
        self.assertEqual(
            h.rows(
                f"PREPARE preview AS SELECT id, score FROM {source} LIMIT 2; "
                "EXECUTE preview; EXECUTE preview"
            ),
            [{"id": 999999, "score": 1}, {"id": 999998, "score": 2}],
        )

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

    def test_partition_key_without_physical_ordering(self):
        # A nonleading integer column remains a valid range partition.
        self.assert_matches("events", partition="score", descending=[True, False])
        h = self.harness
        h.create("unordered", "id Int64, score Int64", order="tuple()")
        h.execute(f"INSERT INTO {h.source('unordered')} VALUES (100,1),(0,3),(50,2)")
        for ratio in ("0", "0.5", "1"):
            self.assert_matches("unordered", ratio=ratio)

    def test_invalid_arguments(self):
        h = self.harness
        cases = [
            (h.function("events", "-0.1"), "between 0 and 1"),
            (h.function("events", "1.1"), "between 0 and 1"),
            (h.function("events", "NULL"), "requires pushdown_ratio"),
            (h.function("events", keys=()), "must not be empty"),
            (h.function("events", descending=[True]), "must match"),
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
            # Bind with a valid default, then inject a read error without changing
            # the schema. Schema discovery can itself read a row on some drivers.
            h.execute(f"ALTER TABLE {h.source(table)} ADD COLUMN score Int64 DEFAULT 1")
            alter = (
                f"ALTER TABLE {h.source(table)} MODIFY COLUMN score Int64 "
                f"DEFAULT intDiv(1, toInt64(id) - {bad_id})"
            )
            result = h.run(
                f"PREPARE failing AS SELECT * FROM {h.function(table)}; "
                f"CALL adbc_execute({literal(h.profile)}, {literal(alter)}); EXECUTE failing",
                check=False,
                timeout=60,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("division", result.stderr.lower())
            if bad_id == 90:
                self.assertIn(
                    "remote partition" if h.partitioned else "remote branch",
                    result.stderr,
                )

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
            for threads, force_external in (
                (1, False),
                (1, True),
                (4, False),
                (4, True),
            ):
                with self.subTest(threads=threads, force_external=force_external):
                    # Native DuckDB sorting also needs more than 64MB for four
                    # workers on this payload; retain the 64MB serial coverage.
                    memory_limit = "64MB" if threads == 1 else "128MB"
                    query = (
                        f"SET memory_limit='{memory_limit}'; SET threads={threads}; "
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


class PartitionedSortTests(PartialSortTests):
    def test_profile_overlap_queue_and_spill(self):
        h = self.harness
        h.create("large", "id UInt64, score Int64, payload String")
        h.execute(
            f"INSERT INTO {h.source('large')} SELECT number, toInt64(1000000-number), repeat('x',128) FROM numbers(1000000)"
        )
        with tempfile.TemporaryDirectory() as temporary:
            profile = Path(temporary) / "profile.json"
            output = Path(temporary) / "sorted.csv"
            for concurrency, force_external in (
                (1, False),
                (2, False),
                (4, False),
                (4, True),
            ):
                with self.subTest(
                    concurrency=concurrency, force_external=force_external
                ):
                    function = h.function(
                        "large", partition_count=40, max_remote_concurrency=concurrency
                    )
                    destination = output if concurrency == 1 else os.devnull
                    h.run(
                        "SET memory_limit='64MB'; SET threads=1; "
                        f"SET debug_force_external={'true' if force_external else 'false'}; "
                        f"PRAGMA enable_profiling='json'; SET profiling_output={literal(profile)}; "
                        f"COPY (SELECT * FROM {function}) TO {literal(destination)} (FORMAT CSV, HEADER false)"
                    )
                    data = json.loads(profile.read_text())
                    extra = find_partial_sort_metrics(data)
                    self.assertEqual(int(extra["Generated Partitions"]), 40)
                    self.assertEqual(int(extra["Local Partitions"]), 20)
                    self.assertEqual(int(extra["Remote Partitions"]), 20)
                    self.assertEqual(int(extra["Local Rows"]), 500000)
                    self.assertEqual(int(extra["Remote Rows"]), 500000)
                    self.assertEqual(
                        int(extra["Peak Active Remote Queries"]), concurrency
                    )
                    self.assertLessEqual(int(extra["Queue High Watermark"]), 2)
                    self.assertEqual(int(extra["Intermediate Merge Passes"]), 1)
                    self.assertGreater(system_metric(data, "peak_temp_dir_size"), 0)
                    if concurrency == 1:
                        count = 0
                        with output.open(newline="") as stream:
                            for count, row in enumerate(csv.reader(stream), 1):
                                self.assertEqual(
                                    (int(row[0]), int(row[1]), row[2]),
                                    (1000000 - count, count, "x" * 128),
                                )
                        self.assertEqual(count, 1000000)

        h.execute("SYSTEM FLUSH LOGS")
        prefix = f'SELECT "id", "score", "payload" FROM {h.source("large")} WHERE'
        intervals = h.rows(
            "SELECT * FROM "
            + h.read(
                "SELECT toUnixTimestamp64Micro(query_start_time_microseconds) AS start_us, "
                "query_duration_ms * 1000 AS duration_us FROM system.query_log "
                f"WHERE type = 'QueryFinish' AND startsWith(query, {literal(prefix)}) "
                "AND position(query, 'ORDER BY') > 0"
            )
        )
        self.assertEqual(len(intervals), 80)
        events = []
        for row in intervals:
            start = int(row["start_us"])
            events.extend(((start, 1), (start + int(row["duration_us"]), -1)))
        active = peak = 0
        for _, change in sorted(events):
            active += change
            peak = max(peak, active)
        self.assertGreater(peak, 1)
        self.assertLessEqual(peak, 4)

    def test_partition_counts_and_multiple_merge_passes(self):
        h = self.harness
        h.create("small_domain", "id Int64, score Int64")
        h.execute(f"INSERT INTO {h.source('small_domain')} VALUES (-1,3),(0,2),(1,1)")
        with tempfile.TemporaryDirectory() as temporary:
            profile = Path(temporary) / "profile.json"
            for table, count, ratio, expected in (
                ("small_domain", 20, "0.3", (3, 0, 0)),
                ("small_domain", 20, "1", (3, 3, 0)),
                ("events", 300, "0.3", (300, 90, 2)),
                ("events", 1, "0.5", (1, 0, 0)),
            ):
                with self.subTest(table=table, count=count, ratio=ratio):
                    function = h.function(
                        table, ratio, partition_count=count, max_remote_concurrency=2
                    )
                    h.run(
                        f"PRAGMA enable_profiling='json'; SET profiling_output={literal(profile)}; "
                        f"COPY (SELECT * FROM {function}) TO {literal(os.devnull)} (FORMAT CSV)"
                    )
                    extra = find_partial_sort_metrics(json.loads(profile.read_text()))
                    self.assertEqual(
                        tuple(
                            int(extra[key])
                            for key in (
                                "Generated Partitions",
                                "Remote Partitions",
                                "Intermediate Merge Passes",
                            )
                        ),
                        expected,
                    )
                    self.assertEqual(
                        int(extra["Local Rows"]) + int(extra["Remote Rows"]),
                        12000 if table == "events" else 3,
                    )
        self.assert_matches("small_domain", nulls_first=[True, False])

    def test_partition_options(self):
        h = self.harness
        for option in ("partition_count", "max_remote_concurrency"):
            for value in ("0", "-1", "NULL"):
                result = h.run(
                    "SELECT * FROM " + h.function("events", **{option: value}),
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("positive integer", result.stderr)

    def test_interrupt_active_workers(self):
        h = self.harness
        h.create("cancel", "id UInt64, score Int64")
        h.execute(
            f"INSERT INTO {h.source('cancel')} SELECT number, toInt64(1000000-number) FROM numbers(1000000)"
        )
        # Delay actual range reads so the interrupt reaches active workers.
        h.execute(
            f"ALTER TABLE {h.source('cancel')} ADD COLUMN payload String "
            "DEFAULT concat(repeat('x',128), toString(sleepEachRow(0.00001)))"
        )
        function = h.function("cancel", "1")
        process = subprocess.Popen(
            [h.duckdb, "-unsigned", "-batch", "-bail", "-no-agent"],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            process.stdin.write(
                f"LOAD {literal(h.extension)}; "
                f"COPY (SELECT * FROM {function}) TO {literal(os.devnull)} (FORMAT CSV);\n"
            )
            process.stdin.close()
            process.stdin = None
            prefix = f'SELECT "id", "score", "payload" FROM {h.source("cancel")} WHERE'
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                rows = h.rows(
                    "SELECT * FROM "
                    + h.read(
                        f"SELECT count(*) AS n FROM system.processes WHERE startsWith(query, {literal(prefix)}) "
                        "AND position(query, 'ORDER BY') > 0"
                    )
                )
                if int(rows[0]["n"]) > 0:
                    break
                self.assertIsNone(
                    process.poll(), "Query finished before the interruption check"
                )
                time.sleep(0.05)
            else:
                self.fail("No active remote partition query observed")
            process.send_signal(signal.SIGINT)
            process.communicate(timeout=30)
            # The shell suppresses printing once interrupted. A normal error
            # exit (rather than termination by SIGINT) verifies its cleanup path.
            self.assertEqual(process.returncode, 1)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()
        self.assertEqual(
            h.rows(f"SELECT count(*) AS n FROM {h.function('events')}"), [{"n": 12000}]
        )


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
        "--mode", choices=("partial", "partitioned", "both"), default="both"
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
        suite = unittest.TestSuite()
        for test_class, partitioned in (
            (PartialSortTests, False),
            (PartitionedSortTests, True),
        ):
            mode = "partitioned" if partitioned else "partial"
            if args.mode not in (mode, "both"):
                continue
            test_class.harness = Harness(
                args.duckdb,
                args.extension,
                profile,
                f"adbc_partial_sort_test_{os.getpid()}_{mode}",
                partitioned=partitioned,
            )
            names = args.test or unittest.defaultTestLoader.getTestCaseNames(test_class)
            # This generic read_adbc regression only needs to run once.
            if partitioned and not args.test:
                names = [
                    name for name in names if name != "test_read_adbc_full_remote_sort"
                ]
            suite.addTests(test_class(name) for name in names)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        raise SystemExit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
