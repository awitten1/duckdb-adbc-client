#!/usr/bin/env python3
"""Reproduce the tracked SF5/SF10/SF50 report matrices on the CloudLab host.

Run the whole script inside the DuckDB slice:
  bash scripts/run_duckdb_cgroup.sh python3 build/experiments/report-repro-20261110/run_matrix.py [PHASE ...]

Differences from the workstation reports (by request): the existing 16-CPU
systemd slices, DuckDB threads=16, memory_limit=56GiB (plus 9/20 GiB SF50
memory runs), three alternating measured passes after a warmup, and the full
alpha sweep for every sort key at every scale factor.
"""

from pathlib import Path
import json
import subprocess
import sys
import time

REPO = Path("/users/awitt/duckdb-adbc-client")
ROOT = REPO / "build/experiments/report-repro-20261110"
RUNNER = REPO / "scripts/experiments/run_cases.py"
RECORD = REPO / "scripts/experiments/record_queries.py"
LIBS = Path("/users/awitt/src/adbc-clickhouse-libs")
THREADS = 16
MEMORY = "56GiB"
KEYS = ("o_orderkey", "o_orderdate", "o_totalprice", "o_clerk")
ALPHA_CASES = ("partial0", "partial0.25", "partial0.5", "partial0.75", "partial1", "native", "read_sorted")
NARROW = "o_orderkey,o_totalprice"
ROWS = {5: 7_500_000, 10: 15_000_000, 50: 75_000_000}

PROFILES = {"stock": "profile://myclickhouse"}
for variant in ("lz4", "nocompress", "schema_nocompress", "schema_nocompress_queryid"):
    path = ROOT / "profiles" / f"{variant}.toml"
    PROFILES[variant] = f"profile://{path}"


def log(message):
    line = f"{time.strftime('%H:%M:%S')} {message}"
    print(line, flush=True)
    with (ROOT / "matrix.log").open("a") as handle:
        handle.write(line + "\n")


def status(entry):
    with (ROOT / "status").open("a") as handle:
        handle.write(entry + "\n")


def run(subdir, tag, sf, key="o_orderdate", projection="*", cases=ALPHA_CASES,
        profile="stock", memory=MEMORY, threads=THREADS):
    root = ROOT / subdir
    root.mkdir(parents=True, exist_ok=True)
    if all((root / f"{tag}-{case}-summary.json").exists() for case in cases):
        log(f"skip {subdir}/{tag} (already complete)")
        return True
    cmd = [
        sys.executable, str(RUNNER), "--root", str(root), "--tag", tag,
        "--profile", PROFILES[profile], "--database", f"tpch_sf{sf}",
        "--expected-rows", str(ROWS[sf]), "--partition-key", "o_orderkey",
        "--sort-columns", key, "--projection", projection,
        "--threads", str(threads), "--memory-limit", memory, "--cases", *cases,
    ]
    started = time.monotonic()
    with (root / f"{tag}.log").open("w") as out:
        result = subprocess.run(cmd, cwd=REPO, stdout=out, stderr=subprocess.STDOUT)
    log(f"{'ok  ' if result.returncode == 0 else 'FAIL'} {subdir}/{tag} {time.monotonic() - started:.0f}s")
    return result.returncode == 0


def alpha_matrix(sf, subdir, profile, passes=3):
    """SF5/SF10 report matrix, extended to three passes."""
    run(subdir, "warm", sf, cases=("read",) + ALPHA_CASES, profile=profile)
    narrow_cases = ("partial0", "partial0.5", "partial1", "native", "read_sorted")
    for repeat in range(1, passes + 1):
        forward = repeat % 2 == 1
        keys = KEYS if forward else tuple(reversed(KEYS))
        cases = ALPHA_CASES if forward else tuple(reversed(ALPHA_CASES))
        for key in keys:
            run(subdir, f"{key[2:]}-r{repeat}", sf, key=key, cases=cases, profile=profile)
        run(subdir, f"read-wide-r{repeat}", sf, cases=("read",), profile=profile)
        run(subdir, f"totalprice-narrow-r{repeat}", sf, key="o_totalprice", projection=NARROW,
            cases=narrow_cases if forward else tuple(reversed(narrow_cases)), profile=profile)
        run(subdir, f"read-narrow-r{repeat}", sf, projection=NARROW, cases=("read",), profile=profile)


def curl_formats(sf, subdir, sql, cases_passes):
    sql_file = ROOT / f"{subdir}.sql"
    sql_file.write_text(sql + "\n")
    for suffix, cases in cases_passes:
        root = ROOT / f"{subdir}-{suffix}"
        if (root / "results.json").exists():
            log(f"skip {root.name}")
            continue
        cmd = [
            sys.executable, str(RECORD), "--root", str(root), "--no-perf", "--repeats", "3",
            "--cases", *cases, "--sql-file", str(sql_file), "--threads", str(THREADS),
            "--expected-rows", str(ROWS[sf]), "--min-network-bytes", "500000000",
            # record_queries resolves symlinks; the multi-call binary must be invoked as `client`.
            "--clickhouse-client", str(ROOT / "clickhouse-client.wrapper"),
        ]
        started = time.monotonic()
        with (ROOT / f"{root.name}.log").open("w") as out:
            result = subprocess.run(cmd, cwd=REPO, stdout=out, stderr=subprocess.STDOUT)
        log(f"{'ok  ' if result.returncode == 0 else 'FAIL'} {root.name} {time.monotonic() - started:.0f}s")


def curl_null(sf, subdir, sql, repeats=3):
    """Null-format control: server scan/sort without serialization or transfer."""
    root = ROOT / subdir
    root.mkdir(parents=True, exist_ok=True)
    out = root / "results.jsonl"
    if out.exists():
        return
    query = sql + f" SETTINGS max_threads={THREADS}"
    rows = []
    for repeat in range(-1, repeats):
        started = time.perf_counter()
        subprocess.run(
            ["curl", "-fsS", "-o", "/dev/null",
             "http://127.0.0.1:8123/?default_format=Null&enable_http_compression=0",
             "--data-binary", query],
            check=True,
        )
        rows.append({"repeat": repeat, "warmup": repeat < 0, "wall_seconds": time.perf_counter() - started})
    out.write_text("".join(json.dumps(row) + "\n" for row in rows))
    log(f"ok   {subdir}")


def phase_sf5():
    alpha_matrix(5, "sf5", "stock")


def phase_sf10():
    alpha_matrix(10, "sf10", "stock")


def phase_sf10_compression():
    subdir = "sf10-compression"
    for kind in ("lz4", "nocompress"):
        run(subdir, f"warm-{kind}", 10, cases=("read", "native"), profile=kind)
    for repeat in (1, 2, 3):
        kinds = ("lz4", "nocompress") if repeat % 2 else ("nocompress", "lz4")
        for kind in kinds:
            run(subdir, f"date-{kind}-r{repeat}", 10, key="o_orderdate",
                cases=("native", "partial0.5", "read_sorted"), profile=kind)
            run(subdir, f"price-{kind}-r{repeat}", 10, key="o_totalprice",
                cases=("read", "native", "partial0.5", "read_sorted"), profile=kind)


def phase_sf10_schema_off():
    subdir = "sf10-schema-off"
    cases = ("read", "native", "read_sorted", "partial0.5")
    profile = "schema_nocompress"
    run(subdir, "warm", 10, cases=cases, profile=profile)
    for repeat in (1, 2, 3):
        order = cases if repeat % 2 else tuple(reversed(cases))
        specs = [("date", "o_orderdate", "*"), ("price", "o_totalprice", "*"), ("price-narrow", "o_totalprice", NARROW)]
        if repeat % 2 == 0:
            specs.reverse()
        for name, key, projection in specs:
            run(subdir, f"{name}-r{repeat}", 10, key=key, projection=projection, cases=order, profile=profile)


def phase_sf10_curl():
    sql = "SELECT * FROM tpch_sf10.orders ORDER BY o_orderdate ASC NULLS LAST"
    curl_formats(10, "sf10-curl", sql, [("a", ["http_native", "http_arrow"]), ("b", ["http_arrow", "http_native"])])


def phase_sf50():
    alpha_matrix(50, "sf50", "schema_nocompress_queryid")


def phase_sf50_memory():
    subdir = "sf50-memory"
    profile = "schema_nocompress_queryid"
    cases = ("partial0", "partial0.25", "partial0.5", "partial0.75", "partial1", "native")
    for repeat in (1, 2, 3):
        memories = ("9GiB", "20GiB") if repeat % 2 else ("20GiB", "9GiB")
        for memory in memories:
            for key in ("o_orderdate", "o_totalprice"):
                run(subdir, f"{key[2:]}-m{memory[:-3]}-r{repeat}", 50, key=key,
                    cases=cases if repeat % 2 else tuple(reversed(cases)), profile=profile, memory=memory)


def phase_sf50_curl():
    sql = "SELECT * FROM tpch_sf50.orders"
    curl_formats(50, "sf50-curl", sql, [("a", ["http_native", "http_arrow"]), ("b", ["http_arrow", "http_native"])])
    curl_null(50, "sf50-curl-null", sql)


PHASES = {
    "sf5": phase_sf5,
    "sf10": phase_sf10,
    "sf10_compression": phase_sf10_compression,
    "sf10_schema_off": phase_sf10_schema_off,
    "sf10_curl": phase_sf10_curl,
    "sf50": phase_sf50,
    "sf50_memory": phase_sf50_memory,
    "sf50_curl": phase_sf50_curl,
}


def main():
    names = sys.argv[1:] or list(PHASES)
    for name in names:
        log(f"=== phase {name}")
        started = time.monotonic()
        try:
            PHASES[name]()
            status(f"{name}=0")
        except Exception as error:  # keep later phases running
            log(f"phase {name} raised {error!r}")
            status(f"{name}=1")
        log(f"=== phase {name} finished in {time.monotonic() - started:.0f}s")
    status("matrix_done")


if __name__ == "__main__":
    main()
