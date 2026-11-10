#!/usr/bin/env python3
"""TPC-H lineitem alpha / sort-key matrix on the CloudLab host.

Run inside the DuckDB slice:
  bash scripts/run_duckdb_cgroup.sh python3 build/experiments/lineitem-20261110/run_lineitem.py [PHASE ...]

Connector: adbc_clickhouse 2c374ad + schema-only ExecuteSchema, no response
compression, separate schema query IDs, and the Date/Decimal mapping fixes.
DuckDB threads=16, memory_limit=56GiB, on the existing 16-CPU slices.
"""

from pathlib import Path
import subprocess
import sys
import time

REPO = Path("/users/awitt/duckdb-adbc-client")
ROOT = REPO / "build/experiments/lineitem-20261110"
RUNNER = REPO / "scripts/experiments/run_cases.py"
PROFILE = f"profile://{REPO}/build/experiments/report-repro-20261110/profiles/schema_nocompress_queryid.toml"
THREADS = 16
MEMORY = "56GiB"
KEYS = ("l_orderkey", "l_shipdate", "l_extendedprice", "l_comment")
ALPHA_CASES = ("partial0", "partial0.25", "partial0.5", "partial0.75", "partial1", "native", "read_sorted")
NARROW = "l_orderkey,l_extendedprice"
ROWS = {5: 29_999_795, 10: 59_986_052, 25: 149_996_355}


def log(message):
    line = f"{time.strftime('%H:%M:%S')} {message}"
    print(line, flush=True)
    with (ROOT / "matrix.log").open("a") as handle:
        handle.write(line + "\n")


def status(entry):
    with (ROOT / "status").open("a") as handle:
        handle.write(entry + "\n")


def run(subdir, tag, sf, key="l_shipdate", projection="*", cases=ALPHA_CASES, memory=MEMORY):
    root = ROOT / subdir
    root.mkdir(parents=True, exist_ok=True)
    if all((root / f"{tag}-{case}-summary.json").exists() for case in cases):
        log(f"skip {subdir}/{tag} (already complete)")
        return True
    cmd = [
        sys.executable, str(RUNNER), "--root", str(root), "--tag", tag, "--profile", PROFILE,
        "--database", f"tpch_sf{sf}", "--table", "lineitem", "--expected-rows", str(ROWS[sf]),
        "--partition-key", "l_orderkey", "--sort-columns", key, "--projection", projection,
        "--threads", str(THREADS), "--memory-limit", memory, "--cases", *cases,
    ]
    started = time.monotonic()
    with (root / f"{tag}.log").open("w") as out:
        result = subprocess.run(cmd, cwd=REPO, stdout=out, stderr=subprocess.STDOUT)
    log(f"{'ok  ' if result.returncode == 0 else 'FAIL'} {subdir}/{tag} {time.monotonic() - started:.0f}s")
    return result.returncode == 0


def alpha_matrix(sf, keys=KEYS, passes=3, narrow=True):
    subdir = f"sf{sf}"
    run(subdir, "warm", sf, cases=("read",) + ALPHA_CASES)
    narrow_cases = ("partial0", "partial0.5", "partial1", "native", "read_sorted")
    for repeat in range(1, passes + 1):
        forward = repeat % 2 == 1
        order = keys if forward else tuple(reversed(keys))
        cases = ALPHA_CASES if forward else tuple(reversed(ALPHA_CASES))
        for key in order:
            run(subdir, f"{key[2:]}-r{repeat}", sf, key=key, cases=cases)
        run(subdir, f"read-wide-r{repeat}", sf, cases=("read",))
        if narrow:
            run(subdir, f"extendedprice-narrow-r{repeat}", sf, key="l_extendedprice", projection=NARROW,
                cases=narrow_cases if forward else tuple(reversed(narrow_cases)))
            run(subdir, f"read-narrow-r{repeat}", sf, projection=NARROW, cases=("read",))


PHASES = {
    "sf5": lambda: alpha_matrix(5),
    "sf10": lambda: alpha_matrix(10),
    # SF25: date and price only; a full-width string sort of 150M rows adds hours.
    "sf25": lambda: alpha_matrix(25, keys=("l_shipdate", "l_extendedprice"), narrow=False),
}


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    for name in sys.argv[1:] or list(PHASES):
        log(f"=== phase {name}")
        started = time.monotonic()
        try:
            PHASES[name]()
            status(f"{name}=0")
        except Exception as error:
            log(f"phase {name} raised {error!r}")
            status(f"{name}=1")
        log(f"=== phase {name} finished in {time.monotonic() - started:.0f}s")
    status("matrix_done")


if __name__ == "__main__":
    main()
