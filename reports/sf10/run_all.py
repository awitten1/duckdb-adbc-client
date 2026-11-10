#!/usr/bin/env python3
"""Reproduce the SF5 EXPLAIN ANALYZE matrix on SF10, sequentially."""

from pathlib import Path
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[2]
ROOT = REPO / "build/local-experiments/sf10-sweep"
PROFILE = "profile://./build/local-experiments/clickhouse.toml"
RUNNER = REPO / "scripts/experiments/run_cases.py"
LAUNCHER = REPO / "scripts/local_experiments.py"


def run(tag, key="o_orderdate", projection="*", threads=4, cases=()):
    cmd = [
        sys.executable, str(LAUNCHER), "run", "--", sys.executable, str(RUNNER),
        "--root", str(ROOT), "--tag", tag, "--profile", PROFILE,
        "--database", "tpch_sf10", "--expected-rows", "15000000",
        "--partition-key", "o_orderkey", "--sort-columns", key,
        "--projection", projection, "--threads", str(threads),
        "--memory-limit", "9GiB", "--no-server-metrics", "--cases", *cases,
    ]
    started = time.monotonic()
    print(f"Starting {tag}", flush=True)
    with (ROOT / f"{tag}.log").open("w") as log:
        subprocess.run(cmd, cwd=REPO, stdout=log, stderr=subprocess.STDOUT, check=True)
    print(f"Finished {tag} in {time.monotonic() - started:.1f}s", flush=True)


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    keys = ("o_orderkey", "o_orderdate", "o_totalprice", "o_clerk")
    cases = ("partial0", "partial0.25", "partial0.5", "partial0.75", "partial1", "native", "read_sorted")
    for repeat, order, case_order in (
        (1, keys, cases),
        (2, tuple(reversed(keys)), tuple(reversed(cases))),
    ):
        for key in order:
            run(f"{key[2:]}-r{repeat}", key=key, cases=case_order)
    narrow_cases = ("partial0", "partial0.5", "partial1", "native", "read_sorted")
    run("totalprice-narrow-r1", key="o_totalprice", projection="o_orderkey,o_totalprice", cases=narrow_cases)
    run("totalprice-narrow-r2", key="o_totalprice", projection="o_orderkey,o_totalprice", cases=tuple(reversed(narrow_cases)))
    for width, projection in (("wide", "*"), ("narrow", "o_orderkey,o_totalprice")):
        for repeat in (1, 2):
            run(f"read-{width}-r{repeat}", projection=projection, cases=("read",))
    for key in ("o_orderdate", "o_totalprice"):
        run(f"{key[2:]}-t8", key=key, threads=8, cases=("native", "partial0", "partial0.5"))


if __name__ == "__main__":
    main()
