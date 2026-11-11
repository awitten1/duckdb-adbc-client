#!/usr/bin/env python3
"""A/B the conversion benchmark: alternate two binaries and compare per-fixture medians.

Usage (pinned, inside the DuckDB slice):
  taskset -c 10 python3 bench/ab_compare.py BEFORE_BINARY AFTER_BINARY [ROUNDS] [OUT_JSON]
Each run verifies every converted value before timing and exits non-zero on a mismatch.
"""
import json
import statistics
import subprocess
import sys

before, after = sys.argv[1], sys.argv[2]
rounds = int(sys.argv[3]) if len(sys.argv) > 3 else 2
out_path = sys.argv[4] if len(sys.argv) > 4 else None
results = {"before": {}, "after": {}}


def run(binary):
    command = [binary, "--benchmark_repetitions=3", "--benchmark_report_aggregates_only=true",
               "--benchmark_format=json"]
    output = subprocess.run(command, capture_output=True, text=True)
    if output.returncode != 0:
        sys.exit(f"{binary} failed:\n{output.stderr[-2000:]}")
    for bench in json.loads(output.stdout)["benchmarks"]:
        if bench["name"].endswith("_median"):
            name = bench["name"][len("ArrowToDuckDB/"):-len("_median")]
            yield name, bench["ns_per_row"] * 1e9


for round_index in range(rounds):
    for label, binary in (("before", before), ("after", after)):
        for name, ns in run(binary):
            results[label].setdefault(name, []).append(ns)
        print(f"round {round_index + 1}: {label} done", file=sys.stderr, flush=True)

print(f"{'fixture':<22}{'before ns/row':>14}{'after ns/row':>14}{'speedup':>9}  runs before / after")
for name in results["before"]:
    b, a = statistics.median(results["before"][name]), statistics.median(results["after"][name])
    spread = (f"{min(results['before'][name]):.2f}-{max(results['before'][name]):.2f} / "
              f"{min(results['after'][name]):.2f}-{max(results['after'][name]):.2f}")
    print(f"{name:<22}{b:>14.2f}{a:>14.2f}{b / a:>8.2f}x  {spread}")
if out_path:
    with open(out_path, "w") as handle:
        json.dump(results, handle, indent=2)
