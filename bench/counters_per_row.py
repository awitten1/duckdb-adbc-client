#!/usr/bin/env python3
"""Per-row hardware counters for the conversion loop alone.

Runs each benchmark fixture under `perf stat` at two fixed iteration counts (no
calibration passes) and divides the difference in counter totals by the difference in
rows converted. That cancels fixture generation and verification done at startup.

Usage (pinned, inside the DuckDB slice):
  taskset -c 10 python3 bench/counters_per_row.py BENCH_BINARY [FIXTURE ...]
Environment: SHORT_ITERATIONS (default 10), LONG_ITERATIONS (default 80). A larger gap
reduces noise from the startup work that the subtraction cancels.
"""
import json
import os
import subprocess
import sys

EVENTS = ["cycles:u", "instructions:u", "ld_blocks.store_forward:u"]
binary = sys.argv[1]
fixtures = sys.argv[2:] or ["int64", "decimal_15_2", "string_1", "string_6_15", "string_15", "string_19_78"]


def run(fixture, iterations):
    command = ["perf", "stat", "-x,", "-e", ",".join(EVENTS), "--", binary,
               f"--benchmark_filter=ArrowToDuckDB/{fixture}$", f"--benchmark_min_time={iterations}x",
               "--benchmark_format=json"]
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    bench = json.loads(result.stdout)["benchmarks"][0]
    counts = {}
    for line in result.stderr.splitlines():
        fields = line.split(",")
        if len(fields) > 2 and fields[2] in EVENTS and fields[0].replace(".", "").isdigit():
            counts[fields[2]] = float(fields[0])
    return bench["iterations"] * bench["rows"], counts


print(f"{'fixture':<14}{'cycles/row':>12}{'instr/row':>11}{'IPC':>6}{'store-fwd blocks/row':>22}")
for fixture in fixtures:
    rows_short, short = run(fixture, int(os.environ.get("SHORT_ITERATIONS", 10)))
    rows_long, long = run(fixture, int(os.environ.get("LONG_ITERATIONS", 80)))
    rows = rows_long - rows_short
    per = {event: (long[event] - short[event]) / rows for event in EVENTS}
    ipc = per["instructions:u"] / per["cycles:u"]
    print(f"{fixture:<14}{per['cycles:u']:>12.1f}{per['instructions:u']:>11.1f}{ipc:>6.2f}"
          f"{per['ld_blocks.store_forward:u']:>22.3f}", flush=True)
