#!/bin/bash
# Read a ClickHouse table through ADBC as N concurrent key-range streams, each in
# its own DuckDB process, and compare wall time with a single stream. Tests
# whether parallel Arrow streams lower the transfer floor seen in the reports.
#
# Usage:   ./run.sh [STREAM_COUNT ...]           (default: 1 2 4)
# Example: bash scripts/run_duckdb_cgroup.sh ./run.sh 1 2 4
#          (run inside the DuckDB slice for numbers comparable to the reports)
#
# Environment overrides: PROFILE DATABASE TABLE KEY THREADS MEMORY_LIMIT
#                        REPEATS WARMUP CH_URL OUT

set -euo pipefail
cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"  # repository root

PROFILE=${PROFILE:-profile://myclickhouse}
DATABASE=${DATABASE:-tpch_sf50}
TABLE=${TABLE:-orders}
KEY=${KEY:-o_orderkey}            # integer column used to split the table
THREADS=${THREADS:-16}            # DuckDB threads per process
MEMORY_LIMIT=${MEMORY_LIMIT:-56GiB}
# ClickHouse max_threads per query. Unset: server default. A number: used as is.
# "split": floor(ConcurrencyControlSoftLimit / streams), so concurrent queries
# share ClickHouse's CPU slots evenly instead of the first ones taking most of them.
CH_MAX_THREADS=${CH_MAX_THREADS:-}
REPEATS=${REPEATS:-3}
WARMUP=${WARMUP:-1}               # discarded single-stream runs before measuring
CH_URL=${CH_URL:-http://127.0.0.1:8123/}
OUT=${OUT:-build/experiments/parallel-read-$(date +%Y%m%dT%H%M%S)}
DUCKDB=./build/release/duckdb
EXTENSION=$PWD/build/release/extension/adbc/adbc.duckdb_extension

[[ $# -gt 0 ]] || set -- 1 2 4
mkdir -p "$OUT"

ch() { curl -fsS "$CH_URL" --data-binary "$1"; }
read -r LO HI ROWS < <(ch "SELECT min($KEY), max($KEY), count() FROM $DATABASE.$TABLE FORMAT TSV") || true
[[ -n ${ROWS:-} ]] || { echo "could not read bounds of $DATABASE.$TABLE from $CH_URL" >&2; exit 1; }
echo "$DATABASE.$TABLE: $ROWS rows, $KEY in [$LO, $HI]; results in $OUT" >&2
SLOTS=$(ch "SELECT value FROM system.metrics WHERE metric = 'ConcurrencyControlSoftLimit' FORMAT TSV")
echo "ClickHouse CPU slot soft limit: ${SLOTS:-unknown}; max_threads per query: ${CH_MAX_THREADS:-server default}" >&2

# SETTINGS clause for one of n concurrent queries.
ch_settings() {  # n
    case $CH_MAX_THREADS in
        "") ;;
        split) echo " SETTINGS max_threads=$(( SLOTS / $1 > 0 ? SLOTS / $1 : 1 ))" ;;
        *) echo " SETTINGS max_threads=$CH_MAX_THREADS" ;;
    esac
}

# Write one stream's SQL: contiguous, disjoint key ranges that cover [LO, HI].
stream_sql() {  # n i -> path
    local n=$1 i=$2 width=$((HI - LO + 1))
    local start=$((LO + width * i / n)) end=$((LO + width * (i + 1) / n - 1))
    local path=$OUT/n$n-s$i.sql
    cat > "$path" <<EOF
LOAD '$EXTENSION';
SET threads=$THREADS;
SET memory_limit='$MEMORY_LIMIT';
EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM read_adbc('$PROFILE', 'SELECT * FROM $DATABASE.$TABLE WHERE $KEY BETWEEN $start AND $end$(ch_settings "$n")');
EOF
    echo "$path"
}

# Run n concurrent streams; append one TSV line per stream to $OUT/results.tsv.
run_batch() {  # n tag
    local n=$1 tag=$2 pids=() i
    local started ended
    started=$(date +%s.%N)
    for ((i = 0; i < n; i++)); do
        "$DUCKDB" -unsigned -batch -bail -json -no-init \
            < "$(stream_sql "$n" "$i")" > "$OUT/$tag-s$i.json" 2> "$OUT/$tag-s$i.stderr" &
        pids+=($!)
    done
    local failed=0
    for i in "${!pids[@]}"; do
        wait "${pids[$i]}" || { echo "stream $i of $tag failed: $(head -c 300 "$OUT/$tag-s$i.stderr")" >&2; failed=1; }
    done
    ended=$(date +%s.%N)
    ((failed == 0)) || return 1
    python3 - "$OUT" "$tag" "$n" "$started" "$ended" <<'PY'
import json, sys
out, tag, n, started, ended = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5])


def operators(node):  # same row count rule as scripts/experiments/run_cases.py
    for child in node.get("operator", []) + node.get("children", []):
        yield child
        yield from operators(child)


with open(f"{out}/results.tsv", "a") as results:
    for i in range(n):
        profile = json.load(open(f"{out}/{tag}-s{i}.json"))
        rows = max((node.get("intermediate_rows", 0) for node in operators(profile)), default=0)
        results.write(f"{tag}\t{n}\t{i}\t{ended - started:.3f}\t{profile['query']['total_time']:.3f}\t{rows}\n")
PY
}

printf "tag\tstreams\tstream\tbatch_wall_s\tstream_query_s\trows\n" > "$OUT/results.tsv"
for ((w = 0; w < WARMUP; w++)); do
    run_batch 1 "warmup$w"
done
for ((r = 1; r <= REPEATS; r++)); do
    # Alternate the order of stream counts between repeats.
    counts=("$@")
    if ((r % 2 == 0)); then
        mapfile -t counts < <(printf '%s\n' "$@" | tac)
    fi
    for n in "${counts[@]}"; do
        run_batch "$n" "n$n-r$r"
        echo "done: $n streams, repeat $r" >&2
    done
done

python3 - "$OUT/results.tsv" "$ROWS" <<'PY'
import csv, statistics, sys
from collections import defaultdict
path, expected = sys.argv[1], int(sys.argv[2])
batches = defaultdict(dict)
for row in csv.DictReader(open(path), delimiter="\t"):
    if row["tag"].startswith("warmup"):
        continue
    batch = batches[(int(row["streams"]), row["tag"])]
    batch["wall"] = float(row["batch_wall_s"])
    batch.setdefault("query", []).append(float(row["stream_query_s"]))
    batch["rows"] = batch.get("rows", 0) + int(row["rows"])
by_n = defaultdict(list)
for (n, _), batch in batches.items():
    if batch["rows"] != expected:
        print(f"WARNING: {n} streams read {batch['rows']} rows, expected {expected}", file=sys.stderr)
    by_n[n].append(batch)
base = statistics.median(b["wall"] for b in by_n[min(by_n)])
print("streams  median_wall_s  min-max_wall_s   slowest_stream_query_s  speedup")
for n in sorted(by_n):
    walls = [b["wall"] for b in by_n[n]]
    slowest = statistics.median(max(b["query"]) for b in by_n[n])
    print(f"{n:>7}  {statistics.median(walls):>13.3f}  {min(walls):>6.3f}-{max(walls):<7.3f}  "
          f"{slowest:>22.3f}  {base / statistics.median(walls):>6.2f}x")
PY
