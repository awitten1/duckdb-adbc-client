#!/usr/bin/env bash
# Parallel read_adbc key-range streams (o_orderkey BETWEEN lo AND hi) against the modified ClickHouse, with each
# stream unsorted, sorted by the key, or sorted by o_custkey. DuckDB in its 16-CPU slice on the other socket,
# reader thread on, ClickHouse max_threads per query = floor(32 / N).
set -euo pipefail
cd /users/awitt/duckdb-adbc-client
E=build/experiments/modified-clickhouse-20261110
OUT=$E/sorted-streams
mkdir -p $OUT
Q() { curl -fsS http://127.0.0.1:8123/ --data-binary "$1"; }
$E/start_modified_clickhouse.sh start >/dev/null
trap '$E/start_modified_clickhouse.sh stop >/dev/null; echo "restored $(Q "SELECT version()")"' EXIT
echo "server $(Q 'SELECT version()')"
for sort in none o_orderkey o_custkey; do
    started=$(date +%s)
    order=()
    [[ $sort != none ]] && order=(--order-by=$sort)
    echo "=== ORDER BY per stream: $sort"
    scripts/run_duckdb_cgroup.sh env ADBC_READER_THREAD=1 build/release/extension/adbc/bench/parallel_read_adbc \
        --ch-max-threads=split "${order[@]}" --repeats=2 1 2 3 4 6 8 10 > $OUT/$sort.tsv
    Q "SYSTEM FLUSH LOGS"
    echo "--- ClickHouse view (each N is one max_threads value; per-query times are across all streams of all repeats)"
    Q "SELECT
           intDiv(32, toUInt64(extract(query, 'max_threads=(\\d+)'))) AS streams,
           count() AS queries,
           round(median(query_duration_ms) / 1e3, 2) AS median_query_s,
           round(max(query_duration_ms) / 1e3, 2) AS slowest_query_s,
           round(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6 / (count() / streams), 1) AS server_cpu_s_per_batch,
           round(median(ProfileEvents['NetworkSendElapsedMicroseconds']) / 1e6, 2) AS median_send_blocked_s,
           formatReadableSize(median(memory_usage)) AS median_peak_mem
       FROM system.query_log
       WHERE type = 'QueryFinish' AND event_time >= toDateTime($started)
         AND query LIKE 'SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN%SETTINGS max_threads=%'
       GROUP BY streams ORDER BY streams
       FORMAT PrettyCompactMonoBlock"
done
