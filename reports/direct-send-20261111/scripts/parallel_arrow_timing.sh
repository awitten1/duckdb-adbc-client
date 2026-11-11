#!/usr/bin/env bash
# Timing of parallel ArrowStream formatting (prototype a5be5f0c7), serial vs parallel on the same binary:
#  A. raw curl downloads of SF50 orders, curl on ClickHouse's socket (CPU 9) and the other socket (CPU 19);
#  B. parallel_read_adbc key-range streams, N = 1..10, DuckDB in its slice with the reader thread on.
# Run from the repo root with the prototype server running.
set -euo pipefail
cd /users/awitt/duckdb-adbc-client
E=build/experiments/modified-clickhouse-20261110
OUT=${OUT_DIR:-$E/parallel-arrowstream}
mkdir -p $OUT
SQL="SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000"
BASE="http://127.0.0.1:8123/?default_format=ArrowStream&enable_http_compression=0&output_format_arrow_compression_method=none&max_threads=32&log_processors_profiles=1"
Q() { curl -fsS http://127.0.0.1:8123/ --data-binary "$1"; }
tag="patime$RANDOM$RANDOM"

echo "=== A. Raw curl downloads"
for cpu in 9 19; do
    for rep in 1 2; do
        for parallel in 0 1; do
            id="$tag-par$parallel-cpu$cpu-$rep"
            taskset -c "$cpu" curl -fsS -o /dev/null -w "${id#$tag-}: curl %{time_total} s, %{size_download} bytes\n" \
                "$BASE&output_format_parallel_formatting=$parallel&query_id=$id" --data-binary "$SQL"
        done
    done
done
Q "SYSTEM FLUSH LOGS"
Q "SELECT
       replaceOne(q.query_id, '$tag-', '') AS run,
       round(q.query_duration_ms / 1e3, 2) AS wall_s,
       round(q.ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6, 1) AS server_cpu_s,
       p.output AS output_stage,
       round(p.busy / 1e6, 2) AS output_busy_s,
       round(p.first_input_wait / 1e6, 2) AS first_input_wait_s,
       round(q.ProfileEvents['NetworkSendElapsedMicroseconds'] / 1e6, 2) AS send_blocked_s,
       round((p.busy - q.ProfileEvents['NetworkSendElapsedMicroseconds']) / 1e6, 2) AS busy_not_sending_s,
       round(p.readers_out_wait / 1e6, 2) AS readers_blocked_max_s
   FROM system.query_log AS q
   INNER JOIN
   (
       SELECT query_id,
              argMaxIf(name, elapsed_us, name IN ('ArrowIPCBlockOutputFormat', 'ParallelFormattingOutputFormat')) AS output,
              maxIf(elapsed_us, name IN ('ArrowIPCBlockOutputFormat', 'ParallelFormattingOutputFormat')) AS busy,
              maxIf(input_wait_elapsed_us, name IN ('ArrowIPCBlockOutputFormat', 'ParallelFormattingOutputFormat')) AS first_input_wait,
              maxIf(output_wait_elapsed_us, name LIKE 'MergeTreeSelect%') AS readers_out_wait
       FROM system.processors_profile_log
       WHERE query_id LIKE '$tag-%'
       GROUP BY query_id
   ) AS p USING (query_id)
   WHERE q.type = 'QueryFinish' AND q.query_id LIKE '$tag-%'
   ORDER BY run
   FORMAT PrettyCompactMonoBlock" | tee $OUT/curl.txt

echo
echo "=== B. read_adbc key-range streams"
for parallel in 0 1; do
    started=$(date +%s)
    echo "--- output_format_parallel_formatting=$parallel"
    scripts/run_duckdb_cgroup.sh env ADBC_READER_THREAD=1 build/release/extension/adbc/bench/parallel_read_adbc \
        --ch-max-threads=split --ch-settings=output_format_parallel_formatting=$parallel --repeats=2 \
        1 2 3 4 6 8 10 > $OUT/streams-par$parallel.tsv
    Q "SYSTEM FLUSH LOGS"
    Q "SELECT
           intDiv(32, toUInt64(extract(query, 'max_threads=(\\d+)'))) AS streams,
           count() AS queries,
           round(median(query_duration_ms) / 1e3, 2) AS median_query_s,
           round(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6 / (count() / streams), 1) AS server_cpu_s_per_batch,
           round(median(ProfileEvents['NetworkSendElapsedMicroseconds']) / 1e6, 2) AS median_send_blocked_s
       FROM system.query_log
       WHERE type = 'QueryFinish' AND event_time >= toDateTime($started)
         AND query LIKE 'SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN%output_format_parallel_formatting=$parallel%'
       GROUP BY streams ORDER BY streams
       FORMAT PrettyCompactMonoBlock" | tee $OUT/streams-par$parallel-clickhouse.txt
done
