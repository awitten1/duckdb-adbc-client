#!/usr/bin/env bash
# Raw HTTP downloads of SF50 orders as RowBinary with ClickHouse's parallel formatting off and on (ArrowStream
# for reference, which never uses it), then the output stage from query_log and processors_profile_log.
# Usage: parallel_formatting.sh   (modified ClickHouse must be running)
set -euo pipefail
SQL="SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000"
BASE="http://127.0.0.1:8123/?enable_http_compression=0&output_format_arrow_compression_method=none&max_threads=32&log_processors_profiles=1"
Q() { curl -fsS http://127.0.0.1:8123/ --data-binary "$1"; }
tag="pfmt$RANDOM$RANDOM"
for cpu in 9 19; do
    for run in ArrowStream:0 RowBinary:0 RowBinary:1; do
        fmt=${run%:*} parallel=${run#*:}
        for rep in 1 2; do
            id="$tag-$fmt-par$parallel-cpu$cpu-$rep"
            taskset -c "$cpu" curl -fsS -o /dev/null -w "${id#$tag-}: curl wall %{time_total} s, %{size_download} bytes\n" \
                "$BASE&default_format=$fmt&output_format_parallel_formatting=$parallel&query_id=$id" --data-binary "$SQL"
        done
    done
done
Q "SYSTEM FLUSH LOGS"
echo
Q "SELECT
       replaceOne(q.query_id, '$tag-', '') AS run,
       round(q.query_duration_ms / 1e3, 2) AS wall_s,
       round(q.ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6, 1) AS cpu_s,
       round(q.ProfileEvents['NetworkSendElapsedMicroseconds'] / 1e6, 2) AS send_blocked_s,
       round(q.ProfileEvents['NetworkSendBytes'] / 1e9, 2) AS sent_gb,
       p.output_name,
       round(p.busy / 1e6, 2) AS output_busy_s,
       round(p.formatter_busy / 1e6, 2) AS formatter_threads_busy_s
   FROM system.query_log AS q
   LEFT JOIN
   (
       SELECT query_id,
              argMaxIf(name, elapsed_us, name IN ('ArrowIPCBlockOutputFormat', 'RowBinary', 'ParallelFormattingOutputFormat')) AS output_name,
              maxIf(elapsed_us, name IN ('ArrowIPCBlockOutputFormat', 'RowBinary', 'ParallelFormattingOutputFormat')) AS busy,
              sumIf(elapsed_us, name LIKE '%Formatting%' AND name != 'ParallelFormattingOutputFormat') AS formatter_busy
       FROM system.processors_profile_log
       WHERE query_id LIKE '$tag-%'
       GROUP BY query_id
   ) AS p USING (query_id)
   WHERE q.type = 'QueryFinish' AND q.query_id LIKE '$tag-%'
   ORDER BY q.event_time_microseconds
   FORMAT PrettyCompactMonoBlock"
echo "--- processor names seen in the parallel runs"
Q "SELECT DISTINCT name FROM system.processors_profile_log WHERE query_id LIKE '$tag-RowBinary-par1%' FORMAT TSV"
