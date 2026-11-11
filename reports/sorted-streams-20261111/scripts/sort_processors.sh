#!/usr/bin/env bash
# Raw HTTP downloads of SF50 orders with no ORDER BY, ORDER BY the table's sort key, and ORDER BY a non-key column,
# each with Null and ArrowStream output, then query_log and processors_profile_log for every run.
# Usage: sort_processors.sh   (modified ClickHouse must be running)
set -euo pipefail
BASE="http://127.0.0.1:8123/?enable_http_compression=0&output_format_arrow_compression_method=none&max_threads=32&log_processors_profiles=1"
Q() { curl -fsS http://127.0.0.1:8123/ --data-binary "$1"; }
tag="sortproc$RANDOM$RANDOM"
declare -A ORDER=([nosort]="" [orderkey]="ORDER BY o_orderkey" [custkey]="ORDER BY o_custkey")
for cpu in 9 19; do
    for sort in nosort orderkey custkey; do
        for fmt in Null ArrowStream; do
            id="$tag-$sort-$fmt-cpu$cpu"
            sql="SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000 ${ORDER[$sort]}"
            taskset -c "$cpu" curl -fsS -o /dev/null -w "${id#$tag-}: curl wall %{time_total} s, %{size_download} bytes\n" \
                "$BASE&default_format=$fmt&query_id=$id" --data-binary "$sql"
        done
    done
done
Q "SYSTEM FLUSH LOGS"

echo; echo "=== query_log"
Q "SELECT
       replaceOne(query_id, '$tag-', '') AS run,
       round(query_duration_ms / 1e3, 2) AS wall_s,
       round(ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6, 2) AS cpu_s,
       round(ProfileEvents['NetworkSendElapsedMicroseconds'] / 1e6, 2) AS send_blocked_s,
       round(ProfileEvents['NetworkSendBytes'] / 1e9, 2) AS sent_gb,
       formatReadableSize(memory_usage) AS peak_mem
   FROM system.query_log
   WHERE type = 'QueryFinish' AND query_id LIKE '$tag-%'
   ORDER BY event_time_microseconds
   FORMAT PrettyCompactMonoBlock"

echo; echo "=== processors_profile_log, per processor name (seconds; max is over instances)"
Q "SELECT
       replaceOne(query_id, '$tag-', '') AS run,
       name,
       count() AS n,
       round(sum(elapsed_us) / 1e6, 2) AS busy_sum,
       round(max(elapsed_us) / 1e6, 2) AS busy_max,
       round(max(input_wait_elapsed_us) / 1e6, 2) AS in_wait_max,
       round(max(output_wait_elapsed_us) / 1e6, 2) AS out_wait_max
   FROM system.processors_profile_log
   WHERE query_id LIKE '$tag-%'
   GROUP BY query_id, name
   HAVING busy_sum >= 0.1 OR name IN ('Null', 'ArrowIPCBlockOutputFormat')
   ORDER BY min(event_time_microseconds), busy_max DESC
   FORMAT PrettyCompactMonoBlock"
