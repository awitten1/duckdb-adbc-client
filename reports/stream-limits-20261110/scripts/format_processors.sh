#!/usr/bin/env bash
# One raw HTTP download per (format, client CPU), then system.processors_profile_log and system.query_log for each.
# Usage: format_processors.sh   (modified ClickHouse must be running)
set -euo pipefail
SQL="SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000"
BASE="http://127.0.0.1:8123/?enable_http_compression=0&output_format_arrow_compression_method=none&max_threads=32&log_processors_profiles=1"
Q() { curl -fsS http://127.0.0.1:8123/ --data-binary "$1"; }
tag="fmtproc$RANDOM$RANDOM"
for cpu in 9 19; do
    for fmt in Null Native ArrowStream; do
        id="$tag-$fmt-cpu$cpu"
        taskset -c "$cpu" curl -fsS -o /dev/null -w "$id: curl wall %{time_total} s, %{size_download} bytes\n" \
            "$BASE&default_format=$fmt&query_id=$id" --data-binary "$SQL"
    done
done
Q "SYSTEM FLUSH LOGS"

echo; echo "=== query_log (server-side wall, CPU, socket send)"
Q "SELECT
       replaceOne(query_id, '$tag-', '') AS run,
       round(query_duration_ms / 1e3, 2) AS server_wall_s,
       round(ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6, 2) AS server_cpu_s,
       round(ProfileEvents['NetworkSendElapsedMicroseconds'] / 1e6, 2) AS send_blocked_s,
       round(ProfileEvents['NetworkSendBytes'] / 1e9, 2) AS sent_gb
   FROM system.query_log
   WHERE type = 'QueryFinish' AND query_id LIKE '$tag-%'
   ORDER BY event_time_microseconds
   FORMAT PrettyCompactMonoBlock"

echo; echo "=== processors_profile_log, summed over processor instances (seconds)"
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
   HAVING busy_sum >= 0.05 OR name LIKE '%Output%'
   ORDER BY query_id, busy_sum DESC
   FORMAT PrettyCompactMonoBlock"
