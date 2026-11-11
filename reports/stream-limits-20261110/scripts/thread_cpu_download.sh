#!/usr/bin/env bash
# User/kernel CPU for curl and for each ClickHouse thread of the query during one raw ArrowStream download.
# ClickHouse's per-thread numbers come from system.query_thread_log (log_query_threads=1).
# Usage: thread_cpu_download.sh CLIENT_CPU   (modified ClickHouse must be running)
set -euo pipefail
cpu=$1
id="threadcpu-cpu$cpu-$RANDOM$RANDOM"
SQL="SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000"
URL="http://127.0.0.1:8123/?default_format=ArrowStream&enable_http_compression=0&output_format_arrow_compression_method=none&max_threads=32&log_query_threads=1&query_id=$id"
Q() { curl -fsS http://127.0.0.1:8123/ --data-binary "$1"; }

start=$(date +%s.%N)
times=$( { /usr/bin/time -f "%U %S" taskset -c "$cpu" curl -fsS -o /dev/null "$URL" --data-binary "$SQL"; } 2>&1 )
end=$(date +%s.%N)
wall=$(echo "$end - $start" | bc)
read -r cu cs <<< "$times"
socket=$([[ $cpu -lt 10 || ( $cpu -ge 20 && $cpu -lt 30 ) ]] && echo 0 || echo 1)
printf "curl on CPU %s (socket %s): wall %.2f s; curl user %.2f s + kernel %.2f s = busy %.0f%%\n" \
    "$cpu" "$socket" "$wall" "$cu" "$cs" "$(echo "100 * ($cu + $cs) / $wall" | bc -l)"
Q "SYSTEM FLUSH LOGS"
Q "SELECT
       thread_name,
       count() AS threads,
       round(sum(ProfileEvents['UserTimeMicroseconds']) / 1e6, 2) AS user_s,
       round(sum(ProfileEvents['SystemTimeMicroseconds']) / 1e6, 2) AS kernel_s,
       round(max(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6, 2) AS busiest_thread_cpu_s,
       round(sum(ProfileEvents['NetworkSendElapsedMicroseconds']) / 1e6, 2) AS send_wait_s,
       round(sum(ProfileEvents['NetworkSendBytes']) / 1e9, 2) AS sent_gb
   FROM system.query_thread_log
   WHERE query_id = '$id'
   GROUP BY thread_name
   ORDER BY user_s + kernel_s DESC
   FORMAT PrettyCompactMonoBlock"
