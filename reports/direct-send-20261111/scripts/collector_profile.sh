#!/usr/bin/env bash
# Where the parallel-formatting collector thread spends its time during one ArrowStream download:
# per-thread CPU from system.query_thread_log, plus a perf profile of ClickHouse's CPUs split by thread name.
# Usage: collector_profile.sh CLIENT_CPU   (prototype ClickHouse with query_thread_log must be running)
set -euo pipefail
cpu=$1
CH_CPUS=0-7,20-27
SQL="SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000"
id="collector-cpu$cpu-$RANDOM$RANDOM"
URL="http://127.0.0.1:8123/?default_format=ArrowStream&enable_http_compression=0&output_format_arrow_compression_method=none&max_threads=32&output_format_parallel_formatting=1&log_query_threads=1&query_id=$id"
Q() { curl -fsS http://127.0.0.1:8123/ --data-binary "$1"; }
OUT=/tmp/collector-prof-$cpu
mkdir -p $OUT

echo "=== curl on CPU $cpu; http_response_buffer_size=$(Q "SELECT value FROM system.settings WHERE name = 'http_response_buffer_size'"), http_wait_end_of_query=$(Q "SELECT value FROM system.settings WHERE name = 'http_wait_end_of_query'")"
sudo perf record -e cycles -F 999 -a -C "$CH_CPUS" -o $OUT/perf.data 2>/dev/null &
perf_pid=$!
sleep 1
taskset -c "$cpu" curl -fsS -o /dev/null -w "download %{time_total} s, %{size_download} bytes\n" "$URL" --data-binary "$SQL"
sudo kill -INT $perf_pid
wait $perf_pid 2>/dev/null || true

Q "SYSTEM FLUSH LOGS"
echo "--- query_thread_log, per thread name"
Q "SELECT
       thread_name,
       count() AS threads,
       round(max(query_duration_ms) / 1e3, 2) AS longest_thread_s,
       round(sum(ProfileEvents['UserTimeMicroseconds']) / 1e6, 2) AS user_s,
       round(sum(ProfileEvents['SystemTimeMicroseconds']) / 1e6, 2) AS kernel_s,
       round(sum(ProfileEvents['NetworkSendElapsedMicroseconds']) / 1e6, 2) AS in_send_s,
       round(sum(ProfileEvents['NetworkSendBytes']) / 1e9, 2) AS sent_gb
   FROM system.query_thread_log
   WHERE query_id = '$id'
   GROUP BY thread_name
   ORDER BY user_s + kernel_s DESC
   FORMAT PrettyCompactMonoBlock"
Q "SELECT 'query wall', round(query_duration_ms / 1e3, 2) FROM system.query_log WHERE query_id = '$id' AND type = 'QueryFinish' FORMAT TSV"

sudo chmod -R a+r $OUT
for comm in Collector Formatter; do
    total=$(sudo perf report -i $OUT/perf.data --stdio --comms "$comm" --sort comm -F sample 2>/dev/null | awk '/^ +[0-9]/ {s += $1} END {print s + 0}')
    echo "--- perf, thread '$comm': $total samples at 999 Hz (about $(awk -v t="$total" 'BEGIN {printf "%.2f", t / 999}') s of CPU); symbols at 1% or more:"
    sudo perf report -i $OUT/perf.data --stdio --comms "$comm" --no-children --sort dso,sym -F overhead,dso,sym \
        --percent-limit 1 2>/dev/null | grep '%' | grep -v '^#' | sed -E 's/ +/ /g' | cut -c1-110 | head -14 || true
done
sudo rm -rf $OUT
