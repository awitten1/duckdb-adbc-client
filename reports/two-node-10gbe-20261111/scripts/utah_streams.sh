#!/usr/bin/env bash
# Parallel read_adbc streams across a 10 Gb/s link: ClickHouse (custom build c933d005d) on ms1011 at 10.10.1.1,
# DuckDB on this node (ms1006). Correctness check, raw curl downloads, then N = 1..10 key-range streams with
# parallel ArrowStream formatting on and off. Both nodes: all 16 CPUs, clocks fixed at 2.0 GHz.
set -euo pipefail
cd ~/duckdb-adbc-client
CH=http://10.10.1.1:8123/
OUT=~/utah-streams
mkdir -p $OUT
BENCH=build/release/extension/adbc/bench/parallel_read_adbc
COMMON=(--threads=16 --memory-limit=48GiB --ch-max-threads=split)
Q() { curl -fsS "$CH" --data-binary "$1"; }
export ADBC_READER_THREAD=1

echo "=== Setup: ClickHouse $(Q "SELECT version()") on $(Q "SELECT hostName()"); DuckDB node $(hostname)"
Q "SELECT table, sum(rows) AS rows, count() AS parts, formatReadableSize(sum(bytes_on_disk)) AS on_disk
   FROM system.parts WHERE database = 'tpch_sf50' AND table = 'orders' AND active GROUP BY table FORMAT PrettyCompactMonoBlock"

echo; echo "=== 1. Correctness: one full read_adbc vs N range reads (rows, row-hash sum)"
$BENCH "${COMMON[@]}" --checksum 4 10 | tee $OUT/checksum.txt

echo; echo "=== 2. Raw curl downloads of the full table over the link (ArrowStream, max_threads=32)"
SQL="SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000"
for rep in 1 2; do
    for parallel in 1 0; do
        curl -fsS -o /dev/null -w "parallel_formatting=$parallel run $rep: %{time_total} s, %{size_download} bytes\n" \
            "${CH}?default_format=ArrowStream&enable_http_compression=0&output_format_arrow_compression_method=none&max_threads=32&output_format_parallel_formatting=$parallel" \
            --data-binary "$SQL"
    done
done | tee $OUT/curl.txt

for parallel in 1 0; do
    started=$(Q "SELECT toUnixTimestamp(now())")
    echo; echo "=== 3. read_adbc streams, output_format_parallel_formatting=$parallel"
    $BENCH "${COMMON[@]}" --ch-settings=output_format_parallel_formatting=$parallel --repeats=2 1 2 3 4 6 8 10 \
        > $OUT/streams-par$parallel.tsv
    Q "SYSTEM FLUSH LOGS"
    Q "SELECT
           intDiv(32, toUInt64(extract(query, 'max_threads=(\\d+)'))) AS streams,
           count() AS queries,
           round(median(query_duration_ms) / 1e3, 2) AS median_query_s,
           round(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6 / (count() / streams), 1) AS server_cpu_s_per_batch,
           round(median(ProfileEvents['NetworkSendElapsedMicroseconds']) / 1e6, 2) AS median_send_blocked_s,
           round(sum(ProfileEvents['NetworkSendBytes']) / (count() / streams) / 1e9, 2) AS gb_sent_per_batch
       FROM system.query_log
       WHERE type = 'QueryFinish' AND event_time >= toDateTime($started)
         AND query LIKE 'SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN%output_format_parallel_formatting=$parallel%'
       GROUP BY streams ORDER BY streams
       FORMAT PrettyCompactMonoBlock" | tee $OUT/streams-par$parallel-clickhouse.txt
done
