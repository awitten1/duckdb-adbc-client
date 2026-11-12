#!/usr/bin/env bash
# Sorted streams across the 10 Gb/s link: like the two-node report's utah_streams.sh, but every o_orderkey range
# query also carries an ORDER BY, so ClickHouse sorts each stream's partition before sending it. Parallel
# ArrowStream formatting on.
#   o_orderkey  the table's sort key (and the partition key): ranges concatenate into one globally sorted result
#   o_custkey   another column: a real sort per stream, N separately sorted runs
# Configurations: uncompressed with both sort keys, and the driver's LZ4 compression with o_orderkey.
# Before the streams, one raw curl download per sort key (one query, full table, max_threads=32).
set -euo pipefail
cd ~/duckdb-adbc-client
CH=http://10.10.1.1:8123/
OUT=${OUT_DIR:-~/utah-sorted}
mkdir -p $OUT
BENCH=build/release/extension/adbc/bench/parallel_read_adbc
Q() { curl -fsS "$CH" --data-binary "$1"; }
export ADBC_READER_THREAD=1
PAR=output_format_parallel_formatting=1

echo "=== Setup: ClickHouse $(Q "SELECT version()") on $(Q "SELECT hostName()"); DuckDB node $(hostname)"
Q "SELECT sorting_key, (SELECT count() FROM system.parts WHERE database = 'tpch_sf50' AND table = 'orders' AND active) AS active_parts
   FROM system.tables WHERE database = 'tpch_sf50' AND name = 'orders' FORMAT PrettyCompactMonoBlock"

echo; echo "=== 1. One query, full table, sorted, raw curl (ArrowStream, max_threads=32, parallel formatting)"
for rep in 1 2; do
    for key in none o_orderkey o_custkey; do
        for comp in none lz4; do
            [[ $comp == lz4 && $key == o_custkey ]] && continue
            SQL="SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000"
            [[ $key != none ]] && SQL+=" ORDER BY $key"
            extra=""; [[ $comp == lz4 ]] && extra="&compress=1"
            curl -fsS -o /dev/null -w "order_by=$key compression=$comp run $rep: %{time_total} s, %{size_download} bytes\n" \
                "${CH}?default_format=ArrowStream&enable_http_compression=0&output_format_arrow_compression_method=none&max_threads=32&$PAR$extra" \
                --data-binary "$SQL"
        done
    done
done | tee $OUT/curl.txt

run() {  # name profile order_by
    local name=$1 profile=$2 key=$3
    local started=$(Q "SELECT toUnixTimestamp(now())")
    echo; echo "=== 2. read_adbc streams: $name (profile $profile, ORDER BY $key per stream)"
    $BENCH --profile=$profile --threads=16 --memory-limit=48GiB --ch-max-threads=split --ch-settings=$PAR \
        --order-by=$key --repeats=2 1 2 3 4 6 8 10 > $OUT/streams-$name.tsv
    cat $OUT/streams-$name.tsv
    Q "SYSTEM FLUSH LOGS"
    Q "SELECT
           intDiv(32, toUInt64(extract(query, 'max_threads=(\\d+)'))) AS streams,
           count() AS queries,
           round(median(query_duration_ms) / 1e3, 2) AS median_query_s,
           round(max(query_duration_ms) / 1e3, 2) AS max_query_s,
           round(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6 / (count() / streams), 1) AS server_cpu_s_per_batch,
           round(median(ProfileEvents['NetworkSendElapsedMicroseconds']) / 1e6, 2) AS median_send_blocked_s,
           round(sum(ProfileEvents['NetworkSendBytes']) / (count() / streams) / 1e9, 2) AS gb_sent_per_batch,
           formatReadableSize(max(memory_usage)) AS max_query_memory
       FROM system.query_log
       WHERE type = 'QueryFinish' AND event_time >= toDateTime($started)
         AND query LIKE 'SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN% ORDER BY $key SETTINGS%'
       GROUP BY streams ORDER BY streams
       FORMAT PrettyCompactMonoBlock" | tee $OUT/streams-$name-clickhouse.txt
}
run nocompress-orderkey profile://myclickhouse o_orderkey
run nocompress-custkey profile://myclickhouse o_custkey
run lz4-orderkey profile://myclickhouse-lz4 o_orderkey
echo; echo "=== done"

# Run separately after the timings above: correctness with each sort, one full read_adbc vs 4 and 10 range reads
# (rows and row-hash sum; data/checksum-*.txt), and the table's parts (data/parts.txt).
#   for k in o_orderkey o_custkey; do
#       $BENCH --profile=profile://myclickhouse --threads=16 --memory-limit=48GiB --ch-max-threads=split \
#           --ch-settings=$PAR --order-by=$k --checksum 4 10 | tee $OUT/checksum-$k.txt
#   done
#   Q "SELECT _part, count() AS rows, min(o_orderkey), max(o_orderkey) FROM tpch_sf50.orders
#      GROUP BY _part ORDER BY _part FORMAT PrettyCompactMonoBlock" | tee $OUT/parts.txt
