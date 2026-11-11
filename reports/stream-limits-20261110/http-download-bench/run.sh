#!/usr/bin/env bash
# Raw ArrowStream download speed: curl vs hyper vs clickhouse-rs, no Arrow decoding. Run inside the DuckDB
# slice so the client uses the same CPUs as DuckDB. Rotates the client order each round.
set -euo pipefail
cd "$(dirname "$0")"
BIN=target/release/http-download-bench
SQL="SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000"
URL="http://127.0.0.1:8123/?default_format=ArrowStream&enable_http_compression=0&output_format_arrow_compression_method=none&max_threads=32"
ROUNDS=${ROUNDS:-3}
modes=(curl hyper hyper-mt hyper-bigbuf clickhouse-rs)
ids=()

for ((round = 1; round <= ROUNDS; round++)); do
    order=("${modes[@]:round-1}" "${modes[@]:0:round-1}")
    for mode in "${order[@]}"; do
        id="dl-$mode-r$round-$RANDOM$RANDOM"
        ids+=("$id")
        if [[ $mode == curl ]]; then
            start=$(date +%s.%N)
            bytes=$(curl -fsS -o /dev/null -w '%{size_download}' "$URL&query_id=$id" --data-binary "$SQL")
            end=$(date +%s.%N)
            awk -v b="$bytes" -v s="$start" -v e="$end" \
                'BEGIN { printf "%-14s %.3f GB in %.3f s = %.3f GB/s (curl)\n", "curl", b / 1e9, e - s, b / 1e9 / (e - s) }'
        else
            $BIN "$mode" "$id"
        fi
    done
done

curl -fsS http://127.0.0.1:8123/ --data-binary "SYSTEM FLUSH LOGS"
echo "--- ClickHouse view (median over rounds):"
id_list=$(printf "'%s'," "${ids[@]}")
curl -fsS http://127.0.0.1:8123/ --data-binary "
SELECT extract(query_id, '^dl-([a-z-]+)-r') AS client,
       count() AS runs,
       round(median(query_duration_ms) / 1e3, 3) AS query_s,
       round(median(ProfileEvents['NetworkSendElapsedMicroseconds']) / 1e6, 3) AS send_wait_s,
       round(median(ProfileEvents['NetworkSendBytes']) / 1e9, 3) AS sent_gb,
       round(median(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6, 1) AS server_cpu_s
FROM system.query_log
WHERE type = 'QueryFinish' AND query_id IN (${id_list%,})
GROUP BY client ORDER BY query_s
FORMAT PrettyCompactMonoBlock"
