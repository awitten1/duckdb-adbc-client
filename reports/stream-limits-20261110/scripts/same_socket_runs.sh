#!/usr/bin/env bash
# One read_adbc stream with the DuckDB client on ClickHouse's socket (free CPUs 8,9,28,29), comparing the
# current connector with each CPU-saving variant, plus curl on the same CPUs as the transfer ceiling.
set -euo pipefail
cd /users/awitt/duckdb-adbc-client
E=build/experiments/modified-clickhouse-20261110
V=profile://$PWD/build/experiments/connector-variants
CPUS=8,9,28,29
ROUNDS=${ROUNDS:-2}
SQL="SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000"
URL="http://127.0.0.1:8123/?default_format=ArrowStream&enable_http_compression=0&output_format_arrow_compression_method=none&max_threads=32"
summ() { grep -E "^===|inside_get_next|clickhouse:" | sed -E 's/ +/ /g'; }
variants=(curl current reserve novalidate overlap)

for ((round = 1; round <= ROUNDS; round++)); do
    order=("${variants[@]:round-1}" "${variants[@]:0:round-1}")
    for variant in "${order[@]}"; do
        case $variant in
        curl)
            start=$(date +%s.%N)
            bytes=$(taskset -c $CPUS curl -fsS -o /dev/null -w '%{size_download}' "$URL" --data-binary "$SQL")
            end=$(date +%s.%N)
            awk -v b="$bytes" -v s="$start" -v e="$end" -v r="$round" \
                'BEGIN { printf "=== curl (transfer ceiling), run %s: %.3f s = %.3f GB/s\n", r, e - s, b / 1e9 / (e - s) }'
            ;;
        current)
            taskset -c $CPUS env READER=on bash $E/scan_timing_runs.sh "current connector, run $round" 4 2>&1 | summ
            ;;
        *)
            taskset -c $CPUS env READER=on PROFILE=$V/$variant.toml \
                bash $E/scan_timing_runs.sh "$variant, run $round" 4 2>&1 | summ
            ;;
        esac
    done
done
