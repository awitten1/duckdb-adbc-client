#!/usr/bin/env bash
# TPC-H SF50 into the local ClickHouse: generate and export to Parquet, drop the DuckDB file, then load.
set -euo pipefail
cd ~/duckdb-adbc-client
args=(--duckdb ~/duckdb-cli/duckdb --scale-factor 50 --threads 16 --memory-limit 40GiB --url http://127.0.0.1:8123/)
echo "generate started $(date -u +%T)"
python3 scripts/load_clickhouse_tpch.py "${args[@]}" --generate-only
du -sh build/tpch-sf50/* | sort -h
rm -f build/tpch-sf50/tpch.duckdb build/tpch-sf50/tpch.duckdb.wal
echo "load started $(date -u +%T); disk: $(df -h / | tail -1)"
python3 scripts/load_clickhouse_tpch.py "${args[@]}" --load-only
echo "done $(date -u +%T); disk: $(df -h / | tail -1)"
