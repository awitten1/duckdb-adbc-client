#!/usr/bin/env bash
# Every two-node experiment on hp034 (ClickHouse, 10.10.1.1) and hp036 (DuckDB, this node), 25 Gb/s link.
# Runs the two-node report's utah_streams.sh (uncompressed, then the driver's LZ4) and the sorted-streams report's
# utah_sorted.sh, with DuckDB threads set to this node's CPU count instead of 16. Everything else is unchanged,
# including ClickHouse max_threads = floor(32 / N) per query and max_threads=32 for curl.
set -euo pipefail
R=~/duckdb-adbc-client/reports
T=$(nproc)
mkdir -p ~/hp-scripts
sed "s/--threads=16/--threads=$T/" $R/two-node-10gbe-20261111/scripts/utah_streams.sh > ~/hp-scripts/streams.sh
sed "s/--threads=16/--threads=$T/" $R/two-node-sorted-streams-20261111/scripts/utah_sorted.sh > ~/hp-scripts/sorted.sh
chmod +x ~/hp-scripts/*.sh
grep -c -- "--threads=$T" ~/hp-scripts/streams.sh ~/hp-scripts/sorted.sh

echo "##### uncompressed $(date -u +%T)"
OUT_DIR=~/hp-streams ~/hp-scripts/streams.sh
echo "##### LZ4 $(date -u +%T)"
OUT_DIR=~/hp-streams/lz4 PROFILE=profile://myclickhouse-lz4 CURL_PARAMS='&compress=1' ~/hp-scripts/streams.sh
echo "##### sorted $(date -u +%T)"
OUT_DIR=~/hp-sorted ~/hp-scripts/sorted.sh

echo "##### sorted correctness and parts $(date -u +%T)"
cd ~/duckdb-adbc-client
export ADBC_READER_THREAD=1
for k in o_orderkey o_custkey; do
    build/release/extension/adbc/bench/parallel_read_adbc --profile=profile://myclickhouse --threads=$T \
        --memory-limit=48GiB --ch-max-threads=split --ch-settings=output_format_parallel_formatting=1 \
        --order-by=$k --checksum 4 10 | tee ~/hp-sorted/checksum-$k.txt
done
curl -fsS http://10.10.1.1:8123/ --data-binary "SELECT _part, count() AS rows, min(o_orderkey), max(o_orderkey)
    FROM tpch_sf50.orders GROUP BY _part ORDER BY _part FORMAT PrettyCompactMonoBlock" | tee ~/hp-sorted/parts.txt
echo "##### all done $(date -u +%T)"
