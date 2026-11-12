#!/usr/bin/env bash
# The 25 Gb/s experiment (run_all.sh of reports/two-node-25gbe-20261112) with DataFusion instead of DuckDB:
# the same scripts, with parallel_read_adbc replaced by parallel_read_datafusion (~/df-bench). Same options:
# --threads=$(nproc) sets DataFusion's tokio workers and target partitions; ADBC_READER_THREAD has no effect,
# since every DataFusion stream reads on its own thread anyway.
set -euo pipefail
R=~/duckdb-adbc-client/reports
T=$(nproc)
DF=$HOME/df-bench/target/release/parallel_read_datafusion
mkdir -p ~/df-scripts
for pair in "two-node-10gbe-20261111/scripts/utah_streams.sh streams.sh" "two-node-sorted-streams-20261111/scripts/utah_sorted.sh sorted.sh"; do
    set -- $pair
    sed -e "s/--threads=16/--threads=$T/" -e "s|^BENCH=.*|BENCH=$DF|" $R/$1 > ~/df-scripts/$2
done
chmod +x ~/df-scripts/*.sh
grep -H "^BENCH=\|--threads=$T" ~/df-scripts/*.sh

echo "##### uncompressed $(date -u +%T)"
OUT_DIR=~/df-streams ~/df-scripts/streams.sh
echo "##### LZ4 $(date -u +%T)"
OUT_DIR=~/df-streams/lz4 PROFILE=profile://myclickhouse-lz4 CURL_PARAMS='&compress=1' ~/df-scripts/streams.sh
echo "##### sorted $(date -u +%T)"
OUT_DIR=~/df-sorted ~/df-scripts/sorted.sh

echo "##### sorted correctness $(date -u +%T)"
for k in o_orderkey o_custkey; do
    $DF --profile=profile://myclickhouse --threads=$T --memory-limit=48GiB --ch-max-threads=split \
        --ch-settings=output_format_parallel_formatting=1 --order-by=$k --checksum 4 10 | tee ~/df-sorted/checksum-$k.txt
done
echo "##### all done $(date -u +%T)"
