#!/usr/bin/env bash
# Correctness of parallel ArrowStream formatting (prototype a5be5f0c7): byte-identical output to the serial path,
# the fallbacks to serial, and read_adbc results and errors. Run from the repo root; the prototype server must run.
set -euo pipefail
cd /users/awitt/duckdb-adbc-client
URL="http://127.0.0.1:8123/?enable_http_compression=0&output_format_arrow_compression_method=none&log_processors_profiles=1"
Q() { curl -fsS http://127.0.0.1:8123/ --data-binary "$1"; }
tag="pacorr$RANDOM$RANDOM"
fail=0

# Download one query as ArrowStream; print sha256, bytes, and which output processor ran.
fetch() {  # id sql settings
    local id="$tag-$1"
    local out
    out=$(curl -sS "$URL&default_format=ArrowStream&query_id=$id&$3" --data-binary "$2" \
          | tee >(wc -c > /tmp/$id.bytes) | sha256sum | cut -c1-16)
    sleep 0.2
    echo "$out $(cat /tmp/$id.bytes)"
    rm -f /tmp/$id.bytes
}
compare() {  # label sql settings
    local serial parallel
    serial=$(fetch "$1-serial" "$2" "output_format_parallel_formatting=0&$3")
    parallel=$(fetch "$1-parallel" "$2" "output_format_parallel_formatting=1&$3")
    local verdict=SAME
    [[ $serial == "$parallel" ]] || { verdict=DIFFERENT; fail=1; }
    printf "%-34s serial %-30s parallel %-30s %s\n" "$1" "$serial" "$parallel" "$verdict"
}

FULL="SELECT * FROM tpch_sf50.orders"
echo "=== Byte comparison, serial vs parallel formatting (sha256 prefix, bytes)"
compare full-table-1-thread "$FULL" "max_threads=1"
compare ordered-500k-32-threads "$FULL WHERE o_orderkey <= 2000000 ORDER BY o_orderkey" "max_threads=32"
compare empty-result "$FULL WHERE 0" "max_threads=32"
compare limit-10 "$FULL LIMIT 10" "max_threads=32"
compare low-cardinality-dictionary "SELECT o_orderkey, toLowCardinality(o_orderpriority) AS p FROM tpch_sf50.orders WHERE o_orderkey <= 2000000 ORDER BY o_orderkey" "max_threads=32&output_format_arrow_low_cardinality_as_dictionary=1"
compare combined-record-batches "$FULL WHERE o_orderkey <= 2000000 ORDER BY o_orderkey" "max_threads=32&output_format_arrow_record_batch_size=65536"

Q "SYSTEM FLUSH LOGS"
echo
echo "=== Output processor used by each run (expect ParallelFormattingOutputFormat only for parallel runs without a fallback)"
Q "SELECT replaceOne(query_id, '$tag-', '') AS run,
          arrayStringConcat(groupUniqArrayIf(name, name IN ('ArrowIPCBlockOutputFormat', 'ParallelFormattingOutputFormat')), ',') AS output
   FROM system.processors_profile_log WHERE query_id LIKE '$tag-%'
   GROUP BY query_id ORDER BY min(event_time_microseconds) FORMAT PrettyCompactMonoBlock"

echo
echo "=== read_adbc through the connector (arrow-rs decoding), DuckDB in its slice"
cat > /tmp/$tag.sql <<EOF
LOAD 'build/release/extension/adbc/adbc.duckdb_extension';
SET threads=16;
SELECT 'serial' AS formatting, count(*) AS rows, sum(hash(t)::HUGEINT) AS row_hash_sum
FROM read_adbc('profile://myclickhouse', 'SELECT * FROM tpch_sf50.orders SETTINGS max_threads=32, output_format_parallel_formatting=0') t;
SELECT 'parallel' AS formatting, count(*) AS rows, sum(hash(t)::HUGEINT) AS row_hash_sum
FROM read_adbc('profile://myclickhouse', 'SELECT * FROM tpch_sf50.orders SETTINGS max_threads=32, output_format_parallel_formatting=1') t;
EOF
scripts/run_duckdb_cgroup.sh env TZ=UTC build/release/duckdb -unsigned -batch -csv -noheader -no-init < /tmp/$tag.sql | tee /tmp/$tag.out
[[ $(cut -d, -f2,3 /tmp/$tag.out | sort -u | wc -l) == 1 ]] && echo "read_adbc checksums: SAME" || { echo "read_adbc checksums: DIFFERENT"; fail=1; }

echo
echo "=== An error in the middle of the stream must reach DuckDB with parallel formatting too"
for parallel in 0 1; do
    cat > /tmp/$tag.sql <<EOF
LOAD 'build/release/extension/adbc/adbc.duckdb_extension';
SELECT count(*) FROM read_adbc('profile://myclickhouse', 'SELECT *, throwIf(o_orderkey = 150000000, ''injected mid-stream error'') AS e FROM tpch_sf50.orders SETTINGS max_threads=32, output_format_parallel_formatting=$parallel');
EOF
    result=$(scripts/run_duckdb_cgroup.sh env TZ=UTC build/release/duckdb -unsigned -batch -csv -noheader -no-init < /tmp/$tag.sql 2>&1 || true)
    if grep -q "injected mid-stream error" <<< "$result"; then echo "parallel=$parallel: error surfaced"; else echo "parallel=$parallel: NO ERROR: $result" | head -3; fail=1; fi
done
rm -f /tmp/$tag.sql /tmp/$tag.out
echo
[[ $fail == 0 ]] && echo "ALL CHECKS PASSED" || echo "SOME CHECKS FAILED"
