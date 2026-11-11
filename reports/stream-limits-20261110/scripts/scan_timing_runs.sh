#!/usr/bin/env bash
# One single-stream read_adbc scan with ADBC_SCAN_TIMING, plus ClickHouse's view of the same query.
# Usage: scan_timing_runs.sh LABEL DUCKDB_THREADS [EXTRA_SETTINGS]   (inside the DuckDB slice, from the repo root)
# EXTRA_SETTINGS is appended after max_threads=32, e.g. ", output_format_arrow_record_batch_size=65536".
# READER=on sets ADBC_READER_THREAD=1 (the slice wrapper does not pass other environment variables).
# PROFILE selects the ADBC profile (default profile://myclickhouse).
set -euo pipefail
label=$1 threads=$2 extra=${3:-} reader=${READER:-off} profile=${PROFILE:-profile://myclickhouse}
[[ $reader == on ]] && export ADBC_READER_THREAD=1
cat > /tmp/scan-timing.sql <<EOF
LOAD 'build/release/extension/adbc/adbc.duckdb_extension';
SET threads=$threads;
SET memory_limit='56GiB';
EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM read_adbc('$profile', 'SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000 SETTINGS max_threads=32$extra');
EOF
started=$(date +%s)
ADBC_SCAN_TIMING=1 TZ=UTC build/release/duckdb -unsigned -batch -json -no-init < /tmp/scan-timing.sql > /tmp/scan-timing.json 2> /tmp/scan-timing.err
query_s=$(python3 -c "import json; print(round(json.load(open('/tmp/scan-timing.json'))['query']['total_time'], 3))")
echo "=== $label, DuckDB threads=$threads: query ${query_s} s"
grep -A4 ADBC_SCAN_TIMING /tmp/scan-timing.err
curl -fsS http://127.0.0.1:8123/ --data-binary "SYSTEM FLUSH LOGS"
curl -fsS http://127.0.0.1:8123/ --data-binary "SELECT concat('  clickhouse: query_s=', toString(round(query_duration_ms / 1e3, 3)), '  send_wait_s=', toString(round(ProfileEvents['NetworkSendElapsedMicroseconds'] / 1e6, 3)), '  cpu_s=', toString(round(ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6, 1))) FROM system.query_log WHERE type = 'QueryFinish' AND event_time >= toDateTime($started) AND query LIKE 'SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000 SETTINGS max_threads=32%' ORDER BY event_time DESC LIMIT 1 FORMAT TSVRaw"
rm -f /tmp/scan-timing.sql /tmp/scan-timing.json /tmp/scan-timing.err
