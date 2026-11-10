#!/usr/bin/env bash
# Profile the report's hot paths after the timed matrix. Run inside the DuckDB slice:
#   bash scripts/run_duckdb_cgroup.sh bash build/experiments/report-repro-20261110/perf_phase.sh
set -uo pipefail
REPO=/users/awitt/duckdb-adbc-client
R=$REPO/build/experiments/report-repro-20261110
P=$R/perf
cd "$REPO"
mkdir -p "$P/ext"
export TZ=UTC DEBUGINFOD_URLS= HOME_PERF="$P/home"
mkdir -p "$HOME_PERF"
schema="profile://$R/profiles/schema_nocompress.toml"
queryid="profile://$R/profiles/schema_nocompress_queryid.toml"

# 1. Unstripped extension relinked from the measured release objects (same code, symbols kept).
if [[ ! -f $P/ext/adbc.duckdb_extension ]]; then
    link=$(ninja -C build/release -t commands extension/adbc/adbc.duckdb_extension | tail -1)
    link=${link//" -s "/" "}  # the only strip flag; -Wl,-soname must stay intact
    # The metadata step uses the absolute path; replace it before the relative -o path.
    link=${link//"$REPO/build/release/extension/adbc/adbc.duckdb_extension"/"$P/ext/adbc.duckdb_extension"}
    link=${link//" extension/adbc/adbc.duckdb_extension "/" $P/ext/adbc.duckdb_extension "}
    (cd build/release && bash -c "$link") || { echo "relink failed" >&2; exit 1; }
fi
file "$P/ext/adbc.duckdb_extension" > "$P/extension.txt"

# 2. ClickHouse symbols: reuse this morning's verified debug file in a private buildid cache.
debug=$(find "$REPO/build/experiments/cloudlab-rerun-20261110T152937Z/debug" -type f -name clickhouse.debug -print -quit)
HOME=$HOME_PERF perf buildid-cache --add "$debug" 2>"$P/buildid.stderr" || true

ch_pid=$(systemctl show adbc-clickhouse.service --value -p MainPID)
cat > "$P/duckdb-perf" <<EOF
#!/bin/bash
exec perf record -e cycles:u -F 499 -g -o "\$PERF_OUT" -- $REPO/build/release/duckdb "\$@"
EOF
chmod +x "$P/duckdb-perf"

server_case() {  # name sf key case profile projection
    local name=$1 sf=$2 key=$3 case=$4 profile=$5 projection=${6:-*}
    [[ -f $P/$name/server.txt ]] && return
    mkdir -p "$P/$name"
    HOME=$HOME_PERF perf record -e cycles:u -F 499 -g -p "$ch_pid" -o "$P/$name/server.data" 2>"$P/$name/perf.stderr" &
    local perf_pid=$!
    sleep 2
    python3 scripts/experiments/run_cases.py --root "$P/$name" --tag run --profile "$profile" \
        --database "tpch_sf$sf" --expected-rows $((sf * 1500000)) --partition-key o_orderkey \
        --sort-columns "$key" --projection "$projection" --threads 16 --memory-limit 56GiB \
        --no-server-metrics --cases "$case" > "$P/$name/run.log" 2>&1
    kill -INT $perf_pid; wait $perf_pid
    HOME=$HOME_PERF perf report -i "$P/$name/server.data" --stdio --no-children --sort symbol \
        --percent-limit 0.5 -g none > "$P/$name/server.txt" 2>"$P/$name/report.stderr"
    echo "server $name done"
}

client_case() {  # name sf key case profile projection
    local name=$1 sf=$2 key=$3 case=$4 profile=$5 projection=${6:-*}
    [[ -f $P/$name/client.txt ]] && return
    mkdir -p "$P/$name"
    PERF_OUT="$P/$name/client.data" python3 scripts/experiments/run_cases.py --root "$P/$name" --tag run \
        --duckdb "$P/duckdb-perf" --extension "$P/ext/adbc.duckdb_extension" --profile "$profile" \
        --database "tpch_sf$sf" --expected-rows $((sf * 1500000)) --partition-key o_orderkey \
        --sort-columns "$key" --projection "$projection" --threads 16 --memory-limit 56GiB \
        --no-server-metrics --cases "$case" > "$P/$name/run.log" 2>&1
    perf report -i "$P/$name/client.data" --stdio --no-children --sort dso,symbol \
        --percent-limit 0.5 -g none > "$P/$name/client.txt" 2>"$P/$name/report.stderr"
    echo "client $name done"
}

server_case sf50-price-remote 50 o_totalprice read_sorted "$queryid"
server_case sf50-date-remote 50 o_orderdate read_sorted "$queryid"
client_case sf50-price-hybrid05 50 o_totalprice partial0.5 "$queryid"
client_case sf50-date-hybrid05 50 o_orderdate partial0.5 "$queryid"
server_case sf10-price-remote 10 o_totalprice read_sorted "$schema"
client_case sf10-price-hybrid05 10 o_totalprice partial0.5 "$schema"
client_case sf10-price-hybrid05-narrow 10 o_totalprice partial0.5 "$schema" o_orderkey,o_totalprice
echo perf_done >> "$R/status"
