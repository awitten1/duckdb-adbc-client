#!/usr/bin/env bash
# Run the modified ClickHouse build on its own data directory, in the ClickHouse
# slice, on the usual ports. The regular 26.9 service is stopped while it runs.
#   start_modified_clickhouse.sh start   # stop 26.9, copy orders parts, start modified
#   start_modified_clickhouse.sh stop    # stop modified, restart the 26.9 service
set -euo pipefail
REPO=/users/awitt/duckdb-adbc-client
BIN=${CLICKHOUSE_BIN:-/proj/slogora-PG0/ClickHouse/build/programs/clickhouse}   # override to run another build
DATA=$REPO/build/clickhouse-modified-data
E=$REPO/build/experiments/modified-clickhouse-20261110
UNIT=adbc-clickhouse-modified
Q() { curl -fsS http://127.0.0.1:8123/ --data-binary "$1"; }
wait_up() { for _ in $(seq 1 120); do Q "SELECT 1" >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }

case ${1:-} in
start)
    sudo systemctl stop adbc-clickhouse.service
    mkdir -p "$DATA"
    sudo systemd-run --unit="$UNIT" --slice=adbc-clickhouse.slice --uid="$(id -u)" --gid="$(id -g)" \
        --working-directory="$DATA" -p LimitNOFILE=262144 \
        --setenv=TZ=UTC --setenv=HOME="$HOME" --setenv=CLICKHOUSE_WATCHDOG_ENABLE=0 \
        "$BIN" server --config-file="$E/clickhouse-config-modified.xml"
    wait_up || { echo "modified server did not start" >&2; sudo journalctl -u "$UNIT" -n 30 >&2; exit 1; }
    Q "SELECT version()"
    for db in tpch_sf5 tpch_sf10 tpch_sf50; do
        if [[ $(Q "EXISTS TABLE $db.orders") == 1 ]]; then echo "$db.orders already attached"; continue; fi
        Q "CREATE DATABASE IF NOT EXISTS $db"
        Q "$(cat "$E/$db.orders.sql")"
        Q "SYSTEM STOP MERGES $db.orders"   # keep the copied physical layout
        old_path=$(cat "$E/$db.orders.path")   # recorded from the 26.9 server
        new_path=$(Q "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='$db' AND name='orders' FORMAT TSVRaw")
        while read -r part; do
            cp -a "$old_path/$part" "$new_path/detached/$part"
            Q "ALTER TABLE $db.orders ATTACH PART '$part'"
        done < "$E/$db.parts"
        echo "$db.orders: $(Q "SELECT count() FROM $db.orders") rows in $(Q "SELECT count() FROM system.parts WHERE database='$db' AND table='orders' AND active") parts"
    done
    ;;
stop)
    sudo systemctl stop "$UNIT" || true
    sudo systemctl start adbc-clickhouse.service
    wait_up && Q "SELECT version()"
    ;;
*) echo "usage: $0 start|stop" >&2; exit 2 ;;
esac
