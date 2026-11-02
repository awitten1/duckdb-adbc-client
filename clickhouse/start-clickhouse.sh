#!/usr/bin/env bash
set -euo pipefail
export TZ="${TZ:-UTC}"

work_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
clickhouse_bin="${CLICKHOUSE_BIN:-$work_dir/.clickhouse/bin/clickhouse}"
if [[ -z "${CLICKHOUSE_BIN:-}" && ! -x "$clickhouse_bin" ]]; then
    bash "$work_dir/install-clickhouse.sh"
fi
mkdir -p "$work_dir/clickhouse-data"
cd "$work_dir/clickhouse-data"
exec "$clickhouse_bin" server --config-file="$work_dir/clickhouse-config.xml"
