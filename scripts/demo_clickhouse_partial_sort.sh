#!/usr/bin/env bash
# Start a persistent local demo; all database access and loading use ADBC.
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
duckdb="$repo_dir/build/release/duckdb"
extension="$repo_dir/build/release/extension/adbc/adbc.duckdb_extension"
state_dir="$repo_dir/build/partial-sort-demo"
container="adbc-partial-sort-demo"
port=18123
rows=1000000
image="clickhouse/clickhouse-server:26.8.2.7"
label="dev.duckdb-adbc.partial-sort-demo"

usage() {
    cat <<'EOF'
Usage: scripts/demo_clickhouse_partial_sort.sh [options]

Start ClickHouse in Docker, generate an events dataset through ADBC, and print
copyable DuckDB commands for read_adbc_partial_sort. The container and dataset
remain available after the script exits. Reruns reuse an existing dataset.

Options:
  --rows N           Dataset rows (default: 1000000; maximum: 999999999)
  --port N           Local HTTP port, bound to 127.0.0.1 (default: 18123)
  --container NAME   Docker container name (default: adbc-partial-sort-demo)
  --state-dir PATH   Generated profiles/SQL (default: build/partial-sort-demo)
  --duckdb PATH      DuckDB executable (default: build/release/duckdb)
  --extension PATH   Built ADBC extension
  -h, --help         Show this help

Requires Docker, Python 3, dbc, and a local extension build:
  GEN=ninja make release

If absent, the ClickHouse ADBC driver is installed with dbc. No existing
container is reused unless it carries this script's demo label.
EOF
}

die() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

while (($#)); do
    case "$1" in
        -h|--help) usage; exit 0 ;;
        --rows|--port|--container|--state-dir|--duckdb|--extension)
            (($# >= 2)) || die "Missing value for $1"
            case "$1" in
                --rows) rows="$2" ;;
                --port) port="$2" ;;
                --container) container="$2" ;;
                --state-dir) state_dir="$2" ;;
                --duckdb) duckdb="$2" ;;
                --extension) extension="$2" ;;
            esac
            shift 2
            ;;
        *) die "Unknown option: $1 (use --help)" ;;
    esac
done

[[ "$rows" =~ ^[1-9][0-9]{0,8}$ ]] || die "--rows must be an integer from 1 to 999999999"
[[ "$port" =~ ^[0-9]{1,5}$ ]] || die "--port must be an integer from 1 to 65535"
port=$((10#$port))
((port >= 1 && port <= 65535)) || die "--port must be from 1 to 65535"
[[ "$container" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]] || die "Invalid Docker container name"
for dependency in docker python3 dbc; do
    command -v "$dependency" >/dev/null 2>&1 || die "Install $dependency first (see README.md)"
done
[[ -x "$duckdb" ]] || die "DuckDB not found at $duckdb. Run: GEN=ninja make release"
[[ -f "$extension" ]] || die "Extension not found at $extension. Run: GEN=ninja make release"
docker info >/dev/null 2>&1 || die "Docker is unavailable; start Docker and check daemon access"

if ! dbc list --json | python3 -c '
import json, sys
drivers = json.load(sys.stdin)["payload"]["drivers"]
sys.exit(0 if any(driver["driver"] == "clickhouse" for driver in drivers) else 1)
'; then
    printf 'Installing the ClickHouse ADBC driver...\n'
    dbc install 'clickhouse=0.1.1'
fi


if docker container inspect "$container" >/dev/null 2>&1; then
    owner="$(docker inspect --format "{{index .Config.Labels \"$label\"}}" "$container")"
    [[ "$owner" == true ]] || die "Container $container is not owned by this demo; choose another --container"
    if [[ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]]; then
        printf 'Starting existing ClickHouse container %s...\n' "$container"
        docker start "$container"
    fi
else
    printf 'Launching ClickHouse %s on 127.0.0.1:%s...\n' "$image" "$port"
    docker run --detach --name "$container" --label "$label=true" \
        --publish "127.0.0.1:$port:8123" --env CLICKHOUSE_SKIP_USER_SETUP=1 "$image"
fi
published="$(docker port "$container" 8123/tcp)"
[[ "$published" == "127.0.0.1:$port" ]] || die "Container publishes $published; rerun with the matching --port or choose another --container"

mkdir -p -- "$state_dir"
state_dir="$(cd -- "$state_dir" && pwd)"
python3 - "$state_dir" "$extension" "$port" "$rows" <<'PY'
import os
from pathlib import Path
import sys

directory, extension, port, rows = sys.argv[1:]
directory = Path(directory)
extension = Path(extension).resolve()
rows = int(rows)
profile_path = directory / "clickhouse.toml"
profile_path.write_text(
    'profile_version = 1\ndriver = "clickhouse"\n\n[Options]\n'
    f'uri = "http://127.0.0.1:{port}/"\n'
)
profile_path.chmod(0o600)

def sql(value):
    return "'" + str(value).replace("'", "''") + "'"

profile = sql("profile://" + str(profile_path))
load = f"LOAD {sql(extension)};\n"
database = "adbc_partial_sort_demo"
table = f"events_{rows}"
source = f"{database}.{table}"

def function(ratio, aligned=False):
    keys = "['id']" if aligned else "['score', 'id']"
    directions = "[false]" if aligned else "[true, false]"
    return (
        f"read_adbc_partial_sort(\n    {profile}, {sql(database)}, {sql(table)},\n"
        f"    partition_key := 'id', sort_columns := {keys},\n"
        f"    descending := {directions}, pushdown_ratio := {ratio}\n)"
    )

(directory / "ready.sql").write_text(
    load + f"SELECT * FROM read_adbc({profile}, 'SELECT 1 AS ready');\n"
)
create = (
    f"CREATE TABLE IF NOT EXISTS {source} ENGINE = MergeTree ORDER BY id AS "
    "SELECT number AS id, "
    "if(number % 17 = 0, NULL, toInt64(number * 7919 % 1000003)) AS score, "
    f"concat('event-', toString(number)) AS payload FROM numbers({rows})"
)
(directory / "setup.sql").write_text(
    load
    + f"CALL adbc_execute({profile}, {sql('CREATE DATABASE IF NOT EXISTS ' + database)});\n"
    + f"CALL adbc_execute({profile}, {sql(create)});\n"
    + f"SELECT CASE WHEN n = {rows} THEN n ELSE error('Demo table has an unexpected row count') END AS loaded_rows "
    + f"FROM read_adbc({profile}, {sql('SELECT count() AS n FROM ' + source)});\n"
    + "SELECT id, score, payload FROM " + function("0.3") + " LIMIT 5;\n"
)
(directory / "init.sql").write_text(load)
examples = (
    ".timer on\n\n"
    "-- Sort by score DESC, id ASC, with 30% of the id range assigned to ClickHouse.\n"
    "-- NULL scores sort last. No outer ORDER BY is needed for this direct scan.\n"
    "SELECT id, score, payload FROM " + function("0.3") + " LIMIT 20;\n\n"
    "-- Output ordering aligned with the physical storage key.\n"
    "SELECT id, score FROM " + function("0.3", aligned=True) + " LIMIT 20;\n\n"
    "-- Consume all rows to compare local, partial, and remote sorting.\n"
    "-- LIMIT previews do not measure the cost of transferring the complete result.\n"
)
for ratio in ("0", "0.3", "1"):
    examples += (
        "COPY (SELECT * FROM " + function(ratio) + ")\n"
        f"TO {sql(os.devnull)} (FORMAT CSV);\n\n"
    )
(directory / "examples.sql").write_text(examples)
PY

printf 'Waiting for ClickHouse through ADBC'
ready=false
for ((attempt=1; attempt<=60; attempt++)); do
    if "$duckdb" -unsigned -batch -bail -no-agent < "$state_dir/ready.sql" > "$state_dir/ready.log" 2>&1; then
        ready=true
        break
    fi
    printf '.'
    sleep 1
done
printf '\n'
if [[ "$ready" != true ]]; then
    cat "$state_dir/ready.log" >&2
    die "ClickHouse did not become ready through ADBC; inspect: docker logs $container"
fi

printf 'Loading/reusing %s generated events through ADBC (physical ORDER BY id)...\n' "$rows"
if ! "$duckdb" -unsigned -batch -bail -no-agent < "$state_dir/setup.sql" > "$state_dir/setup.log" 2>&1; then
    cat "$state_dir/setup.log" >&2
    die "Demo setup failed; details are in $state_dir/setup.log"
fi
cat "$state_dir/setup.log"

boundary=$((rows - rows * 3 / 10))
printf '\nDataset: adbc_partial_sort_demo.events_%s; id range [0, %s].\n' "$rows" "$((rows - 1))"
printf 'At ratio 0.3: DuckDB sorts id < %s; ClickHouse sorts id >= %s.\n' "$boundary" "$boundary"
printf '\nOpen DuckDB (extension already loaded):\n\n  '
printf '%q -unsigned -init %q\n' "$duckdb" "$state_dir/init.sql"
printf '\nThen paste these DuckDB commands:\n\n'
cat "$state_dir/examples.sql"
printf 'Or run every saved example directly from your shell:\n\n  '
printf '%q -unsigned -batch -bail -init %q < %q\n' "$duckdb" "$state_dir/init.sql" "$state_dir/examples.sql"
printf '\nGenerated profile and SQL: %s\n' "$state_dir"
printf 'Stop ClickHouse when finished (the dataset remains in the container):\n\n  '
printf 'docker stop %q\n' "$container"
