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
the extension LOAD command and minimal partial, partitioned, and remote-sort
queries. The container and dataset remain available after the script exits.
Reruns reuse an existing dataset.

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
    if ! install_output="$(dbc install 'clickhouse=0.1.1' 2>&1)"; then
        printf '%s\n' "$install_output" >&2
        die "Could not install the ClickHouse ADBC driver"
    fi
fi


if docker container inspect "$container" >/dev/null 2>&1; then
    owner="$(docker inspect --format "{{index .Config.Labels \"$label\"}}" "$container")"
    [[ "$owner" == true ]] || die "Container $container is not owned by this demo; choose another --container"
    if [[ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]]; then
        docker start "$container" >/dev/null
    fi
else
    if ! container_output="$(docker run --detach --name "$container" --label "$label=true" \
        --publish "127.0.0.1:$port:8123" --env CLICKHOUSE_SKIP_USER_SETUP=1 "$image" 2>&1)"; then
        printf '%s\n' "$container_output" >&2
        die "Could not launch ClickHouse container $container"
    fi
fi
published="$(docker port "$container" 8123/tcp)"
[[ "$published" == "127.0.0.1:$port" ]] || die "Container publishes $published; rerun with the matching --port or choose another --container"

mkdir -p -- "$state_dir"
state_dir="$(cd -- "$state_dir" && pwd)"
python3 - "$state_dir" "$extension" "$port" "$rows" <<'PY'
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

def function(partitioned=False):
    name = "read_adbc_partitioned_sort" if partitioned else "read_adbc_partial_sort"
    return (
        f"{name}(\n    {profile}, {sql(database)}, {sql(table)},\n"
        "    partition_key := 'id', sort_columns := ['score'], pushdown_ratio := 0.3\n)"
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
    + "SELECT * FROM " + function() + " LIMIT 10;\n"
    + "SELECT * FROM " + function(partitioned=True) + " LIMIT 10;\n"
)
(directory / "init.sql").write_text(load)
remote_query = f"SELECT * FROM {source} ORDER BY score ASC NULLS LAST"
(directory / "examples.sql").write_text(
    load
    + "\nSELECT * FROM " + function() + " LIMIT 10;\n"
    + "\nSELECT * FROM " + function(partitioned=True) + " LIMIT 10;\n"
    + f"\nSELECT * FROM read_adbc(\n    {profile},\n    {sql(remote_query)}\n) LIMIT 10;\n"
)
PY

ready=false
for ((attempt=1; attempt<=60; attempt++)); do
    if "$duckdb" -unsigned -batch -bail -no-agent < "$state_dir/ready.sql" > "$state_dir/ready.log" 2>&1; then
        ready=true
        break
    fi
    sleep 1
done
if [[ "$ready" != true ]]; then
    cat "$state_dir/ready.log" >&2
    die "ClickHouse did not become ready through ADBC; inspect: docker logs $container"
fi

if ! "$duckdb" -unsigned -batch -bail -no-agent < "$state_dir/setup.sql" > "$state_dir/setup.log" 2>&1; then
    cat "$state_dir/setup.log" >&2
    die "Demo setup failed; details are in $state_dir/setup.log"
fi
cat "$state_dir/examples.sql"
