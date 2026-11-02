#!/usr/bin/env bash
# Run as your Ubuntu login user (not root); sudo is used for host configuration.
# CPU/NUMA assignments come from setup_benchmark_cgroups.sh: a 40-thread, 2-socket host.
set -euo pipefail

repo_url="${REPO_URL:-https://github.com/awitten1/duckdb-adbc-client.git}"
branch="${REPO_BRANCH:-feat/clickhouse-partial-sort}"
repo="${REPO_DIR:-$HOME/duckdb-adbc-client}"

[[ $EUID -ne 0 ]] || { echo 'Run as your login user, with sudo access.' >&2; exit 1; }
[[ -f /sys/fs/cgroup/cgroup.controllers ]] || { echo 'Requires cgroup v2.' >&2; exit 1; }

# Compiler, build tools, Python, downloads, and setpriv for experiment commands.
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build git python3 curl \
    ca-certificates pkg-config libssl-dev unzip xz-utils util-linux

# Existing checkouts are reused without resetting or pulling local changes.
if [[ ! -d "$repo/.git" ]]; then
    git clone --branch "$branch" --recurse-submodules "$repo_url" "$repo"
fi
cd "$repo"
git submodule update --init --recursive
[[ -f scripts/load_clickhouse_tpch.py && -f scripts/run_duckdb_cgroup.sh ]] || {
    echo 'This checkout needs the current setup scripts; use the branch containing them.' >&2
    exit 1
}

# Keep the CLIs and driver discovery available in subsequent SSH shells.
mkdir -p "$HOME/.local/bin" "$HOME/.config/adbc/profiles"
cat > "$HOME/.config/adbc/experiments.env" <<'EOF'
export PATH="$HOME/.local/bin:$PATH"
export ADBC_DRIVER_PATH="$HOME/.config/adbc/drivers"
EOF
source "$HOME/.config/adbc/experiments.env"
touch "$HOME/.bashrc"
grep -qxF 'source "$HOME/.config/adbc/experiments.env"' "$HOME/.bashrc" || \
    echo 'source "$HOME/.config/adbc/experiments.env"' >> "$HOME/.bashrc"
if ! command -v dbc >/dev/null; then
    curl -fsSL https://dbc.columnar.tech/install.sh | sh
fi
dbc install 'clickhouse=0.1.1'
dbc install sqlite

# Build a matching DuckDB/ADBC/TPC-H set; install TPC-H locally rather than downloading a dev build.
GEN=ninja CMAKE_BUILD_PARALLEL_LEVEL="${BUILD_JOBS:-8}" make release \
    EXT_FLAGS='-DBUILD_EXTENSIONS=tpch -DLINK_CORE_EXTENSIONS=ON'
./build/release/duckdb -unsigned -batch -bail -c \
    "INSTALL 'build/release/extension/tpch/tpch.duckdb_extension'; LOAD tpch;"
bash clickhouse/install-clickhouse.sh
ln -sfn "$repo/build/release/duckdb" "$HOME/.local/bin/duckdb"
ln -sfn "$repo/clickhouse/.clickhouse/bin/clickhouse" "$HOME/.local/bin/clickhouse"
ln -sfn "$repo/clickhouse/.clickhouse/bin/clickhouse-client" "$HOME/.local/bin/clickhouse-client"
cat > "$HOME/.config/adbc/profiles/myclickhouse.toml" <<'EOF'
profile_version = 1
driver = "clickhouse"
[Options]
uri = "http://127.0.0.1:8123/"
EOF

# systemd owns the resource limits and starts ClickHouse after each reboot.
sudo bash scripts/setup_benchmark_cgroups.sh "$(id -un)"
sudo systemctl restart adbc-clickhouse.service
for attempt in {1..120}; do
    if curl -fsS --max-time 2 http://127.0.0.1:8123/ping >/dev/null 2>&1; then break; fi
    sleep 1
done
curl -fsS --max-time 2 http://127.0.0.1:8123/ping >/dev/null

# Generate/export in DuckDB's slice; ClickHouse imports within its own slice.
bash scripts/run_duckdb_cgroup.sh python3 scripts/load_clickhouse_tpch.py \
    --duckdb "$repo/build/release/duckdb" --scale-factor "${SCALE_FACTOR:-25}" \
    --threads 16 --memory-limit 56GiB --url http://127.0.0.1:8123/

echo "Ready: $repo (ADBC profile: profile://myclickhouse)"
echo 'Server logs: sudo journalctl -u adbc-clickhouse.service'
