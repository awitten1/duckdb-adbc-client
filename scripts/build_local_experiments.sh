#!/usr/bin/env bash
# Build the matching DuckDB CLI, ADBC extension, and TPC-H extension as this user.
set -euo pipefail

repo="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo"
mkdir -p build/local-experiments/ccache
export CCACHE_DIR="$repo/build/local-experiments/ccache"
GEN=ninja CMAKE_BUILD_PARALLEL_LEVEL="${LOCAL_BUILD_JOBS:-4}" make release \
    EXT_FLAGS='-DBUILD_EXTENSIONS=tpch -DLINK_CORE_EXTENSIONS=ON'
./build/release/duckdb -unsigned -batch -bail -no-init -c \
    "INSTALL 'build/release/extension/tpch/tpch.duckdb_extension'; LOAD tpch;"
