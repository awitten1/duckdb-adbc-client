#!/usr/bin/env bash
# Build the two ClickHouse ADBC connector variants used in this report, on the DuckDB node, and an ADBC profile
# for each. Both are upstream v0.1.1 plus the parallel-streams report's connector.diff (ExecuteSchema via DESCRIBE,
# Date/Decimal fixes, SESSION_IS_LOCKED retry). They differ only in HTTP transport compression:
#   nocompress  connector.diff as is: .with_compression(Compression::None)   -> profile://myclickhouse
#   lz4         that one line removed, so the upstream default Compression::Lz4 applies (ClickHouse sends its own
#               LZ4 blocks, as with compress=1)                                  -> profile://myclickhouse-lz4
# Requires a Rust toolchain (rustup); the crate pins its own toolchain version.
set -euo pipefail
REPO=${REPO:-$HOME/duckdb-adbc-client}
CLICKHOUSE_URL=${CLICKHOUSE_URL:-http://10.10.1.1:8123/}
mkdir -p ~/src ~/adbc-libs ~/.config/adbc/profiles
cd ~/src
[[ -d adbc_clickhouse ]] || git clone -q --branch v0.1.1 https://github.com/ClickHouse/adbc_clickhouse.git
cd adbc_clickhouse
git checkout -q v0.1.1 -- src
git apply "$REPO/reports/parallel-streams-20261110/connector.diff"

# The C entry points (AdbcDriverInit, AdbcClickhouseInit) are only exported with the `ffi` feature.
cargo build --release --features ffi
cp target/release/libadbc_clickhouse.so ~/adbc-libs/libadbc_clickhouse-nocompress.so

python3 - <<'EOF'
from pathlib import Path
p = Path("src/lib.rs")
s = p.read_text()
old = """            .with_product_info("adbc_clickhouse", env!("CARGO_PKG_VERSION"))
            .with_compression(Compression::None);
"""
assert s.count(old) == 1
s = s.replace(old, """            .with_product_info("adbc_clickhouse", env!("CARGO_PKG_VERSION"));
""")
p.write_text(s.replace("use clickhouse::{Client, Compression};", "use clickhouse::Client;"))
EOF
cargo build --release --features ffi
cp target/release/libadbc_clickhouse.so ~/adbc-libs/libadbc_clickhouse-lz4.so

for variant in nocompress lz4; do
    name=myclickhouse
    [[ $variant == lz4 ]] && name=myclickhouse-lz4
    printf 'profile_version = 1\ndriver = "%s"\n[Options]\nuri = "%s"\n' \
        "$HOME/adbc-libs/libadbc_clickhouse-$variant.so" "$CLICKHOUSE_URL" > ~/.config/adbc/profiles/$name.toml
done
ls -la ~/adbc-libs
