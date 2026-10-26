# Local ClickHouse

## Bootstrap an Ubuntu experiment host

[`scripts/setup_ubuntu_experiments.sh`](../scripts/setup_ubuntu_experiments.sh)
sets up the whole host. Run it as your Ubuntu login user with sudo access, not
as root. It targets the 40-thread, two-NUMA-node machine described in
[benchmark_cgroups.md](../scripts/benchmark_cgroups.md); adjust the CPU/NUMA
assignments in `setup_benchmark_cgroups.sh` for another topology.

Copy the bootstrap script to a fresh host and run it:

```bash
bash setup_ubuntu_experiments.sh
```

For CloudLab, use the repository's root `profile.py`. Its startup service
downloads and invokes this bootstrap automatically. Set the `setupUser`
profile parameter to your CloudLab login (default: `awitt`). Follow progress
after connecting with `tail -f ~/experiment-setup.log`; setup failures are
also reported by the CloudLab startup service.

It installs build dependencies, [dbc](https://docs.columnar.tech/dbc/getting_started/installation/),
ClickHouse/SQLite ADBC drivers, and the ClickHouse server/client binary. It
clones `https://github.com/awitten1/duckdb-adbc-client.git`, branch
`feat/clickhouse-partial-sort`, into `~/duckdb-adbc-client`, initializes
submodules, and builds DuckDB, ADBC, and TPC-H. The selected remote branch must
contain `load_clickhouse_tpch.py` and the updated cgroup setup script.

The script installs an enabled `adbc-clickhouse.service` that recreates the
cgroups after reboot and starts ClickHouse in its CPU/memory group, running
as your login user. ClickHouse's watchdog is disabled so systemd tracks the
server directly. The service signals its main process on shutdown because
the process joins the experiment's `/clickhouse` cgroup. Logs go to journald.
The bootstrap then generates and loads TPC-H SF25 in DuckDB's cgroup. Data
persists across reboot; loading runs during setup, not on each boot.

`duckdb`, `dbc`, `clickhouse`, and `clickhouse-client` are available in subsequent
Bash SSH sessions, and `profile://myclickhouse` connects to the local server.
For the current shell after setup, run:

```bash
source "$HOME/.config/adbc/experiments.env"
cd "$HOME/duckdb-adbc-client"
clickhouse-client --query 'SHOW TABLES FROM tpch_sf25'
sudo journalctl -u adbc-clickhouse.service
```

Optional environment overrides keep the script's interface small:

```bash
REPO_DIR="$HOME/experiments" BUILD_JOBS=8 SCALE_FACTOR=25 \
    bash setup_ubuntu_experiments.sh
```

`REPO_URL` and `REPO_BRANCH` select another remote/branch. Existing checkouts
are reused without pulling or resetting changes, and matching TPC-H tables
are reused on reruns. Use an absolute `REPO_DIR`. Allow disk space for the
checkout/build, generated DuckDB database, Parquet files, and ClickHouse data.

## Start ClickHouse manually

From the repository root, run:

```sh
./clickhouse/start-clickhouse.sh
```

On first launch, this downloads ClickHouse 26.9.12.8 into `clickhouse/.clickhouse/`
and verifies its SHA-512 checksum. Requires Linux (x86_64 or ARM64), Bash, curl,
tar, and sha512sum; no root access or Docker is needed. The single ClickHouse
binary includes both the server and client. To install without starting:

```sh
./clickhouse/install-clickhouse.sh
```

To use an existing binary instead, select it by absolute path:

```sh
CLICKHOUSE_BIN=/absolute/path/to/clickhouse ./clickhouse/start-clickhouse.sh
```

The server runs in the foreground; stop it with Ctrl-C. Data persists in
`clickhouse/clickhouse-data/`. HTTP listens on `127.0.0.1:8123` and the native
protocol on `127.0.0.1:9000`, using the default user with an empty password.
The launcher defaults to UTC when `TZ` is unset.
The default profile uses LZ4 transport compression for compatibility with the
ClickHouse ADBC 0.1.1 driver.

In another terminal, connect with:

```sh
TZ=UTC ./clickhouse/.clickhouse/bin/clickhouse-client
```

Based on the [data_transfer_benchmarks launcher](https://github.com/awitten1/data_transfer_benchmarks/tree/main/clickhouse).

## Load TPC-H at scale factor 25

With ClickHouse running, execute from the repository root:

```sh
python3 scripts/load_clickhouse_tpch.py --url http://localhost:8123/
```

Requires Python 3 and a DuckDB CLI with the `tpch` and `parquet` extensions.
The script uses `duckdb` on PATH when available, otherwise `build/release/duckdb`;
select another binary with `--duckdb /path/to/duckdb`. It runs `INSTALL tpch;
LOAD tpch; CALL dbgen(sf = 25);`, so the first run may need network access to
install a compatible extension. No ADBC driver or Python packages are required
for loading: Parquet files are streamed directly to ClickHouse over HTTP.

All eight tables (`region`, `nation`, `supplier`, `customer`, `part`, `partsupp`,
`orders`, and `lineitem`) are generated in `build/tpch-sf25/tpch.duckdb`, then
exported as individual ZSTD-compressed Parquet files in the same directory.
`manifest.json` records their row counts and ClickHouse column types. Decimal
columns remain decimals, and dates remain dates. Keep disk space available for
the DuckDB database, Parquet exports, and ClickHouse data simultaneously.

The persistent ClickHouse database is `tpch_sf25`. Tables use MergeTree, ordered
by their keys: for example, `orders` uses `o_orderkey` and `lineitem` uses
`(l_orderkey, l_linenumber)`. Each import goes into a staging table, verifies its
row count, then renames it to the final table. Existing tables with matching
schemas and row counts are skipped; other existing tables cause an error.
Reruns reuse the generated DuckDB database and verified Parquet files without
appending duplicate rows or replacing existing target tables. Matching counts
and schemas do not constitute a content checksum.

Useful options:

```sh
# Quick end-to-end check before generating SF25.
python3 scripts/load_clickhouse_tpch.py --scale-factor 0.01

# Generate/export independently of the server, then load later without DuckDB.
python3 scripts/load_clickhouse_tpch.py --generate-only --threads 16 --memory-limit 8GiB
python3 scripts/load_clickhouse_tpch.py --load-only --url http://localhost:8123/

# Select another location or destination database.
python3 scripts/load_clickhouse_tpch.py --data-dir /data/tpch-sf25 --database tpch_sf25
```

Set `CLICKHOUSE_USER` (or `--user`) and `CLICKHOUSE_PASSWORD` for authenticated
servers. `--url` defaults to `ADBC_CLICKHOUSE_URL`, then `http://localhost:8123/`.
For resource isolation, use the existing [cgroup launch commands](../scripts/benchmark_cgroups.md)
with this Python script in place of the benchmark script; start ClickHouse in
its own group first. `--threads` and `--memory-limit` configure DuckDB generation
and export, while ClickHouse retains its server settings.

The tables remain available for manual profiling, for example after loading
the ADBC extension and configuring a ClickHouse connection profile:

```sql
EXPLAIN ANALYZE
SELECT * FROM read_adbc_partial_sort(
    'profile://myclickhouse', 'tpch_sf25', 'lineitem',
    partition_key := 'l_orderkey',
    sort_columns := ['l_partkey', 'l_orderkey', 'l_linenumber'],
    pushdown_ratio := 0.5
);
```
