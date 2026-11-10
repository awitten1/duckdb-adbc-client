# Local workstation experiments

Measured SF5 and SF10 results, including charts, are in the tracked
[experiment reports](../reports/README.md).

Use this workflow on the current single-socket workstation. It does not use
sudo, systemd units, cgroup writes, or the CloudLab checkout. It keeps its
generated profile and ClickHouse configuration under ignored
`build/local-experiments/` and uses the repository's existing
`clickhouse/clickhouse-data/` directory.

The planner groups allowed logical CPUs by physical core. On this 12-core,
24-thread machine it gives four physical cores (both SMT threads) to each
engine and leaves four cores for desktop activity. It currently proposes a
13 GiB ClickHouse server cap and a 9 GiB DuckDB query budget, based on
available memory. The exact memory plan is recalculated by `prepare`.
CPU affinity is not exclusive: other processes may still use the same CPUs.
The DuckDB memory setting controls query allocations, while ADBC and other
process memory remain outside that limit. This is a local functional and
development setup, not a controlled CloudLab performance comparison.

## Prepare

From the repository root:

```bash
python3 scripts/local_experiments.py plan
bash scripts/build_local_experiments.sh
python3 scripts/local_experiments.py prepare
```

The build uses the checked-in DuckDB submodule and a writable local ccache
directory. Set `LOCAL_BUILD_JOBS=2` if four simultaneous compiler jobs use too
much memory. It also builds and installs the matching TPC-H extension locally.
The build can take several minutes on a fresh checkout.

`prepare` requires a `clickhouse` executable on `PATH` (or `CLICKHOUSE_BIN`),
the built DuckDB CLI and ADBC extension, and the ClickHouse ADBC driver under
`ADBC_DRIVER_PATH` (default `~/.config/adbc/drivers`). The existing `dbc`
installation can supply that driver with `dbc install 'clickhouse=0.1.1'`.
It writes an ADBC profile for `127.0.0.1:8123` and a local ClickHouse XML
config with the planned server memory cap. Re-run `prepare` if available CPUs
or memory change substantially.

To use a specific connector build, set `ADBC_EXPERIMENT_DRIVER` to the
absolute path of its shared library when running `prepare`:

```bash
ADBC_EXPERIMENT_DRIVER=/absolute/path/to/libadbc_clickhouse.so \
  python3 scripts/local_experiments.py prepare
```

The selection is stored in the ignored local config and retained by later
`prepare` runs. Set `ADBC_EXPERIMENT_DRIVER=clickhouse` to return to driver
discovery through `ADBC_DRIVER_PATH`. The SF10
[perf rerun](../reports/sf10/PERF_SCHEMA_OFF.md) used this option for the
schema-only, HTTP-compression-off connector.

## Start and check

In one terminal, start ClickHouse in the foreground:

```bash
python3 scripts/local_experiments.py server
```

Stop it with Ctrl-C. It uses the existing repository data directory. In
another terminal, run the ADBC integration suite through the local DuckDB CPU
set:

```bash
python3 scripts/local_experiments.py run -- python3 scripts/test_clickhouse_partial_sort.py \
  --duckdb build/release/duckdb \
  --extension build/release/extension/adbc/adbc.duckdb_extension \
  --profile profile://./build/local-experiments/clickhouse.toml
```

`run -- COMMAND ...` also wraps any other command in the DuckDB CPU set and
sets driver discovery. It does not impose a generic hard memory limit; pass
the intended `--memory-limit` to DuckDB workloads.

## Load and measure

The local default is TPC-H scale factor 5, with 7.5 million `orders` rows.
This is smaller than the CloudLab SF25 workload. The loader reuses its
existing data and verifies tables before skipping them on subsequent runs.

```bash
python3 scripts/local_experiments.py load
python3 scripts/local_experiments.py benchmark
```

`benchmark` runs the synthetic hybrid-sort sweep with 100,000 rows, one
measured repetition, and output in `build/local-experiments/`. For a local
TPC-H orders comparison after loading SF5:

```bash
python3 scripts/local_experiments.py run -- python3 scripts/experiments/run_cases.py \
  --root build/local-experiments/sf5-cases \
  --profile profile://./build/local-experiments/clickhouse.toml \
  --database tpch_sf5 --expected-rows 7500000 \
  --threads 4 --memory-limit 9GiB --no-server-metrics \
  --cases partial0 partial0.25 partial0.5 partial0.75 partial1 native read_sorted
```

Add `--sort-columns o_orderkey`, `--sort-columns o_totalprice`, or another
source column to sweep different output orders with the same partition key.

Use a new `--root` for each `run_cases.py` invocation because that runner
refuses to overwrite existing results. `load --scale-factor 0.01` is useful
for a quicker setup; then select `tpch_sf0_01` and its matching row count in
the comparison command.

Each `server`, `load`, `benchmark`, and `run` action accepts `--dry-run` before
its other arguments to show the command and CPU set without starting it.
