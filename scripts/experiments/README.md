# Sort and transport experiments

For development on the current workstation, follow the smaller
[local experiment workflow](../local_experiments.md). The commands below
target the CloudLab resource slices and SF25 dataset.

The measured local SF5 and SF10 results are in the tracked
[experiment reports](../../reports/README.md).

These runners preserve the SF25 and transport experiments previously saved under
ignored `build/` directories. Run them from the repository root. Keep outputs
under `build/experiments/` (ignored by Git), or outside the checkout. Use a fresh
`--root` for each experiment; saved case names are not overwritten.

| Script | Experiment |
|---|---|
| `run_cases.py` | Individual SF25 ADBC sort/scan cases, JSON profiles, and process/server counters |
| `compare_sort_versions.py` | Warmups and repeated original/optimized comparisons, full-row checksums, ordering checks, alternating-key input, and integration tests |
| `record_queries.py` | Full Native/Arrow transfers, query and processor logs, optionally server-only Linux `perf` recordings |
| `measure_no_strings.py` | Shuffled all-column versus no-string timing runs in both formats, optionally followed by numeric-only server profiles |

For generated dense/skewed datasets and arbitrary pushdown-ratio sweeps, use the
existing [synthetic benchmark](../benchmark_clickhouse_partial_sort.py).

## Environment

The defaults reproduce the CloudLab workload: ClickHouse database `tpch_sf25`,
table `orders`, all 37,500,000 rows, ordering by `o_orderdate ASC NULLS LAST`,
16 threads, and a DuckDB memory budget of `56GiB`. The ADBC range split uses
`o_orderkey`; its ratio measures key-range width rather than row count.

- Generate/load data with [load_clickhouse_tpch.py](../load_clickhouse_tpch.py)
  (default SF25). This loader uses HTTP for loading; the sort queries use ADBC.
- Build matching DuckDB and ADBC binaries. Before/after extension libraries must
  target the same DuckDB build and platform. Pass the preserved baseline library
  explicitly; these scripts never rebuild, swap libraries, or change source code.
- The ADBC runners default to `profile://myclickhouse`; use `--profile` to select
  another profile. Configure `ADBC_DRIVER_PATH` if needed.
- Native control queries default to
  `clickhouse/.clickhouse/bin/clickhouse-client`, using its default local server
  connection. Use `--clickhouse-client` to select another executable or a wrapper
  that supplies connection options. `--url` selects the HTTP endpoint for curl.
- Set up the [resource slices](../benchmark_cgroups.md) before performance runs.
  ClickHouse runs in its own slice; launch the entire runner in the DuckDB slice.
  Defaults give each engine 16 logical CPUs on separate NUMA nodes and a 64 GiB
  cap. Avoid concurrent experiments or other queries; profiling runners reject
  other active ClickHouse queries. Unrelated host processes are not excluded by
  these CPU sets.
- Server log collection requires `SYSTEM FLUSH LOGS` and access to
  `system.query_log` and `system.processors_profile_log`. Run as the login user.
  The default service is `adbc-clickhouse.service`; `--server-pid` supports servers
  started without systemd. Sort runners can use `--no-server-metrics` to skip
  systemd, `/proc`, and native-client log collection.

The profiling commands require Linux `perf`, sudo access (`sudo -v` beforehand),
and matching ClickHouse debug symbols for useful stacks. No source build is
required. Recordings attach only to the ClickHouse process, across all its
threads; curl and unrelated processes are excluded from the CPU samples.

## SF25 sort cases

```bash
bash scripts/run_duckdb_cgroup.sh python3 scripts/experiments/run_cases.py \
  --root build/experiments/sf25-current \
  --tag current --threads 16 \
  --cases partial0 partial0.5 partial1 native read_sorted
```

| Case | Meaning |
|---|---|
| `partial0` | All sorting in DuckDB through the table function |
| `partial0.25` | Upper quarter of the integer key range sorted remotely |
| `partial0.5` | Hybrid sort with half the integer key range assigned remotely |
| `partial0.75` | Upper three quarters of the integer key range sorted remotely |
| `partial1` | All sorting remotely through the table function |
| `native` | Historical label for a DuckDB `ORDER BY` over unsorted `read_adbc` output; **not** ClickHouse's Native protocol |
| `read_sorted` | `read_adbc` with the complete remote `ORDER BY` |
| `read` | Unsorted transfer baseline |

One invocation measures each listed case once. It writes the exact SQL,
`EXPLAIN (ANALYZE, FORMAT JSON)` output, timing/resource summaries, stderr, and
server query logs when server metrics are enabled. All rows are consumed. The CLI
wall timer includes startup, binding, and cleanup; server sampling can add up to
0.5 seconds of delay. Stage timers can
overlap; do not sum all profiler fields to estimate wall time.

Use `--dry-run` to write/print SQL without loading a driver or contacting a
server. `--database`, `--expected-rows`, `--projection`, `--partition-key`,
`--sort-columns`, `--memory-limit`, `--duckdb`, and `--extension` allow controlled variations.
The default sort column is `o_orderdate`; `--sort-columns` accepts one or more
exact source column names. `--expected-rows` checks
the profile's intermediate row count and must match the source data.

For the repeated original/optimized comparison:

```bash
bash scripts/run_duckdb_cgroup.sh python3 scripts/experiments/compare_sort_versions.py \
  --root build/experiments/sf25-comparison \
  --original-extension /path/to/original/adbc.duckdb_extension \
  --optimized-extension build/release/extension/adbc/adbc.duckdb_extension \
  --warmups 1 --repeats 3 --seed 42
```

This shuffles the original 50/50, optimized 50/50, and DuckDB full-sort cases in
each measured round. It compares full-row hash sum/XOR and the exact decimal
price total against an ADBC scan, and checks global date ordering. It also
creates a unique temporary database containing eight million fully alternating
keys, compares both libraries, checks row values/order, then drops that database
in `finally`. An optional `--before-short-probe-extension` adds the intermediate
build to that microbenchmark; no old temporary build directory is assumed.

`--skip-interleaved` and `--skip-integration` omit those additional checks.
`--interleaved-rows` accepts a smaller even row count for smoke tests. The
comparison also accepts the database/profile/row count/thread/memory options
above. It saves its configuration and individual run evidence under `--root`.
Full-remote cases can be measured separately with `run_cases.py --cases partial1
read_sorted`, as in the original experiment.

## Output format and compression

Measure timings without `perf` first:

```bash
bash scripts/run_duckdb_cgroup.sh python3 scripts/experiments/record_queries.py \
  --root build/experiments/formats-timing --no-perf --repeats 3 \
  --cases native_tcp http_native http_arrow http_arrow_lz4
```

All cases fully transfer the same sorted results and discard them on the client.
`http_native` sets protocol revision 54492; `http_arrow` disables both transport
and Arrow IPC compression. `http_arrow_lz4` enables LZ4 transport only.
`native_tcp` disables compression and uses limited Pretty output while still
receiving every row. Do not substitute `clickhouse-client --format Null`: that
client can suppress data packets and invalidate the full-transfer comparison.

Every transfer checks the server's result-row count and a minimum network-byte
count (1 GB by default). For small fixtures, override `--expected-rows` and
`--min-network-bytes`. `--sql-file` selects another SELECT without trailing
SETTINGS; the default is the full SF25 orders sort. Each case has one warmup
unless `--no-warmup` is supplied. Cases run sequentially, grouped by format.

Capture server-only profiles separately from timing runs:

```bash
sudo -v
bash scripts/run_duckdb_cgroup.sh python3 scripts/experiments/record_queries.py \
  --root build/experiments/formats-perf --repeats 1 \
  --cases http_native http_arrow
```

Each measured case saves `server.data`, the exact command, stdout/stderr,
query IDs, query/processor counters, and client resource usage. `perf` records
hardware cycles at 199 Hz with DWARF stacks. Flamegraph widths represent
aggregate sampled CPU cycles across threads, rather than wall time.

With Brendan Gregg's FlameGraph tools installed, generate each SVG, for example:

```bash
perf script -i build/experiments/formats-perf/http_arrow-r0/server.data \
  | /path/to/FlameGraph/stackcollapse-perf.pl \
  | /path/to/FlameGraph/flamegraph.pl \
  > build/experiments/formats-perf/http_arrow-r0/server.svg
```

## Remove string columns

```bash
bash scripts/run_duckdb_cgroup.sh python3 scripts/experiments/measure_no_strings.py \
  --root build/experiments/no-strings --repeats 3 --warmups 1 --skip-profiles
```

This shuffles all-column and no-string queries in both Native and ArrowStream
formats. The numeric projection selects `o_orderkey`, `o_custkey`,
`o_totalprice`, `o_orderdate`, and `o_shippriority`. Both compression layers are
disabled. It writes raw timings, query/processor logs, transferred bytes, and
median summaries. Removing columns changes reading and sorting work too; this
is not an isolated measurement of string encoding.

Omit `--skip-profiles` to follow timing runs with server-only profiles of the
numeric projection. That invokes the adjacent checked-in `record_queries.py`;
no generated Python wrapper or machine-specific source path is needed.

## What belongs in Git

Track these runners, their methodology, and any focused source changes. Keep
TPC-H databases/Parquet, extension binaries, raw profiles, JSON/CSV results,
flamegraphs, and machine-specific paths under ignored `build/` output directories.
The historical evidence folders are snapshots; they are not runtime dependencies
of the checked-in runners.
