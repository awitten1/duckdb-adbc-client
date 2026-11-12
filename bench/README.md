# Microbenchmarks

- [Arrow → DuckDB conversion](#arrow--duckdb-conversion): `arrow_conversion_bench`, `counters_per_row.py`, `ab_compare.py`
- [Parallel `read_adbc` streams](#parallel-read_adbc-streams): `parallel_read_adbc`
- [The same streams in DataFusion](#the-same-streams-in-datafusion): `datafusion/` (`parallel_read_datafusion`)

## Arrow → DuckDB conversion

`arrow_conversion_bench` times DuckDB's Arrow → DuckDB conversion
(`ArrowTableFunction::ArrowToDuckDB`), which `read_adbc` runs on every ClickHouse
result batch. It needs no database server or network connection.

Each fixture is one column shaped like a TPC-H `orders` column. DuckDB generates
it and exports it to Arrow in 65,536-row batches, in the formats ClickHouse sends:
plain `utf8` strings and `decimal128` decimals. The benchmark converts every batch in
2,048-row DuckDB chunks, exactly as the extension does. Before timing a fixture, it
compares every converted value with the source and exits on any mismatch, so the
same tool validates changes to the conversion code.

### Build and run

Requires Google Benchmark (Ubuntu: `apt install libbenchmark-dev`). The target is
not part of the default build:

```sh
ninja -C build/release arrow_conversion_bench
taskset -c 10 build/release/extension/adbc/bench/arrow_conversion_bench --benchmark_repetitions=3
```

Options:

- `--rows=N` sets the rows per fixture. The default is 4,194,304, which keeps each
  fixture larger than a 25 MB last-level cache.
- Standard Google Benchmark flags also work, such as `--benchmark_filter=string`.

Fixture expressions use only functions built into DuckDB's core library. The
statically linked benchmark does not load the `core_functions` extension.

Three more fixtures cover edge cases rather than TPC-H columns: `string_0_12`
(every inline string length, 0–12 bytes), and `string_19_78_nulls` and
`decimal_15_2_nulls` (10% NULLs). Verification treats NULL as equal only to NULL.

### Comparing two builds

`ab_compare.py` alternates two benchmark binaries for a number of rounds and
prints each fixture's median ns/row for both, with the speedup. Each run verifies
every converted value first, so a wrong result fails the comparison.

```sh
cp build/release/extension/adbc/bench/arrow_conversion_bench /tmp/conversion_before
# ...apply the change and rebuild...
taskset -c 10 python3 bench/ab_compare.py /tmp/conversion_before \
    build/release/extension/adbc/bench/arrow_conversion_bench 3 ab.json
```

The arguments are the two binaries, the number of rounds (default 2) and an
optional JSON file for the raw medians.

### Hardware counters

`counters_per_row.py` reports cycles, instructions and blocked store forwards
(`ld_blocks.store_forward`, an Intel event) per converted row. It runs each fixture
under `perf stat` at two fixed iteration counts and subtracts, which cancels the
fixture setup. Fixed counts matter: Google Benchmark's calibration passes are not
included in its reported iteration count.

```sh
taskset -c 10 python3 bench/counters_per_row.py build/release/extension/adbc/bench/arrow_conversion_bench
```

### Baseline

Measured 2026-11-10 on the CloudLab experiments host (Xeon E5-2660 v3, Haswell),
pinned to one core at a fixed 2.6 GHz (`performance` governor, turbo off), DuckDB
`45ebff1561`. Times are the median of three repetitions; counters are per row.

| Fixture | Like | ns/row | Cycles/row | Blocked store forwards/row |
|---|---|---:|---:|---:|
| `int64` | `o_orderkey` | 0.16 | 0.4 | 0.001 |
| `date32` | `o_orderdate` | 0.16 | — | — |
| `decimal_15_2` | `o_totalprice` | 3.91 | 10.5 | 0.024 |
| `string_1` | `o_orderstatus` | 10.46 | 27.0 | 1.003 |
| `string_6_15` | `o_orderpriority` | 10.78 | 27.3 | 1.003 |
| `string_15` | `o_clerk` | 10.11 | 26.2 | 1.005 |
| `string_19_78` | `o_comment` | 11.20 | 29.1 | 1.002 |

- **Fixed-width columns are zero-copy:** they point DuckDB's vector at the Arrow buffer.
- **Strings cost about the same at every length.** Each row has exactly one blocked
  store forward. The 16-byte `string_t` is assembled in a stack temporary with several
  small stores, then reloaded with one 16-byte load. Each block stalls the core for
  roughly 10–15 cycles, about half of each string's cost.
- **Decimals have no such stalls.** Their cost is the per-value 128-bit → 64-bit
  `TryCast`.

These numbers match the per-column conversion times measured in a full
single-stream ClickHouse scan: 10.7–11.7 ns/row for strings and 4.6 ns/row for the
decimal. The raw Google Benchmark output is in
[baseline.json](./baseline.json).

## Parallel `read_adbc` streams

`parallel_read_adbc` reads one table as N concurrent `read_adbc` streams from a
single DuckDB process. For each stream count N it splits the key column's range
into N equal, contiguous ranges and starts N threads. Each thread opens its own
connection and runs an ordinary query for its range:

```sql
SELECT * FROM read_adbc('profile://myclickhouse',
    'SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN lo AND hi [ORDER BY ...] [SETTINGS max_threads=K]')
```

It reports the wall time from starting the threads until the last one finishes.
Rows are consumed with `EXPLAIN ANALYZE`, so no result is materialized.

### Build and run

The target is not part of the default build. It needs a reachable ADBC source,
for example the ClickHouse server from `clickhouse/start-clickhouse.sh`:

```sh
ninja -C build/release parallel_read_adbc
build/release/extension/adbc/bench/parallel_read_adbc --ch-max-threads=split 1 2 4 8
```

Options:

- `--profile=URI`: ADBC profile (default `profile://myclickhouse`).
- `--database=NAME`, `--table=NAME`: source table (default `tpch_sf50.orders`).
- `--key=COLUMN`: integer column used to split the table (default `o_orderkey`).
- `--order-by=EXPR`: append `ORDER BY EXPR` to each range query, so the server
  sorts every stream.
- `--ch-max-threads=K`: append `SETTINGS max_threads=K` to each range query;
  `split` uses floor(32 / N).
- `--ch-settings=LIST`: add these comma-separated ClickHouse settings to each
  range query's `SETTINGS` clause, for example
  `--ch-settings=output_format_parallel_formatting=0`.
- `--threads=K`: DuckDB worker threads shared by all streams (default 16).
- `--memory-limit=SIZE`: DuckDB memory limit (default 56GiB).
- `--repeats=R`: measured repeats per stream count (default 3), after one
  warmup. Repeats alternate the order of stream counts.
- `--checksum`: instead of timing, check that each stream count's per-range
  row counts and row-hash sums add up to one full `read_adbc`.

It prints one `streams`, `repeat`, `wall_s` line per batch on stdout, and a
median and speedup summary on stderr. Results are in
[reports/stream-limits-20261110](../reports/stream-limits-20261110/README.md),
[reports/sorted-streams-20261111](../reports/sorted-streams-20261111/README.md) and
[reports/parallel-arrowstream-20261111](../reports/parallel-arrowstream-20261111/README.md).

## The same streams in DataFusion

[`datafusion/`](./datafusion/) is a Rust counterpart of `parallel_read_adbc` that reads
the streams with [DataFusion](https://datafusion.apache.org/) instead of DuckDB, to
check whether the client engine affects the results. It takes the same options and
prints the same output, so the same experiment scripts can run either binary.

- It loads the same ADBC driver library through the same ADBC profile, using the ADBC
  driver manager.
  - `adbc_driver_manager` 0.24.0 and DataFusion 55.2.0 both use arrow 59.3.2, so batches
    pass from the driver to DataFusion without conversion.
- Each stream opens its own connection on a dedicated reader thread, like `read_adbc`
  with `ADBC_READER_THREAD=1`, and pushes Arrow batches into a bounded channel.
- DataFusion runs `SELECT * FROM` a `StreamingTable` over that channel and drains every
  batch.
  - All streams share one `SessionContext` and one tokio runtime with `--threads`
    workers.
  - Unlike `read_adbc`, it does not call `ExecuteSchema` first. The schema comes from
    the result stream.
- `--checksum` compares row counts and an order-independent sum of row hashes (FNV-1a
  over arrow's row encoding of every column).
  - The hash values differ from `parallel_read_adbc`'s, which uses DuckDB's `hash()`.
  - Each tool's own full read and range reads must agree.

```sh
cd bench/datafusion && cargo build --release
target/release/parallel_read_datafusion --profile=profile://myclickhouse --threads=20 --ch-max-threads=split 1 2 4 10
```

Results are in [reports/duckdb-vs-datafusion-20261112](../reports/duckdb-vs-datafusion-20261112/README.md).
