# Microbenchmarks

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
