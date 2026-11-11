# Parallel streams across a 10 Gb/s link

Measured 2026-11-11 on two CloudLab Utah nodes, as a follow-up to
[sending parallel-formatted output without a copy](../direct-send-20261111/README.md). Every earlier stream
experiment ran ClickHouse and DuckDB on one machine over loopback. This one puts ClickHouse on one node and DuckDB
on another, connected by a real 10 Gb/s link, and repeats the parallel-streams experiment with the custom
ClickHouse build.

**Summary.**

- **The network is the limit.** One TCP connection reaches the link's full rate (iperf3: 1.17 GB/s), so the
  9.67 GB result needs at least 8.2 s, however many streams there are. Every configuration with enough streams
  lands at 8.4–8.6 s.
- **With parallel formatting, one stream nearly fills the link.** One `read_adbc` stream takes 9.18 s
  (1.05 GB/s), and curl over one connection takes 8.69 s. The best batch, 8 streams, takes 8.40 s, so multiple
  streams add only 8–9%. ClickHouse spends 8.7 s of an 8.9 s single-stream query blocked in `send()`, waiting on
  the network.
- **With serial formatting, one stream runs at about half the link's rate.** One stream takes 15.6 s
  (0.62 GB/s), and curl 15.4 s. On these 2.0 GHz CPUs ClickHouse's single thread that encodes and sends the
  result is slower than the link, and it takes 3 streams to reach the link.
- **So over this network, parallel formatting does what multiple streams did:** one stream gets within 9% of the
  best batch instead of being 1.8× slower.

## Setup

- **Nodes:** two identical CloudLab Utah machines: Intel Xeon D-1548 (8 cores, 16 hardware threads, one socket),
  62 GB RAM, NVMe disk, Ubuntu 24.04.
  - `ms1011` runs ClickHouse.
  - `ms1006` runs DuckDB.
  - They are connected by a 10 Gb/s experiment link (`10.10.1.1` ↔ `10.10.1.2`).
- **Resources:** each side gets the whole machine (16 CPUs, all memory), with no cgroup slices.
- **Clocks:** fixed at 2.0 GHz on both nodes: `performance` governor, turbo off, minimum frequency raised to the
  maximum ([script](./scripts/pin_frequency.sh), [verification](./data/frequency.txt)). The earlier Wisconsin
  host ran at 2.6 GHz.
- **ClickHouse:** the custom 26.11.1.1 build with parallel `ArrowStream` formatting and direct send (commit
  `c933d005d`, the same binary as the direct-send report). It listens on the experiment link
  ([config](./scripts/clickhouse-config-network.xml)).
- **"Serial formatting":** the same binary with `output_format_parallel_formatting=0`. ClickHouse then uses its
  single-threaded Arrow writer, where one thread per query encodes every block and writes it to the socket. Direct
  send does not apply, since it only exists in the parallel path. This behaves like the modified build before the
  two commits (`a2d3db7e5`), not like stock ClickHouse.
- **DuckDB side:**
  - `duckdb-adbc-client` at `e01af01`, built on `ms1006` with the reader-thread patch and the DuckDB conversion
    patch from the [stream-limits report](../stream-limits-20261110/patches/), as in the earlier runs.
  - The ClickHouse ADBC connector is rebuilt from upstream `v0.1.1` plus
    [connector.diff](../parallel-streams-20261110/connector.diff), with cargo feature `ffi`.
- **Data:** TPC-H SF50, generated with the DuckDB 2.0.0 release CLI and loaded with the repo's
  `load_clickhouse_tpch.py` ([script](./scripts/load_sf50.sh), [output](./data/load.txt)).
  - `orders` ended up in 2 parts, not the 5 parts on the Wisconsin host. Merges were enabled and idle before the
    run.
  - The data is identical to Wisconsin's: the row-hash sum below matches.
- **Experiment** ([script](./scripts/utah_streams.sh)):
  - [`bench/parallel_read_adbc`](../../bench/parallel_read_adbc.cpp) with N `o_orderkey` ranges, `max_threads` =
    floor(32 / N) per query, DuckDB `threads=16` and `ADBC_READER_THREAD=1`;
  - `--ch-settings` switches `output_format_parallel_formatting` per run;
  - one warmup batch, then two passes over N = 1–10.

## 1. The link

iperf3, 10 s per test, 1 MB writes ([data](./data/iperf3.txt)):

| Direction | 1 connection | 4 connections |
|---|---:|---:|
| ms1011 → ms1006 (ClickHouse to DuckDB) | 9.34 Gb/s = 1.167 GB/s | 9.41 Gb/s = 1.177 GB/s |
| ms1006 → ms1011 | 9.36 Gb/s = 1.170 GB/s | 9.41 Gb/s = 1.176 GB/s |

One connection already runs at line rate. At 1.177 GB/s the 9.67 GB result needs 8.2 s.

## 2. Correctness

([output](./data/checksum.txt)) One full `read_adbc` and 4 and 10 range reads all return 75,000,000 rows with
row-hash sum `691747052758391932685914880`, the same value as on the Wisconsin host.

## 3. Results

![Stream count vs runtime and throughput over the 10 Gb/s link](./streams.png)

[Vector version](./streams.svg); data in [streams.tsv](./streams.tsv), raw output in
[streams-par1.tsv](./data/streams-par1.tsv), [streams-par0.tsv](./data/streams-par0.tsv) and
[curl.txt](./data/curl.txt).

| | Parallel formatting | Throughput | Serial formatting | Throughput |
|---|---:|---:|---:|---:|
| curl, one connection | **8.66–8.69 s** | 1.11 GB/s | 15.29–15.44 s | 0.63 GB/s |
| 1 stream | **8.95–9.18 s** | 1.05 GB/s | 15.30–15.56 s | 0.62 GB/s |
| 2 streams | 8.49–8.52 s | 1.14 GB/s | 9.57–9.80 s | 0.99 GB/s |
| 3 streams | 8.39–8.44 s | 1.15 GB/s | **8.40–8.41 s** | 1.15 GB/s |
| 4 streams | 8.43–8.46 s | 1.14 GB/s | 8.53–8.64 s | 1.12 GB/s |
| 6 streams | 8.42–8.45 s | 1.15 GB/s | 8.47–8.48 s | 1.14 GB/s |
| 8 streams | **8.37–8.39 s** | 1.15 GB/s | 8.46–8.48 s | 1.14 GB/s |
| 10 streams | 8.48–8.59 s | 1.13 GB/s | 8.40–8.44 s | 1.15 GB/s |

Times are the range of two runs; throughput uses the slower run.

- **From 3 streams up, both settings sit at the link limit** (1.12–1.15 GB/s against 1.17 GB/s).
- **One stream with parallel formatting is 9% from the best batch;** with serial formatting it is 1.8× slower.
- **`read_adbc` adds about 0.5 s over curl for one stream** (9.18 s against 8.69 s), so the DuckDB side is close
  to keeping up with the link.

## 4. ClickHouse's view

From `system.query_log` on `ms1011` ([parallel](./data/streams-par1-clickhouse.txt),
[serial](./data/streams-par0-clickhouse.txt)):

| Streams | Median query time: parallel / serial | Median time blocked in `send()` per query: parallel / serial | ClickHouse CPU per batch: parallel / serial |
|---:|---|---|---|
| 1 | 8.86 / 15.21 s | 8.69 / 7.39 s | 96.2 / 75.7 s |
| 2 | 8.36 / 9.57 s | 8.16 / 4.77 s | 106.1 / 109.7 s |
| 4 | 7.86 / 8.02 s | 7.66 / 5.24 s | 105.6 / 106.6 s |
| 10 | 7.21 / 6.86 s | 7.00 / 5.23 s | 98.5 / 99.4 s |

- **With parallel formatting, ClickHouse waits on the network for almost the whole query** (8.69 of 8.86 s for
  one stream).
- **With serial formatting and one stream, half the query is spent outside `send()`** (15.2 s, of which 7.4 s in
  `send()`). The other half is the single thread encoding, so that thread, not the link, sets the pace.
- **ClickHouse used much more CPU per batch here than on the Wisconsin host** (96–111 CPU-seconds, against 31–52
  there). The slower 2.0 GHz CPU and a real network card instead of loopback explain part of this; the rest is
  not explained without a profile. It does not affect the results, since ClickHouse was not CPU-bound.

## 5. Compared with loopback

The same binary on the Wisconsin host (2.6 GHz, DuckDB on the other CPU socket, from the
[direct-send report](../direct-send-20261111/README.md)):

| | Loopback, one machine | 10 Gb/s, two nodes |
|---|---:|---:|
| curl, one connection, parallel formatting | 6.22–6.40 s | 8.66–8.69 s |
| One `read_adbc` stream, parallel formatting | 7.28 s | 9.18 s |
| Best batch | 3.26 s (serial, 6 streams) | 8.40 s (parallel, 8 streams) |

On loopback, multiple streams paid off because nothing outside the two processes limited throughput. Over
10 Gb/s, the link caps every configuration at about 1.15 GB/s.

## What this suggests

- **Over a network, sending fewer bytes is what helps now.** Arrow IPC compression
  (`output_format_arrow_compression_method=lz4_frame` or `zstd`) runs on the parallel formatter threads, so it is
  cheap for ClickHouse. The connector would then have to decompress inside its single-threaded decoding.
- **A faster link would show whether one stream can keep up beyond 10 Gb/s.** On loopback one stream reached
  about 1.3–1.5 GB/s, so on 25 Gb/s or faster the client's decoding would likely become the limit again.

## Caveats

- Two runs per configuration.
- Different hardware from the earlier reports: 2.0 GHz Xeon D nodes instead of a 2.6 GHz two-socket Haswell host.
- `orders` has 2 parts here instead of 5.
- The extra ClickHouse CPU is unexplained.

## Files

- **[scripts/](./scripts/):**
  - `pin_frequency.sh`: fixes the clocks on a node and checks them under load.
  - `clickhouse-config-network.xml`: the ClickHouse server config.
  - `load_sf50.sh`: generates and loads SF50 on `ms1011`.
  - `utah_streams.sh`: correctness check, curl downloads and the streams sweep, run on `ms1006`.
- **[data/](./data/):** the raw output of each step, including the full run log.
- **[plot_two_node.py](./plot_two_node.py)** draws the chart: `python3 plot_two_node.py data .` (needs matplotlib).
