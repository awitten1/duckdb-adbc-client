# SF10 alpha and sort-key sweep

Measured on 2026-11-09 using the rebuilt DuckDB/ADBC binaries, local
ClickHouse 26.10.1.1810, and TPC-H SF10 `orders` (15,000,000 rows). Each
engine had affinity to four physical cores; DuckDB used `threads=4` and
`memory_limit=9GiB`, and ClickHouse had a 13 GiB server cap. The partition
key was `o_orderkey`. Queries selected all nine `orders` columns unless
marked narrow. Times are `EXPLAIN (ANALYZE, FORMAT JSON)` query `total_time`
in seconds, with no CSV sink. Each main cell is the median of two runs; the
second pass reversed case order. The `o_clerk` DuckDB and alpha=.75 cells
use three runs because one DuckDB sort was a high outlier.

Alpha is the fraction of the integer partition-key range assigned to the
remote branch. For this dataset the row split was proportional. `DuckDB`
means unsorted `read_adbc` followed by DuckDB `ORDER BY`; `Remote` means
`read_adbc` with ClickHouse `ORDER BY`.

| Sort key | DuckDB | Remote | α=0 | α=.25 | α=.5 | α=.75 | α=1 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `o_orderkey` | 2.159 | 2.065 | 2.438 | 2.047 | 1.579 | **1.572** | 1.984 |
| `o_orderdate` | 2.197 | 2.894 | 2.621 | 2.372 | **2.012** | 2.115 | 2.538 |
| `o_totalprice` | **2.333** | 3.627 | 2.703 | 4.073 | 3.621 | 2.704 | 3.203 |
| `o_clerk` | 3.446 | 3.090 | 3.704 | 3.242 | 2.678 | **2.326** | 2.678 |

![Alpha sweep by sort key](./alpha_sweep.png)

The blue line is the hybrid function; whiskers show the observed run range.
Dashed lines are the median DuckDB and remote-sort baselines. Each panel has
its own vertical scale. [Vector version](./alpha_sweep.svg).

The 0.007-second difference between the two best `o_orderkey` alpha values
is within the run-to-run spread; treat α=.5 and α=.75 as effectively tied.

## Bottlenecks and scale comparison

- **Wide ADBC transfer remains a floor.** An unsorted `SELECT *` took 1.969
  seconds for 1.44 GB of logical intermediate output. Selecting only
  `o_orderkey,o_totalprice` took 0.277 seconds for 240 MB. Both are roughly
  twice the SF5 workload's byte count. Each alpha still transfers all rows.
- **Interleaved full-row copying scales with row count.** At α=.5,
  `o_totalprice` copied all 15 million output rows and spent 1.749 seconds
  in the merge's elapsed timer; SF5 copied 7.5 million and spent 0.882
  seconds. For physically ordered `o_orderkey`, only 2,048 rows were copied
  and the merge timer was 0.125 seconds. The timer can include waits for
  remote input, so it is not a pure CPU measurement.
- **Narrow projection reduces transport and local sorting, but still exposes
  the merge cost.** For `o_orderkey,o_totalprice`, DuckDB took 0.527 seconds,
  remote 0.909, α=0 0.544, α=.5 1.102, and α=1 0.800. At α=.5 all 15
  million rows were copied and the merge timer was 0.744 seconds.
- **More local workers do not remove the single-thread output limit.** With
  eight DuckDB threads on the same four physical cores, decimal α=.5 took
  3.524 seconds versus 3.621 at four threads, still slower than DuckDB's
  2.333-second baseline. Date α=.5 changed from 2.012 to 1.968 seconds.
  There was no temporary-directory spill in any case.

![Rows copied and merge elapsed time by sort key](./merge_copy.png)

At α=.5, interleaving ranges from almost no copied rows for `o_orderkey` to
all 15 million for `o_totalprice`. The merge timer is elapsed time and can
include remote-input waits. [Vector version](./merge_copy.svg).

![Full-row versus two-column projection times](./projection_width.png)

The projection comparison uses the same 15 million rows and `o_totalprice`
sort key. The unsorted read provides the transfer baseline.
[Vector version](./projection_width.svg).

At α=.5, local sort and remote read/buffer elapsed times were 1.439/1.163
seconds for `o_orderkey`, 1.800/1.596 for `o_orderdate`, and 1.849/1.989 for
`o_totalprice`. These branches overlap; their times must not be added.
Binding and bounds queries remained around 0.005 and 0.003 seconds. Peak
queued remote buffers were about 1.3–1.45 GB in the wide α=.5 cases, with
DuckDB peak buffer memory around 2.6–2.7 GB, below the 9 GiB limit.

Compared with [SF5](../sf5/README.md), SF10 roughly doubles the
transfer and merge-copy time. The best alpha remains key dependent: a mixed
split helps integer, date, and string sorting, while no alpha beats direct
DuckDB sorting for decimal total price. The `o_clerk` DuckDB baseline ran
3.446, 4.340, and 3.430 seconds; the middle run is a high outlier. Its
alpha=.75 runs were 2.326, 2.387, and 2.289 seconds, so the advantage
persists across all three runs.

## Evidence and limits

The [runner](./run_all.py) executed the matrix sequentially. The charts can be
regenerated from the tracked TSV chart data with [plot_report.py](./plot_report.py)
and Gnuplot. Pass `--refresh-data` to update the TSV files from local raw
profiles. Ignored `build/local-experiments/sf10-sweep/` contains 78 successful
`*-summary.json` files with query/stage metrics, DuckDB profiles, SQL, stderr,
and run logs.
Every profile reported 15 million rows and zero temporary-directory spill.
The server was stopped after measurement; SF10 data remains in the
repository's `build/tpch-sf10/` and `clickhouse/clickhouse-data/` directories.

This is a shared workstation with CPU affinity, not exclusive CPU partitions.
Most cases have two repeats. ClickHouse query logs and CPU profiles were not
collected, so the remote read/buffer timer combines server sorting, Arrow
encoding, transfer, and conversion.
