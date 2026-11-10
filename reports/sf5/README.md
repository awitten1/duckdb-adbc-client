# SF5 alpha and sort-key sweep

Measured on 2026-11-09 with the rebuilt DuckDB/ADBC binaries, local ClickHouse
26.10.1.1810, TPC-H SF5 `orders` (7,500,000 rows), four physical cores per
engine, DuckDB `threads=4`, `memory_limit=9GiB`, and a 13 GiB ClickHouse server
cap. The partition key was `o_orderkey`. All queries used `SELECT *` unless
marked narrow. Each number below is the median of two `EXPLAIN (ANALYZE,
FORMAT JSON)` query `total_time` values in seconds. The second pass reversed
the case order. Every profile reported the expected 7.5 million rows and no
temporary-directory spill.

Alpha is the fraction of the *integer partition-key range* assigned to the
remote branch. For this dataset it also split the row count proportionally.
`DuckDB` means unsorted `read_adbc` followed by a DuckDB `ORDER BY`;
`Remote` means `read_adbc` with a full ClickHouse `ORDER BY`.

| Sort key | DuckDB | Remote | α=0 | α=.25 | α=.5 | α=.75 | α=1 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `o_orderkey` (physical order) | 1.123 | 1.052 | 1.218 | 0.992 | **0.764** | 0.826 | 1.010 |
| `o_orderdate` | 1.098 | 1.513 | 1.339 | 1.247 | **1.029** | 1.071 | 1.337 |
| `o_totalprice` | **1.208** | 1.822 | 1.392 | 1.885 | 1.839 | 1.425 | 1.611 |
| `o_clerk` | 1.685 | 1.618 | 1.841 | 1.819 | 1.437 | **1.192** | 1.362 |

## What limits speed

1. **The full-row ADBC transfer is substantial.** An unsorted `SELECT *`
   scan took 1.029 seconds (720 MB of logical intermediate output). Selecting
   only `o_orderkey,o_totalprice` took 0.171 seconds (120 MB). Every alpha
   still transfers every row through ADBC; the split only divides sorting.
2. **Interleaved branches force payload copies in the final single-thread
   merge.** At α=.5, `o_totalprice` copied all 7.5 million rows and spent
   0.882 seconds in `Merge Seconds`; the physical `o_orderkey` sort copied only
   2,048 rows and spent 0.062 seconds. `Merge Seconds` is elapsed scan time
   and can include waits for remote input, so it is not a pure CPU timer.
3. **The sort keys change the winner.** `o_orderkey` aligns with ClickHouse's
   MergeTree order and hybrid α=.5 is 1.47× faster than the DuckDB baseline.
   Date sorting gives only a small benefit at α=.5. Decimal sorting has no
   winning alpha here: the best hybrid case, α=.75, takes 1.425 seconds
   versus 1.208 seconds for DuckDB. String sorting is best at α=.75, about
   1.41× faster than DuckDB.
4. **The local path has its own overhead.** At α=0 it took 1.218–1.841 seconds
   across these keys, versus 1.098–1.685 seconds for DuckDB's direct sort.
   It reads Arrow batches through ADBC, builds separate local sort states,
   and materializes parallel source output before scanning it. The final
   merge is single-threaded; increasing DuckDB threads from 4 to 8 on the
   same four physical cores left the decimal α=.5 case at 1.844 seconds
   versus 1.839 seconds at four threads.

For the narrow decimal projection (`o_orderkey,o_totalprice`), DuckDB took
0.267 seconds, remote 0.462, α=0 0.300, α=.5 0.582, and α=1 0.420. Alpha=.5
still copied every row and spent 0.377 seconds in the merge. This isolates
copying from the larger cost of moving the other seven columns.

At α=.5, the local sort and remote read/buffer elapsed times were 0.689/0.613
seconds for `o_orderkey`, 0.902/0.810 for `o_orderdate`, and 0.941/1.013 for
`o_totalprice`. These branches overlap, so their durations must not be added.
Binding and bounds queries were about 0.005 and 0.003 seconds, respectively.
Peak queued remote buffers were roughly 0.67–0.72 GB in the wide α=.5 cases;
none spilled under the 9 GiB DuckDB limit.

## Evidence and limits

The sweep produced 76 successful SF5 profiles, including six targeted
eight-thread comparisons. The raw case summaries, DuckDB profiles, SQL, and
stderr remain in ignored `build/local-experiments/sf5-sweep/`. The synthetic
1-million and 10-million-row sweeps remain under `build/local-experiments/`.
Small ClickHouse integration tests
for ratio ordering and comparable sort keys passed.

These results use two repetitions on a shared workstation with CPU affinity,
not exclusive CPU partitions. They describe this SF5 table, its physical
layout, these columns, and the current local resource settings. The server's
query logs and CPU profiles were not collected, so the remote read/buffer
timer does not separate ClickHouse sorting from Arrow encoding and transfer.
