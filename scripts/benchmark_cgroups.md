# Benchmark resource groups

Create the cgroup v2 groups after each reboot:

```bash
sudo bash scripts/setup_benchmark_cgroups.sh
```

This script is specific to this machine's CPU topology. Each group gets 16
logical CPUs (8 physical cores, including both SMT siblings), a hard 64 GiB
memory limit (68,719,476,736 bytes), and no swap. CPU partition roots keep the
two groups' CPUs exclusive; the remaining 8 logical CPUs serve the host.
ClickHouse uses NUMA node 0 and DuckDB uses node 1. Memory is capped, not
preallocated.

| Group | Logical CPUs | NUMA node | Memory limit |
|---|---|---|---|
| `/sys/fs/cgroup/clickhouse` | `0-7,20-27` | 0 | 64 GiB |
| `/sys/fs/cgroup/duckdb` | `10-17,30-37` | 1 | 64 GiB |

Launch ClickHouse from the repository root:

```bash
(
  sudo sh -c 'echo "$1" > /sys/fs/cgroup/clickhouse/cgroup.procs' sh "$BASHPID" &&
  exec ./clickhouse/start-clickhouse.sh
)
```

In another Bash terminal, launch the benchmark in DuckDB's group:

```bash
(
  sudo sh -c 'echo "$1" > /sys/fs/cgroup/duckdb/cgroup.procs' sh "$BASHPID" &&
  exec python3 scripts/benchmark_clickhouse_partial_sort.py \
    --threads 16 --memory-limit 56GiB
)
```

The subshell joins the group before launching the program, so children inherit
the limits and programs still run as your user. Set `ADBC_DRIVER_PATH` and put
`dbc` on PATH as usual. The DuckDB memory setting leaves room below the cgroup
limit for Arrow/driver buffers and other allocations. ClickHouse and DuckDB
retain their application-level settings; the cgroup limits apply to all threads
and child processes combined.

To release the CPU partitions after stopping the workloads:

```bash
sudo sh -c 'echo member > /sys/fs/cgroup/clickhouse/cpuset.cpus.partition'
sudo sh -c 'echo member > /sys/fs/cgroup/duckdb/cpuset.cpus.partition'
sudo rmdir /sys/fs/cgroup/clickhouse /sys/fs/cgroup/duckdb
```
