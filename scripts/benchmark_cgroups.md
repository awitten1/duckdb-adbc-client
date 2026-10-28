# Benchmark resource groups

Install the systemd units from the repository root as your login user:

```bash
sudo bash scripts/setup_benchmark_cgroups.sh
sudo systemctl restart adbc-clickhouse.service
```

The setup script installs two resource slices and enables the ClickHouse
service at boot. It does not restart a running server; the second command
applies the new service configuration. When called directly as root, pass the
login user as the script's first argument. The Ubuntu bootstrap runs both
commands automatically.

Systemd creates the cgroups, enables their controllers, and places processes
in them. The settings persist in `/etc/systemd/system/`; there is no manual
PID migration or root-level `cgroup.subtree_control` setup.

This configuration targets the 40-thread, two-socket machine. Each engine
gets 16 logical CPUs (8 physical cores, including both SMT siblings), a hard
64 GiB memory limit (68,719,476,736 bytes), and no swap. ClickHouse uses NUMA
node 0 and DuckDB uses node 1. Memory is capped, not preallocated.

| Systemd slice | Logical CPUs | CPU quota | NUMA node | Memory limit |
|---|---|---|---|---|
| `adbc-clickhouse.slice` | `0-7,20-27` | 16 CPUs | 0 | 64 GiB |
| `adbc-duckdb.slice` | `10-17,30-37` | 16 CPUs | 1 | 64 GiB |

`AllowedCPUs` selects the CPUs; `CPUQuota=1600%` with a 100 ms period sets
`cpu.max` to `1600000 100000`. `AllowedMemoryNodes`, `MemoryMax`, and
`MemorySwapMax` control NUMA placement, memory, and swap. These limits apply
to all threads and child processes in each slice combined, including all
concurrent DuckDB commands. See the
[systemd resource-control documentation](https://www.freedesktop.org/software/systemd/man/255/systemd.resource-control.html).

The engines have disjoint allowed CPU sets. These settings do not create
exclusive CPU partitions: unrelated host processes can still use those CPUs.
The remaining 8 logical CPUs are outside the two engine sets.

ClickHouse runs as your login user through `adbc-clickhouse.service`, with
its watchdog disabled so systemd tracks the server directly:

```bash
sudo systemctl status adbc-clickhouse.service
sudo journalctl -u adbc-clickhouse.service
```

Run DuckDB interactively in its slice:

```bash
bash scripts/run_duckdb_cgroup.sh ./build/release/duckdb -unsigned
```

The launcher mirrors the slice's CPU quota and memory cap on the transient
scope because this DuckDB build only detects limits on its own cgroup, not
parent cgroups. With the default slice configuration, DuckDB automatically
selects 16 threads and 80% of the 64 GiB cap for memory (displayed as
`51.1 GiB` by this build).

Load the extension. To reproduce the benchmark's explicit 56 GiB memory
budget, leaving room for Arrow/driver buffers below the 64 GiB cap, use:

```sql
LOAD 'build/release/extension/adbc/adbc.duckdb_extension';
SET threads = 16;
SET memory_limit = '56GiB';
```

Run the benchmark in the same slice:

```bash
bash scripts/run_duckdb_cgroup.sh python3 scripts/benchmark_clickhouse_partial_sort.py \
    --threads 16 --memory-limit 56GiB
```

The launcher creates a transient systemd scope inside `adbc-duckdb.slice`.
It uses sudo to create the scope, then drops privileges before executing the
command. It preserves the working directory, terminal, `HOME`, `PATH`, and
`ADBC_DRIVER_PATH` when set. Set driver discovery as usual before launching.
The command's exit status is returned to the shell. Starting DuckDB directly
without the launcher leaves it in the SSH session's cgroup.

Inspect the effective limits:

```bash
systemctl show adbc-clickhouse.slice adbc-duckdb.slice \
    -p ControlGroup -p EffectiveCPUs -p EffectiveMemoryNodes \
    -p CPUQuotaPerSecUSec -p MemoryMax -p MemorySwapMax
```

The cgroup paths are `/sys/fs/cgroup/adbc.slice/adbc-clickhouse.slice` and
`/sys/fs/cgroup/adbc.slice/adbc-duckdb.slice`. ClickHouse's service and
DuckDB's transient scopes are children of their respective slices. Resource
limits declared on a slice apply to all of those children.

When migrating from the old setup, stop ClickHouse before installing the
new units, then start it again. Remove the old `/sys/fs/cgroup/clickhouse`
and `/sys/fs/cgroup/duckdb` directories only after their `cgroup.procs` files
are empty. Do not write resource settings into those old groups.
