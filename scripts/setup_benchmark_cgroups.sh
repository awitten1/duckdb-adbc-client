#!/usr/bin/env bash
# Run with sudo. CPU IDs and NUMA nodes match this 2-socket, 40-thread machine.
set -eu

echo '+cpu +cpuset +memory' > /sys/fs/cgroup/cgroup.subtree_control

mkdir -p /sys/fs/cgroup/clickhouse
echo 0 > /sys/fs/cgroup/clickhouse/cpuset.mems
echo 0-7,20-27 > /sys/fs/cgroup/clickhouse/cpuset.cpus
# Allow 16 CPUs worth of aggregate runtime per 100 ms period (values in microseconds).
echo '1600000 100000' > /sys/fs/cgroup/clickhouse/cpu.max
echo 68719476736 > /sys/fs/cgroup/clickhouse/memory.max
echo 0 > /sys/fs/cgroup/clickhouse/memory.swap.max
echo root > /sys/fs/cgroup/clickhouse/cpuset.cpus.partition

mkdir -p /sys/fs/cgroup/duckdb
echo 1 > /sys/fs/cgroup/duckdb/cpuset.mems
echo 10-17,30-37 > /sys/fs/cgroup/duckdb/cpuset.cpus
echo '1600000 100000' > /sys/fs/cgroup/duckdb/cpu.max
echo 68719476736 > /sys/fs/cgroup/duckdb/memory.max
echo 0 > /sys/fs/cgroup/duckdb/memory.swap.max
echo root > /sys/fs/cgroup/duckdb/cpuset.cpus.partition
