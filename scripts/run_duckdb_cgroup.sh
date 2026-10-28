#!/usr/bin/env bash
# Run a command as the login user inside systemd's shared DuckDB resource slice.
set -euo pipefail

[[ $EUID -ne 0 ]] || { echo 'Run as your login user; this script uses sudo.' >&2; exit 1; }
[[ $# -gt 0 ]] || { echo "Usage: $0 COMMAND [ARG ...]" >&2; exit 1; }

# This DuckDB version only detects limits on its own cgroup, not the parent slice.
# Mirror the slice's limits on the scope; the slice still caps all runs combined.
slice_group="$(systemctl show adbc-duckdb.slice --property=ControlGroup --value)"
[[ -n "$slice_group" ]] || {
    echo 'Start adbc-duckdb.slice with scripts/setup_benchmark_cgroups.sh first.' >&2
    exit 1
}
read -r cpu_quota cpu_period < "/sys/fs/cgroup$slice_group/cpu.max"
read -r memory_max < "/sys/fs/cgroup$slice_group/memory.max"
[[ $cpu_quota != max && $memory_max != max ]] || {
    echo 'adbc-duckdb.slice must have finite CPUQuota and MemoryMax limits.' >&2
    exit 1
}
cpu_percent=$((100 * cpu_quota / cpu_period))

# sudo can replace HOME/PATH; restore driver discovery after dropping privileges.
environment=("HOME=$HOME" "PATH=$PATH")
if [[ -v ADBC_DRIVER_PATH ]]; then
    environment+=("ADBC_DRIVER_PATH=$ADBC_DRIVER_PATH")
fi

exec sudo systemd-run --scope --quiet --collect --expand-environment=no \
    --slice=adbc-duckdb.slice \
    --property="CPUQuota=$cpu_percent%" \
    --property="CPUQuotaPeriodSec=${cpu_period}us" \
    --property="MemoryMax=$memory_max" \
    /usr/bin/setpriv --reuid="$(id -u)" --regid="$(id -g)" --init-groups \
    /usr/bin/env "${environment[@]}" "$@"
