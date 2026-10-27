#!/usr/bin/env bash
# Install systemd resource groups and the ClickHouse service. Run with sudo.
# CPU IDs and NUMA nodes match this 2-socket, 40-thread machine.
set -euo pipefail

[[ $EUID -eq 0 ]] || { echo 'Run with sudo.' >&2; exit 1; }
[[ -f /sys/fs/cgroup/cgroup.controllers ]] || { echo 'Requires cgroup v2.' >&2; exit 1; }
repo="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
login_user="${1:-${SUDO_USER:-}}"
[[ -n "$login_user" && "$login_user" != root ]] || {
    echo 'Pass the login user: sudo bash scripts/setup_benchmark_cgroups.sh USER' >&2
    exit 1
}
id "$login_user" >/dev/null
login_home="$(getent passwd "$login_user" | cut -d: -f6)"

for engine in clickhouse duckdb; do
    if [[ $engine == clickhouse ]]; then
        cpus=0-7,20-27
        node=0
    else
        cpus=10-17,30-37
        node=1
    fi
    cat > "/etc/systemd/system/adbc-$engine.slice" <<EOF
[Unit]
Description=ADBC $engine experiment resources

[Slice]
AllowedCPUs=$cpus
AllowedMemoryNodes=$node
CPUQuota=1600%
CPUQuotaPeriodSec=100ms
MemoryMax=64G
MemorySwapMax=0
EOF
done

cat > /etc/systemd/system/adbc-clickhouse.service <<EOF
[Unit]
Description=ClickHouse for ADBC experiments
After=network.target

[Service]
Type=exec
User=$login_user
Slice=adbc-clickhouse.slice
ExecStart=/bin/bash "$repo/clickhouse/start-clickhouse.sh"
Environment="HOME=$login_home" "TZ=UTC" "CLICKHOUSE_WATCHDOG_ENABLE=0"
Restart=on-failure
RestartSec=5
LimitNOFILE=262144

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl start adbc-clickhouse.slice adbc-duckdb.slice
systemctl enable adbc-clickhouse.service
echo 'Installed systemd resource limits. Start/restart ClickHouse with:'
echo '  sudo systemctl restart adbc-clickhouse.service'
