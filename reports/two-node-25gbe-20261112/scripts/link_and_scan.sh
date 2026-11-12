#!/usr/bin/env bash
# Run on the DuckDB node (hp036) before the experiment: the link's TCP rate with iperf3 (server on hp034, see
# setup_nodes.sh) in both directions with 1 and 4 connections, then ClickHouse's full scan with the Null output
# format, which reads every column but sends nothing. Output: data/iperf3.txt and data/null-format.txt.
set -euo pipefail
CH=http://10.10.1.1:8123/
for P in 1 4; do
    for R in "" "-R"; do
        dir="hp036 -> hp034"; [[ -n $R ]] && dir="hp034 -> hp036 (ClickHouse direction)"
        iperf3 -c 10.10.1.1 -p 5299 -t 10 -P $P $R -J > /tmp/ip.json
        python3 - "$P" "$dir" <<'EOF'
import json, sys
e = json.load(open("/tmp/ip.json"))["end"]
bps, cpu = e["sum_received"]["bits_per_second"], e["cpu_utilization_percent"]
print(f"{sys.argv[1]} connection(s), {sys.argv[2]}: {bps / 1e9:.2f} Gb/s = {bps / 8e9:.3f} GB/s, "
      f"local CPU {cpu['host_total']:.0f}%, remote CPU {cpu['remote_total']:.0f}%, "
      f"retransmits {e['sum_sent'].get('retransmits')}")
EOF
    done
done

for i in 1 2 3; do
    curl -fsS -o /dev/null -w "Null format run $i: %{time_total} s\n" "${CH}?max_threads=32&log_comment=null-scan-$i" \
        --data-binary "SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000 FORMAT Null"
done
curl -fsS $CH --data-binary "SYSTEM FLUSH LOGS"
curl -fsS $CH --data-binary "SELECT log_comment, round(query_duration_ms / 1e3, 2) AS s,
    round(ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6, 1) AS cpu_s, formatReadableSize(memory_usage) AS mem
    FROM system.query_log WHERE type = 'QueryFinish' AND log_comment LIKE 'null-scan-%'
    ORDER BY event_time FORMAT PrettyCompactMonoBlock"
