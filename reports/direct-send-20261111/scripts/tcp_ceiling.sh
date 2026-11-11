#!/usr/bin/env bash
# Single-connection loopback TCP ceiling (iperf3) and ClickHouse ArrowStream downloads (parallel formatting on),
# with the default TCP buffer limits and with 64 MB limits. Restores the original sysctls on exit.
# Usage: tcp_ceiling.sh   (prototype ClickHouse must be running)
set -euo pipefail
SQL="SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000"
URL="http://127.0.0.1:8123/?default_format=ArrowStream&enable_http_compression=0&output_format_arrow_compression_method=none&max_threads=32&output_format_parallel_formatting=1"
Q() { curl -fsS http://127.0.0.1:8123/ --data-binary "$1"; }
SENDER_CPU=3   # one of ClickHouse's CPUs (socket 0)

orig_wmem=$(sysctl -n net.ipv4.tcp_wmem | tr '\t' ' ')
orig_rmem=$(sysctl -n net.ipv4.tcp_rmem | tr '\t' ' ')
restore() {
    sudo sysctl -q -w net.ipv4.tcp_wmem="$orig_wmem" net.ipv4.tcp_rmem="$orig_rmem"
    echo "restored tcp_wmem=[$(sysctl -n net.ipv4.tcp_wmem)] tcp_rmem=[$(sysctl -n net.ipv4.tcp_rmem)]"
}
trap restore EXIT

measure() {  # label
    echo
    echo "##### $1: tcp_wmem=[$(sysctl -n net.ipv4.tcp_wmem)] tcp_rmem=[$(sysctl -n net.ipv4.tcp_rmem)]"
    for cpu in 9 19; do
        for len in 128K 1M; do
            iperf3 -s -1 -A "$cpu" -p 5299 > /dev/null 2>&1 &
            server=$!
            sleep 0.5
            rate=$(iperf3 -c 127.0.0.1 -p 5299 -A "$SENDER_CPU" -t 8 -l "$len" -J \
                   | python3 -c "import json,sys; e=json.load(sys.stdin)['end']; print(f\"{e['sum_received']['bits_per_second']/8e9:.2f} GB/s, sender CPU {e['cpu_utilization_percent']['host_total']:.0f}%, receiver CPU {e['cpu_utilization_percent']['remote_total']:.0f}%\")")
            wait $server || true
            echo "iperf3 one connection, sender CPU $SENDER_CPU -> receiver CPU $cpu, ${len}B writes: $rate"
        done
    done
    for cpu in 9 19; do
        for rep in 1 2; do
            id="tcpceil-$RANDOM$RANDOM"
            taskset -c "$cpu" curl -fsS -o /dev/null -w "%{time_total} %{size_download}\n" "$URL&query_id=$id" \
                --data-binary "$SQL" > /tmp/$id.curl &
            curl_pid=$!
            sleep 3
            skmem=$(ss -tmiH state established '( sport = :8123 )' | grep -o "skmem:([^)]*)" | head -1)
            sendq=$(ss -tnH state established '( sport = :8123 )' | awk '{print $3}' | head -1)
            wait $curl_pid
            read -r secs bytes < /tmp/$id.curl
            rm -f /tmp/$id.curl
            Q "SYSTEM FLUSH LOGS"
            blocked=$(Q "SELECT round(ProfileEvents['NetworkSendElapsedMicroseconds'] / 1e6, 2) FROM system.query_log WHERE query_id = '$id' AND type = 'QueryFinish'")
            printf "ClickHouse download, curl on CPU %s, run %s: %.2f s = %.2f GB/s, server blocked in send() %s s; at 3 s: send queue %s B, server %s\n" \
                "$cpu" "$rep" "$secs" "$(echo "$bytes / 1e9 / $secs" | bc -l)" "$blocked" "$sendq" "$skmem"
        done
    done
}

measure "Default TCP buffer limits"
sudo sysctl -q -w net.ipv4.tcp_wmem="4096 16384 67108864" net.ipv4.tcp_rmem="4096 131072 67108864"
measure "64 MB TCP buffer limits"
