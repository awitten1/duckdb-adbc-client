#!/usr/bin/env bash
# System-wide profile (kernel symbols included, via sudo) of ClickHouse's CPUs and curl's CPU during one raw
# ArrowStream download. Usage: kernel_profile_download.sh CLIENT_CPU   (modified ClickHouse must be running)
set -euo pipefail
cpu=$1
CH_CPUS=0-7,20-27
OUT=/tmp/kprof-$cpu
mkdir -p $OUT
SQL="SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000"
URL="http://127.0.0.1:8123/?default_format=ArrowStream&enable_http_compression=0&output_format_arrow_compression_method=none&max_threads=32"

sudo perf record -e cycles -F 499 -a -C "$CH_CPUS,$cpu" -o $OUT/perf.data 2>/dev/null &
perf_pid=$!
sleep 1
start=$(date +%s.%N)
taskset -c "$cpu" curl -fsS -o /dev/null "$URL" --data-binary "$SQL"
end=$(date +%s.%N)
sudo kill -INT $perf_pid
wait $perf_pid 2>/dev/null || true
sudo chmod -R a+r $OUT
printf "=== curl on CPU %s: download %.2f s\n" "$cpu" "$(echo "$end - $start" | bc)"

report() {  # cpus label
    local total kernel
    total=$(sudo perf report -i $OUT/perf.data --stdio --cpu "$1" --sort pid -F sample 2>/dev/null | awk '/^ +[0-9]/ {s += $1} END {print s}')
    kernel=$(sudo perf report -i $OUT/perf.data --stdio --cpu "$1" --sort dso -F sample,dso 2>/dev/null | awk '/kernel/ {s += $1} END {print s + 0}')
    echo "--- $2: $total samples, $(awk -v k="$kernel" -v t="$total" 'BEGIN {printf "%.0f%%", 100 * k / t}') in the kernel; top symbols:"
    sudo perf report -i $OUT/perf.data --stdio --cpu "$1" --no-children --sort comm,dso,sym -F overhead,comm,sym \
        --percent-limit 1.5 2>/dev/null | grep '%' | grep -v '^#' | sed -E 's/ +/ /g' | cut -c1-120 | head -16
}
report "$CH_CPUS" "ClickHouse CPUs ($CH_CPUS)"
report "$cpu" "curl CPU ($cpu)"
sudo rm -rf $OUT
