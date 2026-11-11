#!/usr/bin/env bash
# Direct HTTP writes under parallel formatting: serial and parallel output must be byte-identical for other
# formats and HTTP modes too. Usage: direct_write_formats.sh   (server under test must be running)
set -euo pipefail
SQL="SELECT * FROM tpch_sf50.orders WHERE o_orderkey <= 8000000 ORDER BY o_orderkey"
URL="http://127.0.0.1:8123/?max_threads=32&enable_http_compression=0&output_format_arrow_compression_method=none"
fail=0
digest() {  # format extra_query_params [curl options...]
    local fmt=$1 extra=$2; shift 2
    curl -sS "$@" "$URL&default_format=$fmt&$extra" --data-binary "$SQL" | sha256sum | cut -c1-16
}
check() {  # label format extra [curl options...]
    local label=$1 fmt=$2 extra=$3; shift 3
    local s p
    s=$(digest "$fmt" "output_format_parallel_formatting=0&$extra" "$@")
    p=$(digest "$fmt" "output_format_parallel_formatting=1&$extra" "$@")
    [[ $s == "$p" ]] && v=SAME || { v=DIFFERENT; fail=1; }
    printf "%-44s serial %s  parallel %s  %s\n" "$label" "$s" "$p" "$v"
}
for fmt in ArrowStream RowBinary TSV CSV JSONEachRow; do
    check "$fmt" "$fmt" ""
done
check "ArrowStream over HTTP/1.0 (not chunked)" ArrowStream "" --http1.0
check "RowBinary over HTTP/1.0 (not chunked)" RowBinary "" --http1.0
check "ArrowStream with progress headers" ArrowStream "send_progress_in_http_headers=1"
check "ArrowStream, gzip response (copy fallback)" ArrowStream "enable_http_compression=1" -H "Accept-Encoding: gzip"
check "ArrowStream, buffer_size=1000000 (cascade)" ArrowStream "buffer_size=1000000"
[[ $fail == 0 ]] && echo "ALL SAME" || echo "SOME DIFFERENT"
