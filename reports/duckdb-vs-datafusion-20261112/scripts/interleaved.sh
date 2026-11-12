#!/usr/bin/env bash
# DuckDB and DataFusion back to back on the same nodes, uncompressed, parallel formatting: 3 rounds, each running
# DuckDB, then DataFusion, then curl over one connection. Each client run is a warm-up single stream plus 3 measured
# batches of N streams (N = 1, then N = 10), timed by the benchmark; the shell's `time` adds the process's CPU time.
set -euo pipefail
cd ~/duckdb-adbc-client
export ADBC_READER_THREAD=1
T=$(nproc)
DUCKDB=build/release/extension/adbc/bench/parallel_read_adbc
DATAFUSION=~/df-bench/target/release/parallel_read_datafusion
ARGS=(--profile=profile://myclickhouse --threads=$T --memory-limit=48GiB --ch-max-threads=split
      --ch-settings=output_format_parallel_formatting=1 --repeats=3)
URL='http://10.10.1.1:8123/?default_format=ArrowStream&enable_http_compression=0&output_format_arrow_compression_method=none&max_threads=32&output_format_parallel_formatting=1'
SQL="SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000"
TIMEFORMAT='%R %U %S'
echo -e "round\tclient\tstreams\trepeat\twall_s"
for round in 1 2 3; do
    for streams in 1 10; do
        for client in duckdb datafusion; do
            bin=$DUCKDB; [[ $client == datafusion ]] && bin=$DATAFUSION
            { time $bin "${ARGS[@]}" $streams 2>/dev/null | tail -n +2 | sed "s/^/$round\t$client\t/" ; } 2> /tmp/cpu.txt
            read real user sys < /tmp/cpu.txt
            echo -e "# cpu\t$round\t$client\t$streams\tprocess_wall=$real\tuser=$user\tsys=$sys"
        done
    done
    curl -fsS -o /dev/null -w "# curl\t$round\t%{time_total}\n" "$URL" --data-binary "$SQL"
done
