# Optional microbenchmarks, built only on request:
#   ninja -C build/release arrow_conversion_bench
# Requires Google Benchmark (Ubuntu: apt install libbenchmark-dev).
find_package(benchmark QUIET)
if(benchmark_FOUND)
  add_executable(arrow_conversion_bench EXCLUDE_FROM_ALL arrow_conversion_bench.cpp)
  target_link_libraries(arrow_conversion_bench duckdb_static benchmark::benchmark)
else()
  message(STATUS "Google Benchmark not found; skipping ADBC microbenchmarks")
endif()
