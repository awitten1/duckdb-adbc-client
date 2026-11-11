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

# N threads in one process, each running an ordinary read_adbc query on one key range
# (the single-process counterpart of run.sh). Not part of the default build:
#   ninja -C build/release parallel_read_adbc
add_executable(parallel_read_adbc EXCLUDE_FROM_ALL parallel_read_adbc.cpp)
target_include_directories(parallel_read_adbc PRIVATE ${CMAKE_SOURCE_DIR}/extension/core_functions/include)
target_link_libraries(parallel_read_adbc adbc_extension core_functions_extension duckdb_static)
