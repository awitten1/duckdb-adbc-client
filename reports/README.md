# Local sort experiments

- [SF5 alpha and sort-key results](./sf5/README.md)
- [SF10 alpha, sort-key, and transport results with charts](./sf10/README.md)
- [SF10 profiling rerun and bottleneck charts](./sf10/PROFILING.md)
- [SF10 perf profiles with schema-only connector and compression off](./sf10/PERF_SCHEMA_OFF.md)
- [SF50 alpha, sort-key, memory, and bottleneck results with charts](./sf50/README.md)
- [CloudLab rerun of the SF5, SF10, and SF50 reports, with alpha charts for every key](./cloudlab-repro-20261110/README.md)
- [lineitem SF5, SF10, and SF25 alpha and sort-key results on CloudLab](./lineitem-20261110/README.md)
- [Parallel ADBC streams: packaged vs modified ClickHouse, stream count vs runtime](./parallel-streams-20261110/README.md)
- [What limits ADBC streams from the modified ClickHouse: output stage, connector, conversion, transfer](./stream-limits-20261110/README.md)
- [Sorting in ClickHouse, one stream and many: per-stream ORDER BY vs stream count](./sorted-streams-20261111/README.md)
- [Parallel ArrowStream formatting in ClickHouse: prototype, correctness, one stream vs many](./parallel-arrowstream-20261111/README.md)
- [Sending parallel-formatted output without a copy: TCP ceiling, collector profile, direct HTTP writes](./direct-send-20261111/README.md)
- [Parallel streams across a 10 Gb/s link: ClickHouse and DuckDB on separate nodes, with and without driver LZ4 compression](./two-node-10gbe-20261111/README.md)
- [Parallel and sorted streams across a 25 Gb/s link: compression, formatting, per-stream ORDER BY, and the 10 vs 25 Gb/s comparison](./two-node-25gbe-20261112/README.md)
- [DuckDB vs DataFusion reading the same ADBC streams: every 25 Gb/s configuration, plus a back-to-back single-stream and client-CPU check](./duckdb-vs-datafusion-20261112/README.md)
- [DuckDB vs DataFusion partial sort: two-node SF50 alpha sweep and direct DataFusion physical plan](./two-node-partial-sort-20261113/README.md)
- [Sorted streams across a 10 Gb/s link: per-stream ORDER BY on two nodes, uncompressed and with LZ4](./two-node-sorted-streams-20261111/README.md)

These reports and chart assets are tracked in Git. Raw DuckDB profiles,
ClickHouse data, Parquet exports, and machine-specific logs remain under the
ignored `build/` directory.
