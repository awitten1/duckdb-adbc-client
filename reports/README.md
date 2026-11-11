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

These reports and chart assets are tracked in Git. Raw DuckDB profiles,
ClickHouse data, Parquet exports, and machine-specific logs remain under the
ignored `build/` directory.
