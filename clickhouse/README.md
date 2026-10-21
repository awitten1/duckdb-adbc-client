# Local ClickHouse

From the repository root, run:

```sh
./clickhouse/start-clickhouse.sh
```

On first launch, this downloads ClickHouse 26.9.12.8 into `clickhouse/.clickhouse/`
and verifies its SHA-512 checksum. Requires Linux (x86_64 or ARM64), Bash, curl,
tar, and sha512sum; no root access or Docker is needed. The single ClickHouse
binary includes both the server and client. To install without starting:

```sh
./clickhouse/install-clickhouse.sh
```

To use an existing binary instead, select it by absolute path:

```sh
CLICKHOUSE_BIN=/absolute/path/to/clickhouse ./clickhouse/start-clickhouse.sh
```

The server runs in the foreground; stop it with Ctrl-C. Data persists in
`clickhouse/clickhouse-data/`. HTTP listens on `127.0.0.1:8123` and the native
protocol on `127.0.0.1:9000`, using the default user with an empty password.
The launcher defaults to UTC when `TZ` is unset.
The default profile uses LZ4 transport compression for compatibility with the
ClickHouse ADBC 0.1.1 driver.

In another terminal, connect with:

```sh
TZ=UTC ./clickhouse/.clickhouse/bin/clickhouse-client
```

Based on the [data_transfer_benchmarks launcher](https://github.com/awitten1/data_transfer_benchmarks/tree/main/clickhouse).
