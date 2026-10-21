# DuckDB ADBC Extension

Use DuckDB (v1.4.5+ or v1.5.4+) to query [Snowflake](https://www.snowflake.com), [Databricks](https://www.databricks.com), [BigQuery](https://cloud.google.com/bigquery), [PostgreSQL](https://www.postgresql.org), [MySQL](https://www.mysql.com), or any other system  with an [ADBC driver](https://columnar.tech/dbc).

![overall](./design/overall.png)

## What is ADBC?

ADBC (Arrow Database Connectivity) is a universal data-access API built on [Apache Arrow](https://arrow.apache.org/), an efficient, columnar data format that almost [every data system](https://arrow.apache.org/powered_by/) supports natively. 

By building on Arrow, ADBC enables:
1. Lightning fast (zero-copy) data transfer between column-oriented analytical databases, bypassing the slow column-to-row and row-to-column conversions typical of legacy row-based APIs like ODBC or JDBC.
2. Seamless interoperability with a large and growing ecosystem of Arrow-compatible systems.

## Extension Installation

Then you can install the extension from DuckDB by running:

```sql
INSTALL adbc FROM community;
LOAD adbc;
```

<details>
  <summary>Click here for instructions on how to build the extension from source with cmake and ninja.</summary>
  
  ```sh
  # Clone the repo and its dependencies
  git clone --recurse-submodules git@github.com:columnar-tech/duckdb-adbc-client.git
  cd duckdb-adbc-client
  # Build the extension from source
  GEN=ninja make release
  # Run DuckDB with the ADBC extension auto-loaded 
  ./build/release/duckdb 
  ```
  
</details>

## Installing ADBC Drivers

[`dbc`](https://columnar.tech/dbc/) is a command-line tool that makes it easy to install and manage ADBC drivers. 

<details>
  <summary>Click here for instructions on how to install dbc..</summary>
  
  ```sh
  # shell
  curl -LsSf https://dbc.columnar.tech/install.sh | sh
  # brew
  brew install columnar-tech/tap/dbc
  # uv
  uv tool install dbc
  # pipx
  pipx install dbc
  # powershell
  powershell -ExecutionPolicy ByPass -c irm https://dbc.columnar.tech/install.ps1 | iex
  # winget
  winget install dbc
  ```
</details>



After installing `dbc`, you can run `dbc install <system>` to install a driver for a new system.

```sh
dbc install sqlite
```

To easily manage connection information for each system, you can create a [connection profile](https://arrow.apache.org/adbc/main/format/connection_profiles.html) for each driver (i.e., `mydb.toml`):

```toml
profile_version = 1
driver = "sqlite"

[Options]
uri = "./games.sqlite"
```

The last step is to save your profile in the correct location:

```sh
# Linux
mv mydb.toml ~/.config/adbc/profiles/
# macOS
mv mydb.toml ~/Library/Application Support/ADBC/Profiles/
# Windows
move "mydb.toml" "%LOCALAPPDATA%\ADBC\Profiles\"
```

## Quickstart

We showcase the functionality of the ADBC extension using a `games` database.

You can download the `games` database as a SQLite file with:

```sh
curl -o games.sqlite "https://data.columnar.tech/games.sqlite"
```

### read_adbc

To read data through ADBC you can call the `read_adbc` table function by providing a URI to a connection profile and a SQL query. 

```sql
-- Install the extension
D INSTALL adbc FROM community;
-- Load it
D LOAD adbc;
-- Read from the ADBC database using read_adbc
D SELECT * FROM read_adbc('profile://mydb', 'SELECT * FROM games');
┌───────┬────────────┬─────────────────────┬─────────┬─────────┬─────────────┬─────────────┬────────────┐
│  id   │    name    │      inventor       │  year   │ min_age │ min_players │ max_players │ list_price │
│ int64 │  varchar   │       varchar       │ varchar │  int64  │    int64    │    int64    │  varchar   │
├───────┼────────────┼─────────────────────┼─────────┼─────────┼─────────────┼─────────────┼────────────┤
│     1 │ Monopoly   │ Elizabeth Magie     │ 1903    │       8 │           2 │           6 │ 19.99      │
│     2 │ Scrabble   │ Alfred Mosher Butts │ 1938    │       8 │           2 │           4 │ 17.99      │
│     3 │ Clue       │ Anthony E. Pratt    │ 1944    │       8 │           2 │           6 │ 9.99       │
│     4 │ Candy Land │ Eleanor Abbott      │ 1948    │       3 │           2 │           4 │ 7.99       │
│     5 │ Risk       │ Albert Lamorisse    │ 1957    │      10 │           2 │           5 │ 29.99      │
└───────┴────────────┴─────────────────────┴─────────┴─────────┴─────────────┴─────────────┴────────────┘
```

### ATTACH

To create a persistent connection to an ADBC database, you can run the `ATTACH` command and then query the ADBC database as if it were a local DuckDB database. We currently support catalog lookups, as well as `SELECT`, `INSERT`, `COPY`, and `CREATE TABLE AS (SELECT ...)` (`CTAS`) statements.

```sql
-- Create a persistent connection to the SQLite database
D ATTACH 'profile://mydb' AS mydb (TYPE adbc);
-- Set the default schema
D USE mydb.main;
-- Display all tables in the attached ADBC database
D SHOW ALL TABLES;
┌──────────┬─────────┬─────────┬───────────────────────────────┬────────────────────────────────┬───────────┐
│ database │ schema  │  name   │         column_names          │          column_types          │ temporary │
│ varchar  │ varchar │ varchar │           varchar[]           │           varchar[]            │  boolean  │
├──────────┼─────────┼─────────┼───────────────────────────────┼────────────────────────────────┼───────────┤
│ mydb     │ main    │ games   │ [id, name, inventor, year,    │ [BIGINT, VARCHAR, VARCHAR,     │ false     │
│          │         │         │  min_age, min_players,        │  VARCHAR, BIGINT, BIGINT,      │           │
│          │         │         │  max_players, list_price]     │  BIGINT, VARCHAR]              │           │
└──────────┴─────────┴─────────┴───────────────────────────────┴────────────────────────────────┴───────────┘
-- Read directly from the attached ADBC table
D SELECT * FROM games;
┌───────┬────────────┬─────────────────────┬─────────┬─────────┬─────────────┬─────────────┬────────────┐
│  id   │    name    │      inventor       │  year   │ min_age │ min_players │ max_players │ list_price │
│ int64 │  varchar   │       varchar       │ varchar │  int64  │    int64    │    int64    │  varchar   │
├───────┼────────────┼─────────────────────┼─────────┼─────────┼─────────────┼─────────────┼────────────┤
│     1 │ Monopoly   │ Elizabeth Magie     │ 1903    │       8 │           2 │           6 │ 19.99      │
│     2 │ Scrabble   │ Alfred Mosher Butts │ 1938    │       8 │           2 │           4 │ 17.99      │
│     3 │ Clue       │ Anthony E. Pratt    │ 1944    │       8 │           2 │           6 │ 9.99       │
│     4 │ Candy Land │ Eleanor Abbott      │ 1948    │       3 │           2 │           4 │ 7.99       │
│     5 │ Risk       │ Albert Lamorisse    │ 1957    │      10 │           2 │           5 │ 29.99      │
└───────┴────────────┴─────────────────────┴─────────┴─────────┴─────────────┴─────────────┴────────────┘
-- Insert into the ADBC database
D INSERT INTO games (SELECT 6, 'Battleship', 'Clifford Von Wickler', 1931, 7, 2, 2, 12.99);
D SELECT * FROM games;
┌───────┬────────────┬──────────────────────┬─────────┬─────────┬─────────────┬─────────────┬────────────┐
│  id   │    name    │       inventor       │  year   │ min_age │ min_players │ max_players │ list_price │
│ int64 │  varchar   │       varchar        │ varchar │  int64  │    int64    │    int64    │  varchar   │
├───────┼────────────┼──────────────────────┼─────────┼─────────┼─────────────┼─────────────┼────────────┤
│     1 │ Monopoly   │ Elizabeth Magie      │ 1903    │       8 │           2 │           6 │ 19.99      │
│     2 │ Scrabble   │ Alfred Mosher Butts  │ 1938    │       8 │           2 │           4 │ 17.99      │
│     3 │ Clue       │ Anthony E. Pratt     │ 1944    │       8 │           2 │           6 │ 9.99       │
│     4 │ Candy Land │ Eleanor Abbott       │ 1948    │       3 │           2 │           4 │ 7.99       │
│     5 │ Risk       │ Albert Lamorisse     │ 1957    │      10 │           2 │           5 │ 29.99      │
│     6 │ Battleship │ Clifford Von Wickler │ 1931    │       7 │           2 │           2 │ 12.99      │
└───────┴────────────┴──────────────────────┴─────────┴─────────┴─────────────┴─────────────┴────────────┘
-- Create a local table in DuckDB of the inventors of each game
D CREATE TABLE memory.inventors AS (SELECT id, inventor FROM games);
-- Create a new table in the attached ADBC database (SQLite) of the inventors
D CREATE TABLE game_inventors(id, inventor) AS (SELECT * FROM memory.inventors);
D SELECT * FROM game_inventors;
┌───────┬──────────────────────┐
│  id   │       inventor       │
│ int64 │       varchar        │
├───────┼──────────────────────┤
│     1 │ Elizabeth Magie      │
│     2 │ Alfred Mosher Butts  │
│     3 │ Anthony E. Pratt     │
│     4 │ Eleanor Abbott       │
│     5 │ Albert Lamorisse     │
│     6 │ Clifford Von Wickler │
└───────┴──────────────────────┘
```

### Custom Delimiters

By default, `ATTACH` delimits all SQL queries with double quotes (i.e., `SELECT * FROM "schema"."table"`). The `DELIMITER` option adds support for systems with different schema/table delimiters (i.e., `[schema].[table]` for SQL Server).

```sql
D ATTACH 'profile://mydb' AS mydb (TYPE adbc, DELIMITER '[]');
```

### adbc_execute

To perform arbitrary operations via ADBC, you can call `adbc_execute`.

```sql
D CALL adbc_execute('profile://mydb', 'DROP TABLE games');
┌─────────┐
│ Success │
│ boolean │
├─────────┤
│ true    │
└─────────┘
```

### adbc_clear_cache

DuckDB caches schema and table metadata from ADBC databases locally. To clear the cached metadata (i.e., after a remote update), you can call `adbc_clear_cache`.

```sql
D CALL adbc_clear_cache();
┌─────────┐
│ Success │
│ boolean │
├─────────┤
│ true    │
└─────────┘
```

## Experimental ADBC partial sort

`read_adbc_partial_sort` divides a SQL table's integer key range between the remote database and DuckDB. Both subsets are transferred as Arrow streams through ADBC. The remote database sorts the upper range while DuckDB reads and sorts the lower range; the function merges their ordered results. Schema discovery and range bounds also use ADBC. The function does not query database-specific system catalogs or require physical-key metadata.

For example, using ClickHouse:

```toml
# clickhouse.toml
profile_version = 1
driver = "clickhouse"

[Options]
uri = "http://localhost:8123/"
```

```sql
SELECT * FROM read_adbc_partial_sort(
    'profile://./clickhouse.toml', 'default', 'events',
    partition_key := 'id',
    sort_columns := ['score', 'id'],
    descending := [true, false],
    nulls_first := [false, false],
    pushdown_ratio := 0.3
);
```

The positional arguments are the ADBC profile URI, schema or database name, and table name. Pass `''` for the second argument to use an unqualified table name. For example, a SQLite profile can use `'main'` or `''`; a PostgreSQL profile would typically use `'public'`.

Partition and output sort keys must be signed or unsigned integers up to 64 bits; NULLs are supported. Other source columns are returned unchanged. Any such integer column can be the partition key. Choosing a leading physical sort key or an indexed column can help the remote database prune range scans; matching the output ordering to physical ordering may also avoid a remote sort. These are performance choices, not correctness requirements, and are not validated by the function. `descending` and `nulls_first` default to all `false`; supplied lists must match `sort_columns` in length. Column names are exact, case-sensitive source names.

The function uses SQL with double-quoted identifiers, `LIMIT 1` for schema discovery, `MIN`/`MAX`/`COUNT`, range predicates, and `ORDER BY ... NULLS FIRST/LAST`. The driver must support SQL queries returning Arrow streams and independent connections used concurrently; the database must support this SQL dialect and return integer keys with compatible ordering. Drivers that infer types from result values must return consistent Arrow column types for schema discovery and both branches; mismatches raise an error. ADBC alone does not guarantee these capabilities. Integration tests cover SQLite and ClickHouse; other backends may need SQL-dialect changes.

**The ratio measures key-range width, not row count.** For minimum 0 and maximum 999999, ratio 0.3 selects `id >= 700000` for the remote database and `id < 700000` for DuckDB. More generally, the inclusive width is `max - min + 1`; the remote database receives the upper `floor(width * ratio)` values. Sparse or skewed keys can produce uneven row counts. Ratios use `DECIMAL(10,9)` precision and widened integer arithmetic, including the full `UInt64` range. NULL partition keys go to DuckDB for intermediate ratios. Ratio 0 sorts everything locally; ratio 1 sorts everything remotely.

This is an experimental function for **unchanged tables and schemas throughout binding and execution**. It does not establish a shared snapshot between connections. Each invocation uses isolated ADBC connections; its internal read concurrency does not enable general concurrent ADBC operations elsewhere in the extension. Partition expressions and composite range boundaries are unsupported. There is no filter/projection pushdown or automatic optimizer rewrite. The function emits an ordered stream, but joins, aggregates, and other order-changing SQL operations may discard that order. An outer `ORDER BY` still performs a normal DuckDB sort.

The local sort uses DuckDB's memory manager and can spill to disk. The remote queue holds at most two Arrow batches, plus one in transfer, with additional memory for active conversion/output buffers. Arrow/driver buffers are additional to DuckDB's `memory_limit`. Cancellation and early `LIMIT` perform cooperative cleanup: queue waits are interrupted, but cleanup can wait for an in-flight ADBC driver call. Configure appropriate server/network timeouts for your environment.

### Testing and benchmarking partial sort

For a persistent local demo, run:

```sh
bash scripts/demo_clickhouse_partial_sort.sh
# Optional: choose a smaller dataset or a different port/container.
bash scripts/demo_clickhouse_partial_sort.sh --rows 100000 --port 18124 \
    --container adbc-sort-demo-small --state-dir build/partial-sort-demo-small
```

The script starts ClickHouse in Docker on a loopback-only port, installs the ClickHouse ADBC driver if needed, and creates a generated events table physically ordered by `id`. All loading and queries use ADBC. It prints an interactive DuckDB launch command plus examples for partial sorting, storage-aligned ordering, and fully consuming local/partial/remote sort results. Profiles and SQL are saved under `build/partial-sort-demo`; the container stays running so the printed commands work. Reruns reuse the dataset, and different row counts use separate tables. Use `--state-dir` to keep separate sets of generated example files. Run with `--help` for options.

The integration suite creates a unique temporary ClickHouse database and populates it through ADBC-executed SQL. It removes that database after the tests, including setup failures. Use a disposable test server: the test account needs database creation/deletion privileges and access to `SYSTEM FLUSH LOGS` and `system.query_log` for the server-overlap assertion. The table function itself does not need query-log access. Install the ClickHouse driver with `dbc install 'clickhouse=0.1.1'`, build the extension, then run against a ClickHouse 26.8.2.7 server:

```sh
python3 scripts/test_clickhouse_partial_sort.py --url http://localhost:8123/
# Or use an existing connection profile:
python3 scripts/test_clickhouse_partial_sort.py --profile profile://myclickhouse

python3 scripts/benchmark_clickhouse_partial_sort.py \
    --url http://localhost:8123/ --rows 1000000 --repeats 3 \
    --output /tmp/partial-sort-benchmark.json
```

Both scripts accept `--duckdb` and `--extension` paths. Set `ADBC_DRIVER_PATH` if your driver installation is outside the driver's standard search locations. A separate ClickHouse CI job runs the integration suite. The ordinary SQL suite also exercises partial sorting with SQLite, without a server or physical-key metadata:

```sh
./build/release/test/unittest test/sql/sqlite/partial_sort.test
```

The benchmark consumes every row for ratios 0, 0.25, 0.5, 0.75, and 1, covering dense/skewed keys and storage-aligned/unaligned output orders. Reported wall time includes CLI startup, binding, bounds discovery, transfer, sorting, merging, and a common CSV output sink. DuckDB JSON profiling includes actual local/remote row counts, queue high watermark, bounds duration, local ingestion/sort duration, branch-active overlap, and peak memory/spill. Branch-active overlap includes I/O and backpressure; it is not a measurement of simultaneous CPU usage. Performance is workload-dependent and all rows still cross the ADBC connection.

## Limitations

### Autocommit Mode

The ADBC extension only supports autocommit mode. In this mode, queries take effect immediately upon execution. 

### Predicate Pushdown

The ADBC extension does not currently perform predicate pushdown for attached ADBC tables.

See [Issue #1](https://github.com/columnar-tech/duckdb-adbc-client/issues/1) and [Issue #2](https://github.com/columnar-tech/duckdb-adbc-client/issues/2) for more details.

To push down predicates, you can directly call `read_adbc` with a SQL query.

```sql
D USE memory;
D CREATE MACRO read_mydb(query) AS TABLE SELECT * FROM read_adbc('profile://mydb', query);
D SELECT inventor FROM read_mydb('SELECT inventor FROM games WHERE name = ''Monopoly''');
┌─────────────────┐
│    inventor     │
│     varchar     │
├─────────────────┤
│ Elizabeth Magie │
└─────────────────┘
```

### Connecting to DuckDB or Quack

The ADBC extension does not currently support connecting to another DuckDB database using the DuckDB or Quack ADBC drivers.

### Concurrency Within a Single Process

Concurrent ADBC operations within a single process are unsupported, except for the isolated read workers inside `read_adbc_partial_sort`.

### Concurrency Within a Single SQL Statement

By default, mixing ADBC reads and writes in the same SQL statement will throw an error to prevent potential concurrency bugs. To override this warning and enable mixing ADBC reads and writes, you can set `adbc_mix_reads_writes` to `true`.

```sql
D USE mydb.main;
D INSERT INTO games (SELECT * FROM games);
Not implemented Error: ...
D SET adbc_mix_reads_writes = true;
D INSERT INTO games (SELECT * FROM games);
D 
```

To materialize all input rows to an `INSERT` or `CTAS` and prevent concurrency bugs when mixing ADBC reads and writes, you can set `adbc_materialize_insert_rows` to `true`.

```sql
D SET adbc_materialize_insert_rows = true;
```

## Tuning

To learn more about the internal design of the ADBC extension for tuning, you can read [DESIGN.md](./DESIGN.md).

### Connection Pool Size

Internally, the ADBC extension creates connections to perform SQL statements. To avoid repeatedly creating and destroying connections, each attached ADBC database maintains a connection pool. The pool is initially empty and grows as SQL statements create new connections, up to a default limit of 50. Once the pool is full, SQL statements create ephemeral connections, that are destroyed immediately after execution. To adjust the ADBC connection pool limit for each attached database, you can modify the value of `adbc_connection_pool_size`.

```sql
D SET adbc_connection_pool_size = 100;
```

### INSERT and CTAS Buffer Sizes

When performing `INSERT` and `CREATE TABLE AS (SELECT ...)` statements, the extension uses ADBC's bulk ingest API. To avoid materializing all input rows at once, the ADBC extension inserts batches of rows at a time. Internally, one thread appends rows to an in-memory buffer, and then another thread empties the buffer and inserts via ADBC. Increasing the buffer size may improve performance, but slow down query cancellation. The buffer is full when it exceeds 50% of the available memory or contains `adbc_insert_buffer_size` chunks of 2048 rows each. The default value of `adbc_insert_buffer_size` is 1000, resulting in a buffer of 1000 x 2048 rows (i.e., ~2M rows). To adjust the insert buffer size, you can modify the value of `adbc_insert_buffer_size`.

```sql
D SET adbc_insert_buffer_size = 10000;
```
