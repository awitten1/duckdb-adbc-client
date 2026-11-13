//! DataFusion counterpart of `bench/parallel_read_adbc.cpp`: N parallel ADBC key-range streams in ONE process.
//!
//! For each stream count N, the table is split into N contiguous key ranges, exactly as `parallel_read_adbc`
//! splits it, and N streams start at once. Each stream:
//!   1. opens its own ADBC database/connection/statement on a dedicated reader thread, through the same driver
//!      library as DuckDB's `read_adbc` (taken from the same ADBC profile), runs the range query, and pushes the
//!      Arrow batches into a bounded channel (like `read_adbc` with `ADBC_READER_THREAD=1`);
//!   2. wraps that channel in a DataFusion `StreamingTable` and runs `SELECT * FROM` it (`read_table` +
//!      `execute_stream`), draining every batch. This is the counterpart of DuckDB's
//!      `EXPLAIN ANALYZE SELECT * FROM read_adbc(...)`, which consumes every row without an output sink.
//! All streams share one `SessionContext` and one tokio runtime with `--threads` workers.
//! Reports the wall time from starting the streams until the last one finishes.
//!
//! Options and output match `parallel_read_adbc`, so the same experiment scripts can run either binary:
//!   --profile=URI --database=NAME --table=NAME --key=COLUMN --order-by=EXPR --ch-max-threads=K|split
//!   --ch-settings=LIST --threads=K --memory-limit=SIZE --repeats=R --checksum [STREAM_COUNT ...]
//! `--checksum` compares the row count and an order-independent sum of row hashes (FNV-1a over arrow's row
//! encoding of every column) for one full read against N range reads.
//! `--alpha=0,0.25,0.5,0.75,1 --order-by=COLUMN` runs a direct physical-plan sort sweep.

mod partial_sort;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Connection as _, Database as _, Driver as _, Statement as _};
use adbc_driver_manager::ManagedDriver;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};

type BatchResult = Result<RecordBatch, DataFusionError>;
type Res<T> = Result<T, String>;

/// Batches buffered between a stream's reader thread and DataFusion.
const CHANNEL_CAPACITY: usize = 4;

struct Options {
    profile: String,
    database: String,
    table: String,
    key: String,
    order_by: String,
    ch_max_threads: String,
    ch_settings: String,
    threads: usize,
    memory_limit: usize,
    repeats: usize,
    checksum: bool,
    alphas: Option<Vec<u32>>,
    streams: Vec<usize>,
}

fn fail(message: impl AsRef<str>) -> ! {
    eprintln!("parallel_read_datafusion: {}", message.as_ref());
    std::process::exit(1);
}

fn parse_size(text: &str) -> usize {
    let t = text.trim();
    let split = t.find(|c: char| !c.is_ascii_digit() && c != '.').unwrap_or(t.len());
    let (number, unit) = t.split_at(split);
    let number: f64 = number.parse().unwrap_or_else(|_| fail(format!("bad size {text}")));
    let scale = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "kib" => 1024.0,
        "mib" => 1024.0 * 1024.0,
        "gib" => 1024.0 * 1024.0 * 1024.0,
        "kb" => 1e3,
        "mb" => 1e6,
        "gb" => 1e9,
        other => fail(format!("unknown size unit {other}")),
    };
    (number * scale) as usize
}

fn parse_options() -> Options {
    let mut o = Options {
        profile: "profile://myclickhouse".into(),
        database: "tpch_sf50".into(),
        table: "orders".into(),
        key: "o_orderkey".into(),
        order_by: String::new(),
        ch_max_threads: String::new(),
        ch_settings: String::new(),
        threads: 16,
        memory_limit: parse_size("56GiB"),
        repeats: 3,
        checksum: false,
        alphas: None,
        streams: Vec::new(),
    };
    for arg in std::env::args().skip(1) {
        let value = |name: &str| arg.strip_prefix(name).and_then(|rest| rest.strip_prefix('=')).map(str::to_string);
        if let Some(v) = value("--profile") {
            o.profile = v;
        } else if let Some(v) = value("--database") {
            o.database = v;
        } else if let Some(v) = value("--table") {
            o.table = v;
        } else if let Some(v) = value("--key") {
            o.key = v;
        } else if let Some(v) = value("--order-by") {
            o.order_by = v;
        } else if let Some(v) = value("--ch-max-threads") {
            o.ch_max_threads = v;
        } else if let Some(v) = value("--ch-settings") {
            o.ch_settings = v;
        } else if let Some(v) = value("--threads") {
            o.threads = v.parse().unwrap_or_else(|_| fail("bad --threads"));
        } else if let Some(v) = value("--memory-limit") {
            o.memory_limit = parse_size(&v);
        } else if let Some(v) = value("--repeats") {
            o.repeats = v.parse().unwrap_or_else(|_| fail("bad --repeats"));
        } else if let Some(v) = value("--alpha") {
            o.alphas = Some(v.split(',').map(partial_sort::parse_alpha).collect());
        } else if arg == "--checksum" {
            o.checksum = true;
        } else if let Ok(n) = arg.parse::<usize>() {
            if n == 0 {
                fail("stream counts must be positive");
            }
            o.streams.push(n);
        } else {
            fail(format!("unknown argument {arg}"));
        }
    }
    if o.alphas.is_some() && (o.order_by.is_empty() || !o.streams.is_empty()) {
        fail("--alpha requires --order-by=COLUMN and takes no stream counts");
    }
    if o.streams.is_empty() {
        o.streams = vec![1, 2, 4];
    }
    o
}

/// The driver library and URI from an ADBC profile (`profile://NAME` -> ~/.config/adbc/profiles/NAME.toml).
fn read_profile(profile: &str) -> (String, String) {
    let name = profile.strip_prefix("profile://").unwrap_or(profile);
    let path = if name.ends_with(".toml") || name.contains('/') {
        PathBuf::from(name)
    } else {
        PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(format!(".config/adbc/profiles/{name}.toml"))
    };
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| fail(format!("{}: {e}", path.display())));
    let field = |key: &str| {
        text.lines()
            .filter_map(|l| l.split_once('='))
            .find(|(k, _)| k.trim() == key)
            .map(|(_, v)| v.trim().trim_matches('"').to_string())
            .unwrap_or_else(|| fail(format!("{}: no {key}", path.display())))
    };
    (field("driver"), field("uri"))
}

fn with_clickhouse_settings(o: &Options, streams: usize, mut sql: String) -> String {
    let mut settings = Vec::new();
    if o.ch_max_threads == "split" {
        settings.push(format!("max_threads={}", std::cmp::max(1, 32 / streams)));
    } else if !o.ch_max_threads.is_empty() {
        settings.push(format!("max_threads={}", o.ch_max_threads));
    }
    if !o.ch_settings.is_empty() {
        settings.push(o.ch_settings.clone());
    }
    if !settings.is_empty() {
        sql += &format!(" SETTINGS {}", settings.join(", "));
    }
    sql
}

fn range_query(o: &Options, streams: usize, lo: i64, hi: i64) -> String {
    let mut sql = format!("SELECT * FROM {}.{} WHERE {} BETWEEN {lo} AND {hi}", o.database, o.table, o.key);
    if !o.order_by.is_empty() {
        sql += &format!(" ORDER BY {}", o.order_by);
    }
    with_clickhouse_settings(o, streams, sql)
}

/// N contiguous, disjoint ranges covering [lo, hi], split exactly as parallel_read_adbc splits them.
fn ranges(lo: i64, hi: i64, n: usize) -> Vec<(i64, i64)> {
    let width = hi as i128 - lo as i128 + 1;
    (0..n as i128)
        .map(|i| {
            let start = lo as i128 + width * i / n as i128;
            let end = lo as i128 + width * (i + 1) / n as i128 - 1;
            (start as i64, end as i64)
        })
        .collect()
}

/// Open the ADBC statement on a new thread and stream its batches into a bounded channel.
fn spawn_reader(
    driver: &ManagedDriver,
    uri: &str,
    sql: String,
) -> (oneshot::Receiver<Res<SchemaRef>>, mpsc::Receiver<BatchResult>, std::thread::JoinHandle<()>) {
    let (schema_tx, schema_rx) = oneshot::channel();
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let mut driver = driver.clone();
    let uri = uri.to_string();
    let handle = std::thread::spawn(move || {
        let opened = (|| -> Res<_> {
            let e = |e: adbc_core::error::Error| format!("{e:?}");
            let database = driver
                .new_database_with_opts([(OptionDatabase::Uri, OptionValue::String(uri))])
                .map_err(e)?;
            let mut connection = database.new_connection().map_err(e)?;
            let mut statement = connection.new_statement().map_err(e)?;
            statement.set_sql_query(&sql).map_err(e)?;
            let reader = statement.execute().map_err(e)?;
            Ok((database, connection, statement, reader))
        })();
        match opened {
            Err(error) => {
                let _ = schema_tx.send(Err(error));
            }
            Ok((_database, _connection, _statement, reader)) => {
                let _ = schema_tx.send(Ok(reader.schema()));
                for batch in reader {
                    if tx.blocking_send(batch.map_err(DataFusionError::from)).is_err() {
                        break;
                    }
                }
            }
        }
    });
    (schema_rx, rx, handle)
}

/// One stream's batches as a DataFusion partition. A partition can be executed once.
#[derive(Debug)]
struct ChannelPartition {
    schema: SchemaRef,
    receiver: Mutex<Option<mpsc::Receiver<BatchResult>>>,
}

impl PartitionStream for ChannelPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let receiver = self.receiver.lock().unwrap().take().expect("partition executed twice");
        let stream = futures::stream::unfold(receiver, |mut rx| async move { rx.recv().await.map(|b| (b, rx)) });
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Run `SELECT * FROM` one ADBC stream in DataFusion and drain it. Returns (rows, row-hash sum or 0).
async fn run_stream(ctx: SessionContext, driver: ManagedDriver, uri: String, sql: String, checksum: bool)
                    -> Res<(u64, u128)> {
    let s = |e: DataFusionError| e.to_string();
    let (schema_rx, receiver, reader_thread) = spawn_reader(&driver, &uri, sql);
    let schema = schema_rx.await.map_err(|_| "reader thread exited without a schema".to_string())??;
    let partition = Arc::new(ChannelPartition { schema: schema.clone(), receiver: Mutex::new(Some(receiver)) });
    let table = StreamingTable::try_new(schema.clone(), vec![partition]).map_err(s)?;
    let mut stream = ctx.read_table(Arc::new(table)).map_err(s)?.execute_stream().await.map_err(s)?;
    let mut converter = if checksum {
        let fields = schema.fields().iter().map(|f| SortField::new(f.data_type().clone())).collect();
        Some(RowConverter::new(fields).map_err(|e| e.to_string())?)
    } else {
        None
    };
    let (mut rows, mut hash) = (0u64, 0u128);
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(s)?;
        rows += batch.num_rows() as u64;
        if let Some(converter) = converter.as_mut() {
            let encoded = converter.convert_columns(batch.columns()).map_err(|e| e.to_string())?;
            for row in encoded.iter() {
                hash = hash.wrapping_add(fnv1a(row.as_ref()) as u128);
            }
        }
    }
    drop(stream);
    tokio::task::spawn_blocking(move || reader_thread.join())
        .await
        .map_err(|e| e.to_string())?
        .map_err(|_| "reader thread panicked".to_string())?;
    Ok((rows, hash))
}

/// Start one stream per SQL text at once; returns the wall time until the last finishes, and each result.
fn run_streams(rt: &tokio::runtime::Runtime, ctx: &SessionContext, driver: &ManagedDriver, uri: &str,
               sqls: Vec<String>, checksum: bool) -> (f64, Vec<(u64, u128)>) {
    rt.block_on(async {
        let started = Instant::now();
        let tasks: Vec<_> = sqls
            .into_iter()
            .map(|sql| tokio::spawn(run_stream(ctx.clone(), driver.clone(), uri.to_string(), sql, checksum)))
            .collect();
        let mut results = Vec::new();
        for (i, task) in tasks.into_iter().enumerate() {
            match task.await {
                Ok(Ok(result)) => results.push(result),
                Ok(Err(e)) => fail(format!("stream {i}: {e}")),
                Err(e) => fail(format!("stream {i}: {e}")),
            }
        }
        (started.elapsed().as_secs_f64(), results)
    })
}

fn main() {
    let o = parse_options();
    let (driver_path, uri) = read_profile(&o.profile);
    let driver = ManagedDriver::load_dynamic_from_filename(&driver_path, Some(b"AdbcDriverInit"), AdbcVersion::V110)
        .unwrap_or_else(|e| fail(format!("loading {driver_path}: {e:?}")));
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(o.threads)
        .enable_all()
        .build()
        .unwrap_or_else(|e| fail(e.to_string()));
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_limit(o.memory_limit, 1.0)
        .build_arc()
        .unwrap_or_else(|e| fail(e.to_string()));
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new().with_target_partitions(o.threads), runtime_env);

    // Key bounds and row count, read through the same driver.
    let source = format!("{}.{}", o.database, o.table);
    let bounds_sql = format!("SELECT toInt64(min({k})) AS lo, toInt64(max({k})) AS hi, toInt64(count()) AS n FROM {source}",
                             k = o.key);
    let (schema_rx, mut rx, reader_thread) = spawn_reader(&driver, &uri, bounds_sql);
    schema_rx.blocking_recv().unwrap_or_else(|_| fail("bounds query: no schema")).unwrap_or_else(|e| fail(e));
    let batch = rx.blocking_recv().unwrap_or_else(|| fail("bounds query: no rows")).unwrap_or_else(|e| fail(e.to_string()));
    while rx.blocking_recv().is_some() {}
    let _ = reader_thread.join();
    let col = |i: usize| batch.column(i).as_any().downcast_ref::<Int64Array>().unwrap_or_else(|| fail("bounds type")).value(0);
    let (lo, hi, total) = (col(0), col(1), col(2));

    if let Some(alphas) = &o.alphas {
        rt.block_on(partial_sort::sweep(&o, &ctx, &driver, &uri, (lo, hi, total), alphas))
            .unwrap_or_else(|e| fail(e));
        return;
    }

    let describe = format!(
        "{source}: {total} rows, {} in [{lo}, {hi}]; {} DataFusion threads shared by all streams; ClickHouse \
         max_threads per query: {}; ORDER BY per stream: {}; extra settings: {}",
        o.key,
        o.threads,
        if o.ch_max_threads.is_empty() { "server default" } else { &o.ch_max_threads },
        if o.order_by.is_empty() { "none" } else { &o.order_by },
        if o.ch_settings.is_empty() { "none" } else { &o.ch_settings },
    );

    if o.checksum {
        println!("{describe}");
        let (_, full) = run_streams(&rt, &ctx, &driver, &uri, vec![format!("SELECT * FROM {source}")], true);
        let (full_n, full_h) = full[0];
        println!("single DataFusion read: {full_n} rows, hash sum {full_h}");
        let mut ok = true;
        for &n in &o.streams {
            let sqls = ranges(lo, hi, n).into_iter().map(|(a, b)| range_query(&o, n, a, b)).collect();
            let (_, results) = run_streams(&rt, &ctx, &driver, &uri, sqls, true);
            let n_sum: u64 = results.iter().map(|r| r.0).sum();
            let h_sum = results.iter().fold(0u128, |acc, r| acc.wrapping_add(r.1));
            let matched = n_sum == full_n && h_sum == full_h;
            ok &= matched;
            println!("{n} threads: {n_sum} rows, hash sum {h_sum}  {}", if matched { "MATCH" } else { "MISMATCH" });
        }
        std::process::exit(if ok { 0 } else { 1 });
    }

    eprintln!("{describe}");
    println!("streams\trepeat\twall_s");
    let measure = |n: usize| {
        let sqls = ranges(lo, hi, n).into_iter().map(|(a, b)| range_query(&o, n, a, b)).collect();
        run_streams(&rt, &ctx, &driver, &uri, sqls, false).0
    };
    measure(1); // warmup
    let mut results: Vec<(usize, Vec<f64>)> = o.streams.iter().map(|&n| (n, Vec::new())).collect();
    for repeat in 1..=o.repeats {
        let mut order = o.streams.clone();
        if repeat % 2 == 0 {
            order.reverse(); // alternate the order of stream counts, as parallel_read_adbc does
        }
        for n in order {
            let seconds = measure(n);
            results.iter_mut().filter(|(k, _)| *k == n).for_each(|(_, v)| v.push(seconds));
            println!("{n}\t{repeat}\t{seconds}");
        }
    }
    eprintln!("streams  median_wall_s  min-max_wall_s   speedup");
    let base = {
        let mut w = results[0].1.clone();
        w.sort_by(f64::total_cmp);
        w[w.len() / 2]
    };
    for (n, walls) in &mut results {
        walls.sort_by(f64::total_cmp);
        let median = walls[walls.len() / 2];
        eprintln!("{n:>7}  {median:>13.3}  {:>6.3}-{:<6.3}  {:>7.2}", walls[0], walls[walls.len() - 1], base / median);
    }
}
