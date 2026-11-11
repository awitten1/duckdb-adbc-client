//! Download a ClickHouse ArrowStream response and discard it, with no Arrow decoding, using the same HTTP
//! stack as the ClickHouse ADBC connector. Compare with `curl -o /dev/null`.
//!
//! Usage: http-download-bench MODE QUERY_ID
//!   MODE: hyper | hyper-mt | hyper-bigbuf | clickhouse-rs
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;

const URL: &str = "http://127.0.0.1:8123/";
const SQL: &str = "SELECT * FROM tpch_sf50.orders WHERE o_orderkey BETWEEN 1 AND 300000000";

#[derive(Default)]
struct Stats {
    bytes: u64,
    pieces: u64,
    sizes: Vec<u32>,
}

impl Stats {
    fn add(&mut self, len: usize) {
        self.bytes += len as u64;
        self.pieces += 1;
        self.sizes.push(len as u32);
    }

    fn report(mut self, mode: &str, elapsed: Duration) {
        self.sizes.sort_unstable();
        let pct = |p: f64| self.sizes[((self.sizes.len() - 1) as f64 * p) as usize];
        let secs = elapsed.as_secs_f64();
        println!(
            "{mode:<14} {:.3} GB in {secs:.3} s = {:.3} GB/s; {} pieces, size p10/p50/p90/max = {}/{}/{}/{} B",
            self.bytes as f64 / 1e9,
            self.bytes as f64 / 1e9 / secs,
            self.pieces,
            pct(0.10),
            pct(0.50),
            pct(0.90),
            self.sizes.last().copied().unwrap_or(0),
        );
    }
}

/// hyper's legacy client configured like clickhouse-rs's default (keepalive 60 s, pool idle 2 s).
async fn hyper_download(query_id: &str, max_buf_size: Option<usize>) -> Stats {
    let mut connector = HttpConnector::new();
    connector.set_keepalive(Some(Duration::from_secs(60)));
    let mut builder = Client::builder(TokioExecutor::new());
    builder.pool_idle_timeout(Duration::from_secs(2));
    if let Some(size) = max_buf_size {
        builder.http1_max_buf_size(size);
    }
    let client: Client<_, Full<Bytes>> = builder.build(connector);
    let uri = format!(
        "{URL}?default_format=ArrowStream&enable_http_compression=0&output_format_arrow_compression_method=none\
         &max_threads=32&query_id={query_id}"
    );
    let request = Request::post(uri).body(Full::new(Bytes::from(SQL))).expect("request");
    let response = client.request(request).await.expect("response");
    assert!(response.status().is_success(), "HTTP {}", response.status());
    let mut body = response.into_body();
    let mut stats = Stats::default();
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame.expect("body frame").into_data() {
            stats.add(data.len());
        }
    }
    stats
}

/// The ClickHouse ADBC connector's own path, minus Arrow: clickhouse-rs `fetch_bytes` drained via BytesCursor.
async fn clickhouse_download(query_id: &str) -> Stats {
    let client = clickhouse::Client::default()
        .with_url(URL)
        .with_compression(clickhouse::Compression::None)
        .with_setting("output_format_arrow_compression_method", "none")
        .with_setting("max_threads", "32")
        .with_setting("query_id", query_id);
    let mut cursor = client.query(SQL).fetch_bytes("ArrowStream").expect("fetch_bytes");
    let mut stats = Stats::default();
    while let Some(bytes) = cursor.next().await.expect("next") {
        stats.add(bytes.len());
    }
    stats
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (mode, query_id) = (args[1].as_str(), args[2].as_str());
    let runtime = if mode == "hyper-mt" {
        tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()
    } else {
        // The connector's default: a current-thread runtime driven by the caller.
        tokio::runtime::Builder::new_current_thread().enable_all().build()
    }
    .expect("runtime");
    let started = Instant::now();
    let stats = runtime.block_on(async {
        match mode {
            "hyper" | "hyper-mt" => hyper_download(query_id, None).await,
            "hyper-bigbuf" => hyper_download(query_id, Some(16 << 20)).await,
            "clickhouse-rs" => clickhouse_download(query_id).await,
            other => panic!("unknown mode {other}"),
        }
    });
    stats.report(mode, started.elapsed());
}
