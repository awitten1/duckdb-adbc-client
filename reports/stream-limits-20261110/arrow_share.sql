-- Arrow output share of each read_adbc_ranges range query since {start}, by stream count.
-- elapsed_us is time inside a processor's work(); for the HTTP output format that
-- includes blocking socket writes, measured separately by NetworkSendElapsedMicroseconds.
WITH
    queries AS
    (
        SELECT
            query_id,
            query_duration_ms / 1e3 AS wall_s,
            ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6 AS cpu_s,
            ProfileEvents['NetworkSendElapsedMicroseconds'] / 1e6 AS send_wait_s,
            toUInt64(extract(query, 'BETWEEN (\\d+)')) AS lo,
            toUInt64(extract(query, 'AND (\\d+)')) AS hi,
            round(300000000 / (hi - lo + 1)) AS streams
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND event_time >= toDateTime({start:UInt64})
          AND query LIKE 'SELECT "o_orderkey", "o_custkey"%tpch_sf50%orders%BETWEEN%SETTINGS max_threads=%'
    ),
    processors AS
    (
        SELECT
            query_id,
            sumIf(elapsed_us, name = 'ArrowIPCBlockOutputFormat') / 1e6 AS arrow_busy_s,
            sumIf(elapsed_us, name LIKE 'MergeTreeSelect%') / 1e6 AS readers_busy_s
        FROM system.processors_profile_log
        WHERE event_time >= toDateTime({start:UInt64}) AND query_id IN (SELECT query_id FROM queries)
        GROUP BY query_id
    )
SELECT
    version() AS server,
    streams,
    count() AS range_queries,
    round(median(wall_s), 2) AS wall,
    round(median(cpu_s), 2) AS server_cpu,
    round(median(arrow_busy_s), 2) AS arrow_busy,
    round(median(send_wait_s), 2) AS send_wait,
    round(median(arrow_busy_s - send_wait_s), 2) AS arrow_encode,
    round(median(readers_busy_s), 2) AS readers_busy,
    round(100 * median(arrow_busy_s / wall_s), 1) AS arrow_busy_pct_wall,
    round(100 * median((arrow_busy_s - send_wait_s) / wall_s), 1) AS encode_pct_wall,
    round(100 * median((arrow_busy_s - send_wait_s) / cpu_s), 1) AS encode_pct_server_cpu
FROM queries
INNER JOIN processors USING (query_id)
GROUP BY streams
ORDER BY streams
FORMAT PrettyCompactNoEscapesMonoBlock
