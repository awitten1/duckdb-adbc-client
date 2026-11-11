// Parallel read_adbc streams in ONE process: for each stream count N, split the table into N contiguous key
// ranges and start N threads. Each thread opens its own DuckDB connection and runs an ordinary read_adbc query
// for its range, the same query run.sh runs in N separate processes. Reports the wall time from starting the
// threads until the last one finishes.
//
// Build: ninja -C build/release parallel_read_adbc
// Run (from the repository root, inside the DuckDB slice):
//   build/release/extension/adbc/bench/parallel_read_adbc [options] [STREAM_COUNT ...]   (default 1 2 4)
// Options:
//   --profile=URI          ADBC profile (default profile://myclickhouse)
//   --database=NAME        source database (default tpch_sf50)
//   --table=NAME           source table (default orders)
//   --key=COLUMN           integer column used to split the table (default o_orderkey)
//   --order-by=EXPR        append "ORDER BY EXPR" to each range query, so ClickHouse sorts every stream
//   --ch-max-threads=K     append "SETTINGS max_threads=K" to each range query; "split" = floor(32 / N)
//   --ch-settings=LIST     add these comma-separated ClickHouse settings to each range query's SETTINGS clause
//   --threads=K            DuckDB worker threads, shared by all streams (default 16)
//   --memory-limit=SIZE    DuckDB memory limit (default 56GiB)
//   --repeats=R            measured repeats per stream count (default 3; one warmup batch first)
//   --checksum             instead of timing, check that the per-range results add up to one full read

#include "adbc_extension.hpp"
#include "core_functions_extension.hpp"
#include "duckdb.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;
using Clock = std::chrono::steady_clock;

namespace {

struct Options {
    string profile = "profile://myclickhouse";
    string database = "tpch_sf50";
    string table = "orders";
    string key = "o_orderkey";
    string order_by;
    string ch_max_threads;
    string ch_settings;
    int threads = 16;
    string memory_limit = "56GiB";
    int repeats = 3;
    bool checksum = false;
    std::vector<idx_t> streams;
};

[[noreturn]] void Fail(const string &message) {
    std::cerr << "parallel_read_adbc: " << message << std::endl;
    std::exit(1);
}

string Quote(const string &text) {
    return "'" + StringUtil::Replace(text, "'", "''") + "'";
}

unique_ptr<QueryResult> Run(Connection &con, const string &sql) {
    auto result = con.Query(sql);
    if (result->HasError()) {
        Fail(result->GetError() + "\nQuery: " + sql);
    }
    return result;
}

Value Single(Connection &con, const string &sql, idx_t column = 0) {
    auto result = Run(con, sql);
    auto chunk = result->Fetch();
    if (!chunk || chunk->size() != 1) {
        Fail("expected one row from: " + sql);
    }
    return chunk->GetValue(column, 0);
}

// The remote SQL for one key range, as run.sh builds it.
string RangeQuery(const Options &options, idx_t streams, int64_t lo, int64_t hi) {
    auto sql = StringUtil::Format("SELECT * FROM %s.%s WHERE %s BETWEEN %lld AND %lld", options.database,
                                  options.table, options.key, static_cast<long long>(lo),
                                  static_cast<long long>(hi));
    if (!options.order_by.empty()) {
        sql += " ORDER BY " + options.order_by;
    }
    vector<string> settings;
    if (options.ch_max_threads == "split") {
        settings.push_back(StringUtil::Format("max_threads=%llu",
                                              static_cast<unsigned long long>(std::max<idx_t>(1, 32 / streams))));
    } else if (!options.ch_max_threads.empty()) {
        settings.push_back("max_threads=" + options.ch_max_threads);
    }
    if (!options.ch_settings.empty()) {
        settings.push_back(options.ch_settings);
    }
    if (!settings.empty()) {
        sql += " SETTINGS " + StringUtil::Join(settings, ", ");
    }
    return sql;
}

// N contiguous, disjoint ranges covering [lo, hi], split exactly as run.sh splits them.
std::vector<std::pair<int64_t, int64_t>> Ranges(int64_t lo, int64_t hi, idx_t n) {
    std::vector<std::pair<int64_t, int64_t>> ranges;
    auto width = hugeint_t(hi) - hugeint_t(lo) + hugeint_t(1);
    for (idx_t i = 0; i < n; ++i) {
        auto start = hugeint_t(lo) + width * hugeint_t(i) / hugeint_t(n);
        auto end = hugeint_t(lo) + width * hugeint_t(i + 1) / hugeint_t(n) - hugeint_t(1);
        ranges.emplace_back(Hugeint::Cast<int64_t>(start), Hugeint::Cast<int64_t>(end));
    }
    return ranges;
}

// Start one thread per range; each runs `sql_for(range)` on its own connection.
// Returns the wall time from starting the threads until the last one finishes.
template <class SQL_FOR, class ON_RESULT>
double RunStreams(DuckDB &db, const std::vector<std::pair<int64_t, int64_t>> &ranges, SQL_FOR &&sql_for,
                  ON_RESULT &&on_result) {
    std::vector<std::thread> workers;
    std::mutex error_mutex;
    string error;
    auto started = Clock::now();
    for (idx_t i = 0; i < ranges.size(); ++i) {
        workers.emplace_back([&, i]() {
            Connection con(db);
            auto result = con.Query(sql_for(ranges[i]));
            if (result->HasError()) {
                std::lock_guard<std::mutex> lock(error_mutex);
                error = StringUtil::Format("stream %llu: %s", static_cast<unsigned long long>(i),
                                           result->GetError());
                return;
            }
            on_result(i, *result);
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    auto seconds = std::chrono::duration<double>(Clock::now() - started).count();
    if (!error.empty()) {
        Fail(error);
    }
    return seconds;
}

Options Parse(int argc, char **argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        auto value = [&](const char *name) -> const char * {
            auto prefix = string(name) + "=";
            return StringUtil::StartsWith(arg, prefix) ? argv[i] + prefix.size() : nullptr;
        };
        if (auto v = value("--profile")) {
            options.profile = v;
        } else if (auto v = value("--database")) {
            options.database = v;
        } else if (auto v = value("--table")) {
            options.table = v;
        } else if (auto v = value("--key")) {
            options.key = v;
        } else if (auto v = value("--order-by")) {
            options.order_by = v;
        } else if (auto v = value("--ch-max-threads")) {
            options.ch_max_threads = v;
        } else if (auto v = value("--ch-settings")) {
            options.ch_settings = v;
        } else if (auto v = value("--threads")) {
            options.threads = std::atoi(v);
        } else if (auto v = value("--memory-limit")) {
            options.memory_limit = v;
        } else if (auto v = value("--repeats")) {
            options.repeats = std::atoi(v);
        } else if (arg == "--checksum") {
            options.checksum = true;
        } else if (!arg.empty() && std::isdigit(static_cast<unsigned char>(arg[0]))) {
            options.streams.push_back(std::strtoull(argv[i], nullptr, 10));
        } else {
            Fail("unknown argument: " + arg);
        }
    }
    if (options.streams.empty()) {
        options.streams = {1, 2, 4};
    }
    return options;
}

} // namespace

int main(int argc, char **argv) {
    auto options = Parse(argc, argv);
    DBConfig config;
    config.SetOptionByName("threads", Value::INTEGER(options.threads));
    config.SetOptionByName("memory_limit", Value(options.memory_limit));
    DuckDB db(nullptr, &config);
    db.LoadStaticExtension<CoreFunctionsExtension>();
    db.LoadStaticExtension<AdbcExtension>();
    Connection con(db);

    auto source = options.database + "." + options.table;
    auto adbc = [&](const string &remote_sql) {
        return "read_adbc(" + Quote(options.profile) + ", " + Quote(remote_sql) + ")";
    };
    auto bounds = Run(con, "SELECT * FROM " +
                               adbc(StringUtil::Format("SELECT min(%s), max(%s), count() FROM %s", options.key,
                                                       options.key, source)));
    auto bounds_chunk = bounds->Fetch();
    auto lo = bounds_chunk->GetValue(0, 0).GetValue<int64_t>();
    auto hi = bounds_chunk->GetValue(1, 0).GetValue<int64_t>();
    auto rows = bounds_chunk->GetValue(2, 0).GetValue<int64_t>();
    std::cerr << source << ": " << rows << " rows, " << options.key << " in [" << lo << ", " << hi << "]; "
              << options.threads << " DuckDB threads shared by all streams; ClickHouse max_threads per query: "
              << (options.ch_max_threads.empty() ? "server default" : options.ch_max_threads)
              << "; ORDER BY per stream: " << (options.order_by.empty() ? "none" : options.order_by)
              << "; extra settings: " << (options.ch_settings.empty() ? "none" : options.ch_settings) << std::endl;

    if (options.checksum) {
        // Row count and an order-independent sum of row hashes, for one full read and for each stream count.
        const string checksum = "count(*) AS n, sum(hash(t)::HUGEINT) AS h";
        auto full = Run(con, "SELECT " + checksum + " FROM " + adbc("SELECT * FROM " + source) + " t");
        auto full_chunk = full->Fetch();
        auto full_n = full_chunk->GetValue(0, 0).GetValue<int64_t>();
        auto full_h = full_chunk->GetValue(1, 0).GetValue<hugeint_t>();
        std::cout << "single read_adbc: " << full_n << " rows, hash sum " << full_h.ToString() << std::endl;
        bool all_match = true;
        for (auto n : options.streams) {
            std::mutex sum_mutex;
            int64_t sum_n = 0;
            hugeint_t sum_h = 0;
            RunStreams(
                db, Ranges(lo, hi, n),
                [&](const std::pair<int64_t, int64_t> &range) {
                    return "SELECT " + checksum + " FROM " + adbc(RangeQuery(options, n, range.first, range.second)) +
                           " t";
                },
                [&](idx_t, QueryResult &result) {
                    auto chunk = result.Fetch();
                    std::lock_guard<std::mutex> lock(sum_mutex);
                    sum_n += chunk->GetValue(0, 0).GetValue<int64_t>();
                    sum_h += chunk->GetValue(1, 0).GetValue<hugeint_t>();
                });
            bool match = sum_n == full_n && sum_h == full_h;
            all_match &= match;
            std::cout << n << " threads: " << sum_n << " rows, hash sum " << sum_h.ToString()
                      << (match ? "  MATCH" : "  MISMATCH") << std::endl;
        }
        return all_match ? 0 : 1;
    }

    // Timing: EXPLAIN ANALYZE consumes every row without an output sink, as in run.sh.
    std::cout << "streams\trepeat\twall_s" << std::endl;
    std::vector<std::pair<idx_t, std::vector<double>>> results;
    auto measure = [&](idx_t n) {
        return RunStreams(
            db, Ranges(lo, hi, n),
            [&](const std::pair<int64_t, int64_t> &range) {
                return "EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM " +
                       adbc(RangeQuery(options, n, range.first, range.second));
            },
            [](idx_t, QueryResult &) {});
    };
    measure(1); // warmup
    for (auto n : options.streams) {
        results.emplace_back(n, std::vector<double>());
    }
    for (int repeat = 1; repeat <= options.repeats; ++repeat) {
        auto order = options.streams;
        if (repeat % 2 == 0) {
            std::reverse(order.begin(), order.end()); // alternate the order of stream counts, as run.sh does
        }
        for (auto n : order) {
            auto seconds = measure(n);
            for (auto &entry : results) {
                if (entry.first == n) {
                    entry.second.push_back(seconds);
                }
            }
            std::cout << n << "\t" << repeat << "\t" << seconds << std::endl;
        }
    }
    auto base = 0.0;
    std::cerr << "streams  median_wall_s  min-max_wall_s   speedup" << std::endl;
    for (auto &entry : results) {
        auto walls = entry.second;
        std::sort(walls.begin(), walls.end());
        auto median = walls[walls.size() / 2];
        if (base == 0.0) {
            base = median;
        }
        std::cerr << StringUtil::Format("%7llu  %13.3f  %6.3f-%-7.3f  %6.2fx", static_cast<unsigned long long>(entry.first),
                                        median, walls.front(), walls.back(), base / median)
                  << std::endl;
    }
    return 0;
}
