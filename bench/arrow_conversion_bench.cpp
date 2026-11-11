// Microbenchmark for DuckDB's Arrow -> DuckDB conversion, the per-batch step that
// read_adbc and read_adbc_ranges run on every ClickHouse result batch.
//
// Each fixture is one column shaped like a TPC-H orders column. DuckDB generates it
// and exports it to Arrow (plain utf8 strings, decimal128 decimals, as ClickHouse
// sends them) in 65,536-row batches. The timed loop converts every batch in
// 2,048-row DuckDB chunks with ArrowTableFunction::ArrowToDuckDB, exactly as the
// extension does. Before timing, every converted value is compared with the source.
//
// Build: ninja -C build/release arrow_conversion_bench
// Run:   build/release/extension/adbc/bench/arrow_conversion_bench [--rows=N] [benchmark flags]

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"

#include <benchmark/benchmark.h>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

using namespace duckdb;

namespace {

constexpr idx_t ARROW_BATCH_ROWS = 65536; // close to ClickHouse's ArrowStream batch size
idx_t fixture_rows = 4 * 1024 * 1024;     // per fixture; larger than the last-level cache

struct FixtureSpec {
    const char *name;
    // DuckDB expression over i = 0 .. rows-1 producing the column.
    const char *expression;
    // Expected Arrow format string of the exported column.
    // Expressions use only functions built into DuckDB's core library: the statically
    // linked benchmark does not load the core_functions extension.
    const char *arrow_format;
};

const FixtureSpec FIXTURES[] = {
    {"int64", "i::BIGINT", "l"},
    {"date32", "DATE '1992-01-01' + (i % 2400)::INTEGER", "tdD"},
    {"decimal_15_2", "(((i * 7919) % 50000000) / 100.0)::DECIMAL(15, 2)",
     "d:15,2,128"}, // decimal128; ClickHouse spells the same type "d:15,2"
    // o_orderstatus: one character (DuckDB inline string).
    {"string_1", "CASE i % 3 WHEN 0 THEN 'F' WHEN 1 THEN 'O' ELSE 'P' END", "u"},
    // o_orderpriority: 6-15 characters, a mix of inline and pointer strings.
    {"string_6_15",
     "CASE i % 5 WHEN 0 THEN '1-URGENT' WHEN 1 THEN '2-HIGH' WHEN 2 THEN '3-MEDIUM' "
     "WHEN 3 THEN '4-NOT SPECIFIED' ELSE '5-LOW' END",
     "u"},
    // o_clerk: exactly 15 characters (pointer strings, longer than the 12-byte inline limit).
    {"string_15", "'Clerk#' || substring((1000000000 + i % 50000)::VARCHAR, 2, 9)", "u"},
    // o_comment: 19-78 characters.
    {"string_19_78",
     "substring(i::VARCHAR || ' furiously regular deposits sleep quickly across the pending ideas; blithely final', "
     "1, 19 + ((i * 7919) % 60)::INTEGER)",
     "u"},
};

// One column exported to Arrow, plus its DuckDB source for verification.
struct Fixture {
    LogicalType type;
    ArrowSchemaWrapper schema;
    ArrowTableSchema table;
    vector<shared_ptr<ArrowArrayWrapper>> batches;
    unique_ptr<ColumnDataCollection> source;
    idx_t rows = 0;
    idx_t arrow_bytes = 0;
};

DuckDB &Database() {
    static DuckDB db(nullptr);
    return db;
}

Connection &Conn() {
    static Connection con(Database());
    return con;
}

[[noreturn]] void Fail(const string &message) {
    std::cerr << "arrow_conversion_bench: " << message << std::endl;
    std::exit(1);
}

// Bytes of Arrow buffers holding one batch's column values (validity excluded; no NULLs here).
idx_t ArrowBytes(const ArrowArray &batch, const string &format) {
    auto &child = *batch.children[0];
    auto rows = idx_t(child.length);
    if (format == "u") { // 32-bit offsets + string data
        auto offsets = static_cast<const int32_t *>(child.buffers[1]) + child.offset;
        return (rows + 1) * sizeof(int32_t) + idx_t(offsets[rows] - offsets[0]);
    }
    idx_t width = format == "l" ? 8 : format == "tdD" ? 4 : 16; // int64, date32, decimal128
    return rows * width;
}

Fixture &GetFixture(const FixtureSpec &spec) {
    static unordered_map<string, unique_ptr<Fixture>> cache;
    auto entry = cache.find(spec.name);
    if (entry != cache.end()) {
        return *entry->second;
    }
    auto fixture = make_uniq<Fixture>();
    auto &con = Conn();
    auto sql = StringUtil::Format("SELECT %s AS c FROM range(%llu) t(i)", spec.expression,
                                  static_cast<unsigned long long>(fixture_rows));
    auto result = con.Query(sql);
    if (result->HasError()) {
        Fail(result->GetError());
    }
    vector<LogicalType> types = result->GetTypes();
    vector<string> names {"c"};
    fixture->type = types[0];
    fixture->source = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
    while (auto chunk = result->Fetch()) {
        if (chunk->size() == 0) {
            break;
        }
        fixture->source->Append(*chunk);
    }
    fixture->rows = fixture->source->Count();

    auto properties = con.context->GetClientProperties();
    ArrowConverter::ToArrowSchema(&fixture->schema.arrow_schema, types, names, properties);
    string format = fixture->schema.arrow_schema.children[0]->format;
    if (format != spec.arrow_format) {
        Fail(StringUtil::Format("%s exported as Arrow format '%s', expected '%s'", spec.name, format,
                                spec.arrow_format));
    }
    ArrowTableFunction::PopulateArrowTableSchema(*con.context, fixture->table, fixture->schema.arrow_schema);

    // Export in ARROW_BATCH_ROWS batches.
    auto &collection = *fixture->source;
    unique_ptr<ArrowAppender> appender;
    auto flush = [&]() {
        if (!appender || appender->RowCount() == 0) {
            return;
        }
        auto batch = make_shared_ptr<ArrowArrayWrapper>();
        batch->arrow_array = appender->Finalize();
        fixture->batches.push_back(std::move(batch));
        appender.reset();
    };
    ColumnDataScanState export_scan;
    collection.InitializeScan(export_scan);
    DataChunk chunk;
    collection.InitializeScanChunk(chunk);
    while (collection.Scan(export_scan, chunk)) {
        if (!appender) {
            appender = make_uniq<ArrowAppender>(types, ARROW_BATCH_ROWS, properties,
                                                unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>>());
        }
        appender->Append(chunk, 0, chunk.size(), chunk.size());
        if (appender->RowCount() >= ARROW_BATCH_ROWS) {
            flush();
        }
    }
    flush();
    for (auto &batch : fixture->batches) {
        fixture->arrow_bytes += ArrowBytes(batch->arrow_array, format);
    }
    auto &built = *fixture;
    cache[spec.name] = std::move(fixture);
    return built;
}

// Convert every batch of a fixture in 2,048-row chunks, calling `consume` on each chunk.
template <class CONSUME>
void ConvertAll(Fixture &fixture, ArrowScanLocalState &state, DataChunk &output, CONSUME &&consume) {
    for (auto &batch : fixture.batches) {
        state.Reset();
        state.chunk = batch;
        state.chunk_offset = 0;
        auto length = idx_t(batch->arrow_array.length);
        while (state.chunk_offset < length) {
            output.Reset();
            auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, length - state.chunk_offset);
            output.SetChildCardinality(count);
            ArrowTableFunction::ArrowToDuckDB(state, fixture.table.GetColumns(), output, false);
            state.chunk_offset += count;
            consume(output);
        }
    }
}

unique_ptr<ArrowScanLocalState> MakeState() {
    auto state = make_uniq<ArrowScanLocalState>(make_uniq<ArrowArrayWrapper>(), *Conn().context);
    state->column_ids.push_back(0);
    return state;
}

// Compare every converted value with the DuckDB source column.
void Verify(const FixtureSpec &spec, Fixture &fixture) {
    auto state = MakeState();
    DataChunk output;
    output.Initialize(*Conn().context, {fixture.type});
    auto &collection = *fixture.source;
    ColumnDataScanState scan;
    collection.InitializeScan(scan);
    DataChunk expected;
    collection.InitializeScanChunk(expected);
    idx_t expected_pos = 0, checked = 0;
    ConvertAll(fixture, *state, output, [&](DataChunk &chunk) {
        for (idx_t row = 0; row < chunk.size(); ++row, ++expected_pos, ++checked) {
            if (expected_pos >= expected.size()) {
                expected.Reset();
                collection.Scan(scan, expected);
                expected_pos = 0;
            }
            auto got = chunk.GetValue(0, row);
            auto want = expected.GetValue(0, expected_pos);
            if (got != want) {
                Fail(StringUtil::Format("%s row %llu: converted %s, expected %s", spec.name,
                                        static_cast<unsigned long long>(checked), got.ToString(),
                                        want.ToString()));
            }
        }
    });
    if (checked != fixture.rows) {
        Fail(StringUtil::Format("%s converted %llu rows, expected %llu", spec.name,
                                static_cast<unsigned long long>(checked),
                                static_cast<unsigned long long>(fixture.rows)));
    }
}

void BM_ArrowToDuckDB(benchmark::State &bench, const FixtureSpec *spec) {
    auto &fixture = GetFixture(*spec);
    static unordered_map<string, bool> verified;
    if (!verified[spec->name]) {
        Verify(*spec, fixture);
        verified[spec->name] = true;
    }
    auto state = MakeState();
    DataChunk output;
    output.Initialize(*Conn().context, {fixture.type});
    for (auto _ : bench) {
        ConvertAll(fixture, *state, output, [](DataChunk &chunk) {
            benchmark::DoNotOptimize(FlatVector::GetData(chunk.data[0]));
            benchmark::ClobberMemory();
        });
    }
    auto rows = double(fixture.rows) * double(bench.iterations());
    bench.SetItemsProcessed(int64_t(rows));
    bench.SetBytesProcessed(int64_t(double(fixture.arrow_bytes) * double(bench.iterations())));
    bench.counters["ns_per_row"] =
        benchmark::Counter(rows, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    bench.counters["rows"] = double(fixture.rows);
    bench.counters["arrow_bytes_per_row"] = double(fixture.arrow_bytes) / double(fixture.rows);
}

} // namespace

int main(int argc, char **argv) {
    // --rows=N (consumed here; the rest go to Google Benchmark).
    vector<char *> args;
    for (int i = 0; i < argc; ++i) {
        if (std::strncmp(argv[i], "--rows=", 7) == 0) {
            fixture_rows = std::strtoull(argv[i] + 7, nullptr, 10);
        } else {
            args.push_back(argv[i]);
        }
    }
    int benchmark_argc = int(args.size());
    for (auto &spec : FIXTURES) {
        benchmark::RegisterBenchmark((string("ArrowToDuckDB/") + spec.name).c_str(), BM_ArrowToDuckDB, &spec)
            ->Unit(benchmark::kMillisecond);
    }
    benchmark::Initialize(&benchmark_argc, args.data());
    if (benchmark::ReportUnrecognizedArguments(benchmark_argc, args.data())) {
        return 1;
    }
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
