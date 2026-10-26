#include "adbc_partial_sort.hpp"
#include "adbc_util.hpp"

#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/profiler/profiling_node.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <thread>

namespace duckdb {
namespace adbc {

namespace {

constexpr int64_t RATIO_SCALE = 1000000000;

string QuoteIdentifier(const string &name) {
    return "\"" + StringUtil::Replace(name, "\"", "\"\"") + "\"";
}

bool IntegerKey(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::UBIGINT:
        return true;
    default:
        return false;
    }
}

// Every query owns an independent connection. No handle is used concurrently.
class AdbcQuery {
public:
    explicit AdbcQuery(const string &uri) : connection(make_uniq<AdbcConnection>(uri)) {
    }

    void Execute(const string &sql) {
        stream.reset();
        statement = connection->MakeStatement(sql);
        Handle<Private::AdbcError> error;
        stream = make_uniq<ArrowArrayStreamWrapper>();
        CHECK_ADBC(AdbcStatementExecuteQuery(statement.get(), &stream->arrow_array_stream, nullptr, error.get()),
                   IOException);
    }

    Handle<ArrowSchema> GetSchema() {
        Handle<ArrowSchema> schema;
        if (stream->arrow_array_stream.get_schema(&stream->arrow_array_stream, schema.get()) != 0) {
            throw IOException("ADBC partial sort: %s", StreamError());
        }
        return schema;
    }

    unique_ptr<ArrowArrayWrapper> Next() {
        auto batch = make_uniq<ArrowArrayWrapper>();
        if (stream->arrow_array_stream.get_next(&stream->arrow_array_stream, &batch->arrow_array) != 0) {
            throw IOException("ADBC partial sort: %s", StreamError());
        }
        return batch;
    }

private:
    string StreamError() {
        auto message = stream->GetError();
        return message ? message : "Arrow stream failed without an error message";
    }

    unique_ptr<AdbcConnection> connection;
    Handle<Private::AdbcStatement> statement;
    unique_ptr<ArrowArrayStreamWrapper> stream;
};

// Reuse DuckDB's Arrow conversion, with a sequential cursor over owned batches.
class ArrowCursor {
public:
    ArrowCursor(ClientContext &context, Handle<ArrowSchema> schema_p)
        : schema(std::move(schema_p)), state(make_uniq<ArrowArrayWrapper>(), context) {
        ArrowTableFunction::PopulateArrowTableSchema(context, table, *schema.get());
        for (idx_t i = 0; i < table.GetTypes().size(); ++i) {
            state.column_ids.push_back(i);
        }
    }

    void Validate(const vector<string> &names, const vector<LogicalType> &types) {
        if (table.GetNames() != names || table.GetTypes() != types) {
            throw IOException("ADBC partial sort: source schema changed since binding");
        }
    }

    void SetBatch(shared_ptr<ArrowArrayWrapper> batch, idx_t offset = 0) {
        state.Reset();
        state.chunk = std::move(batch);
        state.chunk_offset = offset;
    }

    void ReleaseBatch() {
        state.Reset();
        state.chunk = make_uniq<ArrowArrayWrapper>();
    }

    bool Next(DataChunk &output, const std::function<unique_ptr<ArrowArrayWrapper>()> &next_batch) {
        output.Reset();
        if (finished) {
            return false;
        }
        while (state.chunk_offset >= idx_t(state.chunk->arrow_array.length)) {
            auto batch = next_batch();
            if (!batch->arrow_array.release) {
                finished = true;
                return false;
            }
            state.Reset();
            state.chunk = shared_ptr<ArrowArrayWrapper>(batch.release());
        }
        auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.chunk->arrow_array.length - state.chunk_offset);
        output.SetChildCardinality(count);
        ArrowTableFunction::ArrowToDuckDB(state, table.GetColumns(), output, false);
        state.chunk_offset += count;
        return true;
    }

    ArrowTableSchema table;

private:
    Handle<ArrowSchema> schema;
    ArrowScanLocalState state;
    bool finished = false;
};

vector<vector<Value>> ReadRows(ClientContext &context, const string &uri, const string &sql) {
    AdbcQuery query(uri);
    query.Execute(sql);
    ArrowCursor cursor(context, query.GetSchema());
    DataChunk chunk;
    chunk.Initialize(context, cursor.table.GetTypes());
    vector<vector<Value>> rows;
    while (cursor.Next(chunk, [&]() { return query.Next(); })) {
        context.InterruptCheck();
        for (idx_t row = 0; row < chunk.size(); ++row) {
            vector<Value> values;
            for (idx_t col = 0; col < chunk.ColumnCount(); ++col) {
                values.push_back(chunk.GetValue(col, row));
            }
            rows.push_back(std::move(values));
        }
    }
    return rows;
}

struct PartialSortBindData : public TableFunctionData {
    string uri;
    string database;
    string table;
    string partition_key;
    vector<string> names;
    vector<LogicalType> types;
    vector<idx_t> sort_columns;
    vector<OrderModifiers> modifiers;
    int64_t ratio;
    double bind_seconds = 0;

    string Source() const {
        return (database.empty() ? "" : QuoteIdentifier(database) + ".") + QuoteIdentifier(table);
    }

    string Select() const {
        vector<string> columns;
        for (auto &name : names) {
            columns.push_back(QuoteIdentifier(name));
        }
        return "SELECT " + StringUtil::Join(columns, ", ") + " FROM " + Source();
    }

    string Order() const {
        vector<string> columns;
        for (idx_t i = 0; i < sort_columns.size(); ++i) {
            columns.push_back(
                QuoteIdentifier(names[sort_columns[i]]) +
                (modifiers[i].order_type == OrderType::DESCENDING ? " DESC" : " ASC") +
                (modifiers[i].null_type == OrderByNullType::NULLS_FIRST ? " NULLS FIRST" : " NULLS LAST"));
        }
        return " ORDER BY " + StringUtil::Join(columns, ", ");
    }
};

// The bound schema describes the whole table. Each execution owns a narrower
// layout containing the requested columns and any hidden keys needed by the merge.
struct ProjectedSortData : public PartialSortBindData {
    ProjectedSortData(const PartialSortBindData &bound, const vector<column_t> &requested)
        : PartialSortBindData(bound) {
        names.clear();
        types.clear();
        sort_columns.clear();
        vector<idx_t> mapping(bound.types.size(), DConstants::INVALID_INDEX);
        auto add_column = [&](idx_t column) {
            if (column >= bound.types.size()) {
                throw InternalException("ADBC partial sort: invalid projected column");
            }
            if (mapping[column] == DConstants::INVALID_INDEX) {
                mapping[column] = names.size();
                names.push_back(bound.names[column]);
                types.push_back(bound.types[column]);
            }
            return mapping[column];
        };
        for (auto column : requested) {
            if (column == COLUMN_IDENTIFIER_ROW_ID || column == COLUMN_IDENTIFIER_EMPTY) {
                // COUNT(*) and constant projections only need the row cardinality.
                output_columns.push_back(DConstants::INVALID_INDEX);
            } else {
                output_columns.push_back(add_column(column));
            }
        }
        for (auto column : bound.sort_columns) {
            sort_columns.push_back(add_column(column));
        }
    }

    void Project(DataChunk &internal, DataChunk &output) const {
        D_ASSERT(output.ColumnCount() == output_columns.size());
        output.Reset();
        for (idx_t i = 0; i < output_columns.size(); ++i) {
            if (output_columns[i] == DConstants::INVALID_INDEX) {
                output.data[i].SetVectorType(VectorType::CONSTANT_VECTOR);
                ConstantVector::SetNull(output.data[i], true);
            } else {
                output.data[i].Reference(internal.data[output_columns[i]]);
            }
        }
        output.SetChildCardinality(internal.size());
    }

    vector<idx_t> output_columns;
};

Value Required(TableFunctionBindInput &input, const string &name) {
    auto it = input.named_parameters.find(Identifier(name));
    if (it == input.named_parameters.end() || it->second.IsNull()) {
        throw BinderException("ADBC partial sort requires %s", name);
    }
    return it->second;
}

idx_t ResolveColumn(const PartialSortBindData &data, const string &name) {
    for (idx_t i = 0; i < data.names.size(); ++i) {
        if (data.names[i] == name) {
            if (!IntegerKey(data.types[i])) {
                throw BinderException("ADBC partial sort key %s must be an integer of at most 64 bits", name);
            }
            return i;
        }
    }
    throw BinderException("ADBC partial sort column does not exist: %s", name);
}

vector<bool> ReadFlags(TableFunctionBindInput &input, const string &name, idx_t count) {
    vector<bool> flags(count, false);
    auto it = input.named_parameters.find(Identifier(name));
    if (it == input.named_parameters.end()) {
        return flags;
    }
    if (it->second.IsNull()) {
        throw BinderException("%s cannot be NULL", name);
    }
    auto &values = ListValue::GetChildren(it->second);
    if (values.size() != count) {
        throw BinderException("%s must match the number of sort_columns", name);
    }
    for (idx_t i = 0; i < count; ++i) {
        if (values[i].IsNull()) {
            throw BinderException("%s cannot contain NULL", name);
        }
        flags[i] = values[i].GetValue<bool>();
    }
    return flags;
}

unique_ptr<FunctionData> BindPartialSort(ClientContext &context,
                                         TableFunctionBindInput &input,
                                         vector<LogicalType> &types,
                                         vector<Identifier> &names) {
    auto bind_started = std::chrono::steady_clock::now();
    if (!Settings::Get<EnableExternalAccessSetting>(context)) {
        throw PermissionException("ADBC partial sort requires external access");
    }
    for (idx_t i = 0; i < input.inputs.size(); ++i) {
        auto &value = input.inputs[i];
        if (value.IsNull() || (i != 1 && value.GetValue<string>().empty())) {
            throw BinderException("ADBC partial sort requires nonempty URI and table names and a non-NULL namespace");
        }
    }
    auto data = make_uniq<PartialSortBindData>();
    data->uri = input.inputs[0].GetValue<string>();
    data->database = input.inputs[1].GetValue<string>();
    data->table = input.inputs[2].GetValue<string>();
    data->partition_key = Required(input, "partition_key").GetValue<string>();
    auto ratio = Required(input, "pushdown_ratio");
    data->ratio = ratio.GetValueUnsafe<int64_t>();
    if (data->ratio < 0 || data->ratio > RATIO_SCALE) {
        throw BinderException("pushdown_ratio must be between 0 and 1");
    }
    auto columns_value = Required(input, "sort_columns");
    auto &columns = ListValue::GetChildren(columns_value);
    if (columns.empty()) {
        throw BinderException("sort_columns must not be empty");
    }
    auto descending = ReadFlags(input, "descending", columns.size());
    auto nulls_first = ReadFlags(input, "nulls_first", columns.size());

    {
        AdbcQuery query(data->uri);
        // Some drivers infer Arrow types from result rows rather than declared SQL types.
        query.Execute("SELECT * FROM " + data->Source() + " LIMIT 1");
        ArrowCursor cursor(context, query.GetSchema());
        data->types = cursor.table.GetTypes();
        data->names = cursor.table.GetNames();
    }
    for (idx_t i = 0; i < data->names.size(); ++i) {
        for (idx_t j = 0; j < i; ++j) {
            if (data->names[i] == data->names[j]) {
                throw BinderException("ADBC partial sort requires unique source column names");
            }
        }
    }
    ResolveColumn(*data, data->partition_key);
    for (idx_t i = 0; i < columns.size(); ++i) {
        if (columns[i].IsNull()) {
            throw BinderException("sort_columns cannot contain NULL");
        }
        data->sort_columns.push_back(ResolveColumn(*data, columns[i].GetValue<string>()));
        data->modifiers.emplace_back(descending[i] ? OrderType::DESCENDING : OrderType::ASCENDING,
                                     nulls_first[i] ? OrderByNullType::NULLS_FIRST : OrderByNullType::NULLS_LAST);
    }
    types = data->types;
    names = StringsToIdentifiers(data->names);
    data->bind_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - bind_started).count();
    return std::move(data);
}

unique_ptr<Sort> CreateLocalSort(ClientContext &context, const PartialSortBindData &data) {
    vector<BoundOrderByNode> orders;
    vector<idx_t> projection;
    for (idx_t i = 0; i < data.sort_columns.size(); ++i) {
        auto col = data.sort_columns[i];
        orders.emplace_back(data.modifiers[i].order_type,
                            data.modifiers[i].null_type,
                            make_uniq<BoundReferenceExpression>(data.types[col], col));
    }
    for (idx_t col = 0; col < data.types.size(); ++col) {
        projection.push_back(col);
    }
    return make_uniq<Sort>(context, orders, data.types, projection);
}

void MakeSortKeys(const PartialSortBindData &data, DataChunk &chunk, Vector &keys) {
    DataChunk key_chunk;
    vector<LogicalType> types;
    for (auto column : data.sort_columns) {
        types.push_back(data.types[column]);
    }
    key_chunk.InitializeEmpty(types);
    key_chunk.ReferenceColumns(chunk, data.sort_columns);
    Vector fresh_keys(LogicalType::BLOB);
    keys.Reference(fresh_keys);
    CreateSortKeyHelpers::CreateSortKey(key_chunk, data.modifiers, keys);
}

struct LocalSortConcurrency {
    std::mutex mutex;
    idx_t active = 0;
    idx_t peak = 0;
};

class LocalSortTask : public BaseExecutorTask {
    class ActiveTask {
    public:
        explicit ActiveTask(LocalSortConcurrency &state_p) : state(state_p) {
            std::lock_guard<std::mutex> lock(state.mutex);
            state.peak = MaxValue(state.peak, ++state.active);
        }

        ~ActiveTask() {
            std::lock_guard<std::mutex> lock(state.mutex);
            --state.active;
        }

    private:
        LocalSortConcurrency &state;
    };

public:
    LocalSortTask(TaskExecutor &executor, LocalSortConcurrency &concurrency_p, std::function<void()> work_p)
        : BaseExecutorTask(executor), concurrency(concurrency_p), work(std::move(work_p)) {
    }

    void ExecuteTask() override {
        ActiveTask active(concurrency);
        work();
    }

    string TaskType() const override {
        return "AdbcLocalSortTask";
    }

private:
    LocalSortConcurrency &concurrency;
    std::function<void()> work;
};

class LocalSortWorker {
public:
    LocalSortWorker(ClientContext &context_p,
                    Sort &sort_p,
                    GlobalSinkState &global_sink_p,
                    Handle<ArrowSchema> schema,
                    const PartialSortBindData &data)
        : context(context_p), sort(sort_p), global_sink(global_sink_p), thread_context(context),
          cursor(context, std::move(schema)) {
        cursor.Validate(data.names, data.types);
        chunk.Initialize(context, data.types);
        ExecutionContext execution(context, thread_context, nullptr);
        local_sink = sort.GetLocalSinkState(execution);
    }

    void SinkBatch(shared_ptr<ArrowArrayWrapper> batch, idx_t begin, idx_t end) {
        cursor.SetBatch(std::move(batch), begin);
        ExecutionContext execution(context, thread_context, nullptr);
        OperatorSinkInput input{global_sink, *local_sink, interrupt};
        for (idx_t offset = begin; offset < end;) {
            context.InterruptCheck();
            if (!cursor.Next(chunk, []() { return make_uniq<ArrowArrayWrapper>(); })) {
                throw InternalException("ADBC local sort exhausted its batch");
            }
            D_ASSERT(offset + chunk.size() <= end);
            offset += chunk.size();
            sort.Sink(execution, chunk, input);
        }
        chunk.Reset();
        cursor.ReleaseBatch();
    }

    void Combine() {
        context.InterruptCheck();
        ExecutionContext execution(context, thread_context, nullptr);
        OperatorSinkCombineInput input{global_sink, *local_sink, interrupt};
        sort.Combine(execution, input);
    }

private:
    ClientContext &context;
    Sort &sort;
    GlobalSinkState &global_sink;
    ThreadContext thread_context;
    InterruptState interrupt;
    ArrowCursor cursor;
    DataChunk chunk;
    unique_ptr<LocalSinkState> local_sink;
};

// Owns one execution's local sort, remote producer, and ordered merge.
// The scan thread sorts local input and merges both branches; the producer queues remote Arrow batches.
// Merge positions persist across scan calls, and teardown stops and joins the producer.
class PartialSortState : public GlobalTableFunctionState {
public:
    using Clock = std::chrono::steady_clock;
    PartialSortState(ClientContext &context_p, const PartialSortBindData &data_p, const vector<column_t> &columns)
        : context(context_p), data(data_p, columns), thread_context(context), local_keys(LogicalType::BLOB),
          remote_keys(LogicalType::BLOB),
          thread_count(MaxValue<idx_t>(1, TaskScheduler::GetScheduler(context).NumberOfThreads())) {
        merged_chunk.Initialize(context, data.types);
        local_chunk.Initialize(context, data.types);
        remote_chunk.Initialize(context, data.types);
    }

    ~PartialSortState() override {
        Stop();
    }

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            stopped = true;
        }
        queue_cv.notify_all();
        if (worker.joinable()) {
            worker.join();
        }
    }

    void CheckWorker() {
        context.InterruptCheck();
        std::lock_guard<std::mutex> lock(queue_mutex);
        if (!worker_error.empty()) {
            throw IOException("ADBC partial sort remote branch: %s", worker_error);
        }
    }

    void ReadRemote(unique_ptr<AdbcQuery> query, const string &sql) {
        try {
            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                remote_started = Clock::now();
            }
            query->Execute(sql);
            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                remote_schema = query->GetSchema();
                schema_ready = true;
            }
            queue_cv.notify_all();
            for (;;) {
                {
                    std::unique_lock<std::mutex> lock(queue_mutex);
                    queue_cv.wait(lock, [&]() { return stopped || queue.size() < 2; });
                    if (stopped) {
                        break;
                    }
                }
                auto batch = query->Next();
                if (!batch->arrow_array.release) {
                    break;
                }
                if (!batch->arrow_array.length) {
                    continue;
                }
                {
                    std::lock_guard<std::mutex> lock(queue_mutex);
                    if (stopped) {
                        break;
                    }
                    queue.push_back(std::move(batch));
                    remote_rows += queue.back()->arrow_array.length;
                    queue_high_water = MaxValue<idx_t>(queue_high_water, queue.size());
                }
                queue_cv.notify_all();
            }
        } catch (const std::exception &error) {
            std::lock_guard<std::mutex> lock(queue_mutex);
            worker_error = error.what();
        } catch (...) {
            std::lock_guard<std::mutex> lock(queue_mutex);
            worker_error = "unknown driver error";
        }
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            remote_done = true;
            remote_finished = Clock::now();
        }
        queue_cv.notify_all();
    }

    unique_ptr<ArrowArrayWrapper> Pop() {
        std::unique_lock<std::mutex> lock(queue_mutex);
        while (queue.empty() && !remote_done) {
            queue_cv.wait_for(lock, std::chrono::milliseconds(20));
            context.InterruptCheck();
        }
        if (!worker_error.empty()) {
            throw IOException("ADBC partial sort remote branch: %s", worker_error);
        }
        if (queue.empty()) {
            return make_uniq<ArrowArrayWrapper>();
        }
        auto batch = std::move(queue.front());
        queue.pop_front();
        lock.unlock();
        queue_cv.notify_all();
        return batch;
    }

    void SortLocalQuery(AdbcQuery &query) {
        if (thread_count == 1) {
            LocalSortWorker local(context, *sort, *sort_sink, query.GetSchema(), data);
            for (;;) {
                CheckWorker();
                auto batch = query.Next();
                if (!batch->arrow_array.release) {
                    break;
                }
                auto count = NumericCast<idx_t>(batch->arrow_array.length);
                if (!count) {
                    continue;
                }
                local_rows += count;
                local_sort_workers = 1;
                local.SinkBatch(shared_ptr<ArrowArrayWrapper>(batch.release()), 0, count);
            }
            local.Combine();
            local_sort_concurrency.peak = local_rows ? 1 : 0;
            return;
        }

        {
            ArrowCursor cursor(context, query.GetSchema());
            cursor.Validate(data.names, data.types);
        }
        vector<unique_ptr<LocalSortWorker>> locals;
        // The executor drains on every exit before the worker states are destroyed.
        TaskExecutor executor(context);
        bool exhausted = false;
        while (!exhausted) {
            CheckWorker();
            auto batch = query.Next();
            if (!batch->arrow_array.release) {
                break;
            }
            auto count = NumericCast<idx_t>(batch->arrow_array.length);
            if (!count) {
                continue;
            }
            // Read ahead one batch while the preceding batch is being sunk.
            executor.WorkOnTasks();
            CheckWorker();
            vector<shared_ptr<ArrowArrayWrapper>> batches;
            batches.emplace_back(batch.release());
            local_rows += count;
            auto chunks = (count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
            // Small driver batches should not limit the number of sort workers.
            while (batches.size() < thread_count && chunks < thread_count * 8) {
                CheckWorker();
                auto next = query.Next();
                if (!next->arrow_array.release) {
                    exhausted = true;
                    break;
                }
                auto next_count = NumericCast<idx_t>(next->arrow_array.length);
                if (next_count) {
                    local_rows += next_count;
                    chunks += (next_count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
                    batches.emplace_back(next.release());
                }
            }
            auto workers = MinValue(thread_count, chunks);
            while (locals.size() < workers) {
                locals.push_back(make_uniq<LocalSortWorker>(context, *sort, *sort_sink, query.GetSchema(), data));
            }
            local_sort_workers = MaxValue(local_sort_workers, workers);
            for (idx_t i = 0; i < workers; ++i) {
                auto begin = chunks * i / workers;
                auto end = chunks * (i + 1) / workers;
                auto &local = *locals[i];
                executor.ScheduleTask(
                    make_uniq<LocalSortTask>(executor, local_sort_concurrency, [&local, batches, begin, end]() mutable {
                        idx_t offset = 0;
                        for (auto &input : batches) {
                            auto count = NumericCast<idx_t>(input->arrow_array.length);
                            auto chunks = (count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
                            auto first = MaxValue(begin, offset);
                            auto last = MinValue(end, offset + chunks);
                            if (first < last) {
                                local.SinkBatch(std::move(input),
                                                (first - offset) * STANDARD_VECTOR_SIZE,
                                                MinValue(count, (last - offset) * STANDARD_VECTOR_SIZE));
                            } else {
                                input.reset();
                            }
                            offset += chunks;
                        }
                        batches.clear();
                    }));
            }
        }
        executor.WorkOnTasks();
        CheckWorker();
        for (auto &local : locals) {
            executor.ScheduleTask(
                make_uniq<LocalSortTask>(executor, local_sort_concurrency, [&worker = *local]() { worker.Combine(); }));
        }
        executor.WorkOnTasks();
        CheckWorker();
    }

    void Initialize() {
        string local_sql = data.Select();
        string remote_sql;
        if (data.ratio == RATIO_SCALE) {
            local_sql.clear();
            remote_sql = data.Select() + data.Order();
        } else if (data.ratio > 0) {
            auto bounds_started = Clock::now();
            auto key = QuoteIdentifier(data.partition_key);
            auto bounds =
                ReadRows(context,
                         data.uri,
                         "SELECT min(" + key + "), max(" + key + "), count(" + key + ") FROM " + data.Source());
            if (bounds.size() != 1) {
                throw IOException("ADBC partial sort: invalid range bounds result");
            }
            bounds_seconds = std::chrono::duration<double>(Clock::now() - bounds_started).count();
            if (bounds[0][2].GetValue<uint64_t>() > 0) {
                auto minimum = bounds[0][0].GetValue<hugeint_t>();
                auto maximum = bounds[0][1].GetValue<hugeint_t>();
                auto width = maximum - minimum + hugeint_t(1);
                auto remote_width = width * hugeint_t(data.ratio) / hugeint_t(RATIO_SCALE);
                if (remote_width > hugeint_t(0)) {
                    auto boundary = (maximum + hugeint_t(1) - remote_width).ToString();
                    local_sql += " WHERE " + key + " IS NULL OR " + key + " < " + boundary;
                    remote_sql = data.Select() + " WHERE " + key + " >= " + boundary + data.Order();
                }
            }
        }

        unique_ptr<AdbcQuery> local_query;
        unique_ptr<AdbcQuery> remote_query;
        if (!local_sql.empty()) {
            local_query = make_uniq<AdbcQuery>(data.uri);
        }
        if (!remote_sql.empty()) {
            remote_query = make_uniq<AdbcQuery>(data.uri);
        }
        if (remote_query) {
            worker = std::thread([this, query = std::move(remote_query), remote_sql]() mutable {
                ReadRemote(std::move(query), remote_sql);
            });
        } else {
            remote_done = true;
        }
        if (local_query) {
            local_started = Clock::now();
            sort = CreateLocalSort(context, data);
            sort_sink = sort->GetGlobalSinkState(context);
            ExecutionContext execution(context, thread_context, nullptr);
            local_query->Execute(local_sql);
            SortLocalQuery(*local_query);
            CheckWorker();
            OperatorSinkFinalizeInput finalize{*sort_sink, interrupt};
            sort->Finalize(context, finalize);
            local_finished = Clock::now();
            sort_source = sort->GetGlobalSourceState(context, *sort_sink);
            sort_local_source = sort->GetLocalSourceState(execution, *sort_source);
        }
        if (worker.joinable()) {
            std::unique_lock<std::mutex> lock(queue_mutex);
            while (!schema_ready && !remote_done) {
                queue_cv.wait_for(lock, std::chrono::milliseconds(20));
                context.InterruptCheck();
            }
            if (!worker_error.empty()) {
                throw IOException("ADBC partial sort remote branch: %s", worker_error);
            }
            remote_cursor = make_uniq<ArrowCursor>(context, std::move(remote_schema));
            remote_cursor->Validate(data.names, data.types);
        }
        initialized = true;
    }

    void MakeKeys(DataChunk &chunk, Vector &keys) {
        MakeSortKeys(data, chunk, keys);
    }

    bool FillLocal() {
        if (local_pos < local_chunk.size()) {
            return true;
        }
        local_chunk.Reset();
        if (!sort || local_done) {
            return false;
        }
        ExecutionContext execution(context, thread_context, nullptr);
        OperatorSourceInput input{*sort_source, *sort_local_source, interrupt};
        local_done = sort->GetData(execution, local_chunk, input) == SourceResultType::FINISHED;
        local_pos = 0;
        if (!local_chunk.size()) {
            return false;
        }
        MakeKeys(local_chunk, local_keys);
        return true;
    }

    bool FillRemote() {
        if (remote_pos < remote_chunk.size()) {
            return true;
        }
        remote_pos = 0;
        if (!remote_cursor || !remote_cursor->Next(remote_chunk, [&]() { return Pop(); })) {
            return false;
        }
        MakeKeys(remote_chunk, remote_keys);
        return true;
    }

    bool LocalFirst() {
        auto left = local_keys.Values<string_t>()[local_pos].GetValue();
        auto right = remote_keys.Values<string_t>()[remote_pos].GetValue();
        auto size = MinValue<idx_t>(left.GetSize(), right.GetSize());
        auto comparison = memcmp(left.GetData(), right.GetData(), size);
        return comparison < 0 || (comparison == 0 && left.GetSize() <= right.GetSize());
    }

    void ScanInternal(DataChunk &output) {
        if (!initialized) {
            Initialize();
        }
        CheckWorker();
        SelectionVector selection(STANDARD_VECTOR_SIZE);
        while (output.size() < STANDARD_VECTOR_SIZE) {
            auto have_local = FillLocal();
            auto have_remote = FillRemote();
            if (!have_local && !have_remote) {
                break;
            }
            auto take_local = have_local && (!have_remote || LocalFirst());
            auto &chunk = take_local ? local_chunk : remote_chunk;
            auto &position = take_local ? local_pos : remote_pos;
            idx_t count = 0;
            do {
                selection.set_index(count++, position++);
            } while (position < chunk.size() && output.size() + count < STANDARD_VECTOR_SIZE &&
                     (!have_local || !have_remote || LocalFirst() == take_local));
            output.Append(chunk, selection, count);
        }
    }

    void Scan(DataChunk &output) {
        merged_chunk.Reset();
        ScanInternal(merged_chunk);
        data.Project(merged_chunk, output);
    }

    void Metrics(OperatorMetrics &metrics) {
        std::lock_guard<std::mutex> lock(queue_mutex);
        metrics.rows_scanned = local_rows + remote_rows;
        metrics.AddExtraInfo("Local Rows", std::to_string(local_rows));
        metrics.AddExtraInfo("Remote Rows", std::to_string(remote_rows));
        metrics.AddExtraInfo("Thread Count", std::to_string(thread_count));
        metrics.AddExtraInfo("Local Sort Workers", std::to_string(local_sort_workers));
        metrics.AddExtraInfo("Peak Active Local Sort Tasks", std::to_string(local_sort_concurrency.peak));
        metrics.AddExtraInfo("Queue High Watermark", std::to_string(queue_high_water));
        metrics.AddExtraInfo("Bounds Seconds", std::to_string(bounds_seconds));
        if (local_finished != Clock::time_point()) {
            metrics.AddExtraInfo("Local Sort Seconds",
                                 std::to_string(std::chrono::duration<double>(local_finished - local_started).count()));
        }
        if (remote_started != Clock::time_point() && local_finished != Clock::time_point()) {
            auto end = remote_done ? MinValue(remote_finished, local_finished) : local_finished;
            auto start = MaxValue(remote_started, local_started);
            auto overlap = MaxValue(0.0, std::chrono::duration<double>(end - start).count());
            metrics.AddExtraInfo("Branch Overlap Seconds", std::to_string(overlap));
        }
    }

private:
    // Query settings, memory management, and cancellation checks.
    ClientContext &context;
    // Requested columns, hidden sort keys, SQL generation, and output mapping.
    ProjectedSortData data;
    // Scan thread's context for DuckDB sort calls; sort workers have their own.
    ThreadContext thread_context;
    // Interrupt state passed to the scan thread's sort operations.
    InterruptState interrupt;

    // DuckDB sort for the local branch; absent for ratio 1.
    unique_ptr<Sort> sort;
    // Shared sink state receiving local workers' sorted runs.
    unique_ptr<GlobalSinkState> sort_sink;
    // Global state for reading the completed local sort.
    unique_ptr<GlobalSourceState> sort_source;
    // Scan thread's cursor into the local sort output.
    unique_ptr<LocalSourceState> sort_local_source;
    // Converts queued remote batches on the scan thread.
    unique_ptr<ArrowCursor> remote_cursor;

    // Merged rows, including hidden keys, before applying the output projection.
    DataChunk merged_chunk;
    // Current sorted local chunk, retained until its rows have been merged.
    DataChunk local_chunk;
    // Current converted remote chunk, retained until its rows have been merged.
    DataChunk remote_chunk;
    // Encoded ordering keys for local_chunk, used by LocalFirst().
    Vector local_keys;
    // Encoded ordering keys for remote_chunk, using the same ordering modifiers.
    Vector remote_keys;
    // Scheduler thread count captured for this execution's local sort budget.
    const idx_t thread_count;
    // Next unconsumed row in local_chunk.
    idx_t local_pos = 0;
    // Next unconsumed row in remote_chunk.
    idx_t remote_pos = 0;
    // Both branch inputs have been prepared by Initialize().
    bool initialized = false;
    // Local sort signaled FINISHED; local_chunk may still contain unread rows.
    bool local_done = false;

    // Rows read from the local ADBC query.
    idx_t local_rows = 0;
    // Maximum number of worker-local sort states used.
    idx_t local_sort_workers = 0;
    // Tracks active/peak local tasks under its own mutex.
    LocalSortConcurrency local_sort_concurrency;
    // Rows queued by the producer, including any not emitted.
    idx_t remote_rows = 0;
    // Largest number of queued remote batches observed.
    idx_t queue_high_water = 0;
    // Elapsed time for the partition bounds query.
    double bounds_seconds = 0;
    // Local sort setup begins, after opening its connection.
    Clock::time_point local_started;
    // Local input consumed and sort sink finalized.
    Clock::time_point local_finished;
    // Producer starts executing the remote query.
    Clock::time_point remote_started;
    // Producer finishes, stops, or encounters an error.
    Clock::time_point remote_finished;

    // Protects the queue, remote schema/status/errors, and remote metrics.
    std::mutex queue_mutex;
    // Signals schema/data/completion, available queue capacity, or stop.
    std::condition_variable queue_cv;
    // At most two remote batches waiting for conversion.
    std::deque<unique_ptr<ArrowArrayWrapper>> queue;
    // Published by the producer, then moved into remote_cursor.
    Handle<ArrowSchema> remote_schema;
    // Producer has published the remote Arrow schema.
    bool schema_ready = false;
    // Producer has exited, or this execution has no remote branch.
    bool remote_done = false;
    // Requests producer shutdown and interrupts its queue-capacity wait.
    bool stopped = false;
    // Producer exception message, rethrown on the scan thread.
    string worker_error;
    // Remote query producer, joined by Stop() before state destruction.
    std::thread worker;
};

unique_ptr<GlobalTableFunctionState> InitPartialSort(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<PartialSortState>(context, input.bind_data->Cast<PartialSortBindData>(), input.column_ids);
}

void ScanPartialSort(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<PartialSortState>();
    try {
        state.Scan(output);
    } catch (...) {
        state.Stop();
        throw;
    }
}

void PartialSortMetrics(TableFunctionGetMetricsInput &input) {
    if (input.bind_data) {
        auto &data = input.bind_data->Cast<PartialSortBindData>();
        input.operator_metrics.AddExtraInfo("Bind Seconds", std::to_string(data.bind_seconds));
    }
    if (input.global_state) {
        input.global_state->Cast<PartialSortState>().Metrics(input.operator_metrics);
    }
}


} // namespace

void RegisterAdbcPartialSort(ExtensionLoader &loader) {
    TableFunction function("read_adbc_partial_sort",
                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                           ScanPartialSort,
                           BindPartialSort,
                           InitPartialSort);
    function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
        options.Add("partition_key", LogicalType::VARCHAR)
            .Add("sort_columns", LogicalType::LIST(LogicalType::VARCHAR))
            .Add("descending", LogicalType::LIST(LogicalType::BOOLEAN))
            .Add("nulls_first", LogicalType::LIST(LogicalType::BOOLEAN))
            .Add("pushdown_ratio", LogicalType::DECIMAL(10, 9));
    });
    function.projection_pushdown = true;
    function.filter_pushdown = false;
    function.order_preservation_type = OrderPreservationType::FIXED_ORDER;
    function.parallelism = TableFunctionParallelism::FORCE_SINGLE_THREADED;
    function.get_metrics = PartialSortMetrics;
    CreateTableFunctionInfo info(function);
    FunctionDescription description;
    description.parameter_names = {"adbc_connection_profile_uri", "schema_or_database", "table"};
    description.description = "Sorts an unchanged SQL table using concurrent ADBC key-range reads, a local "
                              "sort, and an ordered merge. The pushdown ratio measures key-range width, not rows.";
    description.categories = {"adbc"};
    info.descriptions.push_back(std::move(description));
    loader.RegisterFunction(std::move(info));
}

} // namespace adbc
} // namespace duckdb
