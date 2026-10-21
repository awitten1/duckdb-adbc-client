#include "adbc_partial_sort.hpp"
#include "adbc_util.hpp"

#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
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
#include <queue>
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
    idx_t partition_count = 20;
    idx_t max_remote_concurrency = 4;

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

unique_ptr<FunctionData> BindSort(ClientContext &context,
                                  TableFunctionBindInput &input,
                                  vector<LogicalType> &types,
                                  vector<Identifier> &names,
                                  bool partitioned) {
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
    if (partitioned) {
        auto read_count = [&](const string &name, idx_t &target) {
            auto it = input.named_parameters.find(Identifier(name));
            if (it != input.named_parameters.end()) {
                if (it->second.IsNull() || it->second.GetValue<int64_t>() <= 0) {
                    throw BinderException("%s must be a positive integer", name);
                }
                target = NumericCast<idx_t>(it->second.GetValue<int64_t>());
            }
        };
        read_count("partition_count", data->partition_count);
        read_count("max_remote_concurrency", data->max_remote_concurrency);
    }

    AdbcQuery query(data->uri);
    // Some drivers infer Arrow types from result rows rather than declared SQL types.
    query.Execute("SELECT * FROM " + data->Source() + " LIMIT 1");
    {
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
    return std::move(data);
}

unique_ptr<FunctionData> BindPartialSort(ClientContext &context,
                                         TableFunctionBindInput &input,
                                         vector<LogicalType> &types,
                                         vector<Identifier> &names) {
    return BindSort(context, input, types, names, false);
}

unique_ptr<FunctionData> BindPartitionedSort(ClientContext &context,
                                             TableFunctionBindInput &input,
                                             vector<LogicalType> &types,
                                             vector<Identifier> &names) {
    return BindSort(context, input, types, names, true);
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

class PartialSortState : public GlobalTableFunctionState {
public:
    using Clock = std::chrono::steady_clock;
    PartialSortState(ClientContext &context_p, const PartialSortBindData &data_p)
        : context(context_p), data(data_p), thread_context(context), local_keys(LogicalType::BLOB),
          remote_keys(LogicalType::BLOB) {
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
        auto thread_count = MaxValue<idx_t>(1, TaskScheduler::GetScheduler(context).NumberOfThreads());
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

    void Scan(DataChunk &output) {
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

    void Metrics(OperatorMetrics &metrics) {
        std::lock_guard<std::mutex> lock(queue_mutex);
        metrics.rows_scanned = local_rows + remote_rows;
        metrics.AddExtraInfo("Local Rows", std::to_string(local_rows));
        metrics.AddExtraInfo("Remote Rows", std::to_string(remote_rows));
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
    ClientContext &context;
    const PartialSortBindData &data;
    ThreadContext thread_context;
    InterruptState interrupt;
    unique_ptr<Sort> sort;
    unique_ptr<GlobalSinkState> sort_sink;
    unique_ptr<GlobalSourceState> sort_source;
    unique_ptr<LocalSourceState> sort_local_source;
    unique_ptr<ArrowCursor> remote_cursor;
    DataChunk local_chunk;
    DataChunk remote_chunk;
    Vector local_keys;
    Vector remote_keys;
    idx_t local_pos = 0;
    idx_t remote_pos = 0;
    bool initialized = false;
    bool local_done = false;
    idx_t local_rows = 0;
    idx_t local_sort_workers = 0;
    LocalSortConcurrency local_sort_concurrency;
    idx_t remote_rows = 0;
    idx_t queue_high_water = 0;
    double bounds_seconds = 0;
    Clock::time_point local_started;
    Clock::time_point local_finished;
    Clock::time_point remote_started;
    Clock::time_point remote_finished;
    std::mutex queue_mutex;
    std::condition_variable queue_cv;
    std::deque<unique_ptr<ArrowArrayWrapper>> queue;
    Handle<ArrowSchema> remote_schema;
    bool schema_ready = false;
    bool remote_done = false;
    bool stopped = false;
    string worker_error;
    std::thread worker;
};

unique_ptr<GlobalTableFunctionState> InitPartialSort(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<PartialSortState>(context, input.bind_data->Cast<PartialSortBindData>());
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
    if (input.global_state) {
        input.global_state->Cast<PartialSortState>().Metrics(input.operator_metrics);
    }
}


// A bounded fan-in merge keeps one scan chunk and encoded key vector per input.
class PartitionRunMerger {
    struct Cursor {
        explicit Cursor(ClientContext &context, ColumnDataCollection &collection_p)
            : collection(collection_p), keys(LogicalType::BLOB) {
            collection.InitializeScan(scan);
            chunk.Initialize(context, collection.Types());
        }
        ColumnDataCollection &collection;
        ColumnDataScanState scan;
        DataChunk chunk;
        Vector keys;
        idx_t position = 0;
    };

public:
    PartitionRunMerger(ClientContext &context_p,
                       const PartialSortBindData &data_p,
                       const vector<unique_ptr<ColumnDataCollection>> &runs,
                       idx_t begin,
                       idx_t end)
        : context(context_p), data(data_p), heap([this](idx_t a, idx_t b) { return Less(b, a); }) {
        for (idx_t i = begin; i < end; ++i) {
            cursors.push_back(make_uniq<Cursor>(context, *runs[i]));
            if (Advance(cursors.size() - 1)) {
                heap.push(cursors.size() - 1);
            }
        }
    }

    void Scan(DataChunk &output) {
        context.InterruptCheck();
        SelectionVector selection(STANDARD_VECTOR_SIZE);
        while (!heap.empty() && output.size() < STANDARD_VECTOR_SIZE) {
            auto index = heap.top();
            heap.pop();
            auto &cursor = *cursors[index];
            idx_t count = 0;
            do {
                selection.set_index(count++, cursor.position++);
            } while (cursor.position < cursor.chunk.size() && output.size() + count < STANDARD_VECTOR_SIZE &&
                     (heap.empty() || Less(index, heap.top())));
            output.Append(cursor.chunk, selection, count);
            if (cursor.position < cursor.chunk.size() || Advance(index)) {
                heap.push(index);
            }
        }
    }

private:
    bool Advance(idx_t index) {
        auto &cursor = *cursors[index];
        cursor.chunk.Reset();
        if (!cursor.collection.Scan(cursor.scan, cursor.chunk) || !cursor.chunk.size()) {
            return false;
        }
        cursor.position = 0;
        MakeSortKeys(data, cursor.chunk, cursor.keys);
        return true;
    }

    bool Less(idx_t a, idx_t b) const {
        auto &left = *cursors[a];
        auto &right = *cursors[b];
        auto l = left.keys.Values<string_t>()[left.position].GetValue();
        auto r = right.keys.Values<string_t>()[right.position].GetValue();
        auto comparison = memcmp(l.GetData(), r.GetData(), MinValue<idx_t>(l.GetSize(), r.GetSize()));
        if (comparison != 0) {
            return comparison < 0;
        }
        return l.GetSize() != r.GetSize() ? l.GetSize() < r.GetSize() : a < b;
    }

    ClientContext &context;
    const PartialSortBindData &data;
    vector<unique_ptr<Cursor>> cursors;
    std::priority_queue<idx_t, std::vector<idx_t>, std::function<bool(idx_t, idx_t)>> heap;
};

class PartitionedSortState : public GlobalTableFunctionState {
    struct RemoteTask {
        explicit RemoteTask(idx_t index_p) : index(index_p) {
        }
        idx_t index;
        std::thread worker;
        Handle<ArrowSchema> schema;
        bool done = false;
        std::deque<unique_ptr<ArrowArrayWrapper>> queue;
        unique_ptr<ArrowCursor> cursor;
        unique_ptr<ColumnDataAppendState> append;
    };

public:
    PartitionedSortState(ClientContext &context_p, const PartialSortBindData &data_p)
        : context(context_p), data(data_p), thread_context(context) {
    }

    ~PartitionedSortState() override {
        Stop();
    }

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            stopped = true;
        }
        cv.notify_all();
        for (auto &slot : slots) {
            if (slot && slot->worker.joinable()) {
                slot->worker.join();
            }
        }
    }

    void Check() {
        context.InterruptCheck();
        std::lock_guard<std::mutex> lock(mutex);
        if (!worker_error.empty()) {
            throw IOException("ADBC partitioned sort %s", worker_error);
        }
    }

    void GeneratePartitions() {
        auto start = std::chrono::steady_clock::now();
        auto key = QuoteIdentifier(data.partition_key);
        auto bounds =
            ReadRows(context,
                     data.uri,
                     "SELECT min(" + key + "), max(" + key + "), count(" + key + "), count(*) FROM " + data.Source());
        if (bounds.size() != 1) {
            throw IOException("ADBC partitioned sort: invalid range bounds result");
        }
        bounds_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        if (bounds[0][3].GetValue<uint64_t>() == 0) {
            return;
        }
        if (bounds[0][2].GetValue<uint64_t>() == 0) {
            predicates.push_back("");
        } else {
            auto minimum = bounds[0][0].GetValue<hugeint_t>();
            auto width = bounds[0][1].GetValue<hugeint_t>() - minimum + hugeint_t(1);
            auto count = data.partition_count;
            if (width < hugeint_t(count)) {
                count = NumericCast<idx_t>(Hugeint::Cast<uint64_t>(width));
            }
            for (idx_t i = 0; i < count; ++i) {
                string predicate;
                if (i > 0) {
                    predicate = key + " >= " + (minimum + width * hugeint_t(i) / hugeint_t(count)).ToString();
                }
                if (i + 1 < count) {
                    if (!predicate.empty()) {
                        predicate += " AND ";
                    }
                    predicate += key + " < " + (minimum + width * hugeint_t(i + 1) / hugeint_t(count)).ToString();
                }
                if (i == 0 && !predicate.empty()) {
                    predicate = "(" + predicate + ") OR " + key + " IS NULL";
                }
                predicates.push_back(std::move(predicate));
            }
        }
        remote_partitions = NumericCast<idx_t>(
            Hugeint::Cast<uint64_t>(hugeint_t(predicates.size()) * hugeint_t(data.ratio) / hugeint_t(RATIO_SCALE)));
        local_partitions = predicates.size() - remote_partitions;
        next_remote = local_partitions;
        for (idx_t i = 0; i < predicates.size(); ++i) {
            runs.push_back(make_uniq<ColumnDataCollection>(context, data.types));
        }
    }

    string Query(idx_t index, bool remote) const {
        return data.Select() + (predicates[index].empty() ? "" : " WHERE " + predicates[index]) +
               (remote ? data.Order() : "");
    }

    void ReadRemote(RemoteTask &task, unique_ptr<AdbcQuery> query) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            ++active_remote;
            peak_active_remote = MaxValue(peak_active_remote, active_remote);
        }
        try {
            query->Execute(Query(task.index, true));
            auto schema = query->GetSchema();
            {
                std::lock_guard<std::mutex> lock(mutex);
                task.schema = std::move(schema);
            }
            for (;;) {
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    cv.wait(lock, [&]() { return stopped || !worker_error.empty() || task.queue.size() < 2; });
                    if (stopped || !worker_error.empty()) {
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
                    std::lock_guard<std::mutex> lock(mutex);
                    if (stopped || !worker_error.empty()) {
                        break;
                    }
                    task.queue.push_back(std::move(batch));
                    queue_high_water = MaxValue<idx_t>(queue_high_water, task.queue.size());
                }
                cv.notify_all();
            }
        } catch (const std::exception &error) {
            SetWorkerError(task.index, error.what());
        } catch (...) {
            SetWorkerError(task.index, "unknown driver error");
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            --active_remote;
            task.done = true;
        }
        cv.notify_all();
    }

    void SetWorkerError(idx_t index, const string &message) {
        std::lock_guard<std::mutex> lock(mutex);
        if (worker_error.empty()) {
            worker_error = "remote partition " + std::to_string(index) + ": " + message;
        }
        cv.notify_all();
    }

    void StartNext(idx_t slot, unique_ptr<AdbcQuery> query = nullptr) {
        Check();
        if (next_remote == predicates.size()) {
            return;
        }
        // Connection initialization stays on the DuckDB thread. Each task owns
        // a new connection; no completed or cancelled driver's session is reused.
        if (!query) {
            query = make_uniq<AdbcQuery>(data.uri);
        }
        slots[slot] = make_uniq<RemoteTask>(next_remote++);
        auto &task = *slots[slot];
        task.worker =
            std::thread([this, &task, query = std::move(query)]() mutable { ReadRemote(task, std::move(query)); });
    }

    bool DrainRemote() {
        Check();
        bool progress = false;
        for (idx_t i = 0; i < slots.size(); ++i) {
            auto &slot = slots[i];
            if (!slot) {
                continue;
            }
            unique_ptr<ArrowArrayWrapper> batch;
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (!slot->queue.empty()) {
                    batch = std::move(slot->queue.front());
                    slot->queue.pop_front();
                }
            }
            if (batch) {
                cv.notify_all();
                progress = true;
                try {
                    if (!slot->cursor) {
                        slot->cursor = make_uniq<ArrowCursor>(context, std::move(slot->schema));
                        slot->cursor->Validate(data.names, data.types);
                        slot->append = make_uniq<ColumnDataAppendState>();
                        runs[slot->index]->InitializeAppend(*slot->append);
                    }
                    idx_t remaining = batch->arrow_array.length;
                    DataChunk chunk;
                    chunk.Initialize(context, data.types);
                    while (remaining) {
                        slot->cursor->Next(chunk, [&]() { return std::move(batch); });
                        remaining -= chunk.size();
                        remote_rows += chunk.size();
                        runs[slot->index]->Append(*slot->append, chunk);
                    }
                } catch (const std::exception &error) {
                    throw IOException("ADBC partitioned sort remote partition %llu: %s", slot->index, error.what());
                }
            }
            bool done;
            {
                std::lock_guard<std::mutex> lock(mutex);
                done = slot->done && slot->queue.empty();
            }
            if (done) {
                slot->worker.join();
                slot.reset();
                StartNext(i);
                progress = true;
            }
        }
        Check();
        return progress;
    }

    void SortLocal(idx_t index) {
        try {
            AdbcQuery query(data.uri);
            query.Execute(Query(index, false));
            ArrowCursor cursor(context, query.GetSchema());
            DataChunk chunk;
            chunk.Initialize(context, cursor.table.GetTypes());
            if (!cursor.Next(chunk, [&]() { return query.Next(); })) {
                return;
            }
            cursor.Validate(data.names, data.types);
            auto sort = CreateLocalSort(context, data);
            auto global_sink = sort->GetGlobalSinkState(context);
            ExecutionContext execution(context, thread_context, nullptr);
            auto local_sink = sort->GetLocalSinkState(execution);
            OperatorSinkInput sink{*global_sink, *local_sink, interrupt};
            do {
                DrainRemote();
                local_rows += chunk.size();
                sort->Sink(execution, chunk, sink);
            } while (cursor.Next(chunk, [&]() { return query.Next(); }));
            OperatorSinkCombineInput combine{*global_sink, *local_sink, interrupt};
            sort->Combine(execution, combine);
            OperatorSinkFinalizeInput finalize{*global_sink, interrupt};
            sort->Finalize(context, finalize);
            auto global_source = sort->GetGlobalSourceState(context, *global_sink);
            auto local_source = sort->GetLocalSourceState(execution, *global_source);
            OperatorSourceInput source{*global_source, *local_source, interrupt};
            ColumnDataAppendState append;
            runs[index]->InitializeAppend(append);
            bool finished;
            do {
                chunk.Reset();
                finished = sort->GetData(execution, chunk, source) == SourceResultType::FINISHED;
                if (chunk.size()) {
                    runs[index]->Append(append, chunk);
                }
                DrainRemote();
            } while (!finished);
        } catch (const std::exception &error) {
            throw IOException("ADBC partitioned sort local partition %llu: %s", index, error.what());
        }
    }

    void Initialize() {
        GeneratePartitions();
        slots.resize(MinValue(remote_partitions, data.max_remote_concurrency));
        vector<unique_ptr<AdbcQuery>> initial_queries;
        for (idx_t i = 0; i < slots.size(); ++i) {
            initial_queries.push_back(make_uniq<AdbcQuery>(data.uri));
        }
        for (idx_t i = 0; i < slots.size(); ++i) {
            StartNext(i, std::move(initial_queries[i]));
        }
        for (idx_t i = 0; i < local_partitions; ++i) {
            DrainRemote();
            SortLocal(i);
        }
        for (;;) {
            bool active = false;
            for (auto &slot : slots) {
                active = active || bool(slot);
            }
            if (!active) {
                break;
            }
            if (!DrainRemote()) {
                std::unique_lock<std::mutex> lock(mutex);
                cv.wait_for(lock, std::chrono::milliseconds(20));
            }
        }
        Check();
        constexpr idx_t FAN_IN = 16;
        while (runs.size() > FAN_IN) {
            vector<unique_ptr<ColumnDataCollection>> merged;
            for (idx_t begin = 0; begin < runs.size(); begin += FAN_IN) {
                auto end = MinValue<idx_t>(begin + FAN_IN, runs.size());
                auto result = make_uniq<ColumnDataCollection>(context, data.types);
                {
                    PartitionRunMerger merge(context, data, runs, begin, end);
                    ColumnDataAppendState append;
                    result->InitializeAppend(append);
                    DataChunk chunk;
                    chunk.Initialize(context, data.types);
                    for (;;) {
                        chunk.Reset();
                        merge.Scan(chunk);
                        if (!chunk.size()) {
                            break;
                        }
                        result->Append(append, chunk);
                    }
                }
                for (idx_t i = begin; i < end; ++i) {
                    runs[i].reset();
                }
                merged.push_back(std::move(result));
            }
            runs = std::move(merged);
            ++merge_passes;
        }
        merger = make_uniq<PartitionRunMerger>(context, data, runs, 0, runs.size());
        initialized = true;
    }

    void Scan(DataChunk &output) {
        if (!initialized) {
            Initialize();
        }
        merger->Scan(output);
    }

    void Metrics(OperatorMetrics &metrics) {
        std::lock_guard<std::mutex> lock(mutex);
        metrics.rows_scanned = local_rows + remote_rows;
        metrics.AddExtraInfo("Generated Partitions", std::to_string(predicates.size()));
        metrics.AddExtraInfo("Local Partitions", std::to_string(local_partitions));
        metrics.AddExtraInfo("Remote Partitions", std::to_string(remote_partitions));
        metrics.AddExtraInfo("Local Rows", std::to_string(local_rows));
        metrics.AddExtraInfo("Remote Rows", std::to_string(remote_rows));
        metrics.AddExtraInfo("Peak Active Remote Queries", std::to_string(peak_active_remote));
        metrics.AddExtraInfo("Queue High Watermark", std::to_string(queue_high_water));
        metrics.AddExtraInfo("Bounds Seconds", std::to_string(bounds_seconds));
        metrics.AddExtraInfo("Intermediate Merge Passes", std::to_string(merge_passes));
    }

private:
    ClientContext &context;
    const PartialSortBindData &data;
    ThreadContext thread_context;
    InterruptState interrupt;
    vector<string> predicates;
    vector<unique_ptr<ColumnDataCollection>> runs;
    vector<unique_ptr<RemoteTask>> slots;
    idx_t local_partitions = 0;
    idx_t remote_partitions = 0;
    idx_t next_remote = 0;
    idx_t local_rows = 0;
    idx_t remote_rows = 0;
    idx_t active_remote = 0;
    idx_t peak_active_remote = 0;
    idx_t queue_high_water = 0;
    idx_t merge_passes = 0;
    double bounds_seconds = 0;
    std::mutex mutex;
    std::condition_variable cv;
    bool stopped = false;
    bool initialized = false;
    string worker_error;
    unique_ptr<PartitionRunMerger> merger;
};

unique_ptr<GlobalTableFunctionState> InitPartitionedSort(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<PartitionedSortState>(context, input.bind_data->Cast<PartialSortBindData>());
}

void ScanPartitionedSort(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<PartitionedSortState>();
    try {
        state.Scan(output);
    } catch (...) {
        state.Stop();
        throw;
    }
}

void PartitionedSortMetrics(TableFunctionGetMetricsInput &input) {
    if (input.global_state) {
        input.global_state->Cast<PartitionedSortState>().Metrics(input.operator_metrics);
    }
}

} // namespace

void RegisterAdbcPartialSort(ExtensionLoader &loader) {
    {
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
        function.projection_pushdown = false;
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
    {
        TableFunction function("read_adbc_partitioned_sort",
                               {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                               ScanPartitionedSort,
                               BindPartitionedSort,
                               InitPartitionedSort);
        function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
            options.Add("partition_key", LogicalType::VARCHAR)
                .Add("sort_columns", LogicalType::LIST(LogicalType::VARCHAR))
                .Add("descending", LogicalType::LIST(LogicalType::BOOLEAN))
                .Add("nulls_first", LogicalType::LIST(LogicalType::BOOLEAN))
                .Add("pushdown_ratio", LogicalType::DECIMAL(10, 9))
                .Add("partition_count", LogicalType::BIGINT)
                .Add("max_remote_concurrency", LogicalType::BIGINT);
        });
        function.projection_pushdown = false;
        function.filter_pushdown = false;
        function.order_preservation_type = OrderPreservationType::FIXED_ORDER;
        function.parallelism = TableFunctionParallelism::FORCE_SINGLE_THREADED;
        function.get_metrics = PartitionedSortMetrics;
        CreateTableFunctionInfo info(function);
        FunctionDescription description;
        description.parameter_names = {"adbc_connection_profile_uri", "schema_or_database", "table"};
        description.description =
            "Automatically partitions an unchanged SQL table into integer key ranges, sorts selected ranges "
            "remotely with bounded concurrency, and merges spillable sorted runs. The pushdown ratio "
            "measures the fraction of generated partitions, not rows.";
        description.categories = {"adbc"};
        info.descriptions.push_back(std::move(description));
        loader.RegisterFunction(std::move(info));
    }
}

} // namespace adbc
} // namespace duckdb
