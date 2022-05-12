#include <amqpcpp.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Storages/NATS/NATSSink.h>
#include <Storages/NATS/NATSSource.h>
#include <Storages/NATS/StorageNATS.h>
#include <Storages/NATS/WriteBufferToNATSProducer.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/parseAddress.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include <Common/logger_useful.h>

namespace DB
{

static const uint32_t QUEUE_SIZE = 100000;
static const auto RESCHEDULE_MS = 500;
static const auto BACKOFF_TRESHOLD = 8000;
static const auto MAX_THREAD_WORK_DURATION_MS = 60000;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_CONNECT_NATS;
    extern const int QUERY_NOT_ALLOWED;
}


StorageNATS::StorageNATS(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<NATSSettings> nats_settings_,
        bool is_attach_)
        : IStorage(table_id_)
        , WithContext(context_->getGlobalContext())
        , nats_settings(std::move(nats_settings_))
        , subjects(parseList(getContext()->getMacros()->expand(nats_settings->nats_subjects)))
        , format_name(getContext()->getMacros()->expand(nats_settings->nats_format))
        , row_delimiter(nats_settings->nats_row_delimiter.value)
        , schema_name(getContext()->getMacros()->expand(nats_settings->nats_schema))
        , num_consumers(nats_settings->nats_num_consumers.value)
        , log(&Poco::Logger::get("StorageNATS (" + table_id_.table_name + ")"))
        , semaphore(0, num_consumers)
        , queue_size(std::max(QUEUE_SIZE, static_cast<uint32_t>(getMaxBlockSize())))
        , milliseconds_to_wait(RESCHEDULE_MS)
        , is_attach(is_attach_)
{
    auto nats_username = getContext()->getMacros()->expand(nats_settings->nats_username);
    auto nats_password = getContext()->getMacros()->expand(nats_settings->nats_password);
    auto nats_token = getContext()->getMacros()->expand(nats_settings->nats_token);

    configuration =
    {
        .url = getContext()->getMacros()->expand(nats_settings->nats_url),
        .servers = parseList(getContext()->getMacros()->expand(nats_settings->nats_server_list)),
        .username = nats_username.empty() ? getContext()->getConfigRef().getString("nats.username", "") : nats_username,
        .password = nats_password.empty() ? getContext()->getConfigRef().getString("nats.password", "") : nats_password,
        .token = nats_token.empty() ? getContext()->getConfigRef().getString("nats.token", "") : nats_token,
        .max_reconnect = static_cast<int>(nats_settings->nats_max_reconnect.value),
        .reconnect_wait = static_cast<int>(nats_settings->nats_reconnect_wait.value),
        .secure = nats_settings->nats_secure.value
    };

    if (configuration.secure)
        SSL_library_init();

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    nats_context = addSettings(getContext());
    nats_context->makeQueryContext();

    try
    {
        connection = std::make_shared<NATSConnectionManager>(configuration, log);
        if (!connection->connect())
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Cannot connect to {}", connection->connectionInfoForLog());
    }
    catch (...)
    {
        tryLogCurrentException(log);
        if (!is_attach)
            throw;
    }

    /// One looping task for all consumers as they share the same connection == the same handler == the same event loop
    looping_task = getContext()->getMessageBrokerSchedulePool().createTask("NATSLoopingTask", [this]{ loopingFunc(); });
    looping_task->deactivate();

    streaming_task = getContext()->getMessageBrokerSchedulePool().createTask("NATSStreamingTask", [this]{ streamingToViewsFunc(); });
    streaming_task->deactivate();

    connection_task = getContext()->getMessageBrokerSchedulePool().createTask("NATSConnectionManagerTask", [this]{ connectionFunc(); });
    connection_task->deactivate();
}


Names StorageNATS::parseList(const String& list)
{
    Names result;
    if (list.empty())
        return result;
    boost::split(result, list, [](char c){ return c == ','; });
    for (String & key : result)
        boost::trim(key);

    return result;
}


String StorageNATS::getTableBasedName(String name, const StorageID & table_id)
{
    if (name.empty())
        return fmt::format("{}_{}", table_id.database_name, table_id.table_name);
    else
        return fmt::format("{}_{}_{}", name, table_id.database_name, table_id.table_name);
}


ContextMutablePtr StorageNATS::addSettings(ContextPtr local_context) const
{
    auto modified_context = Context::createCopy(local_context);
    modified_context->setSetting("input_format_skip_unknown_fields", true);
    modified_context->setSetting("input_format_allow_errors_ratio", 0.);
    modified_context->setSetting("input_format_allow_errors_num", nats_settings->nats_skip_broken_messages.value);

    if (!schema_name.empty())
        modified_context->setSetting("format_schema", schema_name);

    for (const auto & setting : *nats_settings)
    {
        const auto & setting_name = setting.getName();

        /// check for non-nats-related settings
        if (!setting_name.starts_with("nats_"))
            modified_context->setSetting(setting_name, setting.getValue());
    }

    return modified_context;
}


void StorageNATS::loopingFunc()
{
    connection->getHandler().startLoop();
}


void StorageNATS::stopLoop()
{
    connection->getHandler().updateLoopState(Loop::STOP);
}

void StorageNATS::stopLoopIfNoReaders()
{
    /// Stop the loop if no select was started.
    /// There can be a case that selects are finished
    /// but not all sources decremented the counter, then
    /// it is ok that the loop is not stopped, because
    /// there is a background task (streaming_task), which
    /// also checks whether there is an idle loop.
    std::lock_guard lock(loop_mutex);
    if (readers_count)
        return;
    connection->getHandler().updateLoopState(Loop::STOP);
}

void StorageNATS::startLoop()
{
    connection->getHandler().updateLoopState(Loop::RUN);
    looping_task->activateAndSchedule();
}


void StorageNATS::incrementReader()
{
    ++readers_count;
}


void StorageNATS::decrementReader()
{
    --readers_count;
}


void StorageNATS::connectionFunc()
{
    if (!connection->reconnect())
        connection_task->scheduleAfter(RESCHEDULE_MS);
}


/* Need to deactivate this way because otherwise might get a deadlock when first deactivate streaming task in shutdown and then
 * inside streaming task try to deactivate any other task
 */
void StorageNATS::deactivateTask(BackgroundSchedulePool::TaskHolder & task, bool wait, bool stop_loop)
{
    if (stop_loop)
        stopLoop();

    std::unique_lock<std::mutex> lock(task_mutex, std::defer_lock);
    if (lock.try_lock())
    {
        task->deactivate();
        lock.unlock();
    }
    else if (wait) /// Wait only if deactivating from shutdown
    {
        lock.lock();
        task->deactivate();
    }
}


size_t StorageNATS::getMaxBlockSize() const
{
     return nats_settings->nats_max_block_size.changed
         ? nats_settings->nats_max_block_size.value
         : (getContext()->getSettingsRef().max_insert_block_size.value / num_consumers);
}


Pipe StorageNATS::read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & /* query_info */,
        ContextPtr local_context,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t /* max_block_size */,
        unsigned /* num_streams */)
{
    if (num_created_consumers == 0)
        return {};

    if (!local_context->getSettingsRef().stream_like_engine_allow_direct_select)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Direct select is not allowed. To enable use setting `stream_like_engine_allow_direct_select`");

    if (mv_attached)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StorageNATS with attached materialized views");

    std::lock_guard lock(loop_mutex);

    auto sample_block = storage_snapshot->getSampleBlockForColumns(column_names);
    auto modified_context = addSettings(local_context);

    if (!connection->isConnected())
    {
        if (connection->getHandler().loopRunning())
            deactivateTask(looping_task, false, true);
        if (!connection->reconnect())
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "No connection to {}", connection->connectionInfoForLog());
    }

    Pipes pipes;
    pipes.reserve(num_created_consumers);

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto nats_source = std::make_shared<NATSSource>(
            *this, storage_snapshot, modified_context, column_names, 1);

        auto converting_dag = ActionsDAG::makeConvertingActions(
            nats_source->getPort().getHeader().getColumnsWithTypeAndName(),
            sample_block.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

        auto converting = std::make_shared<ExpressionActions>(std::move(converting_dag));
        auto converting_transform = std::make_shared<ExpressionTransform>(nats_source->getPort().getHeader(), std::move(converting));

        pipes.emplace_back(std::move(nats_source));
        pipes.back().addTransform(std::move(converting_transform));
    }

    if (!connection->getHandler().loopRunning() && connection->isConnected())
        startLoop();

    LOG_DEBUG(log, "Starting reading {} streams", pipes.size());
    auto united_pipe = Pipe::unitePipes(std::move(pipes));
    united_pipe.addInterpreterContext(modified_context);
    return united_pipe;
}


SinkToStoragePtr StorageNATS::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    return std::make_shared<NATSSink>(*this, metadata_snapshot, local_context);
}


void StorageNATS::startup()
{
    if (!connection->isConnected())
    {
        connection_task->activateAndSchedule();
    }

    for (size_t i = 0; i < num_consumers; ++i)
    {
        try
        {
            auto buffer = createReadBuffer();
            pushReadBuffer(std::move(buffer));
            ++num_created_consumers;
        }
        catch (...)
        {
            if (!is_attach)
                throw;
            tryLogCurrentException(log);
        }
    }

    streaming_task->activateAndSchedule();
}


void StorageNATS::shutdown()
{
    shutdown_called = true;

    /// In case it has not yet been able to setup connection;
    deactivateTask(connection_task, true, false);

    /// The order of deactivating tasks is important: wait for streamingToViews() func to finish and
    /// then wait for background event loop to finish.
    deactivateTask(streaming_task, true, false);
    deactivateTask(looping_task, true, true);

    /// Just a paranoid try catch, it is not actually needed.
    try
    {
        if (drop_table)
        {
            for (auto & buffer : buffers)
                buffer->unsubscribe();
        }

        /// It is important to close connection here - before removing consumer buffers, because
        /// it will finish and clean callbacks, which might use those buffers data.
        if (connection->getHandler().loopRunning())
            stopLoop();
        connection->disconnect();

        for (size_t i = 0; i < num_created_consumers; ++i)
            popReadBuffer();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void StorageNATS::pushReadBuffer(ConsumerBufferPtr buffer)
{
    std::lock_guard lock(buffers_mutex);
    buffers.push_back(buffer);
    semaphore.set();
}


ConsumerBufferPtr StorageNATS::popReadBuffer()
{
    return popReadBuffer(std::chrono::milliseconds::zero());
}


ConsumerBufferPtr StorageNATS::popReadBuffer(std::chrono::milliseconds timeout)
{
    // Wait for the first free buffer
    if (timeout == std::chrono::milliseconds::zero())
        semaphore.wait();
    else
    {
        if (!semaphore.tryWait(timeout.count()))
            return nullptr;
    }

    // Take the first available buffer from the list
    std::lock_guard lock(buffers_mutex);
    auto buffer = buffers.back();
    buffers.pop_back();

    return buffer;
}


ConsumerBufferPtr StorageNATS::createReadBuffer()
{
    return std::make_shared<ReadBufferFromNATSConsumer>(
        connection, subjects,
        nats_settings->nats_queue_group.changed ? nats_settings->nats_queue_group.value : getStorageID().getFullTableName(),
        log, row_delimiter, queue_size, shutdown_called);
}


ProducerBufferPtr StorageNATS::createWriteBuffer()
{
    return std::make_shared<WriteBufferToNATSProducer>(
        configuration, getContext(), subjects[0], shutdown_called, log,
        row_delimiter ? std::optional<char>{row_delimiter} : std::nullopt, 1, 1024);
}


bool StorageNATS::checkDependencies(const StorageID & table_id)
{
    // Check if all dependencies are attached
    auto dependencies = DatabaseCatalog::instance().getDependencies(table_id);
    if (dependencies.empty())
        return true;

    // Check the dependencies are ready?
    for (const auto & db_tab : dependencies)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(db_tab, getContext());
        if (!table)
            return false;

        // If it materialized view, check it's target table
        auto * materialized_view = dynamic_cast<StorageMaterializedView *>(table.get());
        if (materialized_view && !materialized_view->tryGetTargetTable())
            return false;

        // Check all its dependencies
        if (!checkDependencies(db_tab))
            return false;
    }

    return true;
}


void StorageNATS::streamingToViewsFunc()
{
    try
    {
        auto table_id = getStorageID();

        // Check if at least one direct dependency is attached
        size_t dependencies_count = DatabaseCatalog::instance().getDependencies(table_id).size();
        bool nats_connected = connection->isConnected() || connection->reconnect();

        if (dependencies_count && nats_connected)
        {
            auto start_time = std::chrono::steady_clock::now();

            mv_attached.store(true);

            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (!shutdown_called && num_created_consumers > 0)
            {
                if (!checkDependencies(table_id))
                    break;

                LOG_DEBUG(log, "Started streaming to {} attached views", dependencies_count);

                if (streamToViews())
                {
                    /// Reschedule with backoff.
                    if (milliseconds_to_wait < BACKOFF_TRESHOLD)
                        milliseconds_to_wait *= 2;
                    stopLoopIfNoReaders();
                    break;
                }
                else
                {
                    milliseconds_to_wait = RESCHEDULE_MS;
                }

                auto end_time = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
                if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
                {
                    stopLoopIfNoReaders();
                    LOG_TRACE(log, "Reschedule streaming. Thread work duration limit exceeded.");
                    break;
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    mv_attached.store(false);

    /// If there is no running select, stop the loop which was
    /// activated by previous select.
    if (connection->getHandler().loopRunning())
        stopLoopIfNoReaders();

    if (!shutdown_called)
        streaming_task->scheduleAfter(milliseconds_to_wait);
}


bool StorageNATS::streamToViews()
{
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception("Engine table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(insert, nats_context, false, true, true);
    auto block_io = interpreter.execute();

    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());
    auto column_names = block_io.pipeline.getHeader().getNames();
    auto sample_block = storage_snapshot->getSampleBlockForColumns(column_names);

    auto block_size = getMaxBlockSize();

    // Create a stream for each consumer and join them in a union stream
    std::vector<std::shared_ptr<NATSSource>> sources;
    Pipes pipes;
    sources.reserve(num_created_consumers);
    pipes.reserve(num_created_consumers);

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto source = std::make_shared<NATSSource>(
            *this, storage_snapshot, nats_context, column_names, block_size);
        sources.emplace_back(source);
        pipes.emplace_back(source);

        // Limit read batch to maximum block size to allow DDL
        StreamLocalLimits limits;

        limits.speed_limits.max_execution_time = nats_settings->nats_flush_interval_ms.changed
                                                  ? nats_settings->nats_flush_interval_ms
                                                  : getContext()->getSettingsRef().stream_flush_interval_ms;

        limits.timeout_overflow_mode = OverflowMode::BREAK;

        source->setLimits(limits);
    }

    block_io.pipeline.complete(Pipe::unitePipes(std::move(pipes)));

    if (!connection->getHandler().loopRunning())
        startLoop();

    {
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }

    /* Note: sending ack() with loop running in another thread will lead to a lot of data races inside the library, but only in case
     * error occurs or connection is lost while ack is being sent
     */
    deactivateTask(looping_task, false, true);
    size_t queue_empty = 0;

    if (!connection->isConnected())
    {
        if (shutdown_called)
            return true;

        if (connection->reconnect())
        {
            LOG_DEBUG(log, "Connection restored");
        }
        else
        {
            LOG_TRACE(log, "Reschedule streaming. Unable to restore connection.");
            return true;
        }
    }
    else
    {
        for (auto & source : sources)
        {
            if (source->queueEmpty())
                ++queue_empty;

            connection->getHandler().iterateLoop();
        }
    }

    if (queue_empty == num_created_consumers)
    {
        LOG_TRACE(log, "Reschedule streaming. Queues are empty.");
        return true;
    }
    else
    {
        startLoop();
    }

    /// Do not reschedule, do not stop event loop.
    return false;
}


void registerStorageNATS(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        auto nats_settings = std::make_unique<NATSSettings>();
        bool with_named_collection = getExternalDataSourceConfiguration(args.engine_args, *nats_settings, args.getLocalContext());
        if (!with_named_collection && !args.storage_def->settings)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NATS engine must have settings");

        nats_settings->loadFromQuery(*args.storage_def);

        if (!nats_settings->nats_url.changed
           && !nats_settings->nats_server_list.changed)
                throw Exception("You must specify either `nats_url` or `nats_server_list` settings",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!nats_settings->nats_format.changed)
            throw Exception("You must specify `nats_format` setting", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<StorageNATS>(args.table_id, args.getContext(), args.columns, std::move(nats_settings), args.attach);
    };

    factory.registerStorage("NATS", creator_fn, StorageFactory::StorageFeatures{ .supports_settings = true, });
}


NamesAndTypesList StorageNATS::getVirtuals() const
{
    return NamesAndTypesList{
            {"_subject", std::make_shared<DataTypeString>()}
    };
}

}
