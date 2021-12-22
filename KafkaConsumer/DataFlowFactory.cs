using System.Data;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using Coding4fun.Tpl.DataFlow.Shared;
using Confluent.Kafka;
using KafkaConsumer.Db;
using Microsoft.Extensions.Logging;
using Timer = System.Timers.Timer;

namespace KafkaConsumer;

public class DataFlowFactory
{
    private const int DefaultBufferSize = 10_000;
    private const int DefaultDegreeOfParallelism = 1;

    private readonly ILoggerFactory _loggerFactory;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly DataflowBlockOptions _standardDataflowBlockOptions;
    private readonly KafkaReader _kafkaReader;
    private readonly ILogger _deserializeLogger;
    private readonly ILogger _sqlServerConsumerLogger;

    public DataFlowFactory(
        ILoggerFactory loggerFactory,
        CancellationTokenSource cancellationTokenSource,
        KafkaReader kafkaReader)
    {
        _loggerFactory = loggerFactory;
        _deserializeLogger = CreateLogger(nameof(CreateDeserializerBlock));
        _sqlServerConsumerLogger = CreateLogger(nameof(CreateSqlServerConsumerBlock));
        _cancellationTokenSource = cancellationTokenSource;
        _kafkaReader = kafkaReader;
        _standardDataflowBlockOptions = new DataflowBlockOptions
        {
            BoundedCapacity = 10_000,
            CancellationToken = _cancellationTokenSource.Token
        };
    }

    private ILogger CreateLogger(string methodName) => _loggerFactory
        .CreateLogger(methodName.Replace("Create", ""));

    private ExecutionDataflowBlockOptions CreateExecutionOptions(
        Action<ExecutionDataflowBlockOptions>? customizer = null)
    {
        ExecutionDataflowBlockOptions options = new()
        {
            BoundedCapacity = DefaultBufferSize,
            CancellationToken = _cancellationTokenSource.Token,
            MaxDegreeOfParallelism = DefaultDegreeOfParallelism
        };
        customizer?.Invoke(options);
        return options;
    }

    public BufferBlock<ConsumeResult<Ignore, string>> CreateKafkaReaderBlock()
    {
        return new BufferBlock<ConsumeResult<Ignore, string>>(_standardDataflowBlockOptions);
    }

    public TransformBlock<ConsumeResult<Ignore, string>, IKafkaEntity?> CreateDeserializerBlock(
        Func<string, Type> topic2Type,
        Action<ExecutionDataflowBlockOptions>? customizeOptions = null
    ) => new(message =>
    {
        try
        {
            Type targetType = topic2Type.Invoke(message.Topic);
            IKafkaEntity? kafkaEntity =
                (IKafkaEntity?)JsonSerializer.Deserialize(message.Message.Value, targetType);

            if (kafkaEntity == null)
            {
                _deserializeLogger.LogWarning("Unable to deserialize message: {topic}", message.Topic);
                return null;
            }

            kafkaEntity.Offset = message.TopicPartitionOffset;
            return kafkaEntity;
        }
        catch (Exception exception)
        {
            _deserializeLogger.LogError(exception.Message);
            return null;
        }
    }, CreateExecutionOptions(customizeOptions));

    public ActionBlock<IKafkaEntity?> CreateSqlServerConsumerBlock<TEntity>(
        MsSqlBatchHandler<TEntity> batchHandler,
        TimeSpan batchFlushTimeout)
    where TEntity : class, IKafkaEntity
    {
        Dictionary<Partition, TopicPartitionOffset> offsets = new();
        string entityTypeName = typeof(TEntity).Name;
        SemaphoreSlim batchFlushSemaphoreSlim = new(1, 1); 

        Timer timer = new Timer(batchFlushTimeout.TotalMilliseconds);

        async Task FlushBatchAsync()
        {
            await batchFlushSemaphoreSlim.WaitAsync();
            timer.Stop();
            try
            {
                await batchHandler.FlushBatchAsync();
                
                _sqlServerConsumerLogger.LogDebug("{entityTypeName}: {batchSize} rows has been inserted, {offsets}.",
                    entityTypeName,
                    batchHandler.BatchSize,
                    offsets.Values);
                
                _kafkaReader.CommitOffsets(offsets.Values);
            }
            catch (Exception exception)
            {
                _sqlServerConsumerLogger.LogError("Unable to flush batch.", exception);
                throw;
            }
            finally
            {
                batchFlushSemaphoreSlim.Release();
                timer.Start();
            }
        }
        
        timer.Elapsed += async (_, _) =>
        {
            _sqlServerConsumerLogger.LogDebug("Flush batch by timeout.");
            await FlushBatchAsync();
        };

        return new ActionBlock<IKafkaEntity?>(async entity =>
        {
            try
            {
                if (entity is not TEntity tEntity) return;
                offsets[tEntity.Offset.Partition] = entity.Offset;
                await batchHandler.AddEntityAsync(tEntity);
                
                if (batchHandler.IsBatchFull)
                {
                    _sqlServerConsumerLogger.LogDebug("Flush batch by size.");
                    await FlushBatchAsync();
                }
            }
            catch (Exception exception)
            {
                _sqlServerConsumerLogger.LogError("{entityTypeName}: {exception}", entityTypeName, exception.Message);
            }
        }, CreateExecutionOptions());
    }
}