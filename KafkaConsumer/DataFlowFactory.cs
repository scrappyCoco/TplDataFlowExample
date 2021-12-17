using System.Data;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using Coding4fun.Tpl.DataFlow.Shared;
using Confluent.Kafka;
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
        DataTable dataTable,
        int batchSize,
        Func<Task> flushBatchAsync,
        Func<TEntity, object?[]> fillDataTable)
    where TEntity : class, IKafkaEntity
    {
        Dictionary<Partition, TopicPartitionOffset> offsets = new();
        string entityTypeName = typeof(TEntity).Name;

        Timer timer = new Timer(TimeSpan.FromMinutes(10d).TotalMilliseconds);

        async Task FlushBatchAsync()
        {
            timer.Stop();
            await flushBatchAsync.Invoke();
            _kafkaReader.CommitOffsets(offsets.Values);
            if (_sqlServerConsumerLogger.IsEnabled(LogLevel.Debug))
            {
                _sqlServerConsumerLogger.LogDebug("{entityTypeName}: {batchSize} rows has been inserted, {offsets}",
                    entityTypeName,
                    batchSize,
                    offsets.Values);
            }
            
            timer.Start();
        }
        
        timer.Elapsed += async (_, _) => await FlushBatchAsync();

        return new ActionBlock<IKafkaEntity?>(async entity =>
        {
            try
            {
                if (entity is not TEntity tEntity) return;
                offsets[tEntity.Offset.Partition] = entity.Offset;
                dataTable.Rows.Add(fillDataTable.Invoke(tEntity));
                if (dataTable.Rows.Count >= batchSize) await FlushBatchAsync();
            }
            catch (Exception exception)
            {
                _sqlServerConsumerLogger.LogError("{entityTypeName}: {exception}", entityTypeName, exception.Message);
            }
        }, CreateExecutionOptions());
    }
}