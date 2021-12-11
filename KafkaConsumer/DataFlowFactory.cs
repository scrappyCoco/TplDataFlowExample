using System.Data;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using Coding4fun.Tpl.DataFlow.Shared;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace KafkaConsumer;

public class DataFlowFactory
{
    private const int DefaultBufferSize = 10_000;
    private const int DefaultDegreeOfParallelism = 1;

    private readonly ILoggerFactory _loggerFactory;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly DataflowBlockOptions _standardDataflowBlockOptions;
    private readonly ILogger _deserializeLogger;
    private readonly ILogger _sqlServerConsumerLogger;

    public DataFlowFactory(ILoggerFactory loggerFactory, CancellationTokenSource cancellationTokenSource)
    {
        _loggerFactory = loggerFactory;
        _deserializeLogger = CreateLogger(nameof(CreateDeserializerBlock));
        _sqlServerConsumerLogger = CreateLogger(nameof(CreateSqlServerConsumerBlock));
        _cancellationTokenSource = cancellationTokenSource;
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
        Func<TEntity, object?[]> fillDataTable,
        ConsumerBuilder<Ignore, string> kafkaConsumerBuilder)
    where TEntity : class, IKafkaEntity
    {
        Dictionary<Partition, TopicPartitionOffset> offsets = new();
        string entityTypeName = typeof(TEntity).Name;
        
        return new ActionBlock<IKafkaEntity?>(async entity =>
        {
            try
            {
                if (entity is not TEntity tEntity) return;
                offsets[tEntity.Offset.Partition] = entity.Offset;
                dataTable.Rows.Add(fillDataTable.Invoke(tEntity));
                if (dataTable.Rows.Count > batchSize)
                {
                    await flushBatchAsync.Invoke();
                    KafkaReader.CommitOffsets(kafkaConsumerBuilder, offsets.Values);
                }
            }
            catch (Exception exception)
            {
                _sqlServerConsumerLogger.LogError("{entityTypeName}, {exception}", entityTypeName, exception.Message);
            }
        }, CreateExecutionOptions());
    }
    
    public BroadcastBlock<IKafkaEntity?> CreateBroadcastBlock() => new(it => it, CreateExecutionOptions());
}