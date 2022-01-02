using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using Coding4fun.Tpl.DataFlow.Shared;
using Confluent.Kafka;
using KafkaConsumer.Config;
using KafkaConsumer.Db;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Timer = System.Timers.Timer;

namespace KafkaConsumer;

public class DataFlowFactory
{
    private const int DefaultBufferSize = 10_000;
    private const int DefaultDegreeOfParallelism = 1;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly ILogger _deserializeLogger;
    private readonly KafkaReader _kafkaReader;

    private readonly ILoggerFactory _loggerFactory;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger _sqlServerConsumerLogger;

    public DataFlowFactory(
        IServiceProvider serviceProvider,
        ILoggerFactory loggerFactory,
        CancellationTokenSource cancellationTokenSource,
        KafkaReader kafkaReader)
    {
        ILogger CreateLogger(string methodName) => _loggerFactory
            .CreateLogger(methodName.Replace("Create", ""));

        _serviceProvider = serviceProvider;
        _loggerFactory = loggerFactory;
        _deserializeLogger = CreateLogger(nameof(CreateDeserializerBlock));
        _sqlServerConsumerLogger = CreateLogger(nameof(CreateSqlServerConsumerBlock));
        _cancellationTokenSource = cancellationTokenSource;
        _kafkaReader = kafkaReader;
    }

    private ExecutionDataflowBlockOptions CreateExecutionOptions(
        Action<ExecutionDataflowBlockOptions>? customize = null)
    {
        ExecutionDataflowBlockOptions options = new()
        {
            BoundedCapacity = DefaultBufferSize,
            CancellationToken = _cancellationTokenSource.Token,
            MaxDegreeOfParallelism = DefaultDegreeOfParallelism,
            SingleProducerConstrained = true,
            EnsureOrdered = true
        };
        customize?.Invoke(options);
        return options;
    }

    public TransformBlock<ConsumeResult<Ignore, string>, KafkaEntity> CreateDeserializerBlock(
        Func<string, Type> topic2Type,
        Action<ExecutionDataflowBlockOptions>? customizeOptions = null
    ) => new(message =>
    {
        KafkaEntity kafkaEntity = new KafkaEntity(message);

        try
        {
            Type targetType = topic2Type.Invoke(message.Topic);
            object? deserializedObject = JsonSerializer.Deserialize(message.Message.Value, targetType);
            kafkaEntity.Item = deserializedObject;

            using SHA1 sha1 = SHA1.Create();
            kafkaEntity.MessageHash = sha1.ComputeHash(Encoding.UTF8.GetBytes(message.Message.Value));
        }
        catch (Exception exception)
        {
            _deserializeLogger.LogError("Unable to deserialize: {ErrorMessage} [{Topic}:{Partition}:{Offset}]",
                exception.Message,
                message.Topic,
                message.Partition,
                message.Offset);

            kafkaEntity.Error = exception.Message;
        }

        return kafkaEntity;
    }, CreateExecutionOptions(customizeOptions));

    public ActionBlock<KafkaEntity> CreateSqlServerConsumerBlock<TBatchHandler>()
        where TBatchHandler : MsSqlBatchHandler
    {
        MsSqlConfig msSqlConfig = _serviceProvider.GetService<MsSqlConfig>() ??
                                  throw new InvalidOperationException(
                                      $"Unable to get instance of {nameof(MsSqlConfig)}");

        string batchHandlerName = typeof(TBatchHandler).Name;

        TBatchHandler batchHandler = _serviceProvider.GetService<TBatchHandler>() ??
                                     throw new InvalidOperationException(
                                         $"Unable to get instance of {batchHandlerName}");

        Dictionary<Partition, TopicPartitionOffset> offsets = new();
        SemaphoreSlim batchFlushSemaphoreSlim = new(1, 1);

        Timer timer = new Timer((msSqlConfig.BatchTimeout ?? TimeSpan.FromMinutes(1)).TotalMilliseconds);

        async Task FlushBatchAsync()
        {
            await batchFlushSemaphoreSlim.WaitAsync();
            timer.Stop();
            try
            {
                Stopwatch stopwatch = Stopwatch.StartNew();
                int insertedRowsCount = await batchHandler.FlushBatchAsync();
                stopwatch.Stop();

                _sqlServerConsumerLogger.LogDebug(
                    "{BatchHandlerName}: {InsertedRowsCount} rows has been inserted, {Offsets}, {Elapsed} ms",
                    batchHandlerName,
                    insertedRowsCount,
                    offsets.Values,
                    stopwatch.ElapsedMilliseconds);

                _kafkaReader.CommitOffsets(offsets.Values);
                timer.Start();
            }
            catch (Exception exception)
            {
                _sqlServerConsumerLogger.LogError("Unable to flush batch, {ErrorMessage}",
                    exception.Message);
                throw;
            }
            finally
            {
                batchFlushSemaphoreSlim.Release();
            }
        }

        timer.Elapsed += async (_, _) =>
        {
            _sqlServerConsumerLogger.LogDebug("Flush batch by timeout");
            await FlushBatchAsync();
        };
        timer.Start();

        return new ActionBlock<KafkaEntity>(async entity =>
        {
            try
            {
                offsets[entity.KafkaMessage.Partition] = entity.KafkaMessage.TopicPartitionOffset;
                await batchHandler.AddEntityAsync(entity);

                if (batchHandler.IsBatchFull)
                {
                    _sqlServerConsumerLogger.LogDebug("Flush batch by size");
                    await FlushBatchAsync();
                }
            }
            catch (Exception exception)
            {
                _sqlServerConsumerLogger.LogError("{BatchHandlerName}: {ErrorMessage}", batchHandlerName,
                    exception.Message);
            }
        }, CreateExecutionOptions(o => o.NameFormat = batchHandlerName + "_{1}"));
    }
}