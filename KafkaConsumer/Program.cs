using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks.Dataflow;
using Coding4fun.Tpl.DataFlow.Shared;
using Confluent.Kafka;
using KafkaConsumer;
using KafkaConsumer.Config;
using KafkaConsumer.Db;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;

Thread.CurrentThread.Name = "Main Thread";
CancellationTokenSource cancellationTokenSource = new();
ILogger? logger = null;

try
{
    RootConfig config = new ConfigurationBuilder()
        .AddJsonFile("appsettings.json")
        .Build()
        .Get<RootConfig>();

    ValidationResult[] validationResults = config.Validate();
    if (validationResults.Any())
    {
        foreach (ValidationResult validationResult in validationResults)
        {
            Console.Error.WriteLine(validationResult.ToString());
            return;
        }
    }

    ConsumerBuilder<Ignore, string> consumerBuilder = new(new ConsumerConfig
    {
        BootstrapServers = config.Kafka.BrokerList,
        GroupId = config.Kafka.GroupId,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false
    });

    await using var servicesProvider = new ServiceCollection()
        .AddLogging(loggingBuilder =>
        {
            loggingBuilder.ClearProviders();
            loggingBuilder.SetMinimumLevel(LogLevel.Debug);
            loggingBuilder.AddNLog("NLog.config");
        })
        .AddSingleton(cancellationTokenSource)
        .AddSingleton(consumerBuilder)
        .AddSingleton(_ => config.MsSql)
        .AddSingleton<KafkaReader>()
        .AddTransient<DataFlowFactory>()
        .AddTransient<MsLoginBatchHandler>()
        .AddTransient<MsLogoutBatchHandler>()
        .BuildServiceProvider();

    logger = servicesProvider.GetRequiredService<ILogger<Program>>();

    var kafkaLogger = servicesProvider.GetRequiredService<ILoggerFactory>().CreateLogger("KafkaInternal");

    consumerBuilder.SetPartitionsAssignedHandler((_, topics) =>
    {
        kafkaLogger.LogWarning("Partitions assigned: {Topics}",
            string.Join(",", topics.Select(topic => $"[{topic.Topic}|{topic.Partition.Value}]"))
        );
    });

    consumerBuilder.SetPartitionsRevokedHandler((_, topics) =>
    {
        kafkaLogger.LogWarning("Partitions revoked: {Topics}",
            string.Join(",", topics.Select(topic => $"[{topic.Topic}|{topic.Partition.Value}]"))
        );
    });

    consumerBuilder.SetErrorHandler((_, error) =>
    {
        kafkaLogger.LogWarning("Error occured: {ErrorMessage}", error.ToString());
    });

    _ = Task.Run(() =>
    {
        Thread.CurrentThread.Name = "Exit Thread";
        Console.WriteLine("Press any key to cancel");
        Console.ReadKey();
        cancellationTokenSource.Cancel();
    });

    DataFlowFactory dataFlowFactory = servicesProvider.GetRequiredService<DataFlowFactory>();

    Type MapTopicNameToEntityType(string topic) =>
        topic == config.Kafka.LoginTopicName ? typeof(LoginMessage) :
        topic == config.Kafka.LogoutTopicName ? typeof(LogoutMessage) :
        throw new InvalidOperationException($"Unable to find mapping for ${topic}");

    TransformBlock<ConsumeResult<Ignore, string>, KafkaEntity> deserializeMessageBlock =
        dataFlowFactory.CreateDeserializerBlock(MapTopicNameToEntityType,
            options => options.MaxDegreeOfParallelism = config.DeserializeThreadsCount);

    ActionBlock<KafkaEntity> saveToMsSqlLoginMessageBlock =
        dataFlowFactory.CreateSqlServerConsumerBlock<MsLoginBatchHandler>();

    ActionBlock<KafkaEntity> saveToMsSqlLogoutMessageBlock =
        dataFlowFactory.CreateSqlServerConsumerBlock<MsLogoutBatchHandler>();

    DataflowLinkOptions dataflowLinkOptions = new()
    {
        PropagateCompletion = true
    };

    deserializeMessageBlock.LinkTo(saveToMsSqlLoginMessageBlock, dataflowLinkOptions,
        message => message.KafkaMessage.Topic == config.Kafka.LoginTopicName);

    deserializeMessageBlock.LinkTo(saveToMsSqlLogoutMessageBlock, dataflowLinkOptions,
        message => message.KafkaMessage.Topic == config.Kafka.LogoutTopicName);

    var kafkaReader = servicesProvider.GetRequiredService<KafkaReader>();
    Task kafkaReaderTask = kafkaReader.ReadMessagesAsync(
        deserializeMessageBlock,
        config.Kafka.LoginTopicName,
        config.Kafka.LogoutTopicName);

    await Task.WhenAll(
        kafkaReaderTask,
        saveToMsSqlLoginMessageBlock.Completion,
        saveToMsSqlLogoutMessageBlock.Completion);
}
catch (Exception exception)
{
    if (logger != null)
    {
        logger.LogCritical("{ErrorMessage}", exception.Message);
    }
    else
    {
        Console.Error.WriteLine(exception.Message);
    }
}