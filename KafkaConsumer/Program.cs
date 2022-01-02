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
    var kafkaReader = servicesProvider.GetRequiredService<KafkaReader>();

    _ = Task.Run(() =>
    {
        Thread.CurrentThread.Name = "Exit Thread";
        Console.WriteLine("Press any key to cancel");
        Console.ReadKey();
        cancellationTokenSource.Cancel();
    });

    DataFlowFactory dataFlowFactory = servicesProvider.GetRequiredService<DataFlowFactory>();
    BufferBlock<ConsumeResult<Ignore, string>> kafkaReaderBlock = dataFlowFactory.CreateKafkaReaderBlock();

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

    kafkaReaderBlock.LinkTo(deserializeMessageBlock, dataflowLinkOptions);

    deserializeMessageBlock.LinkTo(saveToMsSqlLoginMessageBlock, dataflowLinkOptions,
        message => message.KafkaMessage.Topic == config.Kafka.LoginTopicName);
    deserializeMessageBlock.LinkTo(saveToMsSqlLogoutMessageBlock, dataflowLinkOptions,
        message => message.KafkaMessage.Topic == config.Kafka.LogoutTopicName);

    Task kafkaReaderTask = kafkaReader.ReadMessagesAsync(
        kafkaReaderBlock,
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
        logger.LogCritical("{errorMessage}", exception.Message);
    }
    else
    {
        var foregroundColor = Console.ForegroundColor;
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine(exception.Message);
        Console.ForegroundColor = foregroundColor;
    }
}