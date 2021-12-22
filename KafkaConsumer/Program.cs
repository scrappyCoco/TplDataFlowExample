using System.Data;
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

    ConsumerBuilder<Ignore, string> consumerBuilder = new (new ConsumerConfig
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
        .AddSingleton<KafkaReader>()
        .AddTransient<DataFlowFactory>()
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

    TransformBlock<ConsumeResult<Ignore, string>, IKafkaEntity?> deserializeMessageBlock =
        dataFlowFactory.CreateDeserializerBlock(topic =>
                topic == config.Kafka.LoginTopicName ? typeof(LoginMessage) :
                topic == config.Kafka.LogoutTopicName ? typeof(LogoutMessage) :
                throw new InvalidOperationException($"Unable to find mapping for ${topic}"),
            options => options.MaxDegreeOfParallelism = config.DeserializeThreadsCount);

    DataTable loginDt = new();
    loginDt.Columns.Add(nameof(LoginMessage.Email), typeof(string));
    loginDt.Columns.Add(nameof(LoginMessage.Time), typeof(DateTime));
    loginDt.Columns.Add(nameof(LoginMessage.SessionId), typeof(string));

    DataTable logoutDt = new();
    logoutDt.Columns.Add(nameof(LogoutMessage.Time), typeof(DateTime));
    logoutDt.Columns.Add(nameof(LogoutMessage.SessionId), typeof(string));

    TimeSpan defaultTimeout = TimeSpan.FromMinutes(5d);
    
    ActionBlock<IKafkaEntity?> saveToMsSqlLoginMessageBlock =
        dataFlowFactory.CreateSqlServerConsumerBlock(new MsLoginBatchHandler(config.MsSql.ConnectionString), defaultTimeout);

    ActionBlock<IKafkaEntity?> saveToMsSqlLogoutMessageBlock =
        dataFlowFactory.CreateSqlServerConsumerBlock(new MsLogoutBatchHandler(config.MsSql.ConnectionString), defaultTimeout);

    DataflowLinkOptions dataflowLinkOptions = new()
    {
        PropagateCompletion = true
    };

    kafkaReaderBlock.LinkTo(deserializeMessageBlock, dataflowLinkOptions);

    deserializeMessageBlock.LinkTo(saveToMsSqlLoginMessageBlock, dataflowLinkOptions, message => message is LoginMessage);
    deserializeMessageBlock.LinkTo(saveToMsSqlLogoutMessageBlock, dataflowLinkOptions, message => message is LogoutMessage);

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
    logger!.LogCritical("{exception}", exception.Message);
}