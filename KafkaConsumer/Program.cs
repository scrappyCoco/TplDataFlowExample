using System.Data;
using System.Threading.Tasks.Dataflow;
using Coding4fun.Tpl.DataFlow.Shared;
using Confluent.Kafka;
using KafkaConsumer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;


Thread.CurrentThread.Name = "Main Thread";
CancellationTokenSource cancellationTokenSource = new();
ILoggerFactory loggerFactory = new NullLoggerFactory();
ILogger logger = loggerFactory.CreateLogger("Main");

try
{
    #if DEBUG
    const int batchSize = 100;
    #else
    const int batchSize = 10_000;
    #endif
    const int deserializeTheadCount = 2;
    
    const string msConnectionString =
        "server=localhost;user id=sa;password=yourStrong(!)Password;Trusted_Connection=True";

    var kafkaConsumerConfig = new ConsumerConfig
    {
        BootstrapServers = KafkaConfig.BrokerList,
        GroupId = "TestGroup",
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false
    };
    var kafkaConsumerBuilder = new ConsumerBuilder<Ignore, string>(kafkaConsumerConfig);

    _ = Task.Run(() =>
    {
        Thread.CurrentThread.Name = "Exit Thread";
        Console.WriteLine("Press any key to cancel");
        Console.ReadKey();
        cancellationTokenSource.Cancel();
    });

    DataFlowFactory dataFlowFactory = new DataFlowFactory(loggerFactory, cancellationTokenSource);
    BufferBlock<ConsumeResult<Ignore, string>> kafkaReaderBlock = dataFlowFactory.CreateKafkaReaderBlock();

    TransformBlock<ConsumeResult<Ignore, string>, IKafkaEntity?> deserializeMessageBlock =
        dataFlowFactory.CreateDeserializerBlock(topic => topic switch
        {
            KafkaConfig.LogIn  => typeof(LoginMessage),
            KafkaConfig.LogOut => typeof(LogoutMessage),
            _                  => throw new InvalidOperationException($"Unable to find mapping for ${topic}")
        }, options => options.MaxDegreeOfParallelism = deserializeTheadCount);

    DataTable loginDt = new();
    loginDt.Columns.Add(nameof(LoginMessage.Email), typeof(string));
    loginDt.Columns.Add(nameof(LoginMessage.Time), typeof(DateTime));
    loginDt.Columns.Add(nameof(LoginMessage.SessionId), typeof(string));

    DataTable logoutDt = new();
    logoutDt.Columns.Add(nameof(LogoutMessage.Time), typeof(DateTime));
    logoutDt.Columns.Add(nameof(LogoutMessage.SessionId), typeof(string));

    ActionBlock<IKafkaEntity?> saveToMsSqlLoginMessageBlock =
        dataFlowFactory.CreateSqlServerConsumerBlock<LoginMessage>(loginDt, batchSize,
            () => MsSqlConsumer.InsertLoginAsync(msConnectionString, loginDt),
            message => new object?[] { message.Email, message.Time, message.SessionId },
            kafkaConsumerBuilder);

    ActionBlock<IKafkaEntity?> saveToMsSqlLogoutMessageBlock =
        dataFlowFactory.CreateSqlServerConsumerBlock<LogoutMessage>(logoutDt, batchSize,
            () => MsSqlConsumer.InsertLogoutAsync(msConnectionString, logoutDt),
            message => new object?[] { message.Time, message.SessionId },
            kafkaConsumerBuilder);

    DataflowLinkOptions dataflowLinkOptions = new()
    {
        PropagateCompletion = true
    };

    kafkaReaderBlock.LinkTo(deserializeMessageBlock, dataflowLinkOptions);

    BroadcastBlock<IKafkaEntity?> broadcastBlock = dataFlowFactory.CreateBroadcastBlock();
    deserializeMessageBlock.LinkTo(broadcastBlock, dataflowLinkOptions);

    broadcastBlock.LinkTo(saveToMsSqlLoginMessageBlock, dataflowLinkOptions, message => message is LoginMessage);
    broadcastBlock.LinkTo(saveToMsSqlLogoutMessageBlock, dataflowLinkOptions, message => message is LogoutMessage);

    Task kafkaReaderTask = KafkaReader.ReadMessagesAsync(
        kafkaConsumerBuilder,
        cancellationTokenSource.Token,
        kafkaReaderBlock,
        KafkaConfig.LogIn,
        KafkaConfig.LogOut);

    await Task.WhenAll(
        kafkaReaderTask,
        saveToMsSqlLoginMessageBlock.Completion,
        saveToMsSqlLogoutMessageBlock.Completion);
}
catch (Exception exception)
{
    logger.LogCritical("{exception}", exception.Message);
}