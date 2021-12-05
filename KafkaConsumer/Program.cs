using System.Data;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using Coding4fun.Tpl.DataFlow.Shared;
using Confluent.Kafka;
using KafkaConsumer;
using Nest;

try
{
    const int maxMessageCount = 100;
    const string msConnectionString =
        "server=localhost;user id=sa;password=yourStrong(!)Password;Trusted_Connection=True";
    
    const string kafkaBrokerList = KafkaConfig.BrokerList;
    const string kafkaLogIn = KafkaConfig.LogIn;
    const string kafkaLogOut = KafkaConfig.LogOut;

    const string esHost = "http://127.0.01:9200";
    const string esIndex = "tpl";
    
    Thread.CurrentThread.Name = "Main Thread";
    CancellationTokenSource cancellationTokenSource = new();

    _ = Task.Run(() =>
    {
        Thread.CurrentThread.Name = "Exit Thread";
        Console.WriteLine("Press any key to cancel");
        Console.ReadKey();
        cancellationTokenSource.Cancel();
    });

    DataflowBlockOptions standardDataflowBlockOptions = new()
    {
        BoundedCapacity = 10_000,
        CancellationToken = cancellationTokenSource.Token
    };

    ExecutionDataflowBlockOptions CreateExecutionOptions(Action<ExecutionDataflowBlockOptions>? customizer = null)
    {
        ExecutionDataflowBlockOptions options = new ()
        {
            BoundedCapacity = 10_000,
            CancellationToken = cancellationTokenSource.Token,
            MaxDegreeOfParallelism = 1
        };
        customizer?.Invoke(options);
        return options;
    }

    BufferBlock<ConsumeResult<Ignore, string>> kafkaReaderBlock = new(standardDataflowBlockOptions);

    TransformBlock<ConsumeResult<Ignore, string>, object?> deserializeMessageBlock = new(message =>
    {
        if (message.Topic == kafkaLogIn) return JsonSerializer.Deserialize<LoginMessage>(message.Message.Value);
        if (message.Topic == kafkaLogOut) return JsonSerializer.Deserialize<LogoutMessage>(message.Message.Value);
        return null;
    }, CreateExecutionOptions(o => o.MaxDegreeOfParallelism = 2));

    DataTable loginDt = new();
    loginDt.Columns.Add(nameof(LoginMessage.Email), typeof(string));
    loginDt.Columns.Add(nameof(LoginMessage.Time), typeof(DateTime));
    loginDt.Columns.Add(nameof(LoginMessage.SessionId), typeof(string));

    DataTable logoutDt = new();
    logoutDt.Columns.Add(nameof(LogoutMessage.Time), typeof(DateTime));
    logoutDt.Columns.Add(nameof(LogoutMessage.SessionId), typeof(string));

    ActionBlock<object?> saveToMsSqlLoginMessageBlock = new(
        async message =>
        {
            var loginMessage = (LoginMessage)message!;
            loginDt.Rows.Add(loginMessage.Email, loginMessage.Time, loginMessage.SessionId);
            if (loginDt.Rows.Count > maxMessageCount) await MsSqlConsumer.InsertLoginAsync(msConnectionString, loginDt);
        }, CreateExecutionOptions()
    );

    ActionBlock<object?> saveToMsSqlLogoutMessageBlock = new(
        async message =>
        {
            var logoutMessage = (LogoutMessage)message!;
            logoutDt.Rows.Add(logoutMessage.Time, logoutMessage.SessionId);
            if (logoutDt.Rows.Count > maxMessageCount) await MsSqlConsumer.InsertLogoutAsync(msConnectionString, logoutDt);
        }, CreateExecutionOptions()
    );

    var seSettings = new ConnectionSettings(new Uri(esHost)).DefaultIndex(esIndex);
    var esClient = new ElasticClient(seSettings);
    
    ActionBlock<object?> saveToElasticSearchBlock = new(
        async message =>
        {
            if (message is LoginMessage loginMessage) await esClient.IndexAsync(new IndexRequest<LoginMessage>(loginMessage, "login"));
            else if (message is LogoutMessage logoutMessage) await esClient.IndexAsync(new IndexRequest<LogoutMessage>(logoutMessage, "logout"));
        });

    DataflowLinkOptions dataflowLinkOptions = new()
    {
        PropagateCompletion = true
    };

    kafkaReaderBlock.LinkTo(deserializeMessageBlock, dataflowLinkOptions);
    
    BroadcastBlock<object?> broadcastBlock = new (it => it, CreateExecutionOptions());
    deserializeMessageBlock.LinkTo(broadcastBlock, dataflowLinkOptions);
    
    broadcastBlock.LinkTo(saveToMsSqlLoginMessageBlock, dataflowLinkOptions, message => message is LoginMessage);
    broadcastBlock.LinkTo(saveToMsSqlLogoutMessageBlock, dataflowLinkOptions, message => message is LogoutMessage);
    broadcastBlock.LinkTo(saveToElasticSearchBlock, dataflowLinkOptions);

    Task kafkaReaderTask = KafkaReader.ReadMessagesAsync(
        kafkaBrokerList,
        cancellationTokenSource.Token,
        kafkaReaderBlock,
        kafkaLogIn,
        kafkaLogOut);

    await Task.WhenAll(
        kafkaReaderTask,
        saveToMsSqlLoginMessageBlock.Completion,
        saveToMsSqlLogoutMessageBlock.Completion,
        saveToElasticSearchBlock.Completion);
}
catch (Exception exception)
{
    Console.Write(exception.ToString());
}