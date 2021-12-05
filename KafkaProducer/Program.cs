// See https://aka.ms/new-console-template for more information

using Coding4fun.Tpl.DataFlow.KafkaProducer;
using Coding4fun.Tpl.DataFlow.Shared;

var cancellationTokenSource = new CancellationTokenSource();

try
{
    _ = Task.Run(() =>
    {
        Thread.CurrentThread.Name = "Exit Thread";
        Console.WriteLine("Press any key to cancel");
        Console.ReadKey();
        cancellationTokenSource.Cancel();
    });
    
    await KafkaWriter.RemoveTopicsAsync(KafkaConfig.BrokerList, KafkaConfig.LogIn, KafkaConfig.LogOut);
    
    // Waiting for remove topics.
    await Task.Delay(5000);

    Task produceLoginsTask = Task.Run(async () =>
        await KafkaWriter.WriteLoginAsync(KafkaConfig.BrokerList, KafkaConfig.LogIn, cancellationTokenSource.Token));

    Task produceLogoutsTask = Task.Run(async () =>
        await KafkaWriter.WriteLogoutAsync(KafkaConfig.BrokerList, KafkaConfig.LogOut, cancellationTokenSource.Token));
    
    await Task.WhenAll(produceLoginsTask, produceLogoutsTask);
}
catch (Exception exception)
{
    Console.WriteLine(exception);
}