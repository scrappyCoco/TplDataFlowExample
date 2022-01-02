using System.Text.Json;
using Coding4fun.Tpl.DataFlow.Shared;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Coding4fun.Tpl.DataFlow.KafkaProducer;

public static class KafkaWriter
{
    public static async Task RemoveTopicsAsync(string brokerList, params string[] topicNames)
    {
        try
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = brokerList
            }).Build();

            try
            {
                await adminClient.DeleteTopicsAsync(topicNames, new DeleteTopicsOptions());
            }
            catch
            {
                // ignored
            }

            await adminClient.CreateTopicsAsync(topicNames.Select(topicName => new TopicSpecification
            {
                Name = topicName
            }));
        }
        catch (Exception exception)
        {
            Console.WriteLine(exception);
        }
    }

    public static async Task WriteLoginAsync(string brokerList, string loginTopic, CancellationToken cancellationToken)
    {
        await WriteAsync(brokerList, loginTopic, cancellationToken, GetLoginMessages(cancellationToken));
    }

    public static async Task WriteLogoutAsync(string brokerList, string logoutTopic,
        CancellationToken cancellationToken)
    {
        await WriteAsync(brokerList, logoutTopic, cancellationToken, GetLogoutMessages(cancellationToken));
    }

    private static async Task WriteAsync<T>(string brokerList, string topicName, CancellationToken cancellationToken,
        IEnumerable<T> enumerable)
    {
        Thread.CurrentThread.Name = $"Producer {topicName} Thread";

        var config = new ProducerConfig
        {
            BootstrapServers = brokerList,
        };
        using var producer = new ProducerBuilder<Null, string>(config).Build();

        try
        {
            int messageNumber = 0;
            foreach (var message in enumerable)
            {
                ++messageNumber;
                await producer.ProduceAsync(topicName, new Message<Null, string>
                {
                    Value = JsonSerializer.Serialize(message)
                }, cancellationToken);

                if (messageNumber % 100 == 0)
                {
                    await Task.Delay(300, cancellationToken);
                    Console.Write($"\r{topicName}: {messageNumber}      ");
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (ProduceException<Null, string> exception)
        {
            Console.WriteLine($"Delivery failed: {exception.Error.Reason}");
        }
        catch (Exception exception)
        {
            Console.WriteLine($"Unexpected exception: {exception}");
        }
    }

    private static IEnumerable<LoginMessage> GetLoginMessages(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            yield return new LoginMessage
            {
                Email = "example@mail.com",
                Time = DateTime.Now,
                SessionId = Guid.NewGuid().ToString()
            };
        }
    }

    private static IEnumerable<LogoutMessage> GetLogoutMessages(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            yield return new LogoutMessage
            {
                Time = DateTime.Now,
                SessionId = Guid.NewGuid().ToString()
            };
        }
    }
}