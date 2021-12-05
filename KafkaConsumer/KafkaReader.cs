using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;

namespace KafkaConsumer;

public static class KafkaReader
{
    public static async Task ReadMessagesAsync(string brokerList, CancellationToken cancellationToken,
        ITargetBlock<ConsumeResult<Ignore, string>> targetBlock, params string[] topics) => await Task.Run(async () =>
        {
            Thread.CurrentThread.Name = "Kafka Reader Thread";

            var config = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = "TestGroup",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };

            try
            {
                using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
                consumer.Subscribe(topics);
                while (!cancellationToken.IsCancellationRequested)
                {
                    ConsumeResult<Ignore, string> consumeResult = consumer.Consume(cancellationToken);
                    await targetBlock.SendAsync(consumeResult, cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            targetBlock.Complete();
        }, cancellationToken);
}