using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;

namespace KafkaConsumer;

public static class KafkaReader
{
    public static async Task ReadMessagesAsync(ConsumerBuilder<Ignore, string> consumerBuilder, CancellationToken cancellationToken,
        ITargetBlock<ConsumeResult<Ignore, string>> targetBlock, params string[] topics) => await Task.Run(async () =>
        {
            Thread.CurrentThread.Name = "Kafka Reader Thread";

            try
            {
                using var consumer = consumerBuilder.Build();
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

    public static void CommitOffsets(ConsumerBuilder<Ignore, string> consumerBuilder, IEnumerable<TopicPartitionOffset> offsets)
    {
        using var consumer = consumerBuilder.Build();
        consumer.Commit(offsets);
    }
}