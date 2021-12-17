using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace KafkaConsumer;

public class KafkaReader
{
    private readonly IConsumer<Ignore, string> _kafkaConsumer;
    private readonly CancellationToken _cancellationToken;
    private readonly ILogger<KafkaReader> _logger;

    public KafkaReader(
        ConsumerBuilder<Ignore, string> consumerBuilder,
        CancellationTokenSource cancellationTokenSource,
        ILogger<KafkaReader> logger)
    {
        _kafkaConsumer = consumerBuilder.Build();
        _cancellationToken = cancellationTokenSource.Token;
        _logger = logger;
    }
    
    public async Task ReadMessagesAsync(ITargetBlock<ConsumeResult<Ignore, string>> targetBlock, params string[] topics) => await Task.Run(async () =>
        {
            Thread.CurrentThread.Name = "Kafka Reader Thread";

            try
            {
                _kafkaConsumer.Subscribe(topics);
                while (!_cancellationToken.IsCancellationRequested)
                {
                    ConsumeResult<Ignore, string> consumeResult = _kafkaConsumer.Consume(_cancellationToken);
                    await targetBlock.SendAsync(consumeResult, _cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception exception)
            {
                _logger.LogError("{exception}", exception);
            }
            finally
            {
                _kafkaConsumer.Dispose();
            }

            targetBlock.Complete();
        }, _cancellationToken);

    public void CommitOffsets(IEnumerable<TopicPartitionOffset> offsets)
    {
        _kafkaConsumer.Commit(offsets);
    }
}