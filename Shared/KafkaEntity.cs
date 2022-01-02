using Confluent.Kafka;

namespace Coding4fun.Tpl.DataFlow.Shared;

public class KafkaEntity
{
    public KafkaEntity(ConsumeResult<Ignore, string> kafkaMessage) => KafkaMessage = kafkaMessage;

    public ConsumeResult<Ignore, string> KafkaMessage { get; }
    public object? Item { get; set; }
    public string? Error { get; set; }
    public byte[]? MessageHash { get; set; }
}