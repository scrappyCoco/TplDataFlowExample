using Confluent.Kafka;

namespace Coding4fun.Tpl.DataFlow.Shared;

public class LogoutMessage: IKafkaEntity
{
    public string SessionId { get; set; }
    public DateTime Time { get; set; }
    public TopicPartitionOffset Offset { get; set; }
}