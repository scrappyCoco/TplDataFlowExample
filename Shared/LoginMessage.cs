using Confluent.Kafka;

namespace Coding4fun.Tpl.DataFlow.Shared;

public class LoginMessage: IKafkaEntity
{
    public string Email { get; set; }
    public DateTime Time { get; set; }
    public string SessionId { get; set; }
    public TopicPartitionOffset Offset { get; set; }
}