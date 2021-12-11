using Confluent.Kafka;

namespace Coding4fun.Tpl.DataFlow.Shared;

public interface IKafkaEntity
{
    TopicPartitionOffset Offset { get; set; }
}