namespace KafkaConsumer.Config;

public class RootConfig
{
    public KafkaConfig Kafka { get; set; }
    public MsSqlConfig MsSql { get; set; }
    public int DeserializeThreadsCount { get; set; }
}