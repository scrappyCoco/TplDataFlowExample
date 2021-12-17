namespace KafkaConsumer.Config;

public class KafkaConfig
{
    public string BrokerList { get; set; }
    public string GroupId { get; set; }
    public string LoginTopicName { get; set; }
    public string LogoutTopicName { get; set; }
}