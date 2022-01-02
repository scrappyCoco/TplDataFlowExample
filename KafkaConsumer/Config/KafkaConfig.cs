using System.ComponentModel.DataAnnotations;
using JetBrains.Annotations;
#pragma warning disable CS8618

namespace KafkaConsumer.Config;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
public class KafkaConfig
{
    [Required]
    public string BrokerList { get; set; }
    
    [Required]
    public string GroupId { get; set; }
    
    [Required]
    public string LoginTopicName { get; set; }
    
    [Required]
    public string LogoutTopicName { get; set; }
}