using System.ComponentModel.DataAnnotations;
using JetBrains.Annotations;
#pragma warning disable CS8618

namespace KafkaConsumer.Config;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
public class MsSqlConfig
{
    [Required]
    public string ConnectionString { get; set; }
 
    public int? BatchSize { get; set; }
    
    public TimeSpan? BatchTimeout { get; set; }
}