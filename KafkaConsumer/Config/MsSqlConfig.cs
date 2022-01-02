using JetBrains.Annotations;

namespace KafkaConsumer.Config;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
public class MsSqlConfig
{
    public string? ConnectionString { get; set; }
    public int? BatchSize { get; set; }
    public TimeSpan? BatchTimeout { get; set; }
}