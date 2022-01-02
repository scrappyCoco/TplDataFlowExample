using System.ComponentModel.DataAnnotations;
#pragma warning disable CS8618

namespace KafkaConsumer.Config;

public class RootConfig
{
    [Required]
    public KafkaConfig Kafka { get; set; }
    
    [Required]
    public MsSqlConfig MsSql { get; set; }
    
    public int DeserializeThreadsCount { get; set; } = 1;
    
    public ValidationResult[] Validate()
    {
        List<ValidationResult> validationResults = new();
        IEnumerable<object> validatedObjects = new object[] { this, Kafka, MsSql };
        foreach (object validatedObject in validatedObjects)
        {
            Validator.TryValidateObject(validatedObject, new ValidationContext(validatedObject), validationResults, true);
            if (validationResults.Any()) return validationResults.ToArray();
        }
        return validationResults.ToArray();
    }
}