namespace Processor;

public record SenderOptions
{
    public bool ProduceData { get; set; }

    public int NumberOfDataPointsPerChocolateStorage { get; set; }

    public string[] Storage { get; set; } = [];
}