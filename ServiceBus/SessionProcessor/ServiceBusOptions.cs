namespace SessionProcessor;

public class ServiceBusOptions
{
    public required string InputQueue { get; set; }
    public required string DestinationQueue { get; set; }
    public required string TopicName { get; set; }
    public int NumberOfDataPointsPerChocolateStorage { get; set; }
    public string[] ChocolateStorage { get; set; } = [];
    public double TemperatureThreshold { get; set; }
    public int NumberOfDataPointsToObserve { get; set; }
}