namespace SessionProcessor;

public record StorageTemperatureChanged
{
    public DateTimeOffset Published { get; init; }
    public double Current { get; init; }
}