namespace Processor;

static partial class LoggingExtensions
{
    [LoggerMessage(
        EventId = 0,
        Level = LogLevel.Information,
        Message = "Sending Swiss Chocolate to #{NumberOfCommands} people with #{NumberOfDuplicates} duplicates")]
    public static partial void SendWithDuplicates(
        this ILogger logger, int numberOfCommands, int numberOfDuplicates);

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Information,
        Message = "SendSwissChocolate command for ID {PersonId} received.")]
    public static partial void SendSwissChocolateReceived(
        this ILogger logger, string personId);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Error,
        Message = "Lost the lock while processing SendSwissChocolate command with ID {PersonId}.")]
    public static partial void SendSwissChocolateLockLost(
        this ILogger logger, Exception exception, string personId);

    [LoggerMessage(
        EventId = 3,
        Message = "#{NumberOfDeliveries} have been delivered.")]
    public static partial void SwissChocolateDelivered(
        this ILogger logger, LogLevel logLevel, long numberOfDeliveries);

    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Information,
        Message = "SwissChocolateDelivered received with label {Subject}.")]
    public static partial void SwissChocolateDeliveredWithSubject(
        this ILogger logger, string subject);
}