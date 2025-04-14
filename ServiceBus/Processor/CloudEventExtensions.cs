using Azure.Messaging.ServiceBus;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;

namespace Processor;

public static class CloudEventExtensions
{
    private static readonly JsonEventFormatter Formatter = new();

    public static ServiceBusMessage ToBinaryMode(this CloudEvent cloudEvent)
    {
        cloudEvent.Id ??= Guid.NewGuid().ToString();
        var serviceBusMessage = new ServiceBusMessage(Formatter.EncodeBinaryModeEventData(cloudEvent))
        {
            ContentType = cloudEvent.DataContentType,
            MessageId = cloudEvent.Id
        };
        foreach (var attribute in cloudEvent.GetPopulatedAttributes())
        {
            serviceBusMessage.ApplicationProperties[$"ce-{attribute.Key}"] = attribute.Value.ToString();
        }
        return serviceBusMessage;
    }

    public static CloudEvent ToCloudEvent(this ServiceBusReceivedMessage message)
    {
        var receivedCloudEvent = new CloudEvent
        {
            Source = new Uri((string)message.ApplicationProperties["ce-source"]),
            Type = (string)message.ApplicationProperties["ce-type"],
            Id = (string)message.ApplicationProperties["ce-id"],
            Time = message.ApplicationProperties.TryGetValue("ce-time", out var time) ? DateTimeOffset.Parse((string)time) : (DateTimeOffset?)null,
            Subject = message.ApplicationProperties.TryGetValue("ce-subject", out var subject) ? (string)subject : null,
            DataContentType = message.ContentType
        };
        Formatter.DecodeBinaryModeEventData(message.Body, receivedCloudEvent);
        return receivedCloudEvent;
    }
}