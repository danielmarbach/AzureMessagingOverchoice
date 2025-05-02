using Azure.Messaging;
using Azure.Messaging.EventHubs;

namespace Processor;

public static class CloudEventExtensions
{
    public static EventData ToEventData(this CloudEvent cloudEvent)
    {
        var eventData = new EventData(BinaryData.FromObjectAsJson(cloudEvent))
        {
            ContentType = "application/cloudevents+json",
            MessageId = cloudEvent.Id,
        };
        if (cloudEvent.ExtensionAttributes.TryGetValue("storage", out var storage))
        {
            eventData.Properties["storage"] = storage.ToString();
        }
        return eventData;
    }

    public static CloudEvent ToCloudEvent(this EventData message)
    {
        var receivedCloudEvent = CloudEvent.Parse(message.EventBody)!;
        return receivedCloudEvent;
    }
}