using Azure.Messaging;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;

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

    private static CloudEvent ToCloudEvent(this EventData message)
    {
        var receivedCloudEvent = CloudEvent.Parse(message.EventBody)!;
        return receivedCloudEvent;
    }

    public static IReadOnlyList<CloudEvent> ToCloudEvents(this IReadOnlyList<EventData> events)
    {
        return events.Select(e => e.ToCloudEvent()).ToList();
    }

    public static ValueTask<TData> DeserializeAsync<TData>(this SchemaRegistryAvroSerializer serializer, CloudEvent cloudEvent, CancellationToken cancellationToken = default)
    {
        return serializer.DeserializeAsync<TData>(new MessageContent
        {
            ContentType = cloudEvent.DataContentType!,
            Data = cloudEvent.Data
        }, cancellationToken);
    }
}