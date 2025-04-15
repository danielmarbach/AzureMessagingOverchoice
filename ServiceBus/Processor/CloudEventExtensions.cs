using Azure.Messaging;
using Azure.Messaging.ServiceBus;

namespace Processor;

public static class CloudEventExtensions
{
    public static ServiceBusMessage ToServiceBusMessage(this CloudEvent cloudEvent)
    {
        var serviceBusMessage = new ServiceBusMessage(BinaryData.FromObjectAsJson(cloudEvent))
        {
            ContentType = "application/cloudevents+json",
            MessageId = cloudEvent.Id
        };
        // Since we are using the structured cloud event format we need to populate some of the cloud event metadata
        // to the application properties to allow filtering within the infrastructure.
        serviceBusMessage.ApplicationProperties.Add("ce-type", cloudEvent.Type);
        if (cloudEvent.Subject is not null)
        {
            serviceBusMessage.ApplicationProperties.Add("ce-subject", cloudEvent.Subject);
        }
        return serviceBusMessage;
    }

    public static CloudEvent ToCloudEvent(this ServiceBusReceivedMessage message)
    {
        var receivedCloudEvent = CloudEvent.Parse(message.Body)!;
        // This is here for demonstration purposes only to show how SQL filter actions can override properties
        if(message.ApplicationProperties.TryGetValue("ce-subject", out var subject))
        {
            receivedCloudEvent.Subject = subject.ToString();
        }
        return receivedCloudEvent;
    }
}