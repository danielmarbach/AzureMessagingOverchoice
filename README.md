# Navigating through the Azure Messaging (over)choice

Message-driven systems are the backbone of reliable, high-throughput solutions. Azure offers a rich variety of messaging services—but with so many options, it’s easy to feel overwhelmed. The abundance of choice can lead to analysis paralysis, where sticking to a familiar solution (**cough** HTTP) feels safer than exploring the tradeoffs.

In this talk, I'll guide you through the essential messaging services in Azure and help you make sense of their unique strengths. Discover when to use Service Bus for reliable, publish/subscribe messaging, Event Hubs for real-time data streams, Event Grid for scalable event-driven workflows, and how to combine those services to unlock their full potential—without the hassle of polling.

Don’t let overchoice hold you back—this talk will equip you with robust coding patterns leveraging the .NET Azure SDKs and the confidence to make informed decisions to unlock the potential of the essential Azure messaging services for your specific needs.

## Reading material

### Event Hubs

- [Validate using an Avro schema when streaming events using Event Hubs .NET SDKs (AMQP)](https://learn.microsoft.com/en-us/azure/event-hubs/schema-registry-dotnet-send-receive-quickstart)
- [Schema Registry for Kafka Applications – Public Preview](https://techcommunity.microsoft.com/t5/messaging-on-azure-blog/json-schema-support-in-azure-event-hubs-schema-registry-for/ba-p/3825655)
- [Building A Custom Event Hubs Event Processor with .NET](https://devblogs.microsoft.com/azure-sdk/custom-event-processor/)
- [Samples](https://github.com/Azure/azure-sdk-for-net/tree/main/sdk/eventhub/Azure.Messaging.EventHubs/samples)
- [Event Processor Samples](https://github.com/Azure/azure-sdk-for-net/tree/main/sdk/eventhub/Azure.Messaging.EventHubs.Processor/samples)

### Event Grid

- [Debugging Azure Function Event Grid Triggers Locally with JetBrains Rider ](https://www.josephguadagno.net/2020/07/20/debugging-azure-function-event-grid-trigger-locally-with-jetbrains-rider)
- [Azure Service Bus vs Event Grid](https://yourazurecoach.com/2021/08/11/azure-service-bus-vs-event-grid/)
- [Azure Service Bus vs Event Grid Pull Delivery](https://yourazurecoach.com/2023/12/22/azure-service-bus-vs-event-grid-pull-delivery/)
- [Azure Service Bus vs Event Grid](https://yourazurecoach.com/2021/08/11/azure-service-bus-vs-event-grid/)
- [Azure Service Bus vs Event Grid Pull Delivery](https://yourazurecoach.com/2023/12/22/azure-service-bus-vs-event-grid-pull-delivery/)

### General

- [Voxxed Athens 2018 - Eventing, Serverless, and the Extensible Enterprise by Clemens Vasters](https://www.youtube.com/watch?v=qCNXUUlhJJE&list=WL)