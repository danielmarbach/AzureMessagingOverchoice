# Event Grid

Azure Event Grid is a highly scalable, fully managed Pub Sub message distribution service that offers flexible message consumption patterns using the Message Queueing Telemetry Transport (MQTT) and HTTP protocols. With Azure Event Grid, you can build data pipelines with device data, integrate applications, and build event-driven serverless architectures.

The service provides an eventing backbone that enables event-driven and reactive programming. It uses the publish-subscribe model. Publishers emit events, but have no expectation about how the events are handled. Subscribers decide on which events they want to handle.

Event Grid is deeply integrated with other Azure services and can be integrated with third-party services. It simplifies event consumption and lowers costs by eliminating the need for constant polling. Event Grid efficiently and reliably routes events from Azure and non-Azure resources. It distributes the events to registered subscriber endpoints. The event message has the information you need to react to changes in services and applications.

## Features

- Elastic Pub/Sub message broker (currently no auto-scaling though)
- Push style distribution of descrete events to serverless (Push / Push)
- Pub Sub message distribution services with flexible consumption patterns
- MQTT (for IoT solutions) and HTTP Protocols
- Supports push and pull delivery
- Supports the CloudEvents 1.0 specification
- Push delivery supports azure services, custom application and external partner systems as destinations
- Pull delivery for Queues
- Light broker for the 80% queueing functionality. Will not have JMS, transactionality etc.
- Will soon support AMQP 1.0 for queues

### Basic Tier vs Namespace (Standard Tier)

| Feature | **Standard Tier (Namespace)** | **Basic Tier** |
|--------|-------------------------------|----------------|
| **Throughput** | High – up to 40 MB/s ingress and 80 MB/s egress (HTTP); MQTT up to 40 MB/s | Low – up to 5 MB/s ingress and egress |
| **Event Retention** | Up to 7 days (namespace topics) | 1 day |
| **Protocols Supported** | HTTP (CloudEvents), MQTT v3.1.1 & v5.0, (AMQP soon) | HTTP only |
| **MQTT Support** | ✅ Yes | ❌ No |
| **Pull Delivery (HTTP)** | ✅ Yes | ❌ No |
| **Push Delivery (HTTP)** | ✅ Yes | ✅ Yes |
| **Push to Event Hubs** | ✅ Yes | ✅ Yes |
| **Push to Azure Services** (Functions, Service Bus, Relay, Storage Queues) | ❌ Not yet | ✅ Yes |
| **Dead Lettering** | ✅ Yes (requires storage account) | ✅ Yes (requires storage account) |
| **CloudEvents Format Support** | ✅ Yes | ✅ Yes |
| **Custom Event Topics** | ✅ Yes | ✅ Yes |
| **Azure System Topics** | ❌ No | ✅ Yes |
| **Partner Topics** | ❌ No | ✅ Yes |
| **Domain Scope Subscriptions** | ❌ No | ✅ Yes |
| **Private Link Support** | ✅ Yes | ❌ No |
| **Advanced Filtering** | ✅ Yes | Limited |
| **Use Case Fit** | High-throughput, IoT, flexible consumption models, isolated tenants | Lightweight eventing, Azure-integrated services, lower throughput needs |

### Namespaces

Namespaces provide advanced capabilities and fine-grained control over event ingestion and delivery, especially suitable for high-throughput and complex eventing scenarios.

Namespaces are ideal when you need:

- Fine-grained delivery and retry control
- -Event isolation across tenants or applications
- MQTT support for IoT workloads
- Pull delivery for queue-based processing

#### Pull delivery

- Enable HTTP applications to consume messages using pull delivery
- Flexible consumption
- High throughput
- Private link support
- Control of event states

#### Push delivery

- Event Sources
  - Azure Services
  - Partner services like SAP
  - Custom applications
- Subscriptions
  - Filtering
  - Delivery retries. By default, Event Grid expires all events that aren't delivered within 24 hours. You [can customize the retry policy](https://learn.microsoft.com/en-us/azure/event-grid/delivery-and-retry) when creating an event subscription. You provide the maximum number of delivery attempts (default is 30) and the event time-to-live (default is 1440 minutes).  [30=7 in the first hour + 23 with one-per-hour]
  - Batching. Event Grid defaults to sending each event individually to subscribers. The subscriber receives an array with a single event. You can configure Event Grid to batch events for delivery for improved HTTP performance in high-throughput scenarios.
  - Dead lettering
    - Requires a storage account + container
    - Dead-lettered events are stored as blobs
