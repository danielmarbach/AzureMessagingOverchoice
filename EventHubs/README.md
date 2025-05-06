# Event Hubs

Purpose: Big data pipeline, realtime event-processing and data-distribution
Type: Event streaming (series). The series of event is the information carrier
When to use: Telemetry and distributed data streaming
Error handling: Read the log stream again from a certain offset or skip the offset. Tombstone certain records and move them to a dedicated stream.

Azure Event Hubs is a big data streaming platform and event ingestion service. It can receive and process millions of events per second. It facilitates the capture, retention, and replay of telemetry and event stream data. The data can come from many concurrent sources. Event Hubs allows telemetry and event data to be made available to various stream-processing infrastructures and analytics services. It's available either as data streams or bundled event batches. This service provides a single solution that enables rapid data retrieval for real-time processing, and repeated replay of stored raw data. It can capture the streaming data into a file for processing and analysis.

Azure Event Hubs is widely adopted in industries that produce high volumes of streaming data. The automotive sector, for example, frequently uses Event Hubs for connected vehicle telemetry. Other common applications include IoT telemetry from devices such as elevators, coffee machines (e.g., Thermoplan), and real-time data replication scenarios.

Event Hubs is also commonly used alongside tools like Kafka and Debezium to implement change data capture (CDC), support outbox patterns, and enable the decoupling of monolithic databases into modular, event-driven components. It is particularly suited for high-throughput, append-only, time-ordered event ingestion.

In contrast, Azure Service Bus is designed for enterprise messaging scenarios that require features like transactions, message sessions, dead-letter queues, and message deduplication. It supports guaranteed delivery, complex routing, and exactly-once processing—making it suitable for systems that depend on message integrity, coordination, and workflow orchestration.

While Event Hubs is closely associated with telemetry-heavy use cases, Service Bus is commonly used across industries wherever reliable, transactional messaging is essential.

## Features

Partitioned consumer model with offset-based checkpointing for parallel processing

- High-throughput streaming (up to millions of events per second per namespace)
- Capture integration to automatically persist data to Azure Blob Storage or Azure Data Lake
- Event timestamping, sequence number, and partition key support
- AMQP and HTTPS protocols, with Kafka-compatible endpoints (native support for Kafka clients)
  - 100% Kafka compatible up to 3.5 with Kafka Streams and Transactions in Preview
  - Kafka Compression GA
  - Schema Registry JSON Schema Formats GA, Protobuf in Preview
- Client SDKs in multiple languages (C#, Java, Python, JavaScript, Go, etc.)
- Built-in support for streaming platforms like Apache Spark, Azure Stream Analytics, and Azure Functions
- Time-retention configuration (from minutes up to 7 days; up to 90 days on Dedicated tier)
- Consumer groups for parallel, independent processing pipelines
- Throughput units (Standard tier) and dedicated capacity (Dedicated tier) for performance scaling
- Geo-disaster recovery with paired namespaces (metadata replication only)
- Private endpoints and Virtual Network integration
- Role-based access control (RBAC) and Azure AD integration
- Automatic load balancing for partitions among active consumers
- Large event support (up to 1 MB per event)
- Availability zones support in supported regions
- Capture support for real-time archiving and replay scenarios
- Standard namespaces support auto-inflation of throughput units while the premium namespaces processing units have to be manually scaled at the moment.

## Event Hub is not

- A publish/subscribe broker. Partitions are not subscriptions. They are chosen by the producer or the broker on ingress. There is also no server-side filtering. _Azure Service Bus, Azure Event Grid_
- A queue broker. Read progress over the log is handled by the client and there is no event-level ownership and delivery state handling. _Azure Service Bus or for basic use cases Azure Event Grid Namespaces_
- A discrete event distribution engine. Event Hubs does not do push deliveries, and delivery failures need to be tracked individually. _Azure Event Grid_
-  A database or long-term event store. Event Hubs exists to catch, store and provide fast access to event data organized around time axis. As data ages (days, not months), you need better indexing. _Azure Cosmos DB, Azure SQL, Azure Table, Azure Synapse..._

Event streaming is not "modern" and queues are not "traditional". Both are patterns of state-of-the-art messaging infrastructures.

## Walk through

- Show the overview images and explain how the data gets assigned to append only streams oldest data on the left and newest data on the right. Explain consumer groups (also possible to demo unique assigments by trying to use the portal to load data while the application is connected)
- Start with the Program.cs and explain the various clients and that the storage client is required for the checkpoint storage
- Explain the Avro Schema Serializer and that currently with Event Hubs there is a built-in schema registry with some simple schema compatibility options. Explain that this space is likely to evolve (see xRegistry)
- Go into the sender and very quickly show how the batching code is very similar to Azure Service Bus. This time we are setting explicitely the partition key to the storage since we want ordering. Mention briefly that parition keys should land themselves naturally across all the available partitions to avoid hot partitions. If no partition key is assigned they will get round robin assigned to partitions
- Switch to the processor code and explain the batch processor does the necessary checkpointing. Also partitions are abstracted away automatically. Explain that concurrent dictionary was only used here because of the convenient AddOrUpdate but technically it is not required.

### RBAC

Attention: The permissions here are generous for demo purposes only

1. Create a new Entra ID application `EventHubsRBAC`
1. Assign the API permission `Microsoft.EventHubs` and `Azure Storage`
1. Create a Client secret `EventHubsClientSecret` under Certificates & Secrets
1. Under the event hubs namespace under Access control (IAM)
  1. Add a role assignment with `Azure Event Hubs Data Owner` and add `EventHubsRBAC` under members 
  1. Add a role assignment with `Schema Registry Contributor (Preview)` and add `EventHubsRBAC` under members
1. Under the storage account under Access control (IAM)
  1. Add a role assignment with `Storage Blob Data Contributor` and add `EventHubsRBAC` under members
1. Add the event schema to the registry with `ProcessorSchemaDemo.TemperatureChanged`
1. Configure launchSettings.json accordingly

#### Schema registry

1. Change the event schema to a new version
1. Start the application
1. Change it to something incompatible and delete the compatible version
1. Start again

#### Application Groups

1. Add an application group and allow one message per second incoming
1. Change application group to allow more messages per second  (or even better remove things again because caching can mess up things)

#### Kafka Consumer

1. Recreate the topicdemo event hubs if you played around with schemas otherwise you run into schema not found problems
1. Delete the blob storage data and start fresh