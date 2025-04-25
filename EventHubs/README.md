# Event Hubs

Purpose: Big data pipeline
Type: Event streaming (series)
When to use: Telemetry and distributed data streaming

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
