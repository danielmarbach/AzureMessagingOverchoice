# Service Bus

Purpose: High-value enterprise messaging that enables

- [Competing consumers](https://docs.microsoft.com/en-us/azure/architecture/patterns/competing-consumers%20)
- [Load leveling](https://learn.microsoft.com/en-us/azure/architecture/patterns/queue-based-load-leveling)
- [Claim check](https://docs.microsoft.com/en-us/azure/architecture/patterns/claim-check%20)
- [Circuit breaker](https://docs.microsoft.com/en-us/azure/architecture/patterns/circuit-breaker%20)
- [Sagas](https://weblogs.asp.net/sfeldman/sagas-with-azure-service-bus%20) with session state or paired with CosmosDB, table storage
- [Publish-subscribe](https://docs.microsoft.com/en-us/azure/architecture/patterns/publisher-subscriber)
- [Message filter](https://www.enterpriseintegrationpatterns.com/Filter.html%20)
- [Message poisoning](https://en.wikipedia.org/wiki/Poison_message%20)
- [Idempotent receiver](https://www.enterpriseintegrationpatterns.com/IdempotentReceiver.html%20)
- [Message Scheduling](https://docs.microsoft.com/en-us/azure/service-bus-messaging/message-sequencing#scheduled-messages)
- [Transactional Queues](https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-transactions)
- [Asynchronous request-reply](https://docs.microsoft.com/en-us/azure/architecture/patterns/async-request-reply)
- [Exactly once processing](https://www.cloudcomputingpatterns.org/exactly_once_delivery/)
- [Transactional outbox](https://docs.microsoft.com/en-us/azure/architecture/best-practices/transactional-outbox-cosmos%20) paired with CosmosDB, table storage...
- [Sequential convoy](https://docs.microsoft.com/en-us/azure/architecture/patterns/sequential-convoy%20)
Type: Message, Event distribution (within the enterprise)
When to use: Order processing and financial transaction

Service Bus is a fully managed enterprise message broker with message queues and publish-subscribe topics. The service is intended for enterprise applications that require transactions, ordering, duplicate detection, and instantaneous consistency. Service Bus enables cloud-native applications to provide reliable state transition management for business processes. When handling high-value messages that can't be lost or duplicated, use Azure Service Bus. This service also facilitates highly secure communication across hybrid cloud solutions and can connect existing on-premises systems to cloud solutions.

## Features

- Queues, Topics and Subscriptions
- Routing and filtering (Correlation- and SQL Filters, Auto-forwarding) plus filter actions
- Message headers (Application properties)
- At-least-once (Peek lock), At-most-once (ReceiveAndDelete) and exactly once (deduplication detection within a defined window) plus transactions
- Transactions
- AMQP and JMS
- Client SDKs in multiple languages
- Strict ordering support via sessions
- Poison message handling via deadlettering
- Role-based access control
- Time to live, batching, auto-delete on idle, partitioning, large-message support up to 100 MB (built-in claim check)
- Private endpoints
- Geo-DR and Availability zones
- Dedicated Resources with auto-scaling