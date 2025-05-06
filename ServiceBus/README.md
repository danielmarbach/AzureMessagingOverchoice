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

Type: Message, Event distribution (within the enterprise). Single message is the information carrier
When to use: Order processing and financial transaction

Error handling: Individual messages can be individually processed several times, put on hold, or marked as processed

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

## Walk through

### Processor

- Show the processor overview and explain queue, topics, subscriptions and routing / filtering. Also mention the high level description of the service
- Show the csproj for the package references
- Switch to SetupInfrastructure and how the moving pieces (explaining ServiceBus has a built-in admin client requiring elevated permissions / manage rights)
  - Explain dedup detection
  - Default rule (1=1)
  - The filter creation
  - In program.cs show the registration
- Switch over to the sender code and explain built-in batching capability (also mention 256 KB and large message support)
  - Explain that we are using Cloud Event structured encoding
  - CloudEvent extension to explain how things are mapped to the application properties
  - In program.cs show the registration
- Switch over to the processor code. Show the transactional client keyed service.
  - Explain on a high level the settings in the processor (also mention ACK per message)
  - Process messages (explain visibility, cancellation of handler code)
  - HandleSendSwissChocolateTo explain transaction and auto-enlistment of the sender
- Switch over to the destination queue processor code. Brief walk through

### RBAC Demo

1. Create a new Entra ID application `ServiceBusRBAC`
1. Assign the API permission `Microsoft.ServiceBus` and give it full access to the Azure Service Bus service
1. Create a Client secret `ServiceBusClientSecret` under Certificates & Secrets
1. Create a queue with name `rbacqueue`
1. Add a role assignment with `Azure Service Bus Data Sender` and add `ServiceBusRBAC` under members directly on the queue `rbacqueue` under Access control (IAM)

### Session Processor

Ordered delivery

- Show SetupInfrastructure
- Show the sender and the cloud event extension setting the session ID which is required for a session enabled queue
- Switch to input queue processor and mention the specific processor type
  - Hint at concurrency being across sessions but no concurrency within the session
  - Show processing logic and then the processing method
  - After that talk about session state being restricted to the size of a message

Hint that ASB session is a great way to opt-in certain places where strict ordering matters but for more real time data ingestion problems it is better to use Event Hubs due to it's streaming characteristics. 