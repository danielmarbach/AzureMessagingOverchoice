using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Options;

namespace Processor;

public class SetupInfrastructure(
    ServiceBusAdministrationClient administrationClient,
    IOptions<ServiceBusOptions> serviceBusOptions)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        #region Not relevant
        if (await administrationClient.QueueExistsAsync(serviceBusOptions.Value.InputQueue, cancellationToken))
        {
            await administrationClient.DeleteQueueAsync(serviceBusOptions.Value.InputQueue, cancellationToken);
        }
        #endregion

        await administrationClient.CreateQueueAsync(new CreateQueueOptions(serviceBusOptions.Value.InputQueue)
        {
            LockDuration = TimeSpan.FromSeconds(5),
            RequiresDuplicateDetection = true,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(1)
        }, cancellationToken);

        #region Not relevant
        if (await administrationClient.QueueExistsAsync(serviceBusOptions.Value.DestinationQueue, cancellationToken))
        {
            await administrationClient.DeleteQueueAsync(serviceBusOptions.Value.DestinationQueue, cancellationToken);
        }

        await administrationClient.CreateQueueAsync(new CreateQueueOptions(serviceBusOptions.Value.DestinationQueue), cancellationToken);

        if (await administrationClient.TopicExistsAsync(serviceBusOptions.Value.TopicName, cancellationToken))
        {
            await administrationClient.DeleteQueueAsync(serviceBusOptions.Value.TopicName, cancellationToken);
        }
        #endregion

        await administrationClient.CreateTopicAsync(new CreateTopicOptions(serviceBusOptions.Value.TopicName),
            cancellationToken);

        #region Not relevant
        var destinationSubscriptionName = $"{serviceBusOptions.Value.DestinationQueue.Replace("/", "-")}-subscription";
        if (await administrationClient.SubscriptionExistsAsync(serviceBusOptions.Value.TopicName,
                destinationSubscriptionName, cancellationToken))
        {
            await administrationClient.DeleteSubscriptionAsync(serviceBusOptions.Value.TopicName, destinationSubscriptionName, cancellationToken);
        }
        #endregion

        await administrationClient.CreateSubscriptionAsync(
            new CreateSubscriptionOptions(serviceBusOptions.Value.TopicName, destinationSubscriptionName)
            {
                ForwardTo = serviceBusOptions.Value.DestinationQueue
            }, cancellationToken);

        await administrationClient.DeleteRuleAsync(serviceBusOptions.Value.TopicName, destinationSubscriptionName,
            "$Default", cancellationToken);

        await administrationClient.CreateRuleAsync(serviceBusOptions.Value.TopicName, destinationSubscriptionName,
            new CreateRuleOptions
            {
                Name = "SwissChocolateDelivered",
                Filter = new CorrelationRuleFilter
                {
                    ApplicationProperties =
                    {
                        { "ce-type", typeof(SwissChocolateDelivered).FullName }
                    }
                }
            }, cancellationToken);

        #region Not relevant
        var inputQueueSubscriptionName = $"{serviceBusOptions.Value.InputQueue.Replace("/", "")}-subscription";
        if (await administrationClient.SubscriptionExistsAsync(serviceBusOptions.Value.TopicName,
                inputQueueSubscriptionName, cancellationToken))
        {
            await administrationClient.DeleteSubscriptionAsync(serviceBusOptions.Value.TopicName, inputQueueSubscriptionName, cancellationToken);
        }

        await administrationClient.CreateSubscriptionAsync(
            new CreateSubscriptionOptions(serviceBusOptions.Value.TopicName, inputQueueSubscriptionName)
            {
                ForwardTo = serviceBusOptions.Value.InputQueue
            }, cancellationToken);

        await administrationClient.DeleteRuleAsync(serviceBusOptions.Value.TopicName, inputQueueSubscriptionName,
            "$Default", cancellationToken);
        #endregion
        await administrationClient.CreateRuleAsync(serviceBusOptions.Value.TopicName, inputQueueSubscriptionName,
            new CreateRuleOptions
            {
                Name = "AllEventsPublishedUnderNamespace",
                Action = new SqlRuleAction("SET [ce-subject] = [ce-type]"),
                Filter = new SqlRuleFilter("user.[ce-type] LIKE 'Processor.%'")
            }, cancellationToken);
    }

    #region Not relevant
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
    #endregion
}