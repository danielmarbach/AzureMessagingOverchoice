using System.Transactions;
using Azure.Messaging;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;

namespace Processor;

public class InputQueueProcessor(
    [FromKeyedServices("TransactionalClient")] ServiceBusClient serviceBusClient,
    IOptions<ServiceBusOptions> serviceBusOptions,
    ILogger<InputQueueProcessor> logger)
    : IHostedService, IAsyncDisposable
{
    private ServiceBusProcessor? queueProcessor;
    private ServiceBusSender? publisher;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        queueProcessor = serviceBusClient.CreateProcessor(serviceBusOptions.Value.InputQueue, new ServiceBusProcessorOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            MaxConcurrentCalls = 10,
            AutoCompleteMessages = true,
            PrefetchCount = 10,
            Identifier = $"Processor-{serviceBusOptions.Value.InputQueue}",
            MaxAutoLockRenewalDuration = TimeSpan.FromSeconds(10),
        });
        queueProcessor.ProcessMessageAsync += ProcessMessages;
        queueProcessor.ProcessErrorAsync += ProcessError;
        publisher = serviceBusClient.CreateSender(serviceBusOptions.Value.TopicName, new ServiceBusSenderOptions
        {
            Identifier = $"Publisher-{serviceBusOptions.Value.TopicName}"
        });

        await queueProcessor.StartProcessingAsync(cancellationToken);
    }

    #region Not relevant
    private Task ProcessError(ProcessErrorEventArgs arg)
    {
        if (arg.Exception is OperationCanceledException)
        {
            return Task.CompletedTask;
        }
        logger.LogError(arg.Exception, "Error processing message");
        return Task.CompletedTask;
    }
    #endregion

    private async Task ProcessMessages(ProcessMessageEventArgs arg)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(arg.CancellationToken);
        try
        {
            arg.MessageLockLostAsync += MessageLockLostHandler;

            var receivedCloudEvent = arg.Message.ToCloudEvent();
            var handlerTask = receivedCloudEvent.Type switch
            {
                "Processor.SendSwissChocolateTo" => HandleSendSwissChocolateTo(receivedCloudEvent, cts.Token),
                "Processor.SwissChocolateDelivered" => HandleSwissChocolateDelivered(receivedCloudEvent, cts.Token),
                _ => Task.CompletedTask
            };
            await handlerTask;
        }
        finally
        {
            arg.MessageLockLostAsync -= MessageLockLostHandler;
        }

        return;

        async Task MessageLockLostHandler(MessageLockLostEventArgs lockLostArgs)
        {
            logger.LogInformation(lockLostArgs.Exception, "Lost the lock while processing message. Cancelling the handler");
            try
            {
                await cts.CancelAsync();
            }
            catch (ObjectDisposedException)
            {
                // ignored
                logger.LogCritical(lockLostArgs.Exception, "Lock lost handler executed but cancellation token source was already disposed.");
            }
        }
    }

    async Task HandleSendSwissChocolateTo(CloudEvent receivedCloudEvent, CancellationToken cancellationToken)
    {
        // will make sure operations will enlist
        using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

        var sendSwissChocolateTo = receivedCloudEvent.Data!.ToObjectFromJson<SendSwissChocolateTo>()!;
        logger.SendSwissChocolateReceived(sendSwissChocolateTo.PersonId);

        try
        {
            var swissChocolateDelivered = new SwissChocolateDelivered
            {
                PersonId = sendSwissChocolateTo.PersonId
            };

            var swissChocolateDeliveredMessage = new CloudEvent(
                "https://swisschoco.delivery/factory/lucerne",
                typeof(SwissChocolateDelivered).FullName!,
                swissChocolateDelivered).ToServiceBusMessage();
            swissChocolateDeliveredMessage.CorrelationId = receivedCloudEvent.Id;

            // the order here doesn't matter because the message will only go out if the rest was successful
            await publisher!.SendMessageAsync(swissChocolateDeliveredMessage, cancellationToken);

            await Task.Delay(TimeSpan.FromSeconds(Random.Shared.Next(1, 15)), cancellationToken);

            scope.Complete();
        }
        catch (OperationCanceledException e)
        {
            logger.SendSwissChocolateLockLost(e, sendSwissChocolateTo.PersonId);
            throw;
        }
    }

    #region Not relevant
    Task HandleSwissChocolateDelivered(CloudEvent receivedCloudEvent, CancellationToken cancellationToken)
    {
        logger.SwissChocolateDeliveredWithSubject(receivedCloudEvent.Subject);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (queueProcessor is not null)
        {
            await queueProcessor.StopProcessingAsync(cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (queueProcessor is not null)
        {
            await queueProcessor.DisposeAsync();
        }

        if (publisher is not null)
        {
            await publisher.DisposeAsync();
        }
    }
    #endregion
}