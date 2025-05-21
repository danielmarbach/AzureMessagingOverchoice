using System.Collections.Concurrent;
using Azure.Messaging;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;

namespace Processor;

public class DestinationProcessor(
    [FromKeyedServices("Client")] ServiceBusClient serviceBusClient,
    IOptions<ServiceBusOptions> serviceBusOptions,
    ILogger<DestinationProcessor> logger)
    : IHostedService, IAsyncDisposable
{
    private ServiceBusProcessor? queueProcessor;
    ConcurrentDictionary<string, bool> receivedMessageIds = new();

    #region Not relevant
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        queueProcessor = serviceBusClient.CreateProcessor(serviceBusOptions.Value.DestinationQueue, new ServiceBusProcessorOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            MaxConcurrentCalls = 10,
            AutoCompleteMessages = true,
            PrefetchCount = 10,
            Identifier = $"Processor-{serviceBusOptions.Value.DestinationQueue}",
        });
        queueProcessor.ProcessMessageAsync += ProcessMessages;
        queueProcessor.ProcessErrorAsync += ProcessError;
        await queueProcessor.StartProcessingAsync(cancellationToken);
    }
    #endregion

    private async Task ProcessMessages(ProcessMessageEventArgs arg)
    {
        var receivedCloudEvent = arg.Message.ToCloudEvent();
        var handlerTask = receivedCloudEvent.Type switch
        {
            "Processor.SwissChocolateDelivered" => HandleSwissChocolateDelivered(receivedCloudEvent, arg.CancellationToken),
            _ => Task.CompletedTask
        };
        await handlerTask;
    }

    Task HandleSwissChocolateDelivered(CloudEvent message, CancellationToken cancellationToken)
    {
        var chocolateDelivered = message.Data!.ToObjectFromJson<SwissChocolateDelivered>()!;
        var alreadyReceived = receivedMessageIds.AddOrUpdate(chocolateDelivered.PersonId, static _ => false, static (_, _) => true);
        logger.SwissChocolateDelivered(alreadyReceived ? LogLevel.Warning : LogLevel.Information, chocolateDelivered.PersonId);
        return Task.CompletedTask;
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
    }
    #endregion
}