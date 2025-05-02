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
    private long chocolateDeliveredCounter = 0;

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
        var chocolateDelivered = Interlocked.Increment(ref chocolateDeliveredCounter);
        logger.SwissChocolateDelivered(chocolateDelivered <= serviceBusOptions.Value.NumberOfCommands ? LogLevel.Information : LogLevel.Warning, chocolateDelivered);
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