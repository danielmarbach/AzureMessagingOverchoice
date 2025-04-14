using System.Text.Json;
using System.Transactions;
using Azure.Messaging.ServiceBus;
using CloudNative.CloudEvents;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Options;

namespace Processor;

public class InputQueueProcessor(
    IAzureClientFactory<ServiceBusClient> clientFactory,
    IOptions<ServiceBusOptions> serviceBusOptions,
    ILogger<InputQueueProcessor> logger)
    : IHostedService, IAsyncDisposable
{
    private readonly ServiceBusClient serviceBusClient = clientFactory.CreateClient("TransactionalClient");
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

    private Task ProcessError(ProcessErrorEventArgs arg)
    {
        if (arg.Exception is OperationCanceledException)
        {
            return Task.CompletedTask;
        }
        logger.LogError(arg.Exception, "Error processing message");
        return Task.CompletedTask;
    }

    private async Task ProcessMessages(ProcessMessageEventArgs arg)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(arg.CancellationToken);
        try
        {
            arg.MessageLockLostAsync += MessageLockLostHandler;
            var receivedCloudEvent = arg.Message.ToCloudEvent();
            var handlerTask = receivedCloudEvent.Type switch
            {
                "Processor.ActivateSensor" => HandleActivateSensor(receivedCloudEvent, cts.Token),
                "Processor.SensorActivated" => HandleSensorActivated(receivedCloudEvent, cts.Token),
                _ => Task.CompletedTask
            };
            await handlerTask;
        }
        finally
        {
            arg.MessageLockLostAsync -= MessageLockLostHandler;
        }

        Task MessageLockLostHandler(MessageLockLostEventArgs lockLostArgs)
        {
            logger.LogInformation(lockLostArgs.Exception, "Lost the lock while processing message. Cancelling the handler");
            try
            {
                cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // ignored
                logger.LogCritical(lockLostArgs.Exception, "Lock lost handler executed but cancellation token source was already disposed.");
            }
            return Task.CompletedTask;
        }
    }

    async Task HandleActivateSensor(CloudEvent receivedCloudEvent, CancellationToken cancellationToken)
    {
        // will make sure operations will enlist
        using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

        var activateSensor = ((JsonElement)receivedCloudEvent.Data!).Deserialize<ActivateSensor>()!;
        logger.ActivateSensorReceived(activateSensor.ChannelId);

        try
        {
            var sensorActivated = new SensorActivated
            {
                ChannelId = activateSensor.ChannelId
            };

            var cloudEvent = new CloudEvent
            {
                Type = typeof(SensorActivated).FullName!,
                Source = new Uri("/cloudevents/example/sender", UriKind.Relative),
                DataContentType = "application/json",
                Data = sensorActivated,
            };
            var sensorActivatedMessage = cloudEvent.ToBinaryMode();
            sensorActivatedMessage.CorrelationId = receivedCloudEvent.Id;

            // the order here doesn't matter because the message will only go out if the rest was successful
            await publisher!.SendMessageAsync(sensorActivatedMessage, cancellationToken);

            await Task.Delay(TimeSpan.FromSeconds(Random.Shared.Next(1, 15)), cancellationToken);

            scope.Complete();
        }
        catch (OperationCanceledException e)
        {
            logger.ActivateSensorLockLost(e, activateSensor.ChannelId);
            throw;
        }
    }

    Task HandleSensorActivated(CloudEvent receivedCloudEvent, CancellationToken cancellationToken)
    {
        logger.SensorActivatedWithSubject(receivedCloudEvent.Subject);
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
}