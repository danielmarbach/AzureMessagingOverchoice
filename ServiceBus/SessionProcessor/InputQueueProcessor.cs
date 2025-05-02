using Azure.Messaging;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;

namespace SessionProcessor;

public class InputQueueProcessor(
    ServiceBusClient serviceBusClient,
    IOptions<ServiceBusOptions> serviceBusOptions,
    ILogger<InputQueueProcessor> logger)
    : IHostedService, IAsyncDisposable
{
    private ServiceBusSessionProcessor? queueProcessor;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        queueProcessor = serviceBusClient.CreateSessionProcessor(serviceBusOptions.Value.InputQueue, new ServiceBusSessionProcessorOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            MaxConcurrentSessions = 10,
            AutoCompleteMessages = true,
            PrefetchCount = 10,
            Identifier = $"SessionProcessor-{serviceBusOptions.Value.InputQueue}",
            MaxAutoLockRenewalDuration = TimeSpan.FromSeconds(10),
        });

        #region Not relevant

        queueProcessor.ProcessMessageAsync += ProcessMessages;
        queueProcessor.ProcessErrorAsync += ProcessError;
        await queueProcessor.StartProcessingAsync(cancellationToken);

        #endregion
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

    private async Task ProcessMessages(ProcessSessionMessageEventArgs arg)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(arg.CancellationToken);
        try
        {
            arg.SessionLockLostAsync += MessageLockLostHandler;

            var receivedCloudEvent = arg.Message.ToCloudEvent();
            var handlerTask = receivedCloudEvent.Type switch
            {
                "SessionProcessor.StorageTemperatureChanged" => HandleStorageTemperatureChanged(new StorageStateProvider(arg), receivedCloudEvent, cts.Token),
                _ => Task.CompletedTask
            };
            await handlerTask;
        }
        finally
        {
            arg.SessionLockLostAsync -= MessageLockLostHandler;
        }

        return;

        async Task MessageLockLostHandler(SessionLockLostEventArgs lockLostArgs)
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

    private record StorageState
    {
        public required string Id { get; init; }
        public int PointsObserved { get; set; }
    }

    private sealed class StorageStateProvider(ProcessSessionMessageEventArgs arg)
    {
        public async Task<StorageState> Load(CancellationToken cancellationToken)
        {
            var sessionState = await arg.GetSessionStateAsync(cancellationToken);
            var storageStage = sessionState?.ToObjectFromJson<StorageState>() ?? new StorageState { Id = arg.SessionId };
            return storageStage;
        }

        public async Task Save(StorageState storageState, CancellationToken cancellationToken)
        {
            await arg.SetSessionStateAsync(BinaryData.FromObjectAsJson(storageState), cancellationToken);
        }
    }

    async Task HandleStorageTemperatureChanged(StorageStateProvider storageStateProvider, CloudEvent receivedCloudEvent, CancellationToken cancellationToken)
    {
        var storageTemperatureChanged = receivedCloudEvent.Data!.ToObjectFromJson<StorageTemperatureChanged>()!;

        var storageState = await storageStateProvider.Load(cancellationToken);

        storageState.PointsObserved +=
            storageTemperatureChanged.Current > serviceBusOptions.Value.TemperatureThreshold
            ? 1 : -storageState.PointsObserved;

        logger.LogTemperature(storageState.PointsObserved, storageState.Id, storageTemperatureChanged, serviceBusOptions.Value);

        await storageStateProvider.Save(storageState, cancellationToken);
    }

    #region Not relevant

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