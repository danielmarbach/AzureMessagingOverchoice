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
        arg.Message.ApplicationProperties.TryGetValue("MessageType", out var messageTypeValue);
        var handlerTask = messageTypeValue switch
        {
            "SessionProcessor.StorageTemperatureChanged" => HandleStorageTemperatureChanged(arg, arg.CancellationToken),
            _ => Task.CompletedTask
        };
        await handlerTask;
    }

    private record StorageState
    {
        public int PointsObserved { get; set; }
    }

    async Task HandleStorageTemperatureChanged(ProcessSessionMessageEventArgs arg, CancellationToken cancellationToken)
    {
        var message = arg.Message;
        var channel = arg.SessionId;
        var storageTemperatureChanged = message.Body.ToObjectFromJson<StorageTemperatureChanged>()!;

        var sessionState = await arg.GetSessionStateAsync(cancellationToken);
        var channelState = sessionState?.ToObjectFromJson<StorageState>() ?? new StorageState();

        channelState.PointsObserved +=
            storageTemperatureChanged.Current > serviceBusOptions.Value.TemperatureThreshold
            ? 1 : -channelState.PointsObserved;

        logger.LogTemperature(channelState.PointsObserved, channel, storageTemperatureChanged, serviceBusOptions.Value);

        await arg.SetSessionStateAsync(BinaryData.FromObjectAsJson(channelState), cancellationToken);
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