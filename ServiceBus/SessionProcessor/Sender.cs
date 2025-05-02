using System.Runtime.CompilerServices;
using Azure.Messaging;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;

namespace SessionProcessor;

public class Sender(
    ServiceBusClient serviceBusClient,
    IOptions<ServiceBusOptions> serviceBusOptions)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await using var commandSender = serviceBusClient.CreateSender(serviceBusOptions.Value.InputQueue, new ServiceBusSenderOptions
        {
            Identifier = $"CommandSender-{serviceBusOptions.Value.InputQueue}"
        });

        foreach (var channel in serviceBusOptions.Value.ChocolateStorage)
        {
            var simulationCommands = CreateSimulationCommands();
            await foreach (var batch in Batches(channel, simulationCommands, commandSender, cancellationToken))
            {
                using var batchToSend = batch;
                await commandSender.SendMessagesAsync(batchToSend, cancellationToken);
            }
        }
    }

    private Queue<StorageTemperatureChanged> CreateSimulationCommands()
    {
        var eventsToSend = new Queue<StorageTemperatureChanged>();
        var yesterday = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromDays(2));
        for (var i = 0; i < serviceBusOptions.Value.NumberOfDataPointsPerChocolateStorage; i++)
        {
            var processTemperatureChange = new StorageTemperatureChanged
            {
                Published = yesterday.Add(TimeSpan.FromSeconds(i)),
                Current = Random.Shared.Next(20, 30) + Random.Shared.NextDouble()
            };
            eventsToSend.Enqueue(processTemperatureChange);
        }

        return eventsToSend;
    }

    private static async IAsyncEnumerable<ServiceBusMessageBatch> Batches(string storage, Queue<StorageTemperatureChanged> queueCommands,
        ServiceBusSender sender, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var currentBatch = default(ServiceBusMessageBatch);
        while (queueCommands.Count > 0)
        {
            var command = queueCommands.Peek();

            currentBatch ??= await sender.CreateMessageBatchAsync(cancellationToken);

            if (!currentBatch.TryAddMessage(new CloudEvent(
                    "https://swisschoco.delivery/factory/lucerne/storage",
                    typeof(StorageTemperatureChanged).FullName!,
                    command)
                {
                    ExtensionAttributes =
                    {
                        { "storage", storage }
                    }
                }.ToServiceBusMessage()))
            {
                if (currentBatch.Count == 0)
                {
                    throw new Exception("There was an event too large to fit into a batch.");
                }

                yield return currentBatch;
                currentBatch = null;
            }
            else
            {
                queueCommands.Dequeue();
            }
        }

        if (currentBatch is { Count: > 0 })
        {
            yield return currentBatch;
        }
    }

    #region Not relevant
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
    #endregion
}