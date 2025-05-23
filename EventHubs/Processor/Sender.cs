using Azure.Messaging;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;
using Microsoft.Extensions.Options;

namespace Processor;

public class Sender(IOptions<SenderOptions> senderOptions, EventHubProducerClient eventHubProducerClient, SchemaRegistryAvroSerializer serializer)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!senderOptions.Value.ProduceData)
        {
            return;
        }

        foreach (var storage in senderOptions.Value.Storage)
        {
            await foreach (var batch in StreamBatches(
                                   await CreateSimulationData(storage, senderOptions.Value.NumberOfDataPointsPerChocolateStorage, serializer),
                                   eventHubProducerClient)
                               .WithCancellation(cancellationToken))
            {
                using var batchToSend = batch;
                await eventHubProducerClient.SendAsync(batchToSend, cancellationToken);
            }
        }
    }

    private static async ValueTask<Queue<EventData>> CreateSimulationData(string storage, int numberOfDatapointPerChannel, SchemaRegistryAvroSerializer serializer)
    {
        var eventsToSend = new Queue<EventData>();
        var yesterday = DateTime.UtcNow.Subtract(TimeSpan.FromDays(2));
        for (var i = 0; i < numberOfDatapointPerChannel; i++)
        {
            var temperatureData = await serializer.SerializeAsync(new StorageTemperatureChanged
            {
                Published = yesterday.Add(TimeSpan.FromSeconds(i)),
                Current = Random.Shared.Next(20, 30) + Random.Shared.NextDouble()
            });

            var cloudEvent = new CloudEvent(
                "https://swisschoco.delivery/factory/lucerne/storage",
                typeof(StorageTemperatureChanged).FullName!,
                temperatureData.Data,
                temperatureData.ContentType?.ToString())
            {
                ExtensionAttributes =
                {
                    { "storage", storage }
                }
            };

            eventsToSend.Enqueue(cloudEvent.ToEventData());
        }

        return eventsToSend;
    }

    static async IAsyncEnumerable<EventDataBatch> StreamBatches(Queue<EventData> queuedEvents,
        EventHubProducerClient producer)
    {
        var currentBatch = default(EventDataBatch);
        while (queuedEvents.Count > 0)
        {
            var eventData = queuedEvents.Peek();

            currentBatch ??= await producer.CreateBatchAsync(new CreateBatchOptions
            {
                PartitionKey = eventData.Properties["storage"].ToString()
            });

            if (!currentBatch.TryAdd(eventData))
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
                queuedEvents.Dequeue();
            }
        }

        if (currentBatch is { Count: > 0 })
        {
            yield return currentBatch;
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}