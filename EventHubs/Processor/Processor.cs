using System.Collections.Concurrent;
using Azure;
using Azure.Messaging;
using Azure.Storage.Blobs;
using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;
using Microsoft.Extensions.Options;

namespace Processor;

public class Processor(
    IOptions<ProcessorOptions> processorOptions,
    BlobContainerClient blobContainerClient,
    BatchProcessor batchProcessor,
    SchemaRegistryAvroSerializer serializer,
    ILogger<Processor> logger)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await RestartFromBeginningIfNecessary(processorOptions.Value.RestartFromBeginning, blobContainerClient);

        var channelObservations = new ConcurrentDictionary<string, int>();
        batchProcessor.ProcessEventAsync += async events =>
        {
            foreach (var cloudEvent in events.ToCloudEvents())
            {
                var temperatureChanged = await serializer.DeserializeAsync<StorageTemperatureChanged>(cloudEvent, cancellationToken);

                var storage = cloudEvent.ExtensionAttributes["storage"].ToString()!;

                var numberOfDataPointsObserved = channelObservations.AddOrUpdate(storage,
                    static (_, _) => 0,
                    static (_,
                        points,
                        options) => options.CurrentTemperature > options.TemperatureThreshold
                        ? points + 1 : 0,
                    (CurrentTemperature: temperatureChanged.Current, TemperatureThreshold: processorOptions.Value.TemperatureThreshold));

                logger.LogTemperature(numberOfDataPointsObserved, storage, temperatureChanged, processorOptions.Value);
            }
        };

        await batchProcessor.StartProcessingAsync(cancellationToken);
    }

    #region Not relevant

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await batchProcessor.StopProcessingAsync(cancellationToken);
    }

    static async Task RestartFromBeginningIfNecessary(bool restartFromBeginning, BlobContainerClient containerClient)
    {
        if (restartFromBeginning)
        {
            try
            {
                await containerClient.DeleteAsync();
            }
            catch (RequestFailedException ex) when(ex.ErrorCode == "ContainerNotFound")
            {
            }
        }
    }

    #endregion
}