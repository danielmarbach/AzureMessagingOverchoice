using System.Runtime.CompilerServices;
using Azure.Messaging;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Options;

namespace Processor;

public class Sender(
    IAzureClientFactory<ServiceBusClient> clientFactory,
    IOptions<ServiceBusOptions> serviceBusOptions,
    ILogger<Sender> logger)
    : IHostedService
{
    private readonly ServiceBusClient serviceBusClient = clientFactory.CreateClient("Client");

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await using var commandSender = serviceBusClient.CreateSender(serviceBusOptions.Value.InputQueue, new ServiceBusSenderOptions
        {
            Identifier = $"CommandSender-{serviceBusOptions.Value.InputQueue}"
        });

        var simulationCommands = CreateSimulationCommands();
        logger.SendWithDuplicates(simulationCommands.Count, simulationCommands.Count - simulationCommands.DistinctBy(c => c.PersonId, StringComparer.Ordinal).Count());

        await foreach (var batch in Batches(simulationCommands, commandSender))
        {
            using var batchToSend = batch;
            await commandSender.SendMessagesAsync(batchToSend, cancellationToken);
        }
    }

    private Queue<SendSwissChocolateTo> CreateSimulationCommands()
    {
        var eventsToSend = new Queue<SendSwissChocolateTo>();
        for (var i = 0; i < serviceBusOptions.Value.NumberOfCommands; i++)
        {
            var activateSensor = new SendSwissChocolateTo
            {
                PersonId = $"person/{Guid.NewGuid()}"
            };
            eventsToSend.Enqueue(activateSensor);

            // create some duplicates
            if (i % 3 == 0)
            {
                eventsToSend.Enqueue(activateSensor);
            }
        }

        return eventsToSend;
    }

    static async IAsyncEnumerable<ServiceBusMessageBatch> Batches(Queue<SendSwissChocolateTo> queueCommands,
        ServiceBusSender sender,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var currentBatch = default(ServiceBusMessageBatch);
        while (queueCommands.Count > 0)
        {
            var command = queueCommands.Peek();

            currentBatch ??= await sender.CreateMessageBatchAsync(cancellationToken);


            var cloudEvent = new CloudEvent("https://swisschoco.delivery/factory/lucerne", typeof(SendSwissChocolateTo).FullName!, command)
            {
                Id = command.PersonId,
            };

            if (!currentBatch.TryAddMessage(cloudEvent.ToServiceBusMessage()))
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

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}