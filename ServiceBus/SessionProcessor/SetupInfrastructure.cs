using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Options;

namespace SessionProcessor;

public class SetupInfrastructure(
    ServiceBusAdministrationClient administrationClient,
    IOptions<ServiceBusOptions> serviceBusOptions)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        #region Not relevant
        if (await administrationClient.QueueExistsAsync(serviceBusOptions.Value.InputQueue, cancellationToken))
        {
            await administrationClient.DeleteQueueAsync(serviceBusOptions.Value.InputQueue, cancellationToken);
        }
        #endregion

        await administrationClient.CreateQueueAsync(new CreateQueueOptions(serviceBusOptions.Value.InputQueue)
        {
            LockDuration = TimeSpan.FromSeconds(5),
            RequiresSession = true,
        }, cancellationToken);
    }
    #region Not relevant
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
    #endregion
}