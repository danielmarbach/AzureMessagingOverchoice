using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Azure.Messaging;
using Confluent.Kafka;
using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;
using Microsoft.Extensions.Options;

namespace Processor;

public class KafkaProcessor(
    IOptions<ProcessorOptions> processorOptions,
    IOptions<EventHubsOptions> eventHubsOptions,
    SchemaRegistryAvroSerializer serializer,
    ILogger<KafkaProcessor> logger)
    : BackgroundService
{
    private IConsumer<string, byte[]>? consumer;
    private string currentToken;

    [MemberNotNull(nameof(consumer))]
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = $"{eventHubsOptions.Value.FullyQualifiedNamespace}:9093",
            GroupId = "$Default",
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer,
            SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
            SaslOauthbearerTokenEndpointUrl =
                $"https://login.microsoftonline.com/{Environment.GetEnvironmentVariable("AZURE_TENANT_ID")}/oauth2/v2.0/token",
            SaslOauthbearerClientId = Environment.GetEnvironmentVariable("AZURE_CLIENT_ID"),
            SaslOauthbearerClientSecret = Environment.GetEnvironmentVariable("AZURE_CLIENT_SECRET"),
            SaslOauthbearerScope = $"https://{eventHubsOptions.Value.FullyQualifiedNamespace}/.default",
            // Every AutoCommitIntervalMs milliseconds, the Confluent.Kafka client will commit the latest offsets for all partitions it has polled. Those offsets are stored in Kafka’s internal __consumer_offsets topic.
            EnableAutoCommit = true, // default is true,
            AutoCommitIntervalMs = 5000, // default is 5000,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            //BrokerVersionFallback = "1.0.0",
            //Debug = "consumer,cgrp,fetch,protocol,broker,security",
        };

        consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        consumer.Subscribe(eventHubsOptions.Value.Name);
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var channelObservations = new ConcurrentDictionary<string, int>();

        while (!stoppingToken.IsCancellationRequested)
        {
            var consumeResult = consumer!.Consume(stoppingToken);

            var storage = consumeResult.Message.Key;
            var cloudEvent = CloudEvent.Parse(BinaryData.FromBytes(consumeResult.Message.Value))!;

            var temperatureChanged = await serializer.DeserializeAsync<StorageTemperatureChanged>(
                new MessageContent { Data = cloudEvent.Data, ContentType = cloudEvent.DataContentType! },
                stoppingToken);

            var numberOfDataPointsObserved = channelObservations.AddOrUpdate(storage,
                static (_, _) => 0,
                static (_, points, options) => options.CurrentTemperature > options.TemperatureThreshold ? points + 1 : 0,
                (CurrentTemperature: temperatureChanged.Current, TemperatureThreshold: processorOptions.Value.TemperatureThreshold));

            logger.LogTemperature(numberOfDataPointsObserved, storage, temperatureChanged, processorOptions.Value);

            // or manually
            // consumer.Commit(consumeResult);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        consumer?.Close();
        consumer?.Dispose();

        await base.StopAsync(cancellationToken);
    }
}