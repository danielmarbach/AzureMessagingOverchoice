using Azure.Storage.Blobs;
using static System.Text.Encoding;

namespace PullDelivery;

public class Sender(BlobContainerClient blobContainerClient, ILogger<Sender> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var id = Guid.NewGuid().ToString();
            var blobName = $"{id}.txt";
            using var memoryStream = new MemoryStream(UTF8.GetBytes($"Person '{id}' uploaded picture of smiling with the mouth full of Swiss Chocolate at '{DateTimeOffset.UtcNow}'"));
            await blobContainerClient.UploadBlobAsync(blobName, memoryStream, stoppingToken);
            logger.BlobUploaded(blobName);
            await Task.Delay(3000, stoppingToken);
        }
    }
}