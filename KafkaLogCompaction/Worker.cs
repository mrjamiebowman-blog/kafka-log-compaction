using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaLogCompaction.Services;

namespace KafkaLogCompaction
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IKafkaService _kafkaService;

        public Worker(ILogger<Worker> logger, IKafkaService kafkaService)
        {
            _logger = logger;
            _kafkaService = kafkaService;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // todo: read setting

            // todo: if no settings then create a setting.

            while (!stoppingToken.IsCancellationRequested)
            {
                // todo: update setting
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                await Task.Delay(20000, stoppingToken);
            }
        }
    }
}
