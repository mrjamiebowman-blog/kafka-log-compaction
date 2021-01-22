using Confluent.Kafka;
using KafkaLogCompaction.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using KafkaLogCompaction.Models;

namespace KafkaLogCompaction
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IKafkaService _kafkaService;

        public List<ProcessSettings> processSettings = new List<ProcessSettings>();

        public List<string> topics = new List<string> { "klc.settings" };

        public Worker(ILogger<Worker> logger, IKafkaService kafkaService)
        {
            _logger = logger;
            _kafkaService = kafkaService;
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            // You can find a lot of similar code on Confluent's examples.
            // https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/examples/Consumer/Program.cs

            using var consumer = _kafkaService.GetConsumer();
            
            try
            {
                consumer.Subscribe(topics);

                while (true)
                {
                    try
                    {
                        // setting the time span to 5 seconds so this will exit if there is no data after 5 seconds.
                        // this happens during the initial start.
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));

                        if (consumeResult == null)
                        {
                            break;
                        }

                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                            break;
                        }

                        Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

                        // setting
                        var setting = JsonSerializer.Deserialize<ProcessSettings>(consumeResult.Message.Value);
                        setting.LastProcessed = consumeResult.Message.Timestamp.UtcDateTime;
                        processSettings.Add(setting);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Closing consumer.");
                consumer.Close();
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                // update setting
                ProcessSettings setting = new ProcessSettings();
                setting.SettingName = "KafkaLogCompaction";
                setting.Value = "value";

                var json = JsonSerializer.Serialize(setting);
                await _kafkaService.ProduceAsync(topics, json, cancellationToken);

                _logger.LogInformation("Worker running at: {time}", setting.LastProcessed);
                await Task.Delay(2000, cancellationToken);
            }
        }
    }
}
