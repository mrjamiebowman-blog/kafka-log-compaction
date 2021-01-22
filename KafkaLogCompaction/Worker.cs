using Confluent.Kafka;
using KafkaLogCompaction.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

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

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            // You can find a lot of similar code on Confluent's examples.
            // https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/examples/Consumer/Program.cs

            using var consumer = _kafkaService.GetConsumer();
            
            try
            {
                var topics = new List<string> { "klc.settings" };
                consumer.Subscribe(topics);

                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);

                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                            continue;
                        }

                        Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

                        //if (consumeResult.Offset % commitPeriod == 0)
                        //{
                        //    // The Commit method sends a "commit offsets" request to the Kafka
                        //    // cluster and synchronously waits for the response. This is very
                        //    // slow compared to the rate at which the consumer is capable of
                        //    // consuming messages. A high performance application will typically
                        //    // commit offsets relatively infrequently and be designed handle
                        //    // duplicate messages in the event of failure.
                        //    try
                        //    {
                        //        consumer.Commit(consumeResult);
                        //    }
                        //    catch (KafkaException e)
                        //    {
                        //        Console.WriteLine($"Commit error: {e.Error.Reason}");
                        //    }
                        //}
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
                // todo: update setting
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                await Task.Delay(20000, cancellationToken);
            }
        }
    }
}
