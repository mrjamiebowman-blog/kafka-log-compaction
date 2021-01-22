using Confluent.Kafka;
using KafkaLogCompaction.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using KafkaLogCompaction.Models;

namespace KafkaLogCompaction.Services
{
    public class KafkaService : IKafkaService
    {
        private readonly ILogger<KafkaService> _logger;
        private readonly IOptionsMonitor<KafkaConfiguration> _kafkaConfiguration;

        public KafkaService(ILogger<KafkaService> logger, IOptionsMonitor<KafkaConfiguration> kafkaConfiguration)
        {
            _logger = logger;
            _kafkaConfiguration = kafkaConfiguration;
        }

        public async Task ProduceAsync(List<string> topics, string msg, CancellationToken token)
        {
            try
            {
                var config = SetupProducerConfig(_kafkaConfiguration.CurrentValue);
                using var producer = GetProducer<string, string>(config);

                foreach (var topic in topics)
                {
                    var deliveryReport = producer.ProduceAsync(topic, new Message<string, string>() { Key = ProcessSettings.Key, Value = msg }, token);

                    await deliveryReport.ContinueWith(task =>
                    {
                        if (task.IsFaulted)
                        {
                            _logger.LogError($"Write to Kafka was faulted. Topic: {topic}. Event Message: {msg}");

                            // have to cast to the expected type of exception if we want details
                            if (task.Exception?.InnerException is ProduceException<Null, string> ex)
                            {
                                _logger.LogError($"Producer Exception: Error Code: {ex.Error.Code}, Reason: {ex.Error.Reason}, Topic: {topic}");
                            }
                            else
                            {
                                _logger.LogError($"Exception: {task.Exception}, Topic: {topic}");
                            }
                        }
                        else
                        {
                            _logger.LogInformation($"Wrote message to offset: {task.Result.Offset} on topic: {task.Result.Topic}, Persistence: {task.Result.Status.ToString()}");
                        }
                    }, token);
                }

                producer.Flush(TimeSpan.FromSeconds(1));
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public IConsumer<Ignore, string> GetConsumer()
        {
            var config = SetupConsumerConfig(_kafkaConfiguration.CurrentValue);
            return GetConsumer(config);
        }

        private IConsumer<Ignore, string> GetConsumer(ConsumerConfig consumerConfig)
        {
            var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
                .SetErrorHandler((_, e) =>
                {
                    _logger.LogInformation($"Error: {e.Reason}");
                })
                .SetStatisticsHandler((_, json) =>
                {
                    _logger.LogInformation($"Statistics: {json}");
                })
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    _logger.LogInformation($"Assigned partitions: [{string.Join(", ", partitions)}]");
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    _logger.LogInformation($"Revoking assignment: [{string.Join(", ", partitions)}]");
                })
                .Build();

            return consumer;
        }

        public IProducer<TK, TV> GetProducer<TK, TV>()
        {
            var config = SetupProducerConfig(_kafkaConfiguration.CurrentValue);
            return GetProducer<TK, TV>(config);
        }

        private IProducer<TK, TV> GetProducer<TK, TV>(ProducerConfig config)
        {
            var producer = new ProducerBuilder<TK, TV>(config).Build();
            return producer;
        }

        public ProducerConfig SetupProducerConfig(KafkaConfiguration kafkaConfiguration)
        {
            var config = new ProducerConfig();
            config.BootstrapServers = $"{kafkaConfiguration.BootstrapServer}:{kafkaConfiguration.Port}";
            config.ClientId = Dns.GetHostName();

            config.Acks = kafkaConfiguration.Acknowledgements?.ToLower() switch
            {
                "all" => Acks.All,
                "leader" => Acks.Leader,
                "none" => Acks.None,
                _ => Acks.All
            };

            if (!String.IsNullOrWhiteSpace(kafkaConfiguration.SaslUsername) &&
                !String.IsNullOrWhiteSpace(kafkaConfiguration.SaslPassword))
            {
                config.SaslUsername = kafkaConfiguration.SaslUsername;
                config.SaslPassword = kafkaConfiguration.SaslPassword;
            }
            else
            {
                config.SecurityProtocol = SecurityProtocol.Plaintext;
            }

            return config;
        }

        public ConsumerConfig SetupConsumerConfig(KafkaConfiguration kafkaConfiguration)
        {
            var config = new ConsumerConfig();
            config.BootstrapServers = $"{kafkaConfiguration.BootstrapServer}:{kafkaConfiguration.Port}";
            config.GroupId = kafkaConfiguration.GroupId;
            config.EnableAutoCommit = kafkaConfiguration.EnableAutoCommit;

            switch (kafkaConfiguration.AutoOffsetReset?.ToLower())
            {
                case "earliest":
                    config.AutoOffsetReset = AutoOffsetReset.Earliest;
                    break;
                case "latest":
                    config.AutoOffsetReset = AutoOffsetReset.Latest;
                    break;
                case "error":
                    config.AutoOffsetReset = AutoOffsetReset.Error;
                    break;
                default:
                    config.AutoOffsetReset = AutoOffsetReset.Earliest;
                    break;
            }

            if (!String.IsNullOrWhiteSpace(kafkaConfiguration.SaslUsername) &&
                !String.IsNullOrWhiteSpace(kafkaConfiguration.SaslPassword))
            {
                config.SaslUsername = kafkaConfiguration.SaslUsername;
                config.SaslPassword = kafkaConfiguration.SaslPassword;
            }
            else
            {
                config.SecurityProtocol = SecurityProtocol.Plaintext;
            }

            return config;
        }
    }
}
