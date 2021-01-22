using Confluent.Kafka;
using KafkaLogCompaction.Configuration;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaLogCompaction.Services
{
    public interface IKafkaService
    {
        Task ProduceAsync(List<string> topics, string msg, CancellationToken token);
        
        IConsumer<Ignore, string> GetConsumer();
        IProducer<TK, TV> GetProducer<TK, TV>();

        ConsumerConfig SetupConsumerConfig(KafkaConfiguration kafkaConfiguration);

        ProducerConfig SetupProducerConfig(KafkaConfiguration kafkaConfiguration);
    }
}
