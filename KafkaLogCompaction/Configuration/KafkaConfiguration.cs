namespace KafkaLogCompaction.Configuration
{
    public class KafkaConfiguration
    {
        public const string Position = "Kafka";

        public string BootstrapServer { get; set; }

        public int? Port { get; set; } = 9092;

        public string SslCaLocation { get; set; }

        public string GroupId { get; set; }

        public string AutoOffsetReset { get; set; }

        public bool EnableAutoCommit { get; set; }

        public string SaslUsername { get; set; }

        public string SaslPassword { get; set; }

        public string Acknowledgements { get; set; }
    }
}
