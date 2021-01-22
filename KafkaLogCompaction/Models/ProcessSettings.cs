using System;

namespace KafkaLogCompaction.Models
{
    public class ProcessSettings
    {
        public string SettingName { get; set; }

        public DateTime? LastProcessed { get; set; }
    }
}
