using System;
using System.Text.Json.Serialization;

namespace KafkaLogCompaction.Models
{
    public class ProcessSettings
    {
        [JsonIgnore] public const string Key = "ProcessSettings";

        public string SettingName { get; set; }

        public string Value { get; set; }

        /// <summary>
        /// UTC
        /// </summary>
        [JsonIgnore]
        public DateTime? LastProcessed { get; set; }
    }
}
