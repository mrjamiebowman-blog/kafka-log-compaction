using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaLogCompaction.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KafkaLogCompaction
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddTransient<IKafkaService, KafkaService>();
                    services.AddHostedService<Worker>();
                });
    }
}
