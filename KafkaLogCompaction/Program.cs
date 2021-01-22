using KafkaLogCompaction.Configuration;
using KafkaLogCompaction.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Threading.Tasks;

namespace KafkaLogCompaction
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var hostBuilder = new HostBuilder()
                .ConfigureHostConfiguration(configHost =>
                {
                    configHost.SetBasePath(Directory.GetCurrentDirectory());
                    configHost.AddEnvironmentVariables();
                    configHost.AddCommandLine(args);
                });

            hostBuilder = hostBuilder.ConfigureAppConfiguration((hostBuilderContext, configApp) =>
            {
                var env = hostBuilderContext.HostingEnvironment;

                configApp.SetBasePath(Directory.GetCurrentDirectory());
                configApp.AddEnvironmentVariables();

                if (args != null)
                {
                    configApp.AddCommandLine(args);
                }

                // appsettings.json
                configApp.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

                var settings = configApp.Build();
            });

            hostBuilder = hostBuilder.ConfigureServices((hostContext, services) =>
            {
                services.AddTransient<IKafkaService, KafkaService>();

                // Add functionality to inject IOptions<T>
                services.AddOptions();

                // Add our Config object so it can be injected
                services.Configure<KafkaConfiguration>(hostContext.Configuration.GetSection(KafkaConfiguration.Position));

                services.AddHostedService<Worker>();
            }).ConfigureLogging((hostContext, configLogging) =>
            {
                configLogging.AddConsole();
            });

            hostBuilder = hostBuilder.UseConsoleLifetime();
            var host = hostBuilder.Build();
            await host.RunAsync();
        }
    }
}
