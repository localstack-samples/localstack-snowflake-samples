using FleetTelemetry.Data;
using FleetTelemetry.Ingest;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// Command-line tool for bulk telemetry:
//   generate <dir> [--seed 42] [--start 2026-09-07] [--days 3]   write synthetic NDJSON exports
//   backfill <dir>                                                 PUT + COPY INTO, then refresh KPIs
if (args is not [var command, var directory, .. var options] || command is not ("generate" or "backfill"))
{
    Console.Error.WriteLine("Usage: FleetTelemetry.Ingest generate <dir> [--seed N] [--start yyyy-MM-dd] [--days N]");
    Console.Error.WriteLine("       FleetTelemetry.Ingest backfill <dir>");
    return 64;
}

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    ContentRootPath = AppContext.BaseDirectory,
});
builder.Configuration.AddSnowflakeEnvironmentVariables();
builder.Services.AddFleetSnowflake();
builder.Services.AddSingleton<IQueryTagSource>(new FixedQueryTag($"ingest {command}"));

using var host = builder.Build();
await host.StartAsync();
using var scope = host.Services.CreateScope();
var cancellationToken = host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping;

return command == "generate"
    ? await Commands.GenerateAsync(scope.ServiceProvider, directory, GenerateOptions.Parse(options), cancellationToken)
    : await Commands.BackfillAsync(scope.ServiceProvider, directory, cancellationToken);
