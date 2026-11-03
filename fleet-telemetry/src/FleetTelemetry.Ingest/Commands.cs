using System.Globalization;
using FleetTelemetry.Data;
using FleetTelemetry.Simulation;
using Microsoft.Extensions.DependencyInjection;

namespace FleetTelemetry.Ingest;

internal sealed record GenerateOptions(int Seed, DateOnly Start, int Days)
{
    public static GenerateOptions Parse(string[] args)
    {
        var options = new GenerateOptions(42, new DateOnly(2026, 9, 7), 3);
        for (var i = 0; i + 1 < args.Length; i += 2)
        {
            options = args[i] switch
            {
                "--seed" => options with { Seed = int.Parse(args[i + 1], CultureInfo.InvariantCulture) },
                "--start" => options with { Start = DateOnly.ParseExact(args[i + 1], "yyyy-MM-dd", CultureInfo.InvariantCulture) },
                "--days" => options with { Days = int.Parse(args[i + 1], CultureInfo.InvariantCulture) },
                _ => throw new ArgumentException($"Unknown option {args[i]}"),
            };
        }

        return options;
    }
}

internal static class Commands
{
    /// <summary>
    /// Simulates the registry's vehicles and writes one NDJSON file per vehicle-day, plus the next
    /// day's first trip of the first vehicle as a JSON array for POST /api/telemetry.
    /// </summary>
    public static async Task<int> GenerateAsync(
        IServiceProvider services, string directory, GenerateOptions options, CancellationToken cancellationToken)
    {
        var vehicles = await services.GetRequiredService<FleetRegistry>().ListVehiclesAsync(cancellationToken);
        var simulated = new FleetSimulator(options.Seed).Simulate(vehicles, options.Start, options.Days + 1);
        var liveDay = options.Start.AddDays(options.Days);

        var files = TelemetryExport.WriteNdjson(directory, simulated.Where(d => d.Date < liveDay));
        Console.WriteLine($"wrote {files.Count} files to {Path.GetRelativePath(Environment.CurrentDirectory, directory)}/, "
            + $"for example {Path.GetFileName(files[0].Path)} ({files[0].Lines:N0} pings)");

        var live = simulated.First(d => d.Date == liveDay);
        var trip = live.Trips[0];
        var livePath = Path.Combine(directory, "live", $"{live.VehicleId}-{liveDay:yyyy-MM-dd}.json");
        Directory.CreateDirectory(Path.GetDirectoryName(livePath)!);
        var pings = live.Pings.Where(p => p.Ts <= trip.EndedAt).ToList();
        await File.WriteAllTextAsync(livePath, TelemetryExport.ToJsonArray(pings), cancellationToken);
        Console.WriteLine($"wrote {Path.GetRelativePath(Environment.CurrentDirectory, livePath)} ({pings.Count} pings, one trip for POST /api/telemetry)");
        Console.WriteLine($"{vehicles.Count} vehicles, {files.Sum(f => f.Lines):N0} pings over {options.Days} days from {options.Start:yyyy-MM-dd}, seed {options.Seed}");
        return 0;
    }

    public static async Task<int> BackfillAsync(IServiceProvider services, string directory, CancellationToken cancellationToken)
    {
        var report = await services.GetRequiredService<TelemetryBackfill>().RunAsync(directory, cancellationToken);

        // List every file with errors, and the clean ones too when there are only a few.
        var files = report.Loaded.OrderBy(f => f.File, StringComparer.Ordinal).ToList();
        var shown = files.Count <= 10 ? files : files.Where(f => f.ErrorsSeen > 0).ToList();
        Console.WriteLine();
        if (shown.Count > 0)
        {
            Console.WriteLine($"{"file",-30} {"status",-17} {"parsed",8} {"loaded",8} {"errors",7}  first error");
            foreach (var file in shown)
            {
                var firstError = file.FirstErrorLine is { } line ? $"line {line}: {file.FirstError}" : "";
                Console.WriteLine($"{file.File,-30} {file.Status,-17} {file.RowsParsed,8:N0} {file.RowsLoaded,8:N0} {file.ErrorsSeen,7:N0}  {firstError}");
            }
        }

        if (files.Count > shown.Count)
        {
            Console.WriteLine($"{files.Count - shown.Count} files loaded without errors");
        }

        if (files.Count == 0)
        {
            Console.WriteLine("No new files: every staged file was loaded by an earlier run.");
        }

        Console.WriteLine(
            $"Uploaded {report.Uploaded.Count(u => u.Status == "UPLOADED")} of {report.Uploaded.Count} files, "
            + $"loaded {report.RowsLoaded:N0} rows from {files.Count} files, {report.RowsFailed:N0} rows failed. FLEET_DAILY_KPIS refreshed.");
        return 0;
    }
}
