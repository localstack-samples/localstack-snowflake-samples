using System.Text.Json;
using FleetTelemetry.Telemetry;

namespace FleetTelemetry.Simulation;

/// <summary>A written export file and the number of pings in it.</summary>
public sealed record ExportFile(string Path, int Lines);

/// <summary>
/// Writes telemetry the way vehicle gateways export it: NDJSON, one file per vehicle per
/// UTC day, named <c>yyyy-MM-dd_VEHICLE.ndjson</c>.
/// </summary>
public static class TelemetryExport
{
    public static string FileName(DateOnly day, string vehicleId) => $"{day:yyyy-MM-dd}_{vehicleId}.ndjson";

    public static IReadOnlyList<ExportFile> WriteNdjson(string directory, IEnumerable<VehicleDay> vehicleDays)
    {
        Directory.CreateDirectory(directory);
        return vehicleDays
            .OrderBy(d => d.Date)
            .ThenBy(d => d.VehicleId, StringComparer.Ordinal)
            .Select(day =>
            {
                var path = Path.Combine(directory, FileName(day.Date, day.VehicleId));
                using var writer = new StreamWriter(path) { NewLine = "\n" };
                foreach (var ping in day.Pings)
                {
                    writer.WriteLine(JsonSerializer.Serialize(ping, TelemetryJson.Options));
                }

                return new ExportFile(path, day.Pings.Count);
            })
            .ToList();
    }

    public static string ToJsonArray(IEnumerable<TelemetryPing> pings) =>
        JsonSerializer.Serialize(pings, TelemetryJson.Options);
}
