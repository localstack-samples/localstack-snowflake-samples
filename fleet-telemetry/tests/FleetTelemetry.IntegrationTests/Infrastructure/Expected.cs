using FleetTelemetry.Simulation;
using FleetTelemetry.Telemetry;

namespace FleetTelemetry.IntegrationTests.Infrastructure;

/// <summary>
/// Expected values computed directly from the simulated pings with LINQ, independently of
/// the SQL under test.
/// </summary>
internal static class Expected
{
    public sealed record Fault(string Code, string Severity, int Occurrences, DateTime FirstSeen, DateTime LastSeen, double MaxCoolantC);

    public sealed record FleetDay(
        DateOnly Day,
        int ActiveVehicles,
        double DistanceKm,
        double FuelUsedL,
        double FuelEconomyLPer100Km,
        double IdleShare,
        double MaxSpeedKmh,
        int VehiclesWithFaults);

    public static IReadOnlyList<Fault> Faults(IEnumerable<TelemetryPing> pings) =>
        pings
            .SelectMany(p => p.Engine.Dtc.Select(code => (Ping: p, Code: code)))
            .GroupBy(x => x.Code.Code)
            .OrderBy(g => g.Key, StringComparer.Ordinal)
            .Select(g => new Fault(
                g.Key,
                g.First().Code.Severity,
                g.Count(),
                g.Min(x => x.Ping.Ts).UtcDateTime,
                g.Max(x => x.Ping.Ts).UtcDateTime,
                g.Max(x => x.Ping.Engine.CoolantC)))
            .ToList();

    /// <summary>
    /// Per day: distance and fuel are each vehicle's last minus first reading, summed; fuel
    /// economy is total fuel over total distance; idle share is stationary pings over all pings.
    /// </summary>
    public static IReadOnlyList<FleetDay> FleetDays(IEnumerable<VehicleDay> vehicleDays) =>
        vehicleDays
            .GroupBy(d => d.Date)
            .OrderBy(g => g.Key)
            .Select(day =>
            {
                var distance = day.Sum(v => v.Pings.Max(p => p.OdometerKm) - v.Pings.Min(p => p.OdometerKm));
                var fuel = day.Sum(v => v.Pings.Max(p => p.Fuel.UsedL) - v.Pings.Min(p => p.Fuel.UsedL));
                var pings = day.SelectMany(v => v.Pings).ToList();
                return new FleetDay(
                    day.Key,
                    day.Count(),
                    Math.Round(distance, 1),
                    Math.Round(fuel, 1),
                    Math.Round(fuel / distance * 100, 1),
                    Math.Round(pings.Count(p => p.SpeedKmh == 0) / (double)pings.Count, 3),
                    pings.Max(p => p.SpeedKmh),
                    day.Count(v => v.Pings.Any(p => p.Engine.Dtc.Count > 0)));
            })
            .ToList();
}
