namespace FleetTelemetry.Kpis;

/// <summary>One row of the FLEET_DAILY_KPIS dynamic table: one vehicle on one UTC day.</summary>
public sealed record VehicleDayKpi(
    DateOnly Day,
    string VehicleId,
    long Samples,
    long IdleSamples,
    double DistanceKm,
    double FuelUsedL,
    double MaxSpeedKmh,
    long FaultSamples);

/// <summary>Fleet-wide KPIs for one day.</summary>
public sealed record FleetDayKpi(
    DateOnly Day,
    int ActiveVehicles,
    double DistanceKm,
    double FuelUsedL,
    double? FuelEconomyLPer100Km,
    double IdleShare,
    double MaxSpeedKmh,
    int VehiclesWithFaults);

public static class FleetDailyKpis
{
    /// <summary>
    /// Rolls vehicle-days up to fleet days. Ratios are computed from the fleet totals
    /// (total fuel over total distance), not averaged across vehicles, so a van that
    /// drove 2 km does not weigh as much as a truck that drove 400.
    /// </summary>
    public static IReadOnlyList<FleetDayKpi> Summarize(IEnumerable<VehicleDayKpi> vehicleDays) =>
        vehicleDays
            .GroupBy(v => v.Day)
            .OrderBy(g => g.Key)
            .Select(day =>
            {
                var distance = day.Sum(v => v.DistanceKm);
                var fuel = day.Sum(v => v.FuelUsedL);
                return new FleetDayKpi(
                    day.Key,
                    ActiveVehicles: day.Select(v => v.VehicleId).Distinct().Count(),
                    DistanceKm: Math.Round(distance, 1),
                    FuelUsedL: Math.Round(fuel, 1),
                    FuelEconomyLPer100Km: KpiMath.FuelEconomyLPer100Km(fuel, distance),
                    IdleShare: KpiMath.IdleShare(day.Sum(v => v.IdleSamples), day.Sum(v => v.Samples)),
                    MaxSpeedKmh: day.Max(v => v.MaxSpeedKmh),
                    VehiclesWithFaults: day.Count(v => v.FaultSamples > 0));
            })
            .ToList();
}
