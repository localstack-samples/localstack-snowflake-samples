using Dapper;
using FleetTelemetry.Kpis;

namespace FleetTelemetry.Data;

/// <summary>Fleet-wide KPIs, read from the FLEET_DAILY_KPIS dynamic table.</summary>
public sealed class FleetAnalytics(FleetDatabase database)
{
    public const string DailyKpisTable = "FLEET_DAILY_KPIS";

    public async Task<IReadOnlyList<VehicleDayKpi>> GetVehicleDaysAsync(
        DateOnly from, DateOnly to, CancellationToken cancellationToken)
    {
        await using var connection = await database.OpenAsync(cancellationToken);
        var rows = await connection.QueryAsync<VehicleDayRow>(new CommandDefinition(
            $"""
            SELECT DAY, VEHICLE_ID, SAMPLES, IDLE_SAMPLES, DISTANCE_KM, FUEL_USED_L, MAX_SPEED_KMH, FAULT_SAMPLES
            FROM {DailyKpisTable}
            WHERE DAY BETWEEN :fromDay AND :toDay
            ORDER BY DAY, VEHICLE_ID
            """,
            new { fromDay = from.ToDateTime(TimeOnly.MinValue), toDay = to.ToDateTime(TimeOnly.MinValue) },
            cancellationToken: cancellationToken));
        return rows
            .Select(r => new VehicleDayKpi(
                DateOnly.FromDateTime(r.Day), r.VehicleId, r.Samples, r.IdleSamples,
                r.DistanceKm, r.FuelUsedL, r.MaxSpeedKmh, r.FaultSamples))
            .ToList();
    }

    /// <summary>Brings the dynamic table up to date now instead of waiting for its target lag.</summary>
    public async Task RefreshAsync(CancellationToken cancellationToken)
    {
        await using var connection = await database.OpenAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(
            $"ALTER DYNAMIC TABLE {DailyKpisTable} REFRESH", cancellationToken: cancellationToken));
    }

    private sealed class VehicleDayRow
    {
        public DateTime Day { get; init; }

        public string VehicleId { get; init; } = "";

        public long Samples { get; init; }

        public long IdleSamples { get; init; }

        public double DistanceKm { get; init; }

        public double FuelUsedL { get; init; }

        public double MaxSpeedKmh { get; init; }

        public long FaultSamples { get; init; }
    }
}
