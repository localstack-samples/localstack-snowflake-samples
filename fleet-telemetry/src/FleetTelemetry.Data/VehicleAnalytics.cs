using Dapper;
using FleetTelemetry.Rules;

namespace FleetTelemetry.Data;

/// <summary>Per-vehicle analytics, computed in Snowflake from the TELEMETRY view.</summary>
public sealed class VehicleAnalytics(FleetDatabase database)
{
    /// <summary>
    /// Trips on one UTC day. A gap of more than <see cref="TripRules.MaxPingGap"/> between
    /// consecutive pings starts a new trip: LAG finds the gaps, a running SUM numbers the trips.
    /// </summary>
    public async Task<IReadOnlyList<TripRow>> GetTripsAsync(string vehicleId, DateOnly day, CancellationToken cancellationToken)
    {
        const string sql = """
            WITH pings AS (
                SELECT TS, SPEED_KMH, ODOMETER_KM, FUEL_USED_L,
                       DATEDIFF('second', LAG(TS) OVER (ORDER BY TS), TS) AS GAP_S,
                       LAG(SPEED_KMH) OVER (ORDER BY TS) AS PREV_SPEED_KMH
                FROM TELEMETRY
                WHERE VEHICLE_ID = :vehicleId AND TS >= :dayStart AND TS < :dayEnd
            ),
            numbered AS (
                SELECT *,
                       SUM(IFF(GAP_S IS NULL OR GAP_S > :maxGapS, 1, 0))
                           OVER (ORDER BY TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS TRIP_NO
                FROM pings
            )
            SELECT TRIP_NO,
                   MIN(TS) AS STARTED_AT,
                   MAX(TS) AS ENDED_AT,
                   DATEDIFF('second', MIN(TS), MAX(TS)) AS DURATION_S,
                   MAX(ODOMETER_KM) - MIN(ODOMETER_KM) AS DISTANCE_KM,
                   MAX(FUEL_USED_L) - MIN(FUEL_USED_L) AS FUEL_USED_L,
                   MAX(SPEED_KMH) AS MAX_SPEED_KMH,
                   SUM(IFF(GAP_S <= :maxGapS AND PREV_SPEED_KMH = 0 AND SPEED_KMH = 0, GAP_S, 0)) AS IDLE_S,
                   COUNT(*) AS SAMPLES
            FROM numbered
            GROUP BY TRIP_NO
            ORDER BY TRIP_NO
            """;
        var dayStart = day.ToDateTime(TimeOnly.MinValue);
        await using var connection = await database.OpenAsync(cancellationToken);
        var rows = await connection.QueryAsync<TripRow>(new CommandDefinition(
            sql,
            new { vehicleId, dayStart, dayEnd = dayStart.AddDays(1), maxGapS = (long)TripRules.MaxPingGap.TotalSeconds },
            cancellationToken: cancellationToken));
        return rows.ToList();
    }

    /// <summary>Consecutive pings whose speed dropped by at least <see cref="DrivingEventRules.HarshBrakingMs2"/>.</summary>
    public async Task<IReadOnlyList<HarshBrakingRow>> GetHarshBrakingAsync(
        string vehicleId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        const string sql = """
            WITH pings AS (
                SELECT TS, LAT, LON, SPEED_KMH,
                       LAG(SPEED_KMH) OVER (ORDER BY TS) AS PREV_SPEED_KMH,
                       DATEDIFF('second', LAG(TS) OVER (ORDER BY TS), TS) AS GAP_S
                FROM TELEMETRY
                WHERE VEHICLE_ID = :vehicleId AND TS >= :fromTs AND TS < :toTs
            )
            SELECT TS AS OCCURRED_AT, LAT, LON,
                   PREV_SPEED_KMH AS FROM_SPEED_KMH,
                   SPEED_KMH AS TO_SPEED_KMH,
                   (PREV_SPEED_KMH - SPEED_KMH) / 3.6 / GAP_S AS DECELERATION_MS2
            FROM pings
            WHERE GAP_S > 0
              AND (PREV_SPEED_KMH - SPEED_KMH) / 3.6 / GAP_S >= :thresholdMs2
            ORDER BY TS
            """;
        await using var connection = await database.OpenAsync(cancellationToken);
        var rows = await connection.QueryAsync<HarshBrakingRow>(new CommandDefinition(
            sql,
            new
            {
                vehicleId,
                fromTs = from,
                toTs = to,
                thresholdMs2 = DrivingEventRules.HarshBrakingMs2,
            },
            cancellationToken: cancellationToken));
        return rows.ToList();
    }

    /// <summary>
    /// Stretches of at least <see cref="DrivingEventRules.MinIdling"/> standing still with the
    /// engine running. Consecutive stationary pings form an island (gaps and islands).
    /// </summary>
    public async Task<IReadOnlyList<IdlingRow>> GetIdlingAsync(
        string vehicleId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        const string sql = """
            WITH pings AS (
                SELECT TS, LAT, LON, SPEED_KMH,
                       LAG(SPEED_KMH) OVER (ORDER BY TS) AS PREV_SPEED_KMH,
                       DATEDIFF('second', LAG(TS) OVER (ORDER BY TS), TS) AS GAP_S
                FROM TELEMETRY
                WHERE VEHICLE_ID = :vehicleId AND TS >= :fromTs AND TS < :toTs
            ),
            islands AS (
                SELECT *,
                       SUM(IFF(SPEED_KMH = 0 AND PREV_SPEED_KMH = 0 AND GAP_S <= :maxGapS, 0, 1))
                           OVER (ORDER BY TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ISLAND
                FROM pings
            )
            SELECT MIN(TS) AS STARTED_AT,
                   MAX(TS) AS ENDED_AT,
                   DATEDIFF('second', MIN(TS), MAX(TS)) AS DURATION_S,
                   ANY_VALUE(LAT) AS LAT,
                   ANY_VALUE(LON) AS LON
            FROM islands
            WHERE SPEED_KMH = 0
            GROUP BY ISLAND
            HAVING DATEDIFF('second', MIN(TS), MAX(TS)) >= :minIdleS
            ORDER BY STARTED_AT
            """;
        await using var connection = await database.OpenAsync(cancellationToken);
        var rows = await connection.QueryAsync<IdlingRow>(new CommandDefinition(
            sql,
            new
            {
                vehicleId,
                fromTs = from,
                toTs = to,
                maxGapS = (long)TripRules.MaxPingGap.TotalSeconds,
                minIdleS = (long)DrivingEventRules.MinIdling.TotalSeconds,
            },
            cancellationToken: cancellationToken));
        return rows.ToList();
    }

    /// <summary>Active fault codes, unnested from each ping's engine.dtc array by the TELEMETRY_FAULTS view.</summary>
    public async Task<IReadOnlyList<FaultRow>> GetFaultsAsync(
        string vehicleId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT CODE,
                   ANY_VALUE(SEVERITY) AS SEVERITY,
                   COUNT(*) AS OCCURRENCES,
                   MIN(TS) AS FIRST_SEEN,
                   MAX(TS) AS LAST_SEEN,
                   MAX(COOLANT_C) AS MAX_COOLANT_C
            FROM TELEMETRY_FAULTS
            WHERE VEHICLE_ID = :vehicleId AND TS >= :fromTs AND TS < :toTs
            GROUP BY CODE
            ORDER BY CODE
            """;
        await using var connection = await database.OpenAsync(cancellationToken);
        var rows = await connection.QueryAsync<FaultRow>(new CommandDefinition(
            sql,
            new { vehicleId, fromTs = from, toTs = to },
            cancellationToken: cancellationToken));
        return rows.ToList();
    }
}
