using FleetTelemetry.Data;
using FleetTelemetry.Kpis;
using FleetTelemetry.Rules;

namespace FleetTelemetry.Api.Endpoints;

internal static class VehicleEndpoints
{
    public const string HarshBraking = "harsh_braking";
    public const string Idling = "idling";

    public static void MapVehicleEndpoints(this IEndpointRouteBuilder app)
    {
        var vehicles = app.MapGroup("/api/vehicles/{vehicleId}");
        vehicles.MapGet("/trips", GetTripsAsync);
        vehicles.MapGet("/events", GetEventsAsync);
        vehicles.MapGet("/faults", GetFaultsAsync);
    }

    /// <summary>Trips on one UTC day; see <see cref="TripRules"/>.</summary>
    private static async Task<IResult> GetTripsAsync(
        string vehicleId,
        DateOnly date,
        FleetRegistry registry,
        VehicleAnalytics analytics,
        CancellationToken cancellationToken)
    {
        if (!await registry.VehicleExistsAsync(vehicleId, cancellationToken))
        {
            return Problems.VehicleNotFound(vehicleId);
        }

        var trips = await analytics.GetTripsAsync(vehicleId, date, cancellationToken);
        return TypedResults.Ok(new VehicleTrips(
            vehicleId,
            date,
            trips.Select(t => new Trip(
                    TripNo: (int)t.TripNo,
                    StartedAt: Problems.Utc(t.StartedAt),
                    EndedAt: Problems.Utc(t.EndedAt),
                    DurationS: t.DurationS,
                    DistanceKm: Math.Round(t.DistanceKm, 3),
                    FuelUsedL: Math.Round(t.FuelUsedL, 3),
                    FuelEconomyLPer100Km: KpiMath.FuelEconomyLPer100Km(t.FuelUsedL, t.DistanceKm),
                    AvgSpeedKmh: KpiMath.AverageSpeedKmh(t.DistanceKm, t.DurationS),
                    MaxSpeedKmh: t.MaxSpeedKmh,
                    IdleS: t.IdleS,
                    Samples: t.Samples))
                .ToList()));
    }

    /// <summary>Driving events: <c>harsh_braking</c> or <c>idling</c>, optionally within [from, to] (UTC days).</summary>
    private static async Task<IResult> GetEventsAsync(
        string vehicleId,
        string? type,
        DateOnly? from,
        DateOnly? to,
        FleetRegistry registry,
        VehicleAnalytics analytics,
        CancellationToken cancellationToken)
    {
        if (type is not (HarshBraking or Idling))
        {
            return Problems.Invalid(new Dictionary<string, string[]>
            {
                ["type"] = [$"Must be '{HarshBraking}' or '{Idling}'."],
            });
        }

        if (Range(from, to) is not var (start, end))
        {
            return InvalidRange();
        }

        if (!await registry.VehicleExistsAsync(vehicleId, cancellationToken))
        {
            return Problems.VehicleNotFound(vehicleId);
        }

        if (type == HarshBraking)
        {
            var events = await analytics.GetHarshBrakingAsync(vehicleId, start, end, cancellationToken);
            return TypedResults.Ok(new VehicleEvents<HarshBrakingEvent>(
                vehicleId,
                type,
                events.Select(e => new HarshBrakingEvent(
                        Problems.Utc(e.OccurredAt), e.Lat, e.Lon, e.FromSpeedKmh, e.ToSpeedKmh, Math.Round(e.DecelerationMs2, 2)))
                    .ToList()));
        }

        var idling = await analytics.GetIdlingAsync(vehicleId, start, end, cancellationToken);
        return TypedResults.Ok(new VehicleEvents<IdlingEvent>(
            vehicleId,
            type,
            idling.Select(e => new IdlingEvent(Problems.Utc(e.StartedAt), Problems.Utc(e.EndedAt), e.DurationS, e.Lat, e.Lon))
                .ToList()));
    }

    /// <summary>Diagnostic trouble codes reported by the vehicle, optionally within [from, to] (UTC days).</summary>
    private static async Task<IResult> GetFaultsAsync(
        string vehicleId,
        DateOnly? from,
        DateOnly? to,
        FleetRegistry registry,
        VehicleAnalytics analytics,
        CancellationToken cancellationToken)
    {
        if (Range(from, to) is not var (start, end))
        {
            return InvalidRange();
        }

        if (!await registry.VehicleExistsAsync(vehicleId, cancellationToken))
        {
            return Problems.VehicleNotFound(vehicleId);
        }

        var faults = await analytics.GetFaultsAsync(vehicleId, start, end, cancellationToken);
        return TypedResults.Ok(new VehicleFaults(
            vehicleId,
            faults.Select(f => new ActiveFault(
                    f.Code, f.Severity, f.Occurrences, Problems.Utc(f.FirstSeen), Problems.Utc(f.LastSeen), f.MaxCoolantC))
                .ToList()));
    }

    /// <summary>[from, to] as UTC day bounds, half-open; both are optional.</summary>
    private static (DateTime Start, DateTime End)? Range(DateOnly? from, DateOnly? to)
    {
        var start = (from ?? DateOnly.MinValue).ToDateTime(TimeOnly.MinValue);
        var end = to is { } last ? last.AddDays(1).ToDateTime(TimeOnly.MinValue) : DateTime.MaxValue;
        return start < end ? (start, end) : null;
    }

    private static IResult InvalidRange() => Problems.Invalid(new Dictionary<string, string[]>
    {
        ["to"] = ["Must not be before 'from'."],
    });
}
