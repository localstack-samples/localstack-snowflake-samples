using FleetTelemetry.Data;
using FleetTelemetry.Kpis;

namespace FleetTelemetry.Api.Endpoints;

internal static class FleetEndpoints
{
    public const int MaxDays = 92;

    public static void MapFleetEndpoints(this IEndpointRouteBuilder app) =>
        app.MapGet("/api/fleet/daily", GetDailyAsync);

    /// <summary>Fleet KPIs per UTC day in [from, to], from the FLEET_DAILY_KPIS dynamic table.</summary>
    private static async Task<IResult> GetDailyAsync(
        DateOnly from,
        DateOnly to,
        FleetAnalytics analytics,
        CancellationToken cancellationToken)
    {
        if (to < from || to.DayNumber - from.DayNumber >= MaxDays)
        {
            return Problems.Invalid(new Dictionary<string, string[]>
            {
                ["to"] = [$"Must be on or after 'from' and at most {MaxDays} days later."],
            });
        }

        var vehicleDays = await analytics.GetVehicleDaysAsync(from, to, cancellationToken);
        return TypedResults.Ok(new FleetDaily(from, to, FleetDailyKpis.Summarize(vehicleDays)));
    }
}
