using FleetTelemetry.Data;
using FleetTelemetry.Telemetry;

namespace FleetTelemetry.Api.Endpoints;

internal static partial class TelemetryEndpoints
{
    public static void MapTelemetryEndpoints(this IEndpointRouteBuilder app) =>
        app.MapPost("/api/telemetry", IngestAsync);

    /// <summary>
    /// Accepts a JSON array of pings. The batch is validated as a whole, then written in one
    /// statement; the landing table keeps each ping exactly as sent.
    /// </summary>
    private static async Task<IResult> IngestAsync(
        HttpRequest request,
        TelemetryWriter writer,
        TimeProvider clock,
        ILoggerFactory loggers,
        CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(request.Body);
        var body = await reader.ReadToEndAsync(cancellationToken);

        switch (TelemetryBatchParser.Parse(body, clock.GetUtcNow()))
        {
            case TelemetryBatchResult.Malformed malformed:
                return Problems.MalformedTelemetry(malformed.Detail);
            case TelemetryBatchResult.Invalid invalid:
                return Problems.Invalid(invalid.Errors);
            case TelemetryBatchResult.Valid valid:
                var inserted = await writer.InsertBatchAsync(body, cancellationToken);
                var vehicles = valid.Pings.Select(p => p.VehicleId).Distinct().Count();
                var logger = loggers.CreateLogger("FleetTelemetry.Api.Telemetry");
                LogAccepted(logger, inserted, vehicles);
                return TypedResults.Accepted((string?)null, new TelemetryAccepted(inserted));
            default:
                throw new InvalidOperationException("Unexpected parse result.");
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Accepted {Pings} pings from {Vehicles} vehicles")]
    private static partial void LogAccepted(ILogger logger, int pings, int vehicles);
}
