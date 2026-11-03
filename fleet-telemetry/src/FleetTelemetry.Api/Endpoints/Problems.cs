namespace FleetTelemetry.Api.Endpoints;

/// <summary>Problem details (RFC 9457) the API returns, beyond framework validation errors.</summary>
internal static class Problems
{
    public static IResult VehicleNotFound(string vehicleId) => TypedResults.Problem(
        statusCode: StatusCodes.Status404NotFound,
        title: "Vehicle not found",
        detail: $"No vehicle '{vehicleId}' in the fleet registry.",
        type: "urn:fleet-telemetry:problem:vehicle-not-found");

    public static IResult MalformedTelemetry(string detail) => TypedResults.Problem(
        statusCode: StatusCodes.Status400BadRequest,
        title: "Malformed telemetry",
        detail: detail,
        type: "urn:fleet-telemetry:problem:malformed-telemetry");

    public static IResult Invalid(IReadOnlyDictionary<string, string[]> errors) => TypedResults.ValidationProblem(
        errors,
        title: "Invalid request",
        type: "urn:fleet-telemetry:problem:validation");

    public static DateTime Utc(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Utc);
}
