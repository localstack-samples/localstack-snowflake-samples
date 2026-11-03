using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace FleetTelemetry.Telemetry;

/// <summary>Outcome of parsing a <c>POST /api/telemetry</c> body.</summary>
public abstract record TelemetryBatchResult
{
    private TelemetryBatchResult()
    {
    }

    /// <summary>The body is not a JSON array of objects.</summary>
    public sealed record Malformed(string Detail) : TelemetryBatchResult;

    /// <summary>The body is a JSON array but some pings break the rules; keys are like <c>[3].speed_kmh</c>.</summary>
    public sealed record Invalid(IReadOnlyDictionary<string, string[]> Errors) : TelemetryBatchResult;

    public sealed record Valid(IReadOnlyList<TelemetryPing> Pings) : TelemetryBatchResult;
}

/// <summary>
/// Validates a batch of pings before it is written to Snowflake. A batch is accepted or
/// rejected as a whole, so a device can safely retry it.
/// </summary>
public static partial class TelemetryBatchParser
{
    public const int MaxBatchSize = 1000;

    /// <summary>Devices may buffer while offline, but their clock must not run ahead of ours.</summary>
    public static readonly TimeSpan MaxClockSkew = TimeSpan.FromMinutes(5);

    public static TelemetryBatchResult Parse(string body, DateTimeOffset receivedAt)
    {
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(body);
        }
        catch (JsonException e)
        {
            return new TelemetryBatchResult.Malformed($"The request body is not valid JSON: {e.Message}");
        }

        using (document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Array)
            {
                return new TelemetryBatchResult.Malformed("The request body must be a JSON array of telemetry pings.");
            }

            var count = document.RootElement.GetArrayLength();
            if (count is 0 or > MaxBatchSize)
            {
                return new TelemetryBatchResult.Invalid(new Dictionary<string, string[]>
                {
                    ["$"] = [$"A batch must contain between 1 and {MaxBatchSize} pings, got {count}."],
                });
            }

            var errors = new Dictionary<string, string[]>();
            var pings = new List<TelemetryPing>(count);
            var index = 0;
            foreach (var element in document.RootElement.EnumerateArray())
            {
                var prefix = $"[{index.ToString(CultureInfo.InvariantCulture)}]";
                try
                {
                    var ping = element.Deserialize<TelemetryPing>(TelemetryJson.Options);
                    if (ping is null)
                    {
                        errors[prefix] = ["A ping must be a JSON object."];
                    }
                    else
                    {
                        foreach (var (field, message) in Validate(ping, receivedAt))
                        {
                            errors[$"{prefix}.{field}"] = [message];
                        }

                        pings.Add(ping);
                    }
                }
                catch (JsonException e)
                {
                    errors[prefix] = [e.Message];
                }

                index++;
            }

            return errors.Count > 0
                ? new TelemetryBatchResult.Invalid(errors)
                : new TelemetryBatchResult.Valid(pings);
        }
    }

    /// <summary>Field rules for a single ping. Field names use the wire format.</summary>
    public static IEnumerable<(string Field, string Message)> Validate(TelemetryPing ping, DateTimeOffset receivedAt)
    {
        ArgumentNullException.ThrowIfNull(ping);

        if (string.IsNullOrWhiteSpace(ping.MessageId) || ping.MessageId.Length > 64)
        {
            yield return ("message_id", "Must be 1 to 64 characters.");
        }

        if (!VehicleIdPattern().IsMatch(ping.VehicleId))
        {
            yield return ("vehicle_id", "Must look like TRK-101 or VAN-206.");
        }

        if (ping.Ts > receivedAt + MaxClockSkew)
        {
            yield return ("ts", "Is in the future.");
        }

        if (ping.Gps.Lat is < -90 or > 90 || double.IsNaN(ping.Gps.Lat))
        {
            yield return ("gps.lat", "Must be between -90 and 90.");
        }

        if (ping.Gps.Lon is < -180 or > 180 || double.IsNaN(ping.Gps.Lon))
        {
            yield return ("gps.lon", "Must be between -180 and 180.");
        }

        if (ping.Gps.HeadingDeg is < 0 or > 359)
        {
            yield return ("gps.heading_deg", "Must be between 0 and 359.");
        }

        if (ping.SpeedKmh is < 0 or > 200 || double.IsNaN(ping.SpeedKmh))
        {
            yield return ("speed_kmh", "Must be between 0 and 200.");
        }

        if (ping.OdometerKm < 0 || double.IsNaN(ping.OdometerKm))
        {
            yield return ("odometer_km", "Must not be negative.");
        }

        if (ping.Fuel.LevelPct is < 0 or > 100 || double.IsNaN(ping.Fuel.LevelPct))
        {
            yield return ("fuel.level_pct", "Must be between 0 and 100.");
        }

        if (ping.Fuel.UsedL < 0 || double.IsNaN(ping.Fuel.UsedL))
        {
            yield return ("fuel.used_l", "Must not be negative.");
        }

        for (var i = 0; i < ping.Engine.Dtc.Count; i++)
        {
            if (!DtcPattern().IsMatch(ping.Engine.Dtc[i].Code))
            {
                yield return ($"engine.dtc[{i.ToString(CultureInfo.InvariantCulture)}].code", "Must be an OBD-II code such as P0301.");
            }
        }
    }

    [GeneratedRegex("^[A-Z]{3}-[0-9]{3}$")]
    private static partial Regex VehicleIdPattern();

    [GeneratedRegex("^[PCBU][0-9A-F]{4}$")]
    private static partial Regex DtcPattern();
}
