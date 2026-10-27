namespace FleetTelemetry.Telemetry;

/// <summary>
/// One telemetry message as a vehicle sends it. Serialized with <see cref="TelemetryJson.Options"/>
/// (snake_case), which is also the shape stored in the landing table.
/// </summary>
public sealed record TelemetryPing(
    string MessageId,
    string VehicleId,
    DateTimeOffset Ts,
    GpsFix Gps,
    double SpeedKmh,
    double OdometerKm,
    FuelReading Fuel,
    EngineReading Engine);

public sealed record GpsFix(double Lat, double Lon, int HeadingDeg);

public sealed record FuelReading(double LevelPct, double UsedL);

/// <summary>Engine diagnostics, including the active diagnostic trouble codes (DTCs).</summary>
public sealed record EngineReading(int Rpm, double CoolantC, IReadOnlyList<FaultCode> Dtc);

public sealed record FaultCode(string Code, string Severity);
