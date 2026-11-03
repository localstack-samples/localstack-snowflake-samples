namespace FleetTelemetry.Data;

// Result rows as Snowflake returns them. Timestamps are UTC (TIMESTAMP_NTZ).

public sealed class TripRow
{
    public long TripNo { get; init; }

    public DateTime StartedAt { get; init; }

    public DateTime EndedAt { get; init; }

    public long DurationS { get; init; }

    public double DistanceKm { get; init; }

    public double FuelUsedL { get; init; }

    public double MaxSpeedKmh { get; init; }

    public long IdleS { get; init; }

    public long Samples { get; init; }
}

public sealed class HarshBrakingRow
{
    public DateTime OccurredAt { get; init; }

    public double Lat { get; init; }

    public double Lon { get; init; }

    public double FromSpeedKmh { get; init; }

    public double ToSpeedKmh { get; init; }

    public double DecelerationMs2 { get; init; }
}

public sealed class IdlingRow
{
    public DateTime StartedAt { get; init; }

    public DateTime EndedAt { get; init; }

    public long DurationS { get; init; }

    public double Lat { get; init; }

    public double Lon { get; init; }
}

public sealed class FaultRow
{
    public string Code { get; init; } = "";

    public string Severity { get; init; } = "";

    public long Occurrences { get; init; }

    public DateTime FirstSeen { get; init; }

    public DateTime LastSeen { get; init; }

    public double MaxCoolantC { get; init; }
}
