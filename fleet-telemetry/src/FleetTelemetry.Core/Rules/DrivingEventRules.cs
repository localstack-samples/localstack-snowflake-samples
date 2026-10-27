namespace FleetTelemetry.Rules;

/// <summary>
/// Thresholds for the driving events dashboards report. The events queries compute the
/// same values in SQL from consecutive pings.
/// </summary>
public static class DrivingEventRules
{
    /// <summary>About 0.25 g, a common harsh-braking threshold for trucks and vans.</summary>
    public const double HarshBrakingMs2 = 2.5;

    /// <summary>A stationary vehicle with the engine running for this long is idling.</summary>
    public static readonly TimeSpan MinIdling = TimeSpan.FromMinutes(5);

    /// <summary>Average deceleration in m/s² between two pings (negative when accelerating).</summary>
    public static double DecelerationMs2(double fromKmh, double toKmh, TimeSpan elapsed)
    {
        if (elapsed <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(elapsed), elapsed, "Pings must be in time order.");
        }

        return (fromKmh - toKmh) / 3.6 / elapsed.TotalSeconds;
    }

    public static bool IsHarshBraking(double fromKmh, double toKmh, TimeSpan elapsed) =>
        DecelerationMs2(fromKmh, toKmh, elapsed) >= HarshBrakingMs2;
}
