namespace FleetTelemetry.Rules;

/// <summary>
/// How pings are grouped into trips. Devices report every few seconds while the ignition
/// is on and stop reporting when it is off, so a silence longer than
/// <see cref="MaxPingGap"/> ends a trip. The trips query applies the same rule in SQL.
/// </summary>
public static class TripRules
{
    public static readonly TimeSpan MaxPingGap = TimeSpan.FromMinutes(10);

    /// <param name="sincePreviousPing">Time since the vehicle's previous ping, or null for its first ping.</param>
    public static bool StartsNewTrip(TimeSpan? sincePreviousPing) =>
        sincePreviousPing is not { } gap || gap > MaxPingGap;
}
