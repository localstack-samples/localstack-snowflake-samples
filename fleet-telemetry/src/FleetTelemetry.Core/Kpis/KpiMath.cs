namespace FleetTelemetry.Kpis;

/// <summary>Derived fleet metrics, shared by trip and daily KPI responses.</summary>
public static class KpiMath
{
    /// <summary>Below this distance a fuel economy figure is noise (a vehicle moved in the yard).</summary>
    public const double MinDistanceForEconomyKm = 1.0;

    /// <summary>Litres per 100 km, rounded to 0.1, or null when the distance is too short to say.</summary>
    public static double? FuelEconomyLPer100Km(double fuelUsedL, double distanceKm) =>
        distanceKm < MinDistanceForEconomyKm ? null : Math.Round(fuelUsedL / distanceKm * 100, 1);

    /// <summary>Share of ignition-on samples with the vehicle stationary, 0 to 1, rounded to 0.001.</summary>
    public static double IdleShare(long idleSamples, long samples)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(idleSamples);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(idleSamples, samples);
        return samples == 0 ? 0 : Math.Round((double)idleSamples / samples, 3);
    }

    /// <summary>Average speed over a trip including stops, rounded to 0.1, or null for a zero-length trip.</summary>
    public static double? AverageSpeedKmh(double distanceKm, long durationSeconds) =>
        durationSeconds <= 0 ? null : Math.Round(distanceKm / (durationSeconds / 3600.0), 1);
}
