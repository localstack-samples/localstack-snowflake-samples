using FleetTelemetry.Kpis;

namespace FleetTelemetry.UnitTests;

public sealed class KpiMathTests
{
    [Theory]
    [InlineData(31.0, 100.0, 31.0)]
    [InlineData(7.081, 22.669, 31.2)] // 31.236 rounds to one decimal
    [InlineData(0.2, 1.0, 20.0)] // exactly the minimum distance
    public void FuelEconomyIsLitresPer100Km(double litres, double km, double expected) =>
        Assert.Equal(expected, KpiMath.FuelEconomyLPer100Km(litres, km));

    [Theory]
    [InlineData(0.05, 0.0)]
    [InlineData(0.05, 0.999)]
    public void FuelEconomyIsUnknownBelowOneKilometre(double litres, double km) =>
        Assert.Null(KpiMath.FuelEconomyLPer100Km(litres, km));

    [Theory]
    [InlineData(145, 1000, 0.145)]
    [InlineData(1, 3, 0.333)]
    [InlineData(0, 0, 0.0)] // no samples: nothing was idle
    [InlineData(12, 12, 1.0)]
    public void IdleShareIsTheStationaryFractionOfSamples(long idle, long samples, double expected) =>
        Assert.Equal(expected, KpiMath.IdleShare(idle, samples));

    [Fact]
    public void IdleShareRejectsMoreIdleSamplesThanSamples() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => KpiMath.IdleShare(11, 10));

    [Fact]
    public void AverageSpeedIncludesStops()
    {
        // 25.754 km in 1,550 s (25 min 50 s) is 59.82 km/h.
        Assert.Equal(59.8, KpiMath.AverageSpeedKmh(25.754, 1_550));
        Assert.Null(KpiMath.AverageSpeedKmh(0, 0));
    }
}
