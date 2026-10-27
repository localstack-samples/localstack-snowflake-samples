using FleetTelemetry.Rules;

namespace FleetTelemetry.UnitTests;

public sealed class DrivingEventRulesTests
{
    private static readonly TimeSpan FiveSeconds = TimeSpan.FromSeconds(5);

    [Fact]
    public void DecelerationIsTheSpeedDropInMetresPerSecondPerSecond()
    {
        // 72 km/h is 20 m/s; losing it in 5 s is 4 m/s².
        Assert.Equal(4.0, DrivingEventRules.DecelerationMs2(72, 0, FiveSeconds), 9);
        Assert.Equal(-1.0, DrivingEventRules.DecelerationMs2(0, 18, FiveSeconds), 9);
    }

    [Theory]
    [InlineData(45, 0, true)] // 12.5 m/s in 5 s = 2.5 m/s², exactly the threshold
    [InlineData(80, 20, true)] // 3.33 m/s²
    [InlineData(44.9, 0, false)] // just below
    [InlineData(80, 66, false)] // ordinary braking, 0.78 m/s²
    [InlineData(0, 50, false)] // accelerating
    public void HarshBrakingStartsAtTheThreshold(double fromKmh, double toKmh, bool harsh) =>
        Assert.Equal(harsh, DrivingEventRules.IsHarshBraking(fromKmh, toKmh, FiveSeconds));

    [Fact]
    public void TheSameDropOverALongerIntervalIsNotHarsh() =>
        Assert.False(DrivingEventRules.IsHarshBraking(45, 0, TimeSpan.FromSeconds(6)));

    [Fact]
    public void PingsOutOfOrderAreRejected() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => DrivingEventRules.DecelerationMs2(50, 0, TimeSpan.Zero));
}
