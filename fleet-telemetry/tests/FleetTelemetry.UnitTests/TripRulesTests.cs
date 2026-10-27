using FleetTelemetry.Rules;

namespace FleetTelemetry.UnitTests;

public sealed class TripRulesTests
{
    [Fact]
    public void FirstPingOfAVehicleStartsATrip() => Assert.True(TripRules.StartsNewTrip(null));

    [Theory]
    [InlineData(5)] // the regular ping interval
    [InlineData(120)] // a traffic jam with the ignition on
    [InlineData(600)] // exactly ten minutes: still the same trip
    public void GapsUpToTenMinutesContinueTheTrip(int seconds) =>
        Assert.False(TripRules.StartsNewTrip(TimeSpan.FromSeconds(seconds)));

    [Theory]
    [InlineData(601)]
    [InlineData(3_600)]
    public void GapsLongerThanTenMinutesStartANewTrip(int seconds) =>
        Assert.True(TripRules.StartsNewTrip(TimeSpan.FromSeconds(seconds)));
}
