using FleetTelemetry.Kpis;

namespace FleetTelemetry.UnitTests;

public sealed class FleetDailyKpisTests
{
    private static readonly DateOnly Monday = new(2026, 9, 7);

    [Fact]
    public void FleetFuelEconomyIsTotalFuelOverTotalDistance()
    {
        var days = FleetDailyKpis.Summarize(
        [
            Day(Monday, "TRK-101", distanceKm: 400, fuelL: 120), // 30 L/100 km
            Day(Monday, "VAN-106", distanceKm: 2, fuelL: 1), // 50 L/100 km, but only 2 km
        ]);

        // (120 + 1) / (400 + 2) * 100 = 30.1, not the 40.0 a mean of the two ratios would give.
        var monday = Assert.Single(days);
        Assert.Equal(30.1, monday.FuelEconomyLPer100Km);
        Assert.Equal(402.0, monday.DistanceKm);
        Assert.Equal(121.0, monday.FuelUsedL);
    }

    [Fact]
    public void RollsUpVehiclesPerDayInDateOrder()
    {
        var tuesday = Monday.AddDays(1);
        var days = FleetDailyKpis.Summarize(
        [
            Day(tuesday, "TRK-101", samples: 100, idleSamples: 10, maxSpeedKmh: 81, faultSamples: 0),
            Day(Monday, "TRK-101", samples: 300, idleSamples: 30, maxSpeedKmh: 80, faultSamples: 12),
            Day(Monday, "VAN-106", samples: 100, idleSamples: 50, maxSpeedKmh: 62.5, faultSamples: 0),
        ]);

        Assert.Equal([Monday, tuesday], days.Select(d => d.Day));
        Assert.Equal(2, days[0].ActiveVehicles);
        Assert.Equal(0.2, days[0].IdleShare); // (30 + 50) / (300 + 100)
        Assert.Equal(80, days[0].MaxSpeedKmh);
        Assert.Equal(1, days[0].VehiclesWithFaults);
        Assert.Equal(1, days[1].ActiveVehicles);
        Assert.Equal(0, days[1].VehiclesWithFaults);
    }

    [Fact]
    public void ADayWithoutDistanceHasNoFuelEconomy()
    {
        var day = Assert.Single(FleetDailyKpis.Summarize([Day(Monday, "TRK-101", distanceKm: 0.4, fuelL: 0.3)]));
        Assert.Null(day.FuelEconomyLPer100Km);
    }

    [Fact]
    public void NoVehicleDaysGiveNoFleetDays() => Assert.Empty(FleetDailyKpis.Summarize([]));

    private static VehicleDayKpi Day(
        DateOnly day,
        string vehicleId,
        double distanceKm = 100,
        double fuelL = 30,
        long samples = 1000,
        long idleSamples = 100,
        double maxSpeedKmh = 80,
        long faultSamples = 0) =>
        new(day, vehicleId, samples, idleSamples, distanceKm, fuelL, maxSpeedKmh, faultSamples);
}
