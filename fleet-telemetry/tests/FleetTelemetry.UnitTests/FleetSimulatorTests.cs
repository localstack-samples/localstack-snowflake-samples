using FleetTelemetry.Rules;
using FleetTelemetry.Simulation;

namespace FleetTelemetry.UnitTests;

/// <summary>
/// The integration tests compare Snowflake's answers with the simulator's ground truth,
/// so the simulator must be deterministic and its truth must follow the business rules.
/// </summary>
public sealed class FleetSimulatorTests
{
    private static readonly SimulatedVehicle[] Vehicles =
    [
        new("TRK-101", "tractor", 845, 51.8890, 4.4310),
        new("TRK-204", "rigid", 400, 51.2600, 4.3600),
        new("VAN-306", "van", 75, 51.3870, 6.6900),
    ];

    private static readonly string[] KnownCodes = ["P0420", "P0301", "P0087", "U0100", "P0217"];

    private static readonly IReadOnlyList<VehicleDay> Fleet =
        new FleetSimulator(seed: 7).Simulate(Vehicles, new DateOnly(2026, 9, 7), days: 5);

    [Fact]
    public void TheSameSeedProducesTheSamePings()
    {
        var again = new FleetSimulator(seed: 7).Simulate(Vehicles, new DateOnly(2026, 9, 7), days: 5);
        Assert.Equal(Fleet.SelectMany(d => d.Pings), again.SelectMany(d => d.Pings), PingComparer.Instance);

        var other = new FleetSimulator(seed: 8).Simulate(Vehicles, new DateOnly(2026, 9, 7), days: 5);
        Assert.NotEqual(Fleet.SelectMany(d => d.Pings), other.SelectMany(d => d.Pings), PingComparer.Instance);
    }

    [Fact]
    public void PingsAreFiveSecondsApartWithinATripAndStayOnTheirDay()
    {
        foreach (var day in Fleet)
        {
            Assert.All(day.Pings, p => Assert.Equal(day.Date, DateOnly.FromDateTime(p.Ts.UtcDateTime)));
            Assert.Equal(day.Pings.Count, day.Pings.Select(p => p.MessageId).Distinct().Count());
            foreach (var trip in day.Trips)
            {
                var pings = day.Pings.Where(p => p.Ts >= trip.StartedAt && p.Ts <= trip.EndedAt).ToList();
                Assert.Equal(trip.Samples, pings.Count);
                Assert.All(pings.Zip(pings.Skip(1)), pair => Assert.Equal(FleetSimulator.PingInterval, pair.Second.Ts - pair.First.Ts));
            }
        }
    }

    [Fact]
    public void SegmentingThePingsWithTheTripRuleGivesTheSimulatedTrips()
    {
        foreach (var day in Fleet)
        {
            var starts = day.Pings
                .Select((p, i) => (Ping: p, Gap: i == 0 ? (TimeSpan?)null : p.Ts - day.Pings[i - 1].Ts))
                .Where(x => TripRules.StartsNewTrip(x.Gap))
                .Select(x => x.Ping.Ts);
            Assert.Equal(day.Trips.Select(t => t.StartedAt), starts);
        }
    }

    [Fact]
    public void OnlyTheSimulatedHardStopsAreHarshBraking()
    {
        foreach (var day in Fleet)
        {
            var harsh = day.Pings.Zip(day.Pings.Skip(1))
                .Where(pair => pair.Second.Ts - pair.First.Ts <= TripRules.MaxPingGap)
                .Where(pair => DrivingEventRules.IsHarshBraking(pair.First.SpeedKmh, pair.Second.SpeedKmh, pair.Second.Ts - pair.First.Ts))
                .Select(pair => new HarshBrakeTruth(pair.Second.Ts, pair.First.SpeedKmh, pair.Second.SpeedKmh));
            Assert.Equal(day.HarshBrakes, harsh);
        }

        Assert.NotEmpty(Fleet.SelectMany(d => d.HarshBrakes));
    }

    [Fact]
    public void OnlyTheSimulatedLoadingDockWaitsAreIdling()
    {
        foreach (var day in Fleet)
        {
            Assert.Equal(day.Idling, StationaryRuns(day).Where(r => r.EndedAt - r.StartedAt >= DrivingEventRules.MinIdling));
        }

        Assert.NotEmpty(Fleet.SelectMany(d => d.Idling));
    }

    [Fact]
    public void FaultCodesComeFromTheCatalogAndStayActiveOnceRaised()
    {
        foreach (var day in Fleet)
        {
            var firstFault = day.Pings.ToList().FindIndex(p => p.Engine.Dtc.Count > 0);
            if (firstFault >= 0)
            {
                Assert.All(day.Pings.Skip(firstFault), p => Assert.NotEmpty(p.Engine.Dtc));
            }
        }

        var codes = Fleet.SelectMany(d => d.Pings).SelectMany(p => p.Engine.Dtc).Select(c => c.Code).Distinct();
        Assert.NotEmpty(codes);
        Assert.All(codes, c => Assert.Contains(c, KnownCodes));
    }

    private static IEnumerable<IdlingTruth> StationaryRuns(VehicleDay day)
    {
        DateTimeOffset? start = null;
        DateTimeOffset last = default;
        foreach (var ping in day.Pings)
        {
            var continues = start is not null && ping.SpeedKmh == 0 && ping.Ts - last <= TripRules.MaxPingGap;
            if (!continues && start is { } s)
            {
                yield return new IdlingTruth(s, last);
                start = null;
            }

            if (ping.SpeedKmh == 0)
            {
                start ??= ping.Ts;
            }

            last = ping.Ts;
        }

        if (start is { } open)
        {
            yield return new IdlingTruth(open, last);
        }
    }

    private sealed class PingComparer : IEqualityComparer<Telemetry.TelemetryPing>
    {
        public static readonly PingComparer Instance = new();

        public bool Equals(Telemetry.TelemetryPing? x, Telemetry.TelemetryPing? y) =>
            x is not null && y is not null
            && x with { Engine = x.Engine with { Dtc = [] } } == y with { Engine = y.Engine with { Dtc = [] } }
            && x.Engine.Dtc.SequenceEqual(y.Engine.Dtc);

        public int GetHashCode(Telemetry.TelemetryPing obj) => obj.MessageId.GetHashCode(StringComparison.Ordinal);
    }
}
