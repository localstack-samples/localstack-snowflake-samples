using System.Net.Http.Json;
using System.Text;
using FleetTelemetry.Api.Endpoints;
using FleetTelemetry.Data;
using FleetTelemetry.IntegrationTests.Infrastructure;
using FleetTelemetry.Simulation;
using Microsoft.Extensions.DependencyInjection;

namespace FleetTelemetry.IntegrationTests;

/// <summary>Daily fleet KPIs, served from the FLEET_DAILY_KPIS dynamic table.</summary>
public sealed class FleetKpiTests(FleetKpiTests.Fixture fixture) : IClassFixture<FleetKpiTests.Fixture>
{
    private static readonly DateOnly FirstDay = new(2026, 9, 7);

    [Fact]
    public async Task DailyKpisAreComputedFromTheBackfilledTelemetry()
    {
        var response = await fixture.Api.GetFromJsonAsync<FleetDaily>(
            "/api/fleet/daily?from=2026-09-07&to=2026-09-09", TestContext.Current.CancellationToken);

        Assert.NotNull(response);
        Assert.Equal(Expected.FleetDays(fixture.Backfilled), response.Days.Select(AsExpected));
    }

    [Fact]
    public async Task TheRangeIsInclusive()
    {
        var response = await fixture.Api.GetFromJsonAsync<FleetDaily>(
            "/api/fleet/daily?from=2026-09-08&to=2026-09-08", TestContext.Current.CancellationToken);

        Assert.NotNull(response);
        Assert.Equal([FirstDay.AddDays(1)], response.Days.Select(d => d.Day));
    }

    [Fact]
    public async Task LiveTelemetryShowsUpOnceTheDynamicTableRefreshes()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var batch = TelemetryExport.ToJsonArray(fixture.Live.Pings);
        using (var post = await fixture.Api.PostAsync(
            "/api/telemetry", new StringContent(batch, Encoding.UTF8, "application/json"), cancellationToken))
        {
            post.EnsureSuccessStatusCode();
        }

        await using (var scope = fixture.CreateScope())
        {
            await scope.ServiceProvider.GetRequiredService<FleetAnalytics>().RefreshAsync(cancellationToken);
        }

        var day = fixture.Live.Date;
        var response = await fixture.Api.GetFromJsonAsync<FleetDaily>(
            $"/api/fleet/daily?from={day:yyyy-MM-dd}&to={day:yyyy-MM-dd}", cancellationToken);

        Assert.NotNull(response);
        Assert.Equal(Expected.FleetDays([fixture.Live]), response.Days.Select(AsExpected));
    }

    private static Expected.FleetDay AsExpected(FleetTelemetry.Kpis.FleetDayKpi d) => new(
        d.Day, d.ActiveVehicles, d.DistanceKm, d.FuelUsedL, d.FuelEconomyLPer100Km ?? double.NaN, d.IdleShare, d.MaxSpeedKmh, d.VehiclesWithFaults);

    public sealed class Fixture(SnowflakeTarget target) : FleetDatabaseFixture(target)
    {
        /// <summary>Four vehicles, 7 to 9 September, bulk-loaded.</summary>
        public IReadOnlyList<VehicleDay> Backfilled { get; private set; } = [];

        /// <summary>One vehicle's first trip on 10 September, posted by a test.</summary>
        public VehicleDay Live { get; private set; } = null!;

        protected override async Task LoadAsync(CancellationToken cancellationToken)
        {
            var fleet = new FleetSimulator(seed: 21).Simulate(await VehiclesAsync(4, cancellationToken), FirstDay, days: 4);
            Backfilled = fleet.Where(d => d.Date < FirstDay.AddDays(3)).ToList();
            var live = fleet.First(d => d.Date == FirstDay.AddDays(3));
            Live = live with { Pings = live.Pings.Where(p => p.Ts <= live.Trips[0].EndedAt).ToList() };

            var directory = Path.Combine(Path.GetTempPath(), $"fleet-kpis-{Guid.NewGuid():N}");
            TelemetryExport.WriteNdjson(directory, Backfilled);
            await using var scope = CreateScope();
            await scope.ServiceProvider.GetRequiredService<TelemetryBackfill>().RunAsync(directory, cancellationToken);
            Directory.Delete(directory, recursive: true);
        }
    }
}
