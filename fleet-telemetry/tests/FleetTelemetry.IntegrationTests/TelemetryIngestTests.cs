using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json.Nodes;
using FleetTelemetry.Api.Endpoints;
using FleetTelemetry.IntegrationTests.Infrastructure;
using FleetTelemetry.Simulation;
using FleetTelemetry.Telemetry;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FleetTelemetry.IntegrationTests;

/// <summary>POST /api/telemetry against an empty database. Each test uses its own vehicle-day.</summary>
public sealed class TelemetryIngestTests(TelemetryIngestTests.Fixture fixture) : IClassFixture<TelemetryIngestTests.Fixture>
{
    [Fact]
    public async Task AnAcceptedBatchIsQueryableAsATrip()
    {
        var day = fixture.Day(vehicle: 0, dayOffset: 0);
        var trip = day.Trips[0];
        var pings = day.Pings.Where(p => p.Ts <= trip.EndedAt).ToList();

        var accepted = await PostAsync(TelemetryExport.ToJsonArray(pings));

        Assert.Equal(pings.Count, accepted);
        var trips = await GetTripsAsync(day.VehicleId, day.Date);
        var only = Assert.Single(trips);
        Assert.Equal((trip.StartedAt.UtcDateTime, trip.EndedAt.UtcDateTime, (long)trip.Samples), (only.StartedAt, only.EndedAt, only.Samples));
        Assert.Equal(trip.DistanceKm, only.DistanceKm, 3);
    }

    [Fact]
    public async Task ARetriedBatchIsCountedOnce()
    {
        var day = fixture.Day(vehicle: 1, dayOffset: 0);
        var pings = day.Pings.Where(p => p.Ts <= day.Trips[0].EndedAt).ToList();
        var batch = TelemetryExport.ToJsonArray(pings);

        await PostAsync(batch);
        await PostAsync(batch); // the device did not see the first 202 and sent it again

        Assert.Equal(2L * pings.Count, await RawCountAsync(day.VehicleId));
        Assert.Equal(pings.Count, Assert.Single(await GetTripsAsync(day.VehicleId, day.Date)).Samples);
    }

    [Fact]
    public async Task APauseOfExactlyTenMinutesKeepsTheTripOneSecondMoreStartsANewOne()
    {
        // Business rule: more than 10 minutes without a ping ends a trip.
        var start = new DateTimeOffset(2026, 9, 14, 7, 0, 0, TimeSpan.Zero);
        var times = new[]
        {
            start,
            start.AddSeconds(5),
            start.AddSeconds(605), // 600 s gap: same trip
            start.AddSeconds(610),
            start.AddSeconds(1211), // 601 s gap: new trip
            start.AddSeconds(1216),
        };

        await PostAsync(TelemetryExport.ToJsonArray(times.Select((t, i) => Ping("TRK-105", t, odometerKm: 1000 + i))));

        var trips = await GetTripsAsync("TRK-105", DateOnly.FromDateTime(start.UtcDateTime));
        Assert.Equal(
            [(times[0].UtcDateTime, times[3].UtcDateTime, 610L, 4L), (times[4].UtcDateTime, times[5].UtcDateTime, 5L, 2L)],
            trips.Select(t => (t.StartedAt, t.EndedAt, t.DurationS, t.Samples)));
    }

    [Fact]
    public async Task MalformedJsonIsA400ProblemAndWritesNothing()
    {
        var day = fixture.Day(vehicle: 2, dayOffset: 0);
        var batch = TelemetryExport.ToJsonArray(day.Pings.Take(10));

        using var response = await fixture.Api.PostAsync(
            "/api/telemetry", Json(batch[..^20]), TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var problem = await response.Content.ReadFromJsonAsync<ProblemDetails>(TestContext.Current.CancellationToken);
        Assert.Equal("urn:fleet-telemetry:problem:malformed-telemetry", problem?.Type);
        Assert.Equal(0, await RawCountAsync(day.VehicleId));
    }

    [Fact]
    public async Task OneInvalidPingRejectsTheWholeBatchWithFieldErrors()
    {
        var day = fixture.Day(vehicle: 3, dayOffset: 0);
        var pings = JsonNode.Parse(TelemetryExport.ToJsonArray(day.Pings.Take(10)))!.AsArray();
        pings[3]!["speed_kmh"] = 412;
        pings[7]!["gps"]!["lon"] = -190;

        using var response = await fixture.Api.PostAsync(
            "/api/telemetry", Json(pings.ToJsonString()), TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var problem = await response.Content.ReadFromJsonAsync<HttpValidationProblemDetails>(TestContext.Current.CancellationToken);
        Assert.Equal(["[3].speed_kmh", "[7].gps.lon"], problem!.Errors.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(0, await RawCountAsync(day.VehicleId));
    }

    private async Task<int> PostAsync(string batch)
    {
        using var response = await fixture.Api.PostAsync("/api/telemetry", Json(batch), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        return (await response.Content.ReadFromJsonAsync<TelemetryAccepted>(TestContext.Current.CancellationToken))!.Accepted;
    }

    private async Task<IReadOnlyList<Trip>> GetTripsAsync(string vehicleId, DateOnly date) =>
        (await fixture.Api.GetFromJsonAsync<VehicleTrips>(
            $"/api/vehicles/{vehicleId}/trips?date={date:yyyy-MM-dd}", TestContext.Current.CancellationToken))!.Trips;

    private Task<long> RawCountAsync(string vehicleId) => fixture.QuerySingleAsync<long>(
        "SELECT COUNT(*) FROM TELEMETRY_RAW WHERE PAYLOAD:vehicle_id::VARCHAR = :vehicleId", new { vehicleId });

    private static StringContent Json(string body) => new(body, Encoding.UTF8, "application/json");

    private static TelemetryPing Ping(string vehicleId, DateTimeOffset ts, double odometerKm) => new(
        $"{vehicleId}-{ts:yyyyMMddHHmmss}",
        vehicleId,
        ts,
        new GpsFix(51.9, 4.4, 90),
        SpeedKmh: 30,
        odometerKm,
        new FuelReading(80, 1000),
        new EngineReading(1300, 88, []));

    public sealed class Fixture(SnowflakeTarget target) : FleetDatabaseFixture(target)
    {
        private IReadOnlyList<VehicleDay> _fleet = [];

        public VehicleDay Day(int vehicle, int dayOffset) => _fleet
            .Where(d => d.VehicleId == _fleet.Select(f => f.VehicleId).Distinct().ElementAt(vehicle))
            .ElementAt(dayOffset);

        protected override async Task LoadAsync(CancellationToken cancellationToken) =>
            _fleet = new FleetSimulator(seed: 5).Simulate(await VehiclesAsync(4, cancellationToken), new DateOnly(2026, 9, 7), days: 1);
    }
}
