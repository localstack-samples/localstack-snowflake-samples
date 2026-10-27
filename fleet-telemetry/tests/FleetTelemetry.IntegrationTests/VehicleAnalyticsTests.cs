using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FleetTelemetry.Api.Endpoints;
using FleetTelemetry.Data;
using FleetTelemetry.IntegrationTests.Infrastructure;
using FleetTelemetry.Simulation;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;

namespace FleetTelemetry.IntegrationTests;

/// <summary>
/// Trips, driving events and faults over HTTP, compared with what the simulator drove.
/// Four vehicles over two days are bulk-loaded once for the class.
/// </summary>
public sealed class VehicleAnalyticsTests(VehicleAnalyticsTests.Fixture fixture) : IClassFixture<VehicleAnalyticsTests.Fixture>
{
    private static readonly DateOnly FirstDay = new(2026, 9, 7);

    [Fact]
    public async Task TripsAreTheDrivesBetweenIgnitionOffPeriods()
    {
        foreach (var day in fixture.Fleet)
        {
            var response = await fixture.Api.GetFromJsonAsync<VehicleTrips>(
                $"/api/vehicles/{day.VehicleId}/trips?date={day.Date:yyyy-MM-dd}", TestContext.Current.CancellationToken);

            Assert.NotNull(response);
            Assert.Equal(day.Trips.Count, response.Trips.Count);
            foreach (var (expected, actual) in day.Trips.Zip(response.Trips))
            {
                Assert.Equal(expected.StartedAt.UtcDateTime, actual.StartedAt);
                Assert.Equal(expected.EndedAt.UtcDateTime, actual.EndedAt);
                Assert.Equal((long)(expected.EndedAt - expected.StartedAt).TotalSeconds, actual.DurationS);
                Assert.Equal(expected.DistanceKm, actual.DistanceKm, 3);
                Assert.Equal(expected.MaxSpeedKmh, actual.MaxSpeedKmh);
                Assert.Equal(expected.Samples, actual.Samples);
            }
        }
    }

    [Fact]
    public async Task HarshBrakingEventsAreTheHardStops()
    {
        Assert.NotEmpty(fixture.Fleet.SelectMany(d => d.HarshBrakes)); // the data must contain some
        foreach (var vehicle in fixture.Fleet.GroupBy(d => d.VehicleId))
        {
            var response = await fixture.Api.GetFromJsonAsync<VehicleEvents<HarshBrakingEvent>>(
                $"/api/vehicles/{vehicle.Key}/events?type=harsh_braking", TestContext.Current.CancellationToken);

            var expected = vehicle.SelectMany(d => d.HarshBrakes).ToList();
            Assert.NotNull(response);
            Assert.Equal(expected.Select(e => e.At.UtcDateTime), response.Events.Select(e => e.OccurredAt));
            Assert.Equal(expected.Select(e => (e.FromKmh, e.ToKmh)), response.Events.Select(e => (e.FromSpeedKmh, e.ToSpeedKmh)));
            // Pings are 5 s apart: deceleration = speed drop in m/s over 5 s.
            Assert.Equal(
                expected.Select(e => Math.Round((e.FromKmh - e.ToKmh) / 3.6 / 5, 2)),
                response.Events.Select(e => e.DecelerationMs2));
        }
    }

    [Fact]
    public async Task IdlingEventsAreTheLoadingDockWaits()
    {
        Assert.NotEmpty(fixture.Fleet.SelectMany(d => d.Idling));
        foreach (var vehicle in fixture.Fleet.GroupBy(d => d.VehicleId))
        {
            var response = await fixture.Api.GetFromJsonAsync<VehicleEvents<IdlingEvent>>(
                $"/api/vehicles/{vehicle.Key}/events?type=idling", TestContext.Current.CancellationToken);

            var expected = vehicle.SelectMany(d => d.Idling).ToList();
            Assert.NotNull(response);
            Assert.Equal(
                expected.Select(e => (e.StartedAt.UtcDateTime, e.EndedAt.UtcDateTime, (long)(e.EndedAt - e.StartedAt).TotalSeconds)),
                response.Events.Select(e => (e.StartedAt, e.EndedAt, e.DurationS)));
        }
    }

    [Fact]
    public async Task EventsCanBeLimitedToOneDay()
    {
        var vehicle = fixture.Fleet.Where(d => d.Date == FirstDay && d.HarshBrakes.Count > 0).Select(d => d.VehicleId).First();
        var response = await fixture.Api.GetFromJsonAsync<VehicleEvents<HarshBrakingEvent>>(
            $"/api/vehicles/{vehicle}/events?type=harsh_braking&from={FirstDay:yyyy-MM-dd}&to={FirstDay:yyyy-MM-dd}",
            TestContext.Current.CancellationToken);

        var expected = fixture.Fleet.Single(d => d.VehicleId == vehicle && d.Date == FirstDay).HarshBrakes;
        Assert.NotNull(response);
        Assert.Equal(expected.Select(e => e.At.UtcDateTime), response.Events.Select(e => e.OccurredAt));
    }

    [Fact]
    public async Task FaultCodesAreUnnestedFromEachPingsDtcArray()
    {
        var byVehicle = fixture.Fleet.GroupBy(d => d.VehicleId).ToList();
        Assert.Contains(byVehicle, v => v.Any(d => d.Pings.Any(p => p.Engine.Dtc.Count > 0)));
        Assert.Contains(byVehicle, v => v.All(d => d.Pings.All(p => p.Engine.Dtc.Count == 0)));

        foreach (var vehicle in byVehicle)
        {
            var response = await fixture.Api.GetFromJsonAsync<VehicleFaults>(
                $"/api/vehicles/{vehicle.Key}/faults", TestContext.Current.CancellationToken);

            Assert.NotNull(response);
            Assert.Equal(
                Expected.Faults(vehicle.SelectMany(d => d.Pings)),
                response.Faults.Select(f => new Expected.Fault(f.Code, f.Severity, (int)f.Occurrences, f.FirstSeen, f.LastSeen, f.MaxCoolantC)));
        }
    }

    [Fact]
    public async Task EveryQueryIsTaggedWithTheRouteAndTheCallersTraceId()
    {
        var traceId = ActivityTraceId.CreateRandom().ToHexString();
        using var request = new HttpRequestMessage(HttpMethod.Get, "/api/vehicles/TRK-101/trips?date=2026-09-07");
        request.Headers.Add("traceparent", $"00-{traceId}-{ActivitySpanId.CreateRandom().ToHexString()}-01");
        using (var response = await fixture.Api.SendAsync(request, TestContext.Current.CancellationToken))
        {
            response.EnsureSuccessStatusCode();
        }

        var queries = await fixture.QueryAsync<(string Text, string Tag)>(
            """
            SELECT QUERY_TEXT, QUERY_TAG
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
            WHERE QUERY_TAG LIKE :pattern AND QUERY_TEXT NOT ILIKE 'ALTER SESSION%'
            """,
            new { pattern = $"%{traceId}%" });

        // The registry lookup and the trips query, both from this one request.
        Assert.Contains(queries, q => q.Text.Contains("FROM VEHICLES", StringComparison.Ordinal));
        Assert.Contains(queries, q => q.Text.Contains("TRIP_NO", StringComparison.Ordinal));
        Assert.All(queries, q =>
        {
            using var tag = JsonDocument.Parse(q.Tag);
            Assert.Equal("GET /api/vehicles/{vehicleId}/trips", tag.RootElement.GetProperty("op").GetString());
            Assert.Equal(traceId, tag.RootElement.GetProperty("id").GetString());
        });
    }

    [Theory]
    [InlineData("/api/vehicles/TRK-999/trips?date=2026-09-07")]
    [InlineData("/api/vehicles/TRK-999/events?type=harsh_braking")]
    [InlineData("/api/vehicles/TRK-999/faults")]
    public async Task AnUnknownVehicleIsA404Problem(string url)
    {
        using var response = await fixture.Api.GetAsync(url, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
        var problem = await response.Content.ReadFromJsonAsync<ProblemDetails>(TestContext.Current.CancellationToken);
        Assert.Equal("urn:fleet-telemetry:problem:vehicle-not-found", problem?.Type);
        Assert.Equal("No vehicle 'TRK-999' in the fleet registry.", problem?.Detail);
    }

    [Theory]
    [InlineData("/api/vehicles/TRK-101/trips", "date")] // required
    [InlineData("/api/vehicles/TRK-101/trips?date=yesterday", "date")]
    [InlineData("/api/vehicles/TRK-101/events?type=speeding", "type")]
    [InlineData("/api/vehicles/TRK-101/faults?from=2026-09-08&to=2026-09-07", "to")]
    [InlineData("/api/fleet/daily?from=2026-09-08&to=2026-09-07", "to")]
    public async Task InvalidQueryParametersAreA400Problem(string url, string parameter)
    {
        using var response = await fixture.Api.GetAsync(url, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
        var problem = await response.Content.ReadFromJsonAsync<HttpValidationProblemDetails>(TestContext.Current.CancellationToken);
        Assert.NotNull(problem);
        Assert.Equal("urn:fleet-telemetry:problem:validation", problem.Type);
        Assert.True(
            problem.Errors.ContainsKey(parameter) || (problem.Detail?.Contains(parameter, StringComparison.Ordinal) ?? false),
            $"Expected the problem to name '{parameter}', got errors [{string.Join(", ", problem.Errors.Keys)}], detail '{problem.Detail}'.");
    }

    public sealed class Fixture(SnowflakeTarget target) : FleetDatabaseFixture(target)
    {
        public IReadOnlyList<VehicleDay> Fleet { get; private set; } = [];

        protected override async Task LoadAsync(CancellationToken cancellationToken)
        {
            Fleet = new FleetSimulator(seed: 3).Simulate(await VehiclesAsync(4, cancellationToken), FirstDay, days: 2);
            var directory = Path.Combine(Path.GetTempPath(), $"fleet-analytics-{Guid.NewGuid():N}");
            TelemetryExport.WriteNdjson(directory, Fleet);
            await using var scope = CreateScope();
            await scope.ServiceProvider.GetRequiredService<TelemetryBackfill>().RunAsync(directory, cancellationToken);
            Directory.Delete(directory, recursive: true);
        }
    }
}
