using System.Text.Json.Nodes;
using FleetTelemetry.Telemetry;

namespace FleetTelemetry.UnitTests;

public sealed class TelemetryBatchParserTests
{
    private static readonly DateTimeOffset ReceivedAt = new(2026, 9, 10, 8, 0, 0, TimeSpan.Zero);

    [Fact]
    public void AcceptsAnArrayOfValidPings()
    {
        var result = TelemetryBatchParser.Parse(Batch(Ping(), Ping("TRK-101-2"), Ping("TRK-101-3")), ReceivedAt);

        var valid = Assert.IsType<TelemetryBatchResult.Valid>(result);
        Assert.Equal(["TRK-101-1", "TRK-101-2", "TRK-101-3"], valid.Pings.Select(p => p.MessageId));
        Assert.Equal("P0301", valid.Pings[0].Engine.Dtc[0].Code);
    }

    [Fact]
    public void ConvertsAnyUtcOffset()
    {
        var ping = Ping();
        ping["ts"] = "2026-09-10T09:30:00+02:00";

        var valid = Assert.IsType<TelemetryBatchResult.Valid>(TelemetryBatchParser.Parse(Batch(ping), ReceivedAt));
        Assert.Equal(new DateTimeOffset(2026, 9, 10, 7, 30, 0, TimeSpan.Zero), valid.Pings[0].Ts);
    }

    [Theory]
    [InlineData("[{\"message_id\": ")]
    [InlineData("not json")]
    [InlineData("{\"message_id\": \"x\"}")] // an object, not an array
    public void RejectsABodyThatIsNotAJsonArray(string body) =>
        Assert.IsType<TelemetryBatchResult.Malformed>(TelemetryBatchParser.Parse(body, ReceivedAt));

    [Fact]
    public void RejectsAnEmptyBatch()
    {
        var invalid = Assert.IsType<TelemetryBatchResult.Invalid>(TelemetryBatchParser.Parse("[]", ReceivedAt));
        Assert.Equal(["$"], invalid.Errors.Keys);
    }

    [Fact]
    public void RejectsABatchOverTheLimit()
    {
        var pings = Enumerable.Range(0, TelemetryBatchParser.MaxBatchSize + 1).Select(i => Ping($"m-{i}")).ToArray();
        var invalid = Assert.IsType<TelemetryBatchResult.Invalid>(TelemetryBatchParser.Parse(Batch(pings), ReceivedAt));
        Assert.Equal(["$"], invalid.Errors.Keys);
    }

    [Fact]
    public void ReportsEveryBrokenFieldByPingIndex()
    {
        var bad = Ping();
        bad["vehicle_id"] = "truck 7";
        bad["speed_kmh"] = -3;
        bad["gps"]!["lat"] = 91;
        bad["fuel"]!["level_pct"] = 101;
        bad["engine"]!["dtc"]![0]!["code"] = "X123";

        var invalid = Assert.IsType<TelemetryBatchResult.Invalid>(TelemetryBatchParser.Parse(Batch(Ping(), bad), ReceivedAt));

        Assert.Equal(
            ["[1].engine.dtc[0].code", "[1].fuel.level_pct", "[1].gps.lat", "[1].speed_kmh", "[1].vehicle_id"],
            invalid.Errors.Keys.Order(StringComparer.Ordinal));
    }

    [Fact]
    public void ReportsMissingRequiredFields()
    {
        var incomplete = Ping();
        incomplete.Remove("odometer_km");

        var invalid = Assert.IsType<TelemetryBatchResult.Invalid>(TelemetryBatchParser.Parse(Batch(incomplete), ReceivedAt));
        Assert.Contains("odometer_km", Assert.Single(invalid.Errors["[0]"]), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(300, true)] // five minutes ahead of our clock is tolerated
    [InlineData(301, false)]
    public void ToleratesSmallDeviceClockSkew(int secondsAhead, bool accepted)
    {
        var ping = Ping();
        ping["ts"] = ReceivedAt.AddSeconds(secondsAhead).ToString("O");

        var result = TelemetryBatchParser.Parse(Batch(ping), ReceivedAt);

        if (accepted)
        {
            Assert.IsType<TelemetryBatchResult.Valid>(result);
        }
        else
        {
            Assert.Equal(["[0].ts"], Assert.IsType<TelemetryBatchResult.Invalid>(result).Errors.Keys);
        }
    }

    private static JsonObject Ping(string messageId = "TRK-101-1") => new()
    {
        ["message_id"] = messageId,
        ["vehicle_id"] = "TRK-101",
        ["ts"] = "2026-09-10T07:00:00Z",
        ["gps"] = new JsonObject { ["lat"] = 51.889, ["lon"] = 4.431, ["heading_deg"] = 87 },
        ["speed_kmh"] = 42.5,
        ["odometer_km"] = 309_199.518,
        ["fuel"] = new JsonObject { ["level_pct"] = 63.2, ["used_l"] = 75_416.832 },
        ["engine"] = new JsonObject
        {
            ["rpm"] = 1450,
            ["coolant_c"] = 88.5,
            ["dtc"] = new JsonArray(new JsonObject { ["code"] = "P0301", ["severity"] = "warning" }),
        },
    };

    private static string Batch(params JsonObject[] pings) => new JsonArray(pings).ToJsonString();
}
