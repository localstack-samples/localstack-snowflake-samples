using FleetTelemetry.Data;
using FleetTelemetry.IntegrationTests.Infrastructure;
using FleetTelemetry.Simulation;
using Microsoft.Extensions.DependencyInjection;

namespace FleetTelemetry.IntegrationTests;

/// <summary>The backfill path: PUT NDJSON exports to the stage, COPY INTO the landing table.</summary>
public sealed class BackfillTests(BackfillTests.Fixture fixture) : IClassFixture<BackfillTests.Fixture>
{
    [Fact]
    public void EveryFileIsLoadedWithOneRowPerPing()
    {
        Assert.Equal(fixture.Files.Count, fixture.FirstRun.Uploaded.Count);
        Assert.All(fixture.FirstRun.Uploaded, u => Assert.Equal("UPLOADED", u.Status));

        var loaded = fixture.FirstRun.Loaded.ToDictionary(f => f.File);
        Assert.Equal(fixture.Files.Select(f => Path.GetFileName(f.Path) + ".gz").Order(), loaded.Keys.Order());
        foreach (var file in fixture.Files.Where(f => f.Path != fixture.CorruptFile))
        {
            var result = loaded[Path.GetFileName(file.Path) + ".gz"];
            Assert.Equal(("LOADED", file.Lines, file.Lines, 0L), (result.Status, result.RowsParsed, result.RowsLoaded, result.ErrorsSeen));
        }

        Assert.Equal(fixture.Pings, fixture.FirstRun.RowsLoaded);
    }

    [Fact]
    public void ACorruptLineIsSkippedAndReportedWithItsLineNumber()
    {
        var expectedPings = fixture.Files.Single(f => f.Path == fixture.CorruptFile).Lines;
        var result = fixture.FirstRun.Loaded.Single(f => f.File == Path.GetFileName(fixture.CorruptFile) + ".gz");

        Assert.Equal("PARTIALLY_LOADED", result.Status);
        Assert.Equal(expectedPings + 1, result.RowsParsed);
        Assert.Equal(expectedPings, result.RowsLoaded);
        Assert.Equal(1, result.ErrorsSeen);
        Assert.Equal(Fixture.CorruptLine, result.FirstErrorLine);
        Assert.False(string.IsNullOrEmpty(result.FirstError));
        Assert.Equal(1, fixture.FirstRun.RowsFailed);
    }

    [Fact]
    public async Task TheLandingTableAndTheTypedViewHoldEveryPing()
    {
        Assert.Equal(fixture.Pings, await fixture.QuerySingleAsync<long>("SELECT COUNT(*) FROM TELEMETRY_RAW"));
        Assert.Equal(fixture.Pings, await fixture.QuerySingleAsync<long>("SELECT COUNT(*) FROM TELEMETRY"));
    }

    [Fact]
    public async Task RunningTheBackfillAgainLoadsNothingTwice()
    {
        await using var scope = fixture.CreateScope();
        var again = await scope.ServiceProvider.GetRequiredService<TelemetryBackfill>()
            .RunAsync(fixture.Directory, TestContext.Current.CancellationToken);

        Assert.All(again.Uploaded, u => Assert.Equal("SKIPPED", u.Status));
        Assert.Empty(again.Loaded);
        Assert.Equal(fixture.Pings, await fixture.QuerySingleAsync<long>("SELECT COUNT(*) FROM TELEMETRY_RAW"));
    }

    public sealed class Fixture(SnowflakeTarget target) : FleetDatabaseFixture(target)
    {
        /// <summary>1-based line of the corrupt record (a truncated copy of the line before it).</summary>
        public const long CorruptLine = 42;

        public string Directory { get; } = Path.Combine(Path.GetTempPath(), $"fleet-backfill-{Guid.NewGuid():N}");

        public IReadOnlyList<ExportFile> Files { get; private set; } = [];

        public string CorruptFile { get; private set; } = "";

        public long Pings { get; private set; }

        public BackfillReport FirstRun { get; private set; } = new([], []);

        protected override async Task LoadAsync(CancellationToken cancellationToken)
        {
            var fleet = new FleetSimulator(seed: 11).Simulate(await VehiclesAsync(3, cancellationToken), new DateOnly(2026, 9, 7), days: 2);
            Files = TelemetryExport.WriteNdjson(Directory, fleet);
            Pings = Files.Sum(f => f.Lines);

            // A gateway that crashed mid-write leaves a truncated record behind.
            CorruptFile = Files[4].Path;
            var lines = (await File.ReadAllLinesAsync(CorruptFile, cancellationToken)).ToList();
            var previous = lines[(int)CorruptLine - 2];
            lines.Insert((int)CorruptLine - 1, previous[..(previous.Length / 2)]);
            await File.WriteAllTextAsync(CorruptFile, string.Join('\n', lines) + "\n", cancellationToken);

            await using var scope = CreateScope();
            FirstRun = await scope.ServiceProvider.GetRequiredService<TelemetryBackfill>().RunAsync(Directory, cancellationToken);
        }
    }
}
