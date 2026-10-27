using System.Globalization;
using Dapper;
using FleetTelemetry.Data;
using FleetTelemetry.Simulation;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FleetTelemetry.IntegrationTests.Infrastructure;

/// <summary>
/// A private database per test class: created with the init scripts, loaded by the
/// subclass, dropped afterwards. Exposes the real API (WebApplicationFactory) and the
/// data-access services (for the backfill) wired to that database.
/// </summary>
public abstract class FleetDatabaseFixture(SnowflakeTarget target) : IAsyncLifetime
{
    private WebApplicationFactory<Program>? _api;
    private ServiceProvider? _services;

    public SnowflakeOptions Settings { get; private set; } = new();

    public HttpClient Api { get; private set; } = new();

    public async ValueTask InitializeAsync()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var database = string.Create(
            CultureInfo.InvariantCulture,
            $"FLEET_IT_{GetType().Name.Replace("Fixture", "", StringComparison.Ordinal).ToUpperInvariant()}_{Random.Shared.Next(0x100000, 0xFFFFFF):X6}");

        await using (var connection = await OpenAsync(target.Settings, cancellationToken))
        {
            await connection.ExecuteAsync(new CommandDefinition($"CREATE DATABASE {database}", cancellationToken: cancellationToken));
            await connection.ExecuteAsync(new CommandDefinition($"CREATE SCHEMA {database}.TELEMETRY", cancellationToken: cancellationToken));
        }

        Settings = target.Settings.With(database: database, schema: "TELEMETRY");
        await SqlScripts.RunAsync(Settings, SqlScripts.Schema, cancellationToken);

        _services = new ServiceCollection()
            .AddSingleton<IConfiguration>(new ConfigurationBuilder().Build())
            .AddLogging(l => l.AddSimpleConsole().SetMinimumLevel(LogLevel.Warning))
            .AddFleetSnowflake()
            .AddSingleton<IQueryTagSource>(new FixedQueryTag($"integration-tests {GetType().Name}"))
            .PostConfigure<SnowflakeOptions>(o => Settings.CopyTo(o))
            .BuildServiceProvider();

        _api = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Development");
            builder.UseSetting("Logging:LogLevel:Default", "Warning");
            builder.ConfigureServices(s => s.PostConfigure<SnowflakeOptions>(o => Settings.CopyTo(o)));
        });
        Api = _api.CreateClient();

        await LoadAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        Api.Dispose();
        if (_api is not null)
        {
            await _api.DisposeAsync();
        }

        if (_services is not null)
        {
            await _services.DisposeAsync();
        }

        if (Settings.Database.Length > 0)
        {
            await using var connection = await OpenAsync(target.Settings, CancellationToken.None);
            await connection.ExecuteAsync($"DROP DATABASE IF EXISTS {Settings.Database}");
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>Loads the class's test data; the database already has the schema and the registry.</summary>
    protected virtual Task LoadAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>A scope with the data-access services, as the ingest tool uses them.</summary>
    public AsyncServiceScope CreateScope() => _services!.CreateAsyncScope();

    public async Task<T> QuerySingleAsync<T>(string sql, object? parameters = null) =>
        (await QueryAsync<T>(sql, parameters)).Single();

    public async Task<IReadOnlyList<T>> QueryAsync<T>(string sql, object? parameters = null)
    {
        await using var connection = await OpenAsync(Settings, TestContext.Current.CancellationToken);
        var rows = await connection.QueryAsync<T>(new CommandDefinition(
            sql, parameters, cancellationToken: TestContext.Current.CancellationToken));
        return rows.ToList();
    }

    /// <summary>The first <paramref name="count"/> registry vehicles, ready for the simulator.</summary>
    protected async Task<IReadOnlyList<SimulatedVehicle>> VehiclesAsync(int count, CancellationToken cancellationToken)
    {
        await using var scope = CreateScope();
        var all = await scope.ServiceProvider.GetRequiredService<FleetRegistry>().ListVehiclesAsync(cancellationToken);
        return all.Take(count).ToList();
    }

    private static Task<System.Data.Common.DbConnection> OpenAsync(SnowflakeOptions settings, CancellationToken cancellationToken) =>
        new FleetTelemetry.Data.FleetDatabase(Options.Create(settings), new FixedQueryTag("integration-tests setup"))
            .OpenAsync(cancellationToken);
}
