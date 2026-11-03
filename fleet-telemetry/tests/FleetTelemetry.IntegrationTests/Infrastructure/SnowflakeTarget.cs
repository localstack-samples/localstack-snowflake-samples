using System.Net;
using System.Security.Cryptography;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using FleetTelemetry.Data;

[assembly: AssemblyFixture(typeof(FleetTelemetry.IntegrationTests.Infrastructure.SnowflakeTarget))]

namespace FleetTelemetry.IntegrationTests.Infrastructure;

/// <summary>
/// Where the suite runs, chosen by <c>TEST_TARGET</c>:
/// <list type="bullet">
///   <item><c>EMULATOR</c> (default): one Snowflake-Next container for the whole run, on a
///   random host port, running the <c>init/*.sf.sql</c> scripts as its init hooks, just like
///   <c>docker compose</c>;</item>
///   <item><c>SNOWFLAKE_CLOUD</c>: the account in the <c>SNOWFLAKE_*</c> variables, provisioned
///   once by an administrator with the same scripts.</item>
/// </list>
/// Either way the tests connect as FLEET_SVC with role FLEET_APP and key-pair authentication,
/// starting in the FLEET database.
/// </summary>
public sealed class SnowflakeTarget : IAsyncLifetime
{
    public const string EmulatorHost = "snowflake.localhost.localstack.cloud";
    private const ushort EmulatorPort = 4566;
    private static readonly TimeSpan ProvisioningTimeout = TimeSpan.FromMinutes(2);

    private IContainer? _container;
    private string? _throwawayKey;

    public static bool IsCloud =>
        string.Equals(Environment.GetEnvironmentVariable("TEST_TARGET"), "SNOWFLAKE_CLOUD", StringComparison.OrdinalIgnoreCase);

    /// <summary>Connection settings for the FLEET database; each test class creates its own from there.</summary>
    public SnowflakeOptions Settings { get; private set; } = new();

    public async ValueTask InitializeAsync()
    {
        if (IsCloud)
        {
            var account = RequireEnv("SNOWFLAKE_ACCOUNT");
            Settings = new SnowflakeOptions
            {
                Account = account,
                Host = Environment.GetEnvironmentVariable("SNOWFLAKE_HOST") ?? $"{account}.snowflakecomputing.com",
                Port = 443,
                User = Env("SNOWFLAKE_USER", "FLEET_SVC"),
                PrivateKeyPath = RequireEnv("SNOWFLAKE_PRIVATE_KEY_PATH"),
                Role = Env("SNOWFLAKE_ROLE", "FLEET_APP"),
                Warehouse = Env("SNOWFLAKE_WAREHOUSE", "FLEET_WH"),
                Database = "FLEET",
                Schema = "TELEMETRY",
                Application = "fleet-telemetry-tests",
            };
            return;
        }

        _container = new ContainerBuilder(Env("SNOWFLAKE_IMAGE", "localstack/snowflake-next:latest"))
            .WithPortBinding(EmulatorPort, assignRandomHostPort: true)
            .WithEnvironment("LOCALSTACK_AUTH_TOKEN", RequireEnv("LOCALSTACK_AUTH_TOKEN"))
            .WithResourceMapping(new DirectoryInfo(SqlScripts.Directory), "/etc/localstack/init/ready.d/")
            .WithWaitStrategy(Wait.ForUnixContainer().AddCustomWaitStrategy(
                new AnswersSql(), o => o.WithTimeout(TimeSpan.FromMinutes(3))))
            .Build();
        await _container.StartAsync();

        _throwawayKey = Path.Combine(Path.GetTempPath(), $"fleet-it-{Guid.NewGuid():N}.p8");
        using (var rsa = RSA.Create(2048))
        {
            await File.WriteAllTextAsync(_throwawayKey, rsa.ExportPkcs8PrivateKeyPem());
        }

        Settings = new SnowflakeOptions
        {
            Account = "snowflake",
            Host = EmulatorHost,
            Port = _container.GetMappedPublicPort(EmulatorPort),
            User = "FLEET_SVC",
            PrivateKeyPath = _throwawayKey,
            Role = "FLEET_APP",
            Warehouse = "FLEET_WH",
            Database = "FLEET",
            Schema = "TELEMETRY",
            Application = "fleet-telemetry-tests",
        };

        await WaitForProvisioningAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>The init hooks run once the emulator answers SQL; the last one creates FLEET_DAILY_KPIS.</summary>
    private async Task WaitForProvisioningAsync(CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + ProvisioningTimeout;
        while (true)
        {
            try
            {
                await using var connection = await new FleetTelemetry.Data.FleetDatabase(
                    Microsoft.Extensions.Options.Options.Create(Settings), new FixedQueryTag("integration-tests setup"))
                    .OpenAsync(cancellationToken);
                await Dapper.SqlMapper.ExecuteScalarAsync<long>(connection, "SELECT COUNT(*) FROM FLEET.TELEMETRY.FLEET_DAILY_KPIS");
                return;
            }
            catch (Snowflake.Data.Client.SnowflakeDbException) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
        }

        if (_throwawayKey is not null)
        {
            File.Delete(_throwawayKey);
        }
    }

    private static string Env(string name, string fallback) =>
        Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : fallback;

    private static string RequireEnv(string name) =>
        Environment.GetEnvironmentVariable(name) is { Length: > 0 } value
            ? value
            : throw new InvalidOperationException(IsCloud
                ? $"TEST_TARGET=SNOWFLAKE_CLOUD needs {name}."
                : $"{name} is not set. Export it, or run `make test`, which reads it from ~/.localstack/default.env.");

    /// <summary>Ready once the emulator answers SELECT 1, checked from the host like scripts/wait-for-emulator.sh.</summary>
    private sealed class AnswersSql : IWaitUntil
    {
        private static readonly HttpClient Http = new(new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.All,
        })
        {
            Timeout = TimeSpan.FromSeconds(10),
        };

        public async Task<bool> UntilAsync(IContainer container)
        {
            var url = $"https://{EmulatorHost}:{container.GetMappedPublicPort(EmulatorPort)}/api/v2/statements";
            using var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = new StringContent("""{"statement":"SELECT 1"}""", Encoding.UTF8, "application/json"),
            };
            request.Headers.Add("Authorization", "Bearer local");
            request.Headers.Add("Accept", "application/json");
            try
            {
                using var response = await Http.SendAsync(request);
                return response.IsSuccessStatusCode;
            }
            catch (HttpRequestException)
            {
                return false;
            }
            catch (TaskCanceledException)
            {
                return false;
            }
        }
    }
}
