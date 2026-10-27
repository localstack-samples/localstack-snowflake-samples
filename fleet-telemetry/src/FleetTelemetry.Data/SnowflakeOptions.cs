using System.Data.Common;
using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace FleetTelemetry.Data;

/// <summary>
/// Connection settings, bound from the <c>Snowflake</c> configuration section. The same
/// shape serves a local emulator and a Snowflake account; only account, host, port and
/// key differ (see appsettings.json and appsettings.Development.json).
/// </summary>
public sealed class SnowflakeOptions
{
    public const string Section = "Snowflake";

    public string Account { get; set; } = "";

    public string Host { get; set; } = "";

    public int Port { get; set; } = 443;

    public string User { get; set; } = "";

    /// <summary>Unencrypted PKCS#8 PEM file for key-pair (JWT) authentication.</summary>
    public string PrivateKeyPath { get; set; } = "";

    public string Role { get; set; } = "";

    public string Warehouse { get; set; } = "";

    public string Database { get; set; } = "";

    public string Schema { get; set; } = "";

    /// <summary>Shown in every query tag, so Snowflake's query history says which app ran a query.</summary>
    public string Application { get; set; } = "fleet-telemetry";

    public string ToConnectionString()
    {
        var builder = new DbConnectionStringBuilder
        {
            ["account"] = Account,
            ["host"] = Host,
            ["port"] = Port.ToString(CultureInfo.InvariantCulture),
            ["scheme"] = "https",
            ["user"] = User,
            ["authenticator"] = "snowflake_jwt",
            ["private_key_file"] = Path.GetFullPath(PrivateKeyPath),
            ["role"] = Role,
        };
        foreach (var (key, value) in new[] { ("warehouse", Warehouse), ("db", Database), ("schema", Schema) })
        {
            if (!string.IsNullOrEmpty(value))
            {
                builder[key] = value;
            }
        }

        return builder.ConnectionString;
    }
}

internal sealed class SnowflakeOptionsValidator : IValidateOptions<SnowflakeOptions>
{
    public ValidateOptionsResult Validate(string? name, SnowflakeOptions options)
    {
        var missing = new (string Key, string Value)[]
            {
                (nameof(options.Account), options.Account),
                (nameof(options.Host), options.Host),
                (nameof(options.User), options.User),
                (nameof(options.PrivateKeyPath), options.PrivateKeyPath),
                (nameof(options.Role), options.Role),
                (nameof(options.Warehouse), options.Warehouse),
                (nameof(options.Database), options.Database),
                (nameof(options.Schema), options.Schema),
            }
            .Where(s => string.IsNullOrWhiteSpace(s.Value))
            .Select(s => $"{SnowflakeOptions.Section}:{s.Key}")
            .ToList();
        if (missing.Count > 0)
        {
            return ValidateOptionsResult.Fail($"Missing Snowflake settings: {string.Join(", ", missing)}.");
        }

        return File.Exists(options.PrivateKeyPath)
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(
                $"Private key '{Path.GetFullPath(options.PrivateKeyPath)}' not found. Run `make up` to generate a development key.");
    }
}

public static class SnowflakeConfigurationExtensions
{
    private static readonly (string Variable, string Key)[] EnvironmentMapping =
    [
        ("SNOWFLAKE_ACCOUNT", nameof(SnowflakeOptions.Account)),
        ("SNOWFLAKE_HOST", nameof(SnowflakeOptions.Host)),
        ("SNOWFLAKE_PORT", nameof(SnowflakeOptions.Port)),
        ("SNOWFLAKE_USER", nameof(SnowflakeOptions.User)),
        ("SNOWFLAKE_PRIVATE_KEY_PATH", nameof(SnowflakeOptions.PrivateKeyPath)),
        ("SNOWFLAKE_ROLE", nameof(SnowflakeOptions.Role)),
        ("SNOWFLAKE_WAREHOUSE", nameof(SnowflakeOptions.Warehouse)),
        ("SNOWFLAKE_DATABASE", nameof(SnowflakeOptions.Database)),
        ("SNOWFLAKE_SCHEMA", nameof(SnowflakeOptions.Schema)),
    ];

    /// <summary>
    /// Lets the conventional <c>SNOWFLAKE_*</c> variables (the ones SnowSQL and the other
    /// Snowflake drivers use) override appsettings, next to the .NET form <c>Snowflake__Port</c>.
    /// </summary>
    public static IConfigurationBuilder AddSnowflakeEnvironmentVariables(this IConfigurationBuilder builder)
    {
        var values = EnvironmentMapping
            .Select(m => (m.Key, Value: Environment.GetEnvironmentVariable(m.Variable)))
            .Where(m => !string.IsNullOrEmpty(m.Value))
            .ToDictionary(m => $"{SnowflakeOptions.Section}:{m.Key}", m => m.Value);
        return builder.AddInMemoryCollection(values);
    }
}
