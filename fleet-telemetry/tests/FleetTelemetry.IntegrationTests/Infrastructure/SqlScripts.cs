using Dapper;
using FleetTelemetry.Data;
using Microsoft.Extensions.Options;

namespace FleetTelemetry.IntegrationTests.Infrastructure;

/// <summary>The repository's <c>init/NN_name.sf.sql</c> scripts, copied next to the test assembly.</summary>
internal static class SqlScripts
{
    public static string Directory { get; } = Path.Combine(AppContext.BaseDirectory, "init");

    /// <summary>
    /// Database-scoped objects (02 onwards: landing table, registry, views, dynamic table),
    /// created in whatever database the session uses. 01 is account-level provisioning.
    /// </summary>
    public static IReadOnlyList<string> Schema { get; } = System.IO.Directory
        .GetFiles(Directory, "*.sf.sql")
        .Where(f => !Path.GetFileName(f).StartsWith("01_", StringComparison.Ordinal))
        .Order(StringComparer.Ordinal)
        .ToList();

    /// <summary>
    /// Runs each statement in order. The scripts start with USE statements that point at
    /// FLEET.TELEMETRY; skipping them runs the scripts in the session's own database
    /// instead, which is how each test class gets a private copy.
    /// </summary>
    public static async Task RunAsync(SnowflakeOptions settings, IEnumerable<string> scripts, CancellationToken cancellationToken)
    {
        await using var connection = await new FleetTelemetry.Data.FleetDatabase(
            Options.Create(settings), new FixedQueryTag("integration-tests setup")).OpenAsync(cancellationToken);
        foreach (var script in scripts)
        {
            foreach (var statement in Statements(await File.ReadAllTextAsync(script, cancellationToken)))
            {
                if (statement.StartsWith("USE ", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                await connection.ExecuteAsync(new CommandDefinition(statement, cancellationToken: cancellationToken));
            }
        }
    }

    /// <summary>Statements end with ';' at the end of a line; whole-line '--' comments are dropped.</summary>
    private static IEnumerable<string> Statements(string sql)
    {
        var lines = sql.Split('\n').Where(l => !l.TrimStart().StartsWith("--", StringComparison.Ordinal));
        var current = new List<string>();
        foreach (var line in lines)
        {
            current.Add(line);
            if (line.TrimEnd().EndsWith(';'))
            {
                yield return string.Join('\n', current).Trim().TrimEnd(';');
                current.Clear();
            }
        }
    }
}
